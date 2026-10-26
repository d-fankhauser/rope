"""Hub: accepts participants from the participant list and stores their streams durably (spec 7, 8).

This first version receives streams from sources. Relaying to consumers and messages are later steps (TASKS.md).
"""

import asyncio
import logging

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from .channel import Channel, now_ms, reap
from .store import GapError, StreamStore

log = logging.getLogger("rope.hub")


class Hub:
    """``participants``: {identifier: role}. ``on_stream_checked(participant, stream, gen, result, path)`` is called
    after a final check (spec 8.4)."""

    def __init__(self, root, participants, operation, hub_id="hub", host="127.0.0.1", port=DEFAULT_PORT,
                 heartbeat_s=2.0, timeout_s=10.0, on_stream_checked=None):
        self.store = StreamStore(root)
        self.participants = dict(participants)
        self.operation = operation
        self.hub_id = hub_id
        self.host = host
        self.port = port
        self.heartbeat_s = heartbeat_s
        self.timeout_s = timeout_s
        self.on_stream_checked = on_stream_checked
        self._server = None
        self._sessions = {}           # participant -> task of its current session
        self._connections = set()     # every connection task, also those still greeting
        self.sessions_started = 0

    async def start(self):
        try:
            self._server = await asyncio.start_server(self._accept, self.host, self.port)
        except OSError as e:
            raise OSError(e.errno, "ROPE hub cannot listen on %s port %d: %s" % (self.host, self.port, e.strerror)) \
                from None
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self._server:
            self._server.close()
        tasks = list(self._connections)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        if self._server:
            try:
                await asyncio.wait_for(self._server.wait_closed(), 5)
            except asyncio.TimeoutError:
                log.warning("hub stop: connections did not close within 5 s")

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    async def _accept(self, reader, writer):
        ch = Channel(reader, writer, send_timeout_s=self.timeout_s)
        participant = None
        me = asyncio.current_task()
        self._connections.add(me)
        try:
            hello = await ch.recv(self.timeout_s)
            if hello.type != F.FrameType.HELLO:
                await ch.error_and_close("bad_frame", "the first frame must be HELLO")
                return
            m = hello.meta
            reason = self._check(m)
            if reason:
                code, text = reason
                await ch.send(F.Frame(F.FrameType.REJECT, {"code": code, "text": text}))
                ch.abort()
                return
            participant = m["participant"]
            # a participant has one session: a new greeting replaces a stale one (e.g. after a radio dropout)
            old = self._sessions.get(participant)
            if old is not None and old is not asyncio.current_task():
                old.cancel()
                try:
                    await old
                except BaseException:
                    pass
            self._sessions[participant] = asyncio.current_task()
            self.sessions_started += 1
            ch.version = max(set(m["versions"]) & {PROTOCOL_VERSION})
            have = await asyncio.to_thread(self.store.have, participant)
            await ch.send(F.Frame(F.FrameType.WELCOME, dict(
                version=ch.version, hub=self.hub_id, t=now_ms(), t_hello=m["t"], heartbeat_s=self.heartbeat_s,
                timeout_s=self.timeout_s, have=have)))
            await self._serve(ch, participant)
        except asyncio.CancelledError:
            ch.abort()
        except F.FrameError as e:
            await ch.error_and_close(e.code, e.text)
        except (ConnectionError, OSError) as e:
            log.info("connection of %s ended: %s", participant or "unknown participant", e)
            ch.abort()
        finally:
            ch.abort()                                  # every way out closes the connection
            self._connections.discard(me)
            if participant and self._sessions.get(participant) is me:
                del self._sessions[participant]

    def _check(self, m):
        p = m["participant"]
        if p not in self.participants:
            return "unknown_participant", "%s is not on the participant list of this hub" % p
        if m["operation"] != self.operation:
            return "wrong_operation", "%s belongs to operation %s, this hub serves %s" % (p, m["operation"],
                                                                                          self.operation)
        if PROTOCOL_VERSION not in m["versions"]:
            return "version", "%s speaks protocol versions %s, this hub speaks %d" % (p, m["versions"],
                                                                                     PROTOCOL_VERSION)
        if m["role"] != self.participants[p]:
            return "role", "%s greets as %s but is listed as %s" % (p, m["role"], self.participants[p])
        return None

    async def _serve(self, ch, participant):
        beat = asyncio.ensure_future(ch.heartbeat(self.heartbeat_s))
        try:
            while True:
                if beat.done():
                    beat.result()                       # re-raise a send failure
                f = await ch.recv(self.timeout_s)
                t = f.type
                if t == F.FrameType.SEGMENT:
                    await self._segment(ch, participant, f)
                elif t == F.FrameType.STREAM_END:
                    await self._stream_end(ch, participant, f.meta)
                elif t == F.FrameType.BYE:
                    return
                elif t in (F.FrameType.HEARTBEAT,) or f.is_extension:
                    pass
                elif t == F.FrameType.ERROR:
                    raise ConnectionError("peer reported %s: %s" % (f.meta["code"], f.meta["text"]))
                else:
                    raise F.FrameError("bad_frame", "unexpected %s from a source" % F.FrameType(t).name)
        finally:
            await reap([beat])

    async def _segment(self, ch, participant, f):
        m = f.meta
        data = F.segment_data(f)                        # FrameError on damage -> ERROR, connection closed
        rs = await asyncio.to_thread(self.store.get, participant, m["stream"], m["gen"])
        try:
            committed = await asyncio.to_thread(rs.write, m["off"], data)
        except GapError as e:
            await ch.send(F.Frame(F.FrameType.RESEND, dict(stream=m["stream"], gen=m["gen"], off=e.committed,
                                                           len=m["off"] + m["len"] - e.committed)))
            return
        except OSError as e:
            await ch.error_and_close("storage", "the hub cannot store data: %s" % e)
            raise ConnectionError("storage failed") from None
        await ch.send(F.Frame(F.FrameType.ACK, dict(stream=m["stream"], gen=m["gen"], committed=committed)))

    async def _stream_end(self, ch, participant, m):
        rs = await asyncio.to_thread(self.store.get, participant, m["stream"], m["gen"])
        result = await asyncio.to_thread(rs.finish, m["size"], m["sha256"])
        await ch.send(F.Frame(F.FrameType.STREAM_CHECK, dict(stream=m["stream"], gen=m["gen"], result=result,
                                                             size=rs.committed)))
        if self.on_stream_checked:
            self.on_stream_checked(participant, m["stream"], m["gen"], result, rs.path)
