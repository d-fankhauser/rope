"""Hub: accepts participants from the participant list, stores the sources' streams durably and relays them to the
consumers (spec 7, 8).

To a consumer, the hub is a sender like a source is to the hub: same window, resume, final check and block repair
(``rope_protocol.sender``). A relayed stream is named ``<source>/<stream>`` so that streams of different sources never
mix. The hub keeps no state per consumer: the consumer reports what it holds when it greets (spec 14.5).
"""

import asyncio
import logging

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from .channel import Channel, now_ms, reap, serve_frames
from .receiver import BLOCK_SIZE, StreamReceiver
from .sender import OutStream, StreamSender
from .store import StreamStore

log = logging.getLogger("rope.hub")

#: Longest stream name in bytes (spec 14.4); a relayed name adds the source identifier and "/".
MAX_STREAM_BYTES = 255


def relayed_name(source, stream):
    return "%s/%s" % (source, stream)


class Hub:
    """``participants``: {identifier: role}. ``on_stream_checked(participant, stream, gen, result, path)`` is called
    after the final check of a source's stream (spec 8.4)."""

    def __init__(self, root, participants, operation, hub_id="hub", host="127.0.0.1", port=DEFAULT_PORT,
                 heartbeat_s=2.0, timeout_s=10.0, on_stream_checked=None, block_size=BLOCK_SIZE,
                 segment_bytes=256 * 1024, window=8, poll_s=0.5, zlib_level=1):
        self.store = StreamStore(root)
        self.participants = dict(participants)
        self.operation = operation
        self.hub_id = hub_id
        self.host = host
        self.port = port
        self.heartbeat_s = heartbeat_s
        self.timeout_s = timeout_s
        self.on_stream_checked = on_stream_checked
        self.block_size = block_size
        self.segment_bytes = segment_bytes
        self.window = window
        self.poll_s = poll_s
        self.zlib_level = zlib_level
        self._server = None
        self._sessions = {}           # participant -> task of its current session
        self._connections = set()     # every connection task, also those still greeting
        self._relays = set()          # StreamSender of every connected consumer
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
            if m["role"] == "consumer":
                await self._serve_consumer(ch, participant, m["have"])
            else:
                await self._serve_source(ch, participant)
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
        if m["role"] not in ("source", "consumer"):
            return "role", "this hub does not serve the role %s yet" % m["role"]
        return None

    async def _run_session(self, ch, tasks):
        """Run the session's tasks (plus heartbeat) until one ends; re-raise why it ended."""
        tasks = [asyncio.ensure_future(ch.heartbeat(self.heartbeat_s))] + tasks
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                t.result()
        finally:
            await reap(tasks)

    # --- from a source -------------------------------------------------------------------------------------------
    async def _serve_source(self, ch, participant):
        receiver = StreamReceiver(self.store, participant, self.block_size, on_written=self._written,
                                  on_checked=self.on_stream_checked, me="hub")

        async def handle(f):
            if f.type in (F.FrameType.SEGMENT, F.FrameType.STREAM_END):
                name = relayed_name(participant, f.meta["stream"])
                if len(name.encode("utf-8")) > MAX_STREAM_BYTES:
                    raise F.FrameError("bad_frame", "stream name %r is too long to relay: at most %d bytes "
                                       "including the source identifier and '/'" % (f.meta["stream"],
                                                                                   MAX_STREAM_BYTES))
            return await receiver.handle(ch, f)
        await self._run_session(ch, [asyncio.ensure_future(serve_frames(ch, self.timeout_s, handle, "source"))])

    def _written(self, *_):
        for relay in self._relays:
            relay.wake.set()

    # --- to a consumer -------------------------------------------------------------------------------------------
    async def _serve_consumer(self, ch, participant, consumer_have):
        outs = {}                                       # (relayed name, gen) -> OutStream, this session only

        async def refresh():
            entries = await asyncio.to_thread(self.store.entries)
            for e in entries:
                if self.participants.get(e["participant"]) != "source":
                    continue
                key = (relayed_name(e["participant"], e["stream"]), e["gen"])
                out = outs.get(key)
                if out is None:
                    out = outs[key] = OutStream(key[0], key[1], e["path"])
                out.size = e["committed"]
                if e["complete"] and e["verified"] == "ok":
                    out.final_size = e["committed"]     # verified at the hub: the consumer may check it, too
            return [outs[k] for k in sorted(outs)]

        sender = StreamSender(refresh, self.segment_bytes, self.window, self.poll_s, self.zlib_level,
                              peer=participant)
        have = {(h["stream"], h["gen"]): h["committed"] for h in consumer_have}
        sender.start_session(await refresh(), have)

        async def handle(f):
            return sender.handle(f)
        self._relays.add(sender)
        try:
            await self._run_session(ch, [asyncio.ensure_future(serve_frames(ch, self.timeout_s, handle, "consumer")),
                                         asyncio.ensure_future(sender.run(ch))])
        finally:
            self._relays.discard(sender)
