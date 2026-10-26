"""Connection loop shared by the participants that connect to the hub (source, consumer): connect, greet, run the
session, reconnect with back-off (spec 8.1, 9)."""

import asyncio
import logging
import random

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from .channel import Channel, now_ms, reap

log = logging.getLogger("rope.participant")


class Rejected(Exception):
    """The hub refused this participant (REJECT); ``code`` and ``text`` say why, in plain language."""

    def __init__(self, code, text):
        super().__init__(text)
        self.code = code
        self.text = text


class Participant:
    """Base class. Subclasses set ``role`` and implement ``_have()`` and ``_active(ch, welcome)``, which returns the
    session's tasks. Runs until ``stop()``; reconnects forever with back-off."""

    role = None

    def __init__(self, participant, operation, hub_host, hub_port=DEFAULT_PORT, timeout_s=10.0, backoff=(1.0, 10.0)):
        self.participant = participant
        self.operation = operation
        self.hub = (hub_host, hub_port)
        self.timeout_s = timeout_s
        self.backoff = backoff
        self.connected = asyncio.Event()
        self.clock_offset_ms = None
        self.sessions = 0
        self.rejected = None
        self._stop = asyncio.Event()
        self._task = None

    def start(self):
        self._task = asyncio.ensure_future(self.run())
        return self

    async def stop(self):
        self._stop.set()
        self._stopping()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    def _stopping(self):
        """Hook: wake up loops when stopping."""

    async def run(self):
        delay = self.backoff[0]
        while not self._stop.is_set():
            try:
                await self._session()
                delay = self.backoff[0]
            except Rejected as e:
                self.rejected = e
                log.error("hub rejected %s: %s", self.participant, e.text)
                delay = self.backoff[1]
            except (ConnectionError, OSError, asyncio.TimeoutError, F.FrameError) as e:
                log.info("%s: connection lost (%s), retrying in %.1f s", self.participant, e, delay)
            finally:
                self.connected.clear()
            if self._stop.is_set():
                return
            await asyncio.sleep(delay * random.uniform(0.8, 1.0))
            delay = min(delay * 2, self.backoff[1])

    async def _session(self):
        reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.hub), self.timeout_s)
        ch = Channel(reader, writer, send_timeout_s=self.timeout_s)
        tasks = []
        try:
            t_hello = now_ms()
            have = await self._have()
            await ch.send(F.Frame(F.FrameType.HELLO, dict(participant=self.participant, operation=self.operation,
                                                          role=self.role, versions=[PROTOCOL_VERSION], t=t_hello,
                                                          have=have)))
            first = await ch.recv(self.timeout_s)
            if first.type == F.FrameType.REJECT:
                raise Rejected(first.meta["code"], first.meta["text"])
            if first.type != F.FrameType.WELCOME:
                raise F.FrameError("bad_frame", "expected WELCOME, got 0x%02x" % first.type)
            w = first.meta
            ch.version = w["version"]
            self.clock_offset_ms = w["t"] - (t_hello + now_ms()) / 2
            self.timeout_s = float(w["timeout_s"])
            tasks = [asyncio.ensure_future(ch.heartbeat(float(w["heartbeat_s"])))]
            tasks += await self._active(ch, w)
            self.sessions += 1
            self.connected.set()
            self.rejected = None
            stopper = asyncio.ensure_future(self._stop.wait())
            done, _ = await asyncio.wait(tasks + [stopper], return_when=asyncio.FIRST_COMPLETED)
            stopper.cancel()
            for t in done:
                if t is not stopper:
                    t.result()                      # re-raise the reason the session ended
            if self._stop.is_set():
                try:
                    await asyncio.wait_for(ch.send(F.Frame(F.FrameType.BYE, {"reason": "stop"})), 1.0)
                except Exception:
                    pass
        finally:
            ch.abort()
            await reap(tasks)

    async def _have(self):
        return []

    async def _active(self, ch, welcome):
        raise NotImplementedError
