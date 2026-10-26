"""Frames over an asyncio stream connection, with timeouts on every network call (spec section 10)."""

import asyncio
import time
from collections import deque

from . import frames as F

READ_SIZE = 64 * 1024


def now_ms():
    return int(time.time() * 1000)


async def reap(tasks, timeout_s=2.0):
    """Cancel helper tasks and collect their results, waiting at most ``timeout_s``.

    Bounded on purpose: on Python 3.11 ``asyncio.wait_for`` can lose a cancellation, so a task might not end at once;
    the caller must never hang on it. Collecting the results keeps late failures out of the "never retrieved" log.
    """
    tasks = [t for t in tasks if t is not None]
    for t in tasks:
        t.add_done_callback(_retrieve)              # also if this cleanup itself is cancelled while waiting
        t.cancel()
    if tasks:
        await asyncio.wait(tasks, timeout=timeout_s)


def _retrieve(task):
    if not task.cancelled():
        task.exception()


async def serve_frames(ch, timeout_s, handle, peer):
    """Receive frames until the connection ends. ``handle(frame)`` (async) returns False for frames it does not
    take; heartbeats and unknown extensions are skipped, anything else ends the session."""
    while True:
        f = await ch.recv(timeout_s)
        if await handle(f):
            continue
        if f.type == F.FrameType.HEARTBEAT or f.is_extension:
            continue
        if f.type == F.FrameType.BYE:
            raise ConnectionError("the %s closed the session: %s" % (peer, f.meta["reason"]))
        if f.type == F.FrameType.ERROR:
            raise ConnectionError("the %s reported %s: %s" % (peer, f.meta["code"], f.meta["text"]))
        raise F.FrameError("bad_frame", "unexpected %s from the %s" % (F.FrameType(f.type).name, peer))


class Wakeup:
    """Like ``asyncio.Event`` with a timeout on ``wait``, for a single waiter, built without ``asyncio.wait_for``
    (see ``reap``): a cancellation always ends the wait."""

    def __init__(self):
        self._flag = False
        self._fut = None

    def set(self):
        self._flag = True
        if self._fut is not None and not self._fut.done():
            self._fut.set_result(None)

    def clear(self):
        self._flag = False

    async def wait(self, timeout_s):
        if self._flag:
            return
        loop = asyncio.get_running_loop()
        fut = self._fut = loop.create_future()
        timer = loop.call_later(timeout_s, lambda: fut.done() or fut.set_result(None))
        try:
            await fut
        finally:
            timer.cancel()
            self._fut = None


class Channel:
    """Send and receive validated frames. Any failure raises ConnectionError (or FrameError for bad frames)."""

    def __init__(self, reader, writer, send_timeout_s=10.0):
        self.reader = reader
        self.writer = writer
        self.send_timeout_s = send_timeout_s
        self.version = F.VERSION
        self._decoder = F.FrameDecoder()
        self._pending = deque()
        self._send_lock = asyncio.Lock()
        self.last_sent = time.monotonic()
        self.last_received = time.monotonic()

    async def send(self, frame):
        frame.version = self.version
        raw = F.encode(frame)
        async with self._send_lock:
            if self.writer.is_closing():
                raise ConnectionError("connection closed")
            self.writer.write(raw)
            try:
                await asyncio.wait_for(self.writer.drain(), self.send_timeout_s)
            except asyncio.TimeoutError:
                raise ConnectionError("send timed out after %.1f s" % self.send_timeout_s) from None
            self.last_sent = time.monotonic()

    @property
    def closed(self):
        return self.writer.is_closing()

    async def recv(self, timeout_s):
        """Next frame; ConnectionError if nothing at all arrives within ``timeout_s``."""
        while not self._pending:
            try:
                data = await asyncio.wait_for(self.reader.read(READ_SIZE), timeout_s)
            except asyncio.TimeoutError:
                raise ConnectionError("no sign of life for %.1f s" % timeout_s) from None
            if not data:
                raise ConnectionError("connection closed by peer")
            self.last_received = time.monotonic()
            self._pending.extend(self._decoder.feed(data))
        frame = self._pending.popleft()
        F.validate(frame)
        return frame

    async def heartbeat(self, interval_s):
        """Send HEARTBEAT whenever nothing else was sent for ``interval_s`` (runs until cancelled or closed)."""
        while True:
            wait = self.last_sent + interval_s - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            await self.send(F.Frame(F.FrameType.HEARTBEAT, {"t": now_ms()}))

    async def error_and_close(self, code, text):
        try:
            await asyncio.wait_for(self.send(F.Frame(F.FrameType.ERROR, {"code": code, "text": text})), 1.0)
        except Exception:
            pass
        self.abort()

    def abort(self):
        try:
            self.writer.transport.abort()
        except Exception:
            pass
