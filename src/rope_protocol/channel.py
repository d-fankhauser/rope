"""Frames over an asyncio stream connection, with timeouts on every network call (spec section 10)."""

import asyncio
import time
from collections import deque

from . import frames as F

READ_SIZE = 64 * 1024


def now_ms():
    return int(time.time() * 1000)


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
