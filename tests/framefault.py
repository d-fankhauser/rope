"""Frame-aware fault proxy for the test bench (spec 13): a TCP proxy that knows where ROPE frames begin and end and
injects exactly one fault at a chosen frame.

``rope_protocol.linksim`` deliberately knows nothing about frames; this helper does, so that a test can cut the
connection before or in the middle of the k-th frame, send a frame twice or damage it - at every step of the
procedure in turn.

Faults (one per proxy, at ``at`` = (frame type, n): the n-th frame of that type in direction ``direction``,
counted over all connections - by type rather than by position, because heartbeats shift positions):

- ``cut``: abort both connections before the frame,
- ``cut_mid``: send the first half of the frame, then abort,
- ``dup``: send the frame twice,
- ``damage``: flip one byte inside the frame (the receiver sees a CRC error),
- ``restart_peer`` / ``restart_hub``: hold the frame back, abort the connections and call ``on_restart(fault)``
  (async), which restarts the participant on this link or the hub - as if it stopped just before the frame.
"""

import asyncio

from rope_protocol import frames as F

UP, DOWN = "up", "down"         # up: client (participant) -> target (hub)
FAULTS = ("cut", "cut_mid", "dup", "damage")
RESTARTS = ("restart_peer", "restart_hub")


class FrameFaultProxy:
    def __init__(self, target, fault=None, direction=UP, at=None):
        assert fault is None or fault in FAULTS + RESTARTS
        self.target = target
        self.fault = fault
        self.direction = direction
        self.at = at
        self.seen = {UP: {}, DOWN: {}}      # frames seen per direction and type, over all connections
        self.fired = None                   # frame type the fault hit, once it fired
        self.on_restart = None
        self.restart_task = None
        self._server = None
        self._transports = set()
        self._tasks = set()

    @property
    def port(self):
        return self._server.sockets[0].getsockname()[1]

    async def start(self):
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def stop(self):
        self._server.close()
        for tr in list(self._transports):
            tr.abort()
        for t in list(self._tasks):
            t.cancel()
        await self._server.wait_closed()

    def _abort_all(self):
        for tr in list(self._transports):
            tr.abort()
        self._transports.clear()

    async def _handle(self, c_reader, c_writer):
        task = asyncio.current_task()
        self._tasks.add(task)
        t_writer = None
        try:
            try:
                t_reader, t_writer = await asyncio.open_connection(*self.target)
            except OSError:
                c_writer.transport.abort()
                return
            self._transports.update((c_writer.transport, t_writer.transport))
            await asyncio.gather(self._pump(c_reader, t_writer, UP), self._pump(t_reader, c_writer, DOWN))
        except asyncio.CancelledError:
            pass
        finally:
            for w in (c_writer, t_writer):
                if w is not None:
                    self._transports.discard(w.transport)
                    w.transport.abort()
            self._tasks.discard(task)

    async def _pump(self, reader, writer, direction):
        buf = bytearray()
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                buf += data
                while len(buf) >= F.HEADER.size:
                    _, _, ftype, m, b = F.HEADER.unpack_from(buf)
                    size = F.HEADER.size + m + b + F.TRAILER.size
                    if len(buf) < size:
                        break
                    frame = bytes(buf[:size])
                    del buf[:size]
                    if not await self._forward(writer, direction, ftype, frame):
                        return
                await writer.drain()
            if writer.can_write_eof():
                writer.write_eof()
        except (ConnectionError, OSError):
            pass

    async def _forward(self, writer, direction, ftype, frame):
        """Send one frame, applying the fault if it is due. False once the connection was cut."""
        n = self.seen[direction].get(ftype, 0)
        self.seen[direction][ftype] = n + 1
        if self.fired is not None or self.fault is None or direction != self.direction or (ftype, n) != self.at:
            writer.write(frame)
            return True
        self.fired = ftype
        if self.fault in RESTARTS:
            self._abort_all()
            self.restart_task = asyncio.ensure_future(self.on_restart(self.fault))
            return False
        if self.fault == "cut":
            self._abort_all()
            return False
        if self.fault == "cut_mid":
            writer.write(frame[:len(frame) // 2])
            await writer.drain()
            self._abort_all()
            return False
        if self.fault == "dup":
            writer.write(frame + frame)
            return True
        damaged = bytearray(frame)
        damaged[len(frame) // 2] ^= 0xFF                # inside meta or blob: the frame CRC no longer matches
        writer.write(bytes(damaged))
        return True
