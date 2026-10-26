"""Receiving side of streams, shared by the hub (from sources) and consumers (from the hub): store durably, then
acknowledge; final check; block sums for the repair (spec 8.3, 8.4, 14.7)."""

import asyncio

from . import frames as F
from .store import GapError

#: Block size and sums per frame for the repair after a mismatch (spec 14.7): 512 sums need about 34 KiB of meta.
BLOCK_SIZE = 1024 * 1024
SUMS_PER_FRAME = 512


class StreamReceiver:
    """Stores the streams one sender sends over a connection. ``owner`` is the key under which the store keeps them
    (the source's identifier at the hub, the hub at a consumer).

    ``on_written(owner, stream, gen, committed)`` after every durable write, ``on_checked(owner, stream, gen, result,
    path)`` after a final check - both called in the event loop.
    """

    def __init__(self, store, owner, block_size=BLOCK_SIZE, on_written=None, on_checked=None, me="receiver"):
        self.store = store
        self.owner = owner
        self.block_size = block_size
        self.on_written = on_written
        self.on_checked = on_checked
        self.me = me

    async def handle(self, ch, f):
        """Process a frame from the sender. Returns False if it is not a receiver frame."""
        if f.type == F.FrameType.SEGMENT:
            await self._segment(ch, f)
        elif f.type == F.FrameType.STREAM_END:
            await self._stream_end(ch, f.meta)
        else:
            return False
        return True

    async def _segment(self, ch, f):
        m = f.meta
        data = F.segment_data(f)                        # FrameError on damage -> ERROR, connection closed
        rs = await asyncio.to_thread(self.store.get, self.owner, m["stream"], m["gen"])
        try:
            committed = await self.store.activity.run(rs.write, m["off"], data, m.get("repair") is True)
        except GapError as e:
            await ch.send(F.Frame(F.FrameType.RESEND, dict(stream=m["stream"], gen=m["gen"], off=e.committed,
                                                           len=m["off"] + m["len"] - e.committed)))
            return
        except OSError as e:
            await ch.error_and_close("storage", "the %s cannot store data: %s" % (self.me, e))
            raise ConnectionError("storage failed") from None
        await ch.send(F.Frame(F.FrameType.ACK, dict(stream=m["stream"], gen=m["gen"], committed=committed)))
        if self.on_written:
            self.on_written(self.owner, m["stream"], m["gen"], committed)

    async def _stream_end(self, ch, m):
        rs = await asyncio.to_thread(self.store.get, self.owner, m["stream"], m["gen"])
        result = await self.store.activity.run(rs.finish, m["size"], m["sha256"])
        if self.on_checked:                 # tell the application as soon as the store says so, not after sending
            self.on_checked(self.owner, m["stream"], m["gen"], result, rs.path)
        await ch.send(F.Frame(F.FrameType.STREAM_CHECK, dict(stream=m["stream"], gen=m["gen"], result=result,
                                                             size=rs.committed)))
        if result != "ok":
            # per-block checksums of what is stored, so that the sender resends only differing blocks
            sums = await asyncio.to_thread(rs.block_sums, self.block_size)
            for first in range(0, max(len(sums), 1), SUMS_PER_FRAME):
                await ch.send(F.Frame(F.FrameType.BLOCK_SUMS, dict(stream=m["stream"], gen=m["gen"],
                                                                   block_size=self.block_size, first=first,
                                                                   sums=sums[first:first + SUMS_PER_FRAME])))
