"""Sending side of streams, shared by the source (to the hub) and the hub (to consumers): window of unacknowledged
segments, resume from the receiver's committed offset, final check and block repair (spec 8.2, 8.4, 14.7).

The sender only reads files: opened briefly, read, closed - no locks. The file on disk is the buffer during an outage;
in memory there are at most ``window`` unacknowledged segments.
"""

import asyncio
import hashlib
import logging

from . import frames as F
from .channel import Wakeup, now_ms

log = logging.getLogger("rope.sender")

#: Rounds of block repair per generation before the mismatch is reported to the application (spec 14.7).
MAX_REPAIR_ROUNDS = 3


def read_range(path, off, n):
    with open(path, "rb") as f:
        f.seek(off)
        return f.read(n)


def sha256_prefix(path, size):
    """SHA-256 of the first ``size`` bytes and how many bytes were actually there."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        left = size
        while left > 0:
            chunk = f.read(min(left, 1024 * 1024))
            if not chunk:
                break
            h.update(chunk)
            left -= len(chunk)
    return h.hexdigest(), size - left


def differing_blocks(path, block_size, sums, receiver_size):
    """Ranges (off, end) of the blocks whose SHA-256 in the file differs from the receiver's ``sums`` (spec 14.7)."""
    out = []
    with open(path, "rb") as f:
        for i, digest in enumerate(sums):
            off = i * block_size
            end = min(off + block_size, receiver_size)
            f.seek(off)
            if hashlib.sha256(f.read(end - off)).hexdigest() != digest:
                out.append((off, end))
    return out


class OutStream:
    """Sending state of one generation of one stream. The owner keeps ``size`` (bytes available to send) and
    ``final_size`` (set once the stream is complete) up to date."""

    def __init__(self, name, gen, path):
        self.name = name
        self.gen = gen
        self.path = path
        self.size = 0
        self.final_size = None
        self.acked = 0
        self.sent = 0
        self.end_sent = False
        self.awaiting_check = False     # STREAM_END sent in this session, its STREAM_CHECK not yet received
        self.result = None              # "ok" / "mismatch" from STREAM_CHECK
        self.checked = asyncio.Event()
        self.repair_rounds = 0          # repairs of this generation that reached the receiver and still mismatched
        self.reset_repair()

    @property
    def key(self):
        return self.name, self.gen

    def reset_repair(self):
        self.receiver_size = 0
        self.block_size = None
        self.sums = None                # {block index: sha256} while BLOCK_SUMS arrive
        self.repairs = []               # [(off, end)] still to resend
        self.repair_sent = False        # repair done, its STREAM_END not yet answered in this session

    def start_session(self, committed):
        self.acked = self.sent = committed
        self.end_sent = False
        self.awaiting_check = False
        self.reset_repair()             # a new final check starts the repair over

    def sums_complete(self):
        if self.sums is None or self.block_size is None:
            return False
        return len(self.sums) >= -(-self.receiver_size // self.block_size)


class StreamSender:
    """Sends the streams returned by ``refresh()`` over one connection at a time.

    ``refresh``: async callable returning the current list of OutStream (sizes updated).
    ``on_replaced``: async callable(stream) for a stream whose blocks mostly differ at the receiver; returns True if
    it handled it (e.g. started a new generation). Without it, the differing blocks are repaired.
    """

    def __init__(self, refresh, segment_bytes=256 * 1024, window=8, poll_s=0.5, zlib_level=1, on_replaced=None,
                 peer="receiver"):
        self.refresh = refresh
        self.segment_bytes = segment_bytes
        self.window = window
        self.poll_s = poll_s
        self.zlib_level = zlib_level
        self.on_replaced = on_replaced
        self.peer = peer
        self.wake = Wakeup()
        self._streams = {}
        self._inflight = []             # [(key, end offset)]

    def start_session(self, streams, have):
        """``have``: {(name, gen): committed} as reported by the receiver in the greeting."""
        self._streams = {s.key: s for s in streams}
        for s in streams:
            s.start_session(have.get(s.key, 0))
        self._inflight = []

    def handle(self, f):
        """Process a frame from the receiver. Returns False if it is not a sender frame."""
        m = f.meta
        t = f.type
        if t not in (F.FrameType.ACK, F.FrameType.RESEND, F.FrameType.STREAM_CHECK, F.FrameType.BLOCK_SUMS):
            return False
        s = self._streams.get((m["stream"], m["gen"]))
        if s is None:
            return True
        if t == F.FrameType.ACK:
            s.acked = max(s.acked, m["committed"])
            self._inflight = [x for x in self._inflight if not (x[0] == s.key and x[1] <= s.acked)]
        elif t == F.FrameType.RESEND:
            s.sent = min(s.sent, m["off"])
            self._inflight = [x for x in self._inflight if x[0] != s.key]
        elif t == F.FrameType.STREAM_CHECK:
            if not s.awaiting_check:            # no STREAM_END of ours is open: a stray answer, never act on it
                log.warning("%s generation %d: unexpected STREAM_CHECK from the %s ignored", s.name, s.gen, self.peer)
                return True
            s.awaiting_check = False
            if s.repair_sent:                   # same session: the repair reached the receiver (TCP keeps order)
                s.repair_rounds += 1
            if m["result"] == "ok" or s.repair_rounds >= MAX_REPAIR_ROUNDS:
                s.result = m["result"]
                s.checked.set()
                if s.result != "ok":
                    log.error("%s generation %d: final check at the %s still fails after %d repairs",
                              s.name, s.gen, self.peer, s.repair_rounds)
            else:
                log.info("%s generation %d: final check differs, comparing blocks", s.name, s.gen)
                s.reset_repair()
                s.receiver_size = m["size"]
                s.sums = {}
        elif t == F.FrameType.BLOCK_SUMS:
            if s.sums is not None:
                s.block_size = m["block_size"]
                for i, digest in enumerate(m["sums"]):
                    s.sums[m["first"] + i] = digest
        self.wake.set()
        return True

    async def _compare(self, s):
        """All BLOCK_SUMS of a failed final check arrived: find the differing blocks (spec 14.7)."""
        count = -(-s.receiver_size // s.block_size)
        sums = [s.sums[i] for i in range(count)]
        s.sums = None
        diff = await asyncio.to_thread(differing_blocks, s.path, s.block_size, sums, s.receiver_size)
        if count and len(diff) * 2 > count and self.on_replaced and await self.on_replaced(s):
            return
        log.info("%s generation %d: resending %d of %d blocks", s.name, s.gen, len(diff), count)
        s.repairs = diff
        if not diff:                            # blocks agree (file changed meanwhile?): check again
            s.repair_sent = True
            s.end_sent = False

    async def run(self, ch):
        while not ch.closed:
            self.wake.clear()
            progressed = False
            streams = await self.refresh()
            for s in streams:
                self._streams.setdefault(s.key, s)
            for s in streams:
                if s.sums_complete():
                    await self._compare(s)
                while s.repairs and not ch.closed:      # resend the differing blocks, overwriting at the receiver
                    off, end = s.repairs[0]
                    data = await asyncio.to_thread(read_range, s.path, off, min(self.segment_bytes, end - off))
                    if not data:
                        s.repairs.clear()               # file became shorter: the owner starts a new generation
                        break
                    frame = await asyncio.to_thread(F.segment, s.name, s.gen, off, data, now_ms(),
                                                    self.zlib_level, repair=True)
                    await ch.send(frame)
                    progressed = True
                    if not s.repairs or s.repairs[0][0] != off:
                        break                           # the repair was restarted meanwhile
                    if off + len(data) >= end:
                        s.repairs.pop(0)
                        if not s.repairs:
                            s.repair_sent = True
                            s.end_sent = False          # check again
                    else:
                        s.repairs[0] = (off + len(data), end)
                limit = s.final_size if s.final_size is not None else s.size
                while len(self._inflight) < self.window and s.sent < limit:
                    data = await asyncio.to_thread(read_range, s.path, s.sent, min(self.segment_bytes, limit - s.sent))
                    if not data:
                        break
                    frame = await asyncio.to_thread(F.segment, s.name, s.gen, s.sent, data, now_ms(),
                                                    self.zlib_level)
                    await ch.send(frame)
                    s.sent += len(data)
                    self._inflight.append((s.key, s.sent))
                    progressed = True
                if s.final_size is not None and not s.end_sent and s.result != "ok" and s.acked >= s.final_size \
                        and s.sums is None and not s.repairs:
                    digest, n = await asyncio.to_thread(sha256_prefix, s.path, s.final_size)
                    await ch.send(F.Frame(F.FrameType.STREAM_END, dict(stream=s.name, gen=s.gen, size=n,
                                                                       sha256=digest)))
                    s.end_sent = True
                    s.awaiting_check = True
            if not progressed:
                await self.wake.wait(self.poll_s)
        raise ConnectionError("connection closed")
