"""Source -> hub over the link simulator: bit-for-bit identical streams despite blackouts, disconnects and restarts
(spec 13 pass criterion)."""

import asyncio
import hashlib
import os
import time

import pytest

from rope_protocol.hub import Hub
from rope_protocol.linksim import LinkProxy, Profile
from rope_protocol.source import Source

FAST = dict(heartbeat_s=0.1, timeout_s=0.6)
SRC = dict(timeout_s=0.6, backoff=(0.05, 0.2), poll_s=0.05, segment_bytes=64 * 1024)


def _sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


async def _until(cond, timeout=20.0):
    end = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > end:
            raise AssertionError("condition not reached within %.0f s" % timeout)
        await asyncio.sleep(0.02)


class Bench:
    """Hub + link simulator + source with one or more files."""

    def __init__(self, tmp, profile="lan", script=None, participants=None, operation="op-1", **hub_kw):
        self.tmp = tmp
        self.profile = profile
        self.script = script
        self.participants = participants or {"source-1": "source"}
        self.operation = operation
        self.checked = []
        self.hub_kw = dict(FAST, **hub_kw)

    async def start_hub(self):
        self.hub = await Hub(os.path.join(self.tmp, "hub"), self.participants, "op-1", port=0,
                             on_stream_checked=lambda *a: self.checked.append(a), **self.hub_kw).start()
        return self.hub

    async def __aenter__(self):
        await self.start_hub()
        self.proxy = await LinkProxy(("127.0.0.1", 0), ("127.0.0.1", self.hub.port), self.profile,
                                     script=self.script).start()
        return self

    async def __aexit__(self, *exc):
        await self.proxy.stop()
        await self.hub.stop()

    def source(self, files, participant="source-1", **kw):
        return Source(participant, self.operation, "127.0.0.1", files, os.path.join(self.tmp, "src-" + participant),
                      hub_port=self.proxy.port, **dict(SRC, **kw))

    def received(self, stream, gen=1, participant="source-1"):
        return self.hub.store.get(participant, stream, gen)


def _patch_file(path, off, data):
    """Change bytes in place, as a recording program updating a header does."""
    with open(path, "r+b") as f:
        f.seek(off)
        f.write(data)


def _file(tmp, name, data):
    p = os.path.join(tmp, name)
    with open(p, "wb") as f:
        f.write(data)
    return p


def test_complete_file_bit_for_bit(tmp_path):
    data = os.urandom(1_500_000)
    path = _file(tmp_path, "rec1.dat", data)

    async def run():
        async with Bench(str(tmp_path)) as b:
            src = b.source({"rec-1": path}).start()
            src.complete("rec-1")
            assert await src.wait_checked("rec-1", 20) == "ok"
            await src.stop()
            rs = b.received("rec-1")
            assert open(rs.path, "rb").read() == data
            assert b.checked and b.checked[0][3] == "ok"
    asyncio.run(run())


def test_growing_file_is_followed_live(tmp_path):
    path = _file(tmp_path, "live.dat", b"")

    async def run():
        async with Bench(str(tmp_path)) as b:
            src = b.source({"live": path}).start()
            parts = [os.urandom(50_000) for _ in range(10)]
            for p in parts:                                     # the "recording program" appends
                with open(path, "ab") as f:
                    f.write(p)
                await asyncio.sleep(0.05)
            await _until(lambda: b.received("live").committed == 500_000)
            src.complete("live")
            assert await src.wait_checked("live", 20) == "ok"
            await src.stop()
            assert open(b.received("live").path, "rb").read() == b"".join(parts)
    asyncio.run(run())


def test_disconnect_resumes_from_committed_offset(tmp_path):
    data = os.urandom(3_000_000)
    path = _file(tmp_path, "big.dat", data)
    slow = Profile("slow", rate=2_000_000)                     # ~1.5 s for the file: the cuts land mid-transfer

    async def run():
        async with Bench(str(tmp_path), slow, script=[(0.5, "disconnect", 0.3), (1.2, "disconnect", 0.3)]) as b:
            src = b.source({"big": path}).start()
            src.complete("big")
            assert await src.wait_checked("big", 30) == "ok"
            await src.stop()
            assert b.proxy.stats.disconnects == 2 and src.sessions >= 3
            assert _sha(b.received("big").path) == hashlib.sha256(data).hexdigest()
            # resumed, not restarted: far less than twice the file went over the link
            assert b.proxy.stats.bytes_up < 1.5 * len(data)
    asyncio.run(run())


def test_short_blackout_keeps_session_long_blackout_reconnects(tmp_path):
    data = os.urandom(400_000)
    path = _file(tmp_path, "r.dat", data)
    slow = Profile("slow", rate=400_000)

    async def run():
        # 0.3 s < timeout 0.6 s: same session; 1.5 s > timeout: reconnect
        async with Bench(str(tmp_path), slow, script=[(0.2, "blackout", 0.3), (1.0, "blackout", 1.5)]) as b:
            src = b.source({"r": path}).start()
            await _until(lambda: src.sessions == 1)
            await asyncio.sleep(0.8)
            assert src.sessions == 1                            # survived the short blackout
            src.complete("r")
            assert await src.wait_checked("r", 30) == "ok"
            assert src.sessions >= 2                            # the long one cost a reconnect, no data
            await src.stop()
            assert _sha(b.received("r").path) == hashlib.sha256(data).hexdigest()
    asyncio.run(run())


def test_hub_restart_during_transfer(tmp_path):
    data = os.urandom(2_000_000)
    path = _file(tmp_path, "h.dat", data)
    slow = Profile("slow", rate=1_000_000)

    async def run():
        async with Bench(str(tmp_path), slow) as b:
            src = b.source({"h": path}).start()
            await _until(lambda: b.received("h").committed > 300_000)
            port = b.hub.port
            await b.hub.stop()                                   # hub process ends
            await asyncio.sleep(0.3)
            b.hub = await Hub(os.path.join(str(tmp_path), "hub"), {"source-1": "source"}, "op-1", port=port,
                              **FAST).start()                    # ... and starts again on the same storage
            src.complete("h")
            assert await src.wait_checked("h", 30) == "ok"
            await src.stop()
            assert _sha(b.received("h").path) == hashlib.sha256(data).hexdigest()
    asyncio.run(run())


def test_source_restart_continues(tmp_path):
    data = os.urandom(1_000_000)
    path = _file(tmp_path, "s.dat", data)

    async def run():
        async with Bench(str(tmp_path), Profile("slow", rate=1_000_000)) as b:
            src = b.source({"s": path}).start()
            await _until(lambda: b.received("s").committed > 200_000)
            await src.stop()
            sent_before = b.proxy.stats.bytes_up
            src2 = b.source({"s": path}).start()
            src2.complete("s")
            assert await src2.wait_checked("s", 30) == "ok"
            await src2.stop()
            assert b.proxy.stats.bytes_up - sent_before < len(data)          # did not start from zero
            assert _sha(b.received("s").path) == hashlib.sha256(data).hexdigest()
    asyncio.run(run())


def test_replaced_file_becomes_new_generation(tmp_path):
    first = os.urandom(300_000)
    path = _file(tmp_path, "g.dat", first)

    async def run():
        async with Bench(str(tmp_path)) as b:
            src = b.source({"g": path}).start()
            await _until(lambda: b.received("g").committed == len(first))
            second = os.urandom(100_000)                        # shorter: not a continuation
            _file(tmp_path, "g.dat", second)
            await _until(lambda: src.streams["g"].gen == 2 and b.received("g", 2).committed == len(second))
            src.complete("g")
            assert await src.wait_checked("g", 20) == "ok"
            await src.stop()
            assert open(b.received("g", 1).path, "rb").read() == first          # never overwritten
            assert open(b.received("g", 2).path, "rb").read() == second
    asyncio.run(run())


@pytest.mark.parametrize("participant,operation,code", [
    ("source-9", "op-1", "unknown_participant"),
    ("source-1", "op-2", "wrong_operation"),
])
def test_rejected_with_reason(tmp_path, participant, operation, code):
    path = _file(tmp_path, "x.dat", b"x")

    async def run():
        async with Bench(str(tmp_path), operation=operation) as b:
            src = b.source({"x": path}, participant=participant).start()
            await _until(lambda: src.rejected is not None, 5)
            assert src.rejected.code == code and src.rejected.text
            await src.stop()
            assert b.hub.store.have(participant) == []
    asyncio.run(run())


def test_radio_profile_soak(tmp_path):
    """A few seconds on a harsh random link (fast-forwarded profile): still bit for bit."""
    data = os.urandom(1_000_000)
    path = _file(tmp_path, "soak.dat", data)
    harsh = Profile("harsh", rate=1_000_000, latency=0.01, jitter=0.02, blackout_every=0.4, blackout_len=(0.05, 0.9),
                    disconnect_every=1.0, outage_len=(0.05, 0.3), description="test only")

    async def run():
        async with Bench(str(tmp_path), harsh) as b:
            b.proxy.link.rng.seed(3)
            src = b.source({"soak": path}).start()
            src.complete("soak")
            assert await src.wait_checked("soak", 60) == "ok"
            await src.stop()
            assert _sha(b.received("soak").path) == hashlib.sha256(data).hexdigest()
    asyncio.run(run())
