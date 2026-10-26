"""Final check and block repair (spec 8.4, 14.7): only differing blocks travel again; a different file becomes a new
generation; a check that keeps failing is reported, never retried forever."""

import asyncio
import hashlib
import os

from rope_protocol import store
from rope_protocol.linksim import Profile
from rope_protocol.store import StreamStore
from test_transfer import Bench, _file, _sha, _until

BLOCK = 64 * 1024
HARSH = Profile("harsh", rate=1_000_000, latency=0.01, jitter=0.02, blackout_every=0.4, blackout_len=(0.05, 0.9),
                disconnect_every=1.0, outage_len=(0.05, 0.3), description="test only")


def _patch(path, off, data):
    with open(path, "r+b") as f:
        f.seek(off)
        f.write(data)


def test_store_repair_write_and_block_sums(tmp_path):
    rs = StreamStore(tmp_path).get("p", "s", 1)
    rs.write(0, b"aaaabbbbcc")
    assert rs.block_sums(4) == [hashlib.sha256(x).hexdigest() for x in (b"aaaa", b"bbbb", b"cc")]
    assert rs.write(4, b"XXXX") == 10                       # plain duplicate: not written
    assert open(rs.path, "rb").read() == b"aaaabbbbcc"
    assert rs.write(4, b"XXXXccdd", repair=True) == 12      # overwrites below committed, appends the rest
    assert open(rs.path, "rb").read() == b"aaaaXXXXccdd"
    assert rs.sha256() == hashlib.sha256(b"aaaaXXXXccdd").hexdigest()
    assert rs.finish(12, hashlib.sha256(b"aaaaXXXXccdd").hexdigest()) == "ok"
    again = StreamStore(tmp_path).get("p", "s", 1)          # after a restart, too
    assert again.sha256() == hashlib.sha256(b"aaaaXXXXccdd").hexdigest()


def test_header_changed_in_place_is_repaired_not_resent(tmp_path):
    """A recording program updates the header after the data was sent: only that block travels again."""
    data = bytearray(os.urandom(1_000_000))
    path = _file(tmp_path, "rec.dat", bytes(data))

    async def run():
        async with Bench(str(tmp_path), block_size=BLOCK) as b:
            src = b.source({"rec": path}).start()
            await _until(lambda: b.received("rec").committed == len(data))
            up_before = b.proxy.stats.bytes_up
            data[2:6] = b"\xff\xfe\xfd\xfc"
            _patch(path, 2, data[2:6])                              # same size, beginning changed
            src.complete("rec")
            assert await src.wait_checked("rec", 20) == "ok"
            await src.stop()
            assert src.streams["rec"].gen == 1                      # not a new generation
            assert open(b.received("rec").path, "rb").read() == bytes(data)
            assert b.proxy.stats.bytes_up - up_before < 3 * BLOCK   # one block, not the whole file
    asyncio.run(run())


def test_repair_over_a_harsh_link(tmp_path, seed=5):
    """Several blocks changed in place, repaired while the link drops out and disconnects."""
    data = bytearray(os.urandom(1_000_000))
    path = _file(tmp_path, "h.dat", bytes(data))

    async def run():
        async with Bench(str(tmp_path), HARSH, block_size=BLOCK) as b:
            b.proxy.link.rng.seed(seed)
            src = b.source({"h": path}).start()
            await _until(lambda: b.received("h").committed == len(data), 60)
            for off in (10, 200_000, 650_000, 999_990):               # four of sixteen blocks
                data[off:off + 8] = os.urandom(8)
                _patch(path, off, data[off:off + 8])
            src.complete("h")
            assert await src.wait_checked("h", 60) == "ok"
            await src.stop()
            assert src.streams["h"].gen == 1
            assert open(b.received("h").path, "rb").read() == bytes(data)
    asyncio.run(run())


def test_damage_at_the_hub_is_repaired(tmp_path):
    """Stored bytes damaged at the hub (e.g. by the disk) are found by the final check and repaired."""
    data = os.urandom(500_000)
    path = _file(tmp_path, "d.dat", data)

    async def run():
        async with Bench(str(tmp_path), block_size=BLOCK) as b:
            src = b.source({"d": path}).start()
            await _until(lambda: b.received("d").committed == len(data))
            rs = b.received("d")
            _patch(rs.path, 300_000, b"\x00" * 100)
            rs._sha = None                                          # as after a restart: hash from the disk
            src.complete("d")
            assert await src.wait_checked("d", 20) == "ok"
            await src.stop()
            assert _sha(rs.path) == hashlib.sha256(data).hexdigest()
            assert src.streams["d"].repair_rounds == 1
    asyncio.run(run())


def test_different_file_of_same_length_becomes_new_generation(tmp_path):
    first = os.urandom(400_000)
    path = _file(tmp_path, "r.dat", first)

    async def run():
        async with Bench(str(tmp_path), block_size=BLOCK) as b:
            src = b.source({"r": path}).start()
            await _until(lambda: b.received("r").committed == len(first))
            second = os.urandom(len(first))                         # replaced, same length
            _file(tmp_path, "r.dat", second)
            src.complete("r")
            await _until(lambda: src.streams["r"].gen == 2)
            src.complete("r")                                       # the application completes the new file
            assert await src.wait_checked("r", 20) == "ok"
            await src.stop()
            assert open(b.received("r", 1).path, "rb").read() == first      # old version kept
            assert open(b.received("r", 2).path, "rb").read() == second
    asyncio.run(run())


def test_check_that_keeps_failing_is_reported(tmp_path, monkeypatch):
    """If the check never passes (here: a hub that always reports mismatch), the source gives up after a few rounds
    and tells the application instead of retrying forever."""
    monkeypatch.setattr(store.ReceivedStream, "finish", lambda self, size, sha: "mismatch")
    path = _file(tmp_path, "m.dat", os.urandom(200_000))

    async def run():
        async with Bench(str(tmp_path), block_size=BLOCK) as b:
            src = b.source({"m": path}).start()
            src.complete("m")
            assert await src.wait_checked("m", 20) == "mismatch"
            await src.stop()
            assert src.streams["m"].gen == 1
    asyncio.run(run())
