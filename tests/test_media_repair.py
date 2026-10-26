"""Block repair over offline media (spec 8.6, 14.7): a receiver whose final check failed asks in its next bundle
(STREAM_CHECK + BLOCK_SUMS), the sender answers with only the differing blocks - from the source to the hub and from
the hub to a consumer, without any network."""

import asyncio
import os
import shutil

import pytest

from rope_protocol import media
from rope_protocol.consumer import Consumer
from test_media import SEG, _hub, _hub_data, _source
from test_relay import BLOCK, _content, _verified
from test_transfer import _file, _patch_file

HEADER = b"\xff\xfe\xfd\xfc"


@pytest.fixture(autouse=True)
def _small_segments(monkeypatch):
    monkeypatch.setattr(media, "SEGMENT_BYTES", SEG)


def _consumer(tmp):
    return Consumer("c-1", "op-1", "127.0.0.1", os.path.join(tmp, "c-1"), hub_port=1, block_size=BLOCK)


def _stick(tmp, n):
    """A fresh stick per trip, so that every import sees only the newest bundle (old ones are tested separately)."""
    return os.path.join(tmp, "stick%d" % n)


def test_header_changed_after_the_stick_left_is_repaired_by_stick(tmp_path):
    """The recording program updates the header after the data went by stick: hub and consumer get only the
    differing block, by stick, and both end up verified - no network at all."""
    tmp = str(tmp_path)
    data = bytearray(os.urandom(int(2.5 * SEG)))
    path = _file(tmp, "rec.dat", bytes(data))
    src, hub, c = _source(tmp, {"rec": path}), _hub(tmp), _consumer(tmp)

    async def run():
        await src.export_media(_stick(tmp, 1))                  # trip 1: the data, not yet complete
        await hub.import_media(_stick(tmp, 1))
        await hub.export_media(_stick(tmp, 2), "c-1")           # the consumer gets the old header
        await c.import_media(_stick(tmp, 2))

        data[2:6] = HEADER
        _patch_file(path, 2, HEADER)                            # header updated at the end of the recording
        src.complete("rec")
        await src.export_media(_stick(tmp, 3))                  # trip 2: only the final check
        [rep] = await hub.import_media(_stick(tmp, 3))
        assert [x[3] for x in rep.checked] == ["mismatch"]

        out = await hub.export_media(_stick(tmp, 4), "source-1")  # trip 3: the hub asks for a repair
        assert out.repair_requests == 1
        [rep] = await src.import_media(_stick(tmp, 4))
        assert rep.ok

        out = await src.export_media(_stick(tmp, 5))            # trip 4: only the differing block
        assert 0 < out.repair_bytes <= BLOCK and out.stream_bytes == 0
        [rep] = await hub.import_media(_stick(tmp, 5))
        assert rep.ok and [x[3] for x in rep.checked] == ["ok"]
        assert _hub_data(hub, "rec") == bytes(data)

        # the consumer: the hub's verified final check fails there (old header), it asks, the hub repairs
        await hub.export_media(_stick(tmp, 6), "c-1")
        [rep] = await c.import_media(_stick(tmp, 6))
        assert [x[3] for x in rep.checked] == ["mismatch"]
        out = await c.export_media(_stick(tmp, 7))
        assert out.repair_requests == 1
        await hub.import_media(_stick(tmp, 7))
        out = await hub.export_media(_stick(tmp, 8), "c-1")
        assert 0 < out.repair_bytes <= BLOCK
        [rep] = await c.import_media(_stick(tmp, 8))
        assert rep.ok and _verified(c, "source-1/rec") and _content(c, "source-1/rec") == bytes(data)

        # requests are done: no further repairs; old sticks imported again change nothing
        assert (await src.export_media(_stick(tmp, 9))).repair_bytes == 0
        assert (await hub.export_media(_stick(tmp, 10), "c-1")).repair_bytes == 0
        for n in range(1, 11):
            for who in (hub, c):
                await who.import_media(_stick(tmp, n))
        assert _hub_data(hub, "rec") == bytes(data) and _content(c, "source-1/rec") == bytes(data)
        assert _verified(c, "source-1/rec") and hub.store.get("source-1", "rec", 1).verified == "ok"
    asyncio.run(run())


def test_most_blocks_changed_becomes_a_new_generation_by_stick(tmp_path):
    tmp = str(tmp_path)
    old = os.urandom(4 * BLOCK)
    new = os.urandom(4 * BLOCK)
    path = _file(tmp, "rec.dat", old)
    src, hub = _source(tmp, {"rec": path}), _hub(tmp)

    async def run():
        await src.export_media(_stick(tmp, 1))
        await hub.import_media(_stick(tmp, 1))
        _patch_file(path, 0, new)                               # replaced, same size
        src.complete("rec")
        await src.export_media(_stick(tmp, 2))
        await hub.import_media(_stick(tmp, 2))
        await hub.export_media(_stick(tmp, 3), "source-1")
        await src.import_media(_stick(tmp, 3))
        out = await src.export_media(_stick(tmp, 4))
        assert out.repair_bytes == 0 and out.stream_bytes == len(new)
        assert src.streams["rec"].gen == 2
        await hub.import_media(_stick(tmp, 4))
        src.complete("rec")                                     # the application completes the new file, as on
        await src.export_media(_stick(tmp, 5))                  # the network (test_repair)
        await hub.import_media(_stick(tmp, 5))
        assert _hub_data(hub, "rec", gen=2) == new and hub.store.get("source-1", "rec", 2).verified == "ok"
        assert _hub_data(hub, "rec", gen=1) == old                # kept, never overwritten
    asyncio.run(run())


def test_lost_or_damaged_repair_stick_is_asked_for_again(tmp_path):
    tmp = str(tmp_path)
    data = bytearray(os.urandom(2 * BLOCK))
    path = _file(tmp, "rec.dat", bytes(data))
    src, hub = _source(tmp, {"rec": path}), _hub(tmp)

    async def run():
        await src.export_media(_stick(tmp, 1))
        await hub.import_media(_stick(tmp, 1))
        data[2:6] = HEADER
        _patch_file(path, 2, HEADER)
        src.complete("rec")
        await src.export_media(_stick(tmp, 2))
        await hub.import_media(_stick(tmp, 2))

        await hub.export_media(_stick(tmp, 3), "source-1")
        await src.import_media(_stick(tmp, 3))
        await src.export_media(_stick(tmp, 4))
        shutil.rmtree(_stick(tmp, 4))                            # the stick with the repair is lost

        await hub.export_media(_stick(tmp, 5), "source-1")      # still a mismatch: the hub asks again
        [bundle] = [p for p, _m in media.find_bundles(_stick(tmp, 5), "op-1", "source-1")]
        frames = os.path.join(bundle, "frames-0001.rope")
        size = os.path.getsize(frames)
        with open(frames, "r+b") as f:                          # ... but this request arrives cut off
            f.truncate(size - 10)
        [rep] = await src.import_media(_stick(tmp, 5))
        assert not rep.ok
        assert (await src.export_media(_stick(tmp, 6))).repair_bytes == 0     # nothing half-understood is sent

        await hub.export_media(_stick(tmp, 7), "source-1")      # third request, intact
        await src.import_media(_stick(tmp, 7))
        await src.export_media(_stick(tmp, 8))
        [rep] = await hub.import_media(_stick(tmp, 8))
        assert [x[3] for x in rep.checked] == ["ok"] and _hub_data(hub, "rec") == bytes(data)
    asyncio.run(run())
