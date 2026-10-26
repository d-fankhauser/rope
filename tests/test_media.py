"""Offline media as a second path (spec 8.6): streams and messages travel on a "stick" (a folder) between participants
and the hub, alone or mixed with the network - bit for bit, every message exactly once, damage reported."""

import asyncio
import os
import shutil

import pytest

from rope_protocol import frames as F
from rope_protocol import media
from rope_protocol.consumer import Consumer
from rope_protocol.hub import Hub
from rope_protocol.recipient import Recipient
from rope_protocol.source import Source
from collections import Counter

from test_relay import BLOCK, HARSH, PARTICIPANTS, RelayBench, _content, _verified
from test_transfer import _file, _patch_file, _until

SEG = 256 * 1024             # segment size on the medium in these tests (1 MiB in use), to keep the tests fast


@pytest.fixture(autouse=True)
def _small_segments(monkeypatch):
    monkeypatch.setattr(media, "SEGMENT_BYTES", SEG)


def _hub(tmp, name="hub"):
    return Hub(os.path.join(tmp, name), PARTICIPANTS, "op-1", block_size=BLOCK)       # not started: no network


def _source(tmp, files, participant="source-1"):
    return Source(participant, "op-1", "127.0.0.1", files, os.path.join(tmp, "src-" + participant), hub_port=1)


def _hub_data(hub, stream, participant="source-1", gen=1):
    rs = hub.store.get(participant, stream, gen)
    with open(rs.path, "rb") as f:
        return f.read(rs.committed)


def _frames_file(bundle):
    return os.path.join(bundle, "frames-0001.rope")


def test_stick_only_from_source_to_consumer_and_recipient(tmp_path):
    """No network at all: recording and task go by stick to the hub, on to consumer and recipient, receipts back."""
    tmp = str(tmp_path)
    data = os.urandom(int(2.5 * SEG))
    path = _file(tmp, "rec.dat", data)
    stick = os.path.join(tmp, "stick")
    told = []

    async def run():
        src = _source(tmp, {"rec": path})
        msg_id = src.send_message(["c-1", "r-1"], "task", b"Check position 7 again")
        src.complete("rec")
        out = await src.export_media(stick)
        assert out.complete and out.messages == 1 and out.stream_bytes == len(data)

        hub = _hub(tmp)
        [rep] = await hub.import_media(stick)
        assert rep.ok and rep.sender == "source-1" and rep.new_bytes == len(data)
        assert [c[:4] for c in rep.checked] == [("source-1", "rec", 1, "ok")]
        assert _hub_data(hub, "rec") == data
        assert hub.message_status(msg_id)["pending"] == ["c-1", "r-1"]

        c = Consumer("c-1", "op-1", "127.0.0.1", os.path.join(tmp, "c-1"), hub_port=1, block_size=BLOCK,
                     on_message=lambda m, body: told.append(("c-1", m["id"], body)))
        r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(tmp, "r-1"), hub_port=1,
                      on_message=lambda m, body: told.append(("r-1", m["id"], body)))
        await hub.export_media(stick, "c-1")
        await hub.export_media(stick, "r-1")
        [rc] = await c.import_media(stick)
        [rr] = await r.import_media(stick)
        assert rc.ok and rr.ok
        assert _content(c, "source-1/rec") == data and _verified(c, "source-1/rec")
        assert sorted(told) == [("c-1", msg_id, b"Check position 7 again"), ("r-1", msg_id, b"Check position 7 again")]

        # receipts back to the hub; the old source bundle is still on the stick and changes nothing
        await c.export_media(stick)
        await r.export_media(stick)
        reports = await hub.import_media(stick)
        assert len(reports) == 3 and all(x.ok for x in reports)
        assert sum(x.new_bytes for x in reports) == 0
        assert sorted(hub.message_status(msg_id)["delivered"]) == ["c-1", "r-1"]

        # and the source learns that the hub holds its message and its stream
        await hub.export_media(stick, "source-1")
        [rs] = await src.import_media(stick)
        assert rs.ok and rs.acks == [("rec", 1, len(data))]
        assert src.outbox()[0]["at_hub"] is True

        # importing everything again tells nobody twice
        await c.import_media(stick)
        await r.import_media(stick)
        assert len(told) == 2
    asyncio.run(run())


def test_network_and_stick_mixed(tmp_path):
    """Part over the network, the rest by stick while the link is down, then the network again: one generation,
    bit for bit at hub and consumer, and the bundle starts where the hub's acknowledgement was."""
    first, more = os.urandom(600_000), os.urandom(900_000)
    path = _file(str(tmp_path), "m.dat", first)
    stick = os.path.join(str(tmp_path), "stick")

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer()
            src = b.source({"m": path}).start()
            await _until(lambda: src.streams["m"].out.acked == len(first))
            await src.stop()                                        # link down
            with open(path, "ab") as f:
                f.write(more)
            out = await src.export_media(stick)
            assert out.stream_bytes == len(more)                    # only what the hub had not acknowledged
            [rep] = await b.hub.import_media(stick)
            assert rep.ok and b.received("m").committed == len(first) + len(more)
            src2 = b.source({"m": path}).start()                    # link up again
            src2.complete("m")
            assert await src2.wait_checked("m", 20) == "ok"
            await _until(lambda: _verified(c, "source-1/m"))
            await src2.stop()
            assert _content(c, "source-1/m") == first + more
            assert b.hub.store.have("source-1") == [dict(stream="m", gen=1, committed=len(first) + len(more))]
    asyncio.run(run())


def test_message_by_stick_and_network_at_once_is_told_once(tmp_path):
    stick = os.path.join(str(tmp_path), "stick")
    told = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({}).start()
            msg_id = src.send_message(["r-1"], "task", b"both ways")
            await _until(lambda: src.outbox()[0]["at_hub"])
            await src.stop()
            await b.hub.export_media(stick, "r-1")
            r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(b.tmp, "recipient-r-1"), hub_port=b.hub.port,
                          timeout_s=0.6, backoff=(0.05, 0.2), poll_s=0.05,
                          on_message=lambda m, body: told.append(m["id"]))
            r.start()
            await r.import_media(stick)
            await _until(lambda: b.hub.message_status(msg_id)["delivered"] == ["r-1"])
            await asyncio.sleep(0.3)
            await r.stop()
            assert told == [msg_id] and len(r.inbox()) == 1
    asyncio.run(run())


def test_bundles_in_parts_gap_and_import_twice(tmp_path):
    tmp = str(tmp_path)
    data = os.urandom(3 * SEG)
    path = _file(tmp, "p.dat", data)
    stick, alone = os.path.join(tmp, "stick"), os.path.join(tmp, "alone")

    async def run():
        src = _source(tmp, {"p": path})
        src.complete("p")
        part1 = await src.export_media(stick, max_bytes=int(1.3 * SEG))
        assert not part1.complete and part1.stream_bytes == SEG             # the medium was full after one segment
        hub = _hub(tmp)
        await hub.import_media(stick)
        assert hub.store.get("source-1", "p", 1).committed == SEG
        await hub.export_media(stick, "source-1")
        await src.import_media(stick)                                       # the source learns the hub has 1 MiB
        part2 = await src.export_media(stick)
        assert part2.complete and part2.stream_bytes == 2 * SEG

        # the second part alone at another hub: a gap, nothing stored, reported in plain language
        shutil.copytree(part2.path, os.path.join(alone, "rope", "op-1", os.path.basename(part2.path)))
        other = _hub(tmp, "other-hub")
        [rep] = await other.import_media(alone)
        assert rep.new_bytes == 0 and "missing" in rep.problems[0] and "earlier bundle" in rep.problems[0]
        assert other.store.get("source-1", "p", 1).committed == 0

        reports = await hub.import_media(stick)                             # both parts, oldest first
        assert all(x.ok for x in reports) and sum(x.new_bytes for x in reports) == 2 * SEG
        assert _hub_data(hub, "p") == data and hub.store.get("source-1", "p", 1).verified == "ok"
        again = await hub.import_media(stick)
        assert all(x.ok for x in again) and sum(x.new_bytes for x in again) == 0
    asyncio.run(run())


def test_damaged_and_cut_off_bundles_are_reported(tmp_path):
    tmp = str(tmp_path)
    data = os.urandom(3 * SEG)
    path = _file(tmp, "d.dat", data)

    async def run():
        src = _source(tmp, {"d": path})
        src.send_message(["r-1"], "note", b"survives")
        src.complete("d")
        good = await src.export_media(os.path.join(tmp, "good"))

        damaged = os.path.join(tmp, "damaged")
        shutil.copytree(os.path.join(tmp, "good"), damaged)
        bundle = os.path.join(damaged, "rope", "op-1", os.path.basename(good.path))
        raw = bytearray(open(_frames_file(bundle), "rb").read())
        raw[int(1.5 * SEG)] ^= 0xFF                                         # inside the second segment
        open(_frames_file(bundle), "wb").write(raw)
        hub = _hub(tmp)
        [rep] = await hub.import_media(damaged)
        assert not rep.ok and "damaged" in rep.problems[0]
        assert _hub_data(hub, "d") == data[:SEG]                            # the part before the damage is stored
        assert len(hub.messages.select(lambda r: True)) == 1                # messages come first in a bundle

        cut = os.path.join(tmp, "cut")
        shutil.copytree(os.path.join(tmp, "good"), cut)
        bundle = os.path.join(cut, "rope", "op-1", os.path.basename(good.path))
        with open(_frames_file(bundle), "r+b") as f:
            f.truncate(int(2.5 * SEG))
        hub2 = _hub(tmp, "hub2")
        [rep] = await hub2.import_media(cut)
        assert any("cut off" in p for p in rep.problems)
        assert _hub_data(hub2, "d") == data[:2 * SEG]

        [rep] = await hub.import_media(os.path.join(tmp, "good"))           # a good copy completes it
        assert rep.ok and _hub_data(hub, "d") == data and hub.store.get("source-1", "d", 1).verified == "ok"
    asyncio.run(run())


def test_old_stick_then_header_change_is_repaired_over_the_network(tmp_path):
    """The stick holds the file before its header was updated. The network repairs the header; importing the old
    stick again afterwards changes nothing."""
    data = bytearray(os.urandom(700_000))
    path = _file(str(tmp_path), "h.dat", bytes(data))
    stick = os.path.join(str(tmp_path), "stick")

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            old = b.source({"h": path})
            old.complete("h")
            await old.export_media(stick)
            [rep] = await b.hub.import_media(stick)
            assert rep.ok and b.received("h").verified == "ok"
            data[0:4] = b"\x09\x08\x07\x06"
            _patch_file(path, 0, data[0:4])
            src = b.source({"h": path}).start()
            src.complete("h")
            assert await src.wait_checked("h", 20) == "ok"                  # mismatch, block repair, ok
            await src.stop()
            assert _hub_data(b.hub, "h") == bytes(data)
            [rep] = await b.hub.import_media(stick)                         # the old stick once more
            assert rep.ok and rep.checked == [] and rep.new_bytes == 0
            assert _hub_data(b.hub, "h") == bytes(data) and b.received("h").verified == "ok"
    asyncio.run(run())


def test_foreign_partial_and_forged_bundles(tmp_path):
    tmp = str(tmp_path)
    stick = os.path.join(tmp, "stick")

    async def run():
        hub = _hub(tmp)
        w = media.BundleWriter(stick, "op-1", "intruder", "hub")               # not on the participant list
        w.add(F.message("a" * 32, "intruder", ["r-1"], 1, "note", b"x"))
        w.close()
        w = media.BundleWriter(stick, "op-1", "source-1", "hub")               # message claims another sender
        w.add(F.message("b" * 32, "source-2", ["r-1"], 1, "note", b"x"))
        w.close()
        w = media.BundleWriter(stick, "op-2", "source-1", "hub")               # other operation
        w.add(F.message("c" * 32, "source-1", ["r-1"], 1, "note", b"x"))
        w.close()
        w = media.BundleWriter(stick, "op-1", "source-1", "hub")               # pulled out during the export
        w.add(F.message("d" * 32, "source-1", ["r-1"], 1, "note", b"x"))
        w._close_file()
        w = media.BundleWriter(stick, "op-1", "source-1", "hub")               # manifest damaged afterwards
        w.add(F.message("e" * 32, "source-1", ["r-1"], 1, "note", b"x"))
        open(os.path.join(w.close().path, media.MANIFEST), "w").write("{damaged")
        reports = await hub.import_media(stick)
        assert len(reports) == 3
        assert any("cannot be read" in p for x in reports for p in x.problems)
        assert any("not on the participant list" in p for x in reports for p in x.problems)
        assert any("claims to be from source-2" in p for x in reports for p in x.problems)
        assert hub.messages.select(lambda r: True) == []

        await hub.export_media(stick, "c-2")
        c1 = Consumer("c-1", "op-1", "127.0.0.1", os.path.join(tmp, "c-1"), hub_port=1)
        [rep] = await c1.import_media(stick)                # not the bundle for c-2; the unreadable one is reported
        assert rep.frames == 0 and "cannot be read" in rep.problems[0]
    asyncio.run(run())


def test_large_bundle_is_split_into_files(tmp_path, monkeypatch):
    monkeypatch.setattr(media, "FILE_BYTES", int(1.5 * SEG))
    tmp = str(tmp_path)
    data = os.urandom(4 * SEG)
    path = _file(tmp, "big.dat", data)
    stick = os.path.join(tmp, "stick")

    async def run():
        src = _source(tmp, {"big": path})
        src.complete("big")
        out = await src.export_media(stick)
        assert len([n for n in os.listdir(out.path) if n.endswith(".rope")]) == 4
        hub = _hub(tmp)
        [rep] = await hub.import_media(stick)
        assert rep.ok and _hub_data(hub, "big") == data and hub.store.get("source-1", "big", 1).verified == "ok"
    asyncio.run(run())


async def _swap_sticks(b, src, c, r, stick):
    """One round trip of a stick: everybody exports, the hub imports and exports, everybody imports."""
    await src.export_media(stick)
    await c.export_media(stick)
    await r.export_media(stick)
    await b.hub.import_media(stick)
    for p in ("source-1", "c-1", "r-1"):
        await b.hub.export_media(stick, p)
    await src.import_media(stick)
    await c.import_media(stick)
    await r.import_media(stick)


def test_harsh_links_and_sticks_at_the_same_time(tmp_path, seed=5):
    """A growing file and messages over harsh links, while sticks go back and forth every few hundred ms."""
    path = _file(str(tmp_path), "g.dat", b"")
    parts = [os.urandom(150_000) for _ in range(6)]
    counts = Counter()

    async def run():
        async with RelayBench(str(tmp_path), HARSH, consumer_profile=HARSH) as b:
            b.proxy.link.rng.seed(seed)
            c = await b.consumer(on_message=lambda m, body: counts.update([("c-1", m["id"])]))
            r = await b.recipient(on_message=lambda m, body: counts.update([("r-1", m["id"])]))
            for i, p in enumerate(b.consumer_proxies):
                p.link.rng.seed(seed + 1000 + i)
            src = b.source({"g": path}).start()
            ids = []
            for i, part in enumerate(parts):
                with open(path, "ab") as f:
                    f.write(part)
                ids.append(src.send_message(["c-1", "r-1"], "note", b"part %d" % i))
                await _swap_sticks(b, src, c, r, os.path.join(b.tmp, "stick-%d" % i))
                await asyncio.sleep(0.2)
            src.complete("g")
            for i in range(6, 9):
                await _swap_sticks(b, src, c, r, os.path.join(b.tmp, "stick-%d" % i))
            assert await src.wait_checked("g", 60) == "ok"
            await _until(lambda: _verified(c, "source-1/g"), 60)
            await _until(lambda: len(counts) == 2 * len(ids), 60)
            await asyncio.sleep(1.0)                                    # late duplicates would show up now
            await src.stop()
            assert _content(c, "source-1/g") == b"".join(parts)
            assert set(counts.values()) == {1}                          # every message exactly once per recipient
            assert b.hub.store.have("source-1") == [dict(stream="g", gen=1, committed=sum(map(len, parts)))]
    asyncio.run(run())
