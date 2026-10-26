"""Delivery report to the sender of a message (spec 8.5): the sender learns which recipients have its message - over
the network, across restarts, over a harsh link and by stick - and the application is told once per new report."""

import asyncio
import os
from collections import Counter

from rope_protocol.hub import Hub
from rope_protocol.messages import Mailbox
from rope_protocol.recipient import Recipient
from rope_protocol.source import Source
from test_relay import BLOCK, HARSH, PARTICIPANTS, RelayBench
from test_transfer import FAST, _until


def _report(participant, msg_id):
    return next(m["report"] for m in participant.outbox() if m["id"] == msg_id)


def test_report_grows_as_recipients_get_the_message(tmp_path):
    reports = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            await b.consumer()
            src = b.source({}, on_delivery=lambda i, rep: reports.append((i, rep))).start()
            msg_id = src.send_message(["c-1", "r-1"], "task", b"Check position 7 again")
            await _until(lambda: reports)
            assert reports == [(msg_id, {"delivered": ["c-1"], "pending": ["r-1"], "unknown": []})]
            await b.recipient()                                         # the second recipient comes later
            await _until(lambda: len(reports) == 2)
            assert reports[1] == (msg_id, {"delivered": ["c-1", "r-1"], "pending": [], "unknown": []})
            assert _report(src, msg_id)["pending"] == [] and src.outbox()[0]["at_hub"]
            await _until(lambda: b.hub.messages.get(msg_id)["state"].get("reported") == 2)
            await asyncio.sleep(0.3)
            assert len(reports) == 2                                    # nothing told twice
            await src.stop()
    asyncio.run(run())


def test_unknown_or_no_recipients_are_reported_at_once(tmp_path):
    reports = {}

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            r = await b.recipient(on_delivery=lambda i, rep: reports.setdefault(i, []).append(rep))
            only_unknown = r.send_message(["nobody"], "note", b"x")
            mixed = r.send_message(["c-1", "nobody"], "note", b"y")     # c-1 is offline
            to_self = r.send_message(["r-1"], "note", b"z")
            await _until(lambda: len(reports) == 3)
            assert reports[only_unknown] == [{"delivered": [], "pending": [], "unknown": ["nobody"]}]
            assert reports[mixed] == [{"delivered": [], "pending": ["c-1"], "unknown": ["nobody"]}]
            assert reports[to_self] == [{"delivered": [], "pending": [], "unknown": []}]
            await b.consumer()
            await _until(lambda: len(reports[mixed]) == 2)
            assert reports[mixed][1] == {"delivered": ["c-1"], "pending": [], "unknown": ["nobody"]}
    asyncio.run(run())


def test_report_waits_for_the_sender_across_restarts(tmp_path):
    """The sender leaves after handing over; the hub restarts; the recipient gets the message; the sender comes back
    and receives the report."""
    reports = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({}).start()
            msg_id = src.send_message(["r-1"], "note", b"for later")
            await _until(lambda: src.outbox()[0]["at_hub"])
            await src.stop()
            port = b.hub.port
            await b.hub.stop()
            b.hub = await Hub(os.path.join(b.tmp, "hub"), PARTICIPANTS, "op-1", port=port, block_size=BLOCK,
                              **FAST).start()
            r = await b.recipient()
            await _until(lambda: r.inbox())
            await _until(lambda: b.hub.message_status(msg_id)["delivered"] == ["r-1"])
            assert _report(src, msg_id) is None
            src2 = b.source({}, on_delivery=lambda i, rep: reports.append(i)).start()
            await _until(lambda: reports)
            assert reports == [msg_id] and _report(src2, msg_id)["delivered"] == ["r-1"]
            await src2.stop()
    asyncio.run(run())


def test_stored_but_untold_report_is_told_after_restart(tmp_path):
    root = str(tmp_path / "box")
    box = Mailbox(root, "source-1")
    msg_id = box.send(["r-1"], "note", b"x")
    assert box.take_report({"id": msg_id, "delivered": ["r-1"], "pending": [], "unknown": []})
    assert not box.take_report({"id": msg_id, "delivered": [], "pending": ["r-1"], "unknown": []})    # older
    told = []

    async def run():
        again = Mailbox(root, "source-1", on_delivery=lambda i, rep: told.append((i, rep["delivered"])))
        await again.notify()
        await again.notify()
    asyncio.run(run())
    assert told == [(msg_id, ["r-1"])]
    assert box.take_report({"id": "f" * 32, "delivered": [], "pending": [], "unknown": []}) is False   # not ours


def test_reports_over_harsh_links_exactly_once(tmp_path, seed=7):
    told = Counter()

    async def run():
        async with RelayBench(str(tmp_path), HARSH, consumer_profile=HARSH) as b:
            b.proxy.link.rng.seed(seed)
            await b.recipient()
            b.consumer_proxies[0].link.rng.seed(seed + 1000)
            src = b.source({}, poll_s=0.05, on_delivery=lambda i, rep: told.update([i])).start()
            ids = []
            for i in range(25):
                ids.append(src.send_message(["r-1"], "note", ("message %d" % i).encode()))
                await asyncio.sleep(0.04)
            await _until(lambda: len(told) == 25, 60)
            await asyncio.sleep(1.0)
            await src.stop()
            assert set(told) == set(ids) and set(told.values()) == {1}
            assert all(m["report"]["delivered"] == ["r-1"] for m in src.outbox())
    asyncio.run(run())


def test_report_by_stick(tmp_path):
    """No network: message out, receipt back, report to the sender, its acknowledgement to the hub - all by stick.
    An old stick imported again changes nothing."""
    tmp = str(tmp_path)
    stick = os.path.join(tmp, "stick")
    told = []

    async def run():
        src = Source("source-1", "op-1", "127.0.0.1", {}, os.path.join(tmp, "src"), hub_port=1,
                     on_delivery=lambda i, rep: told.append((i, rep["delivered"])))
        msg_id = src.send_message(["r-1"], "task", b"by stick")
        await src.export_media(stick)
        hub = Hub(os.path.join(tmp, "hub"), PARTICIPANTS, "op-1", block_size=BLOCK)
        await hub.import_media(stick)
        r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(tmp, "r-1"), hub_port=1)
        await hub.export_media(stick, "r-1")
        await r.import_media(stick)
        await r.export_media(stick)
        await hub.import_media(stick)
        assert hub.message_status(msg_id)["delivered"] == ["r-1"]

        await hub.export_media(stick, "source-1")
        reps = await src.import_media(stick)
        assert [x.deliveries for x in reps if x.deliveries] == [[msg_id]]
        assert told == [(msg_id, ["r-1"])] and src.outbox()[0]["at_hub"]

        await src.export_media(stick)                                   # carries the DELIVERY_ACK
        reps = await hub.import_media(stick)
        assert any(msg_id in x.delivery_acks for x in reps)
        assert hub._reports_for("source-1") == []
        out = await hub.export_media(os.path.join(tmp, "stick2"), "source-1")
        assert out.complete
        await src.import_media(stick)                                   # everything again
        assert len(told) == 1
    asyncio.run(run())
