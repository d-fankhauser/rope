"""Messages with custody (spec 8.5): stored before acknowledged, delivered to each recipient, every message exactly
once - also over a harsh link, while recipients are offline and across hub restarts."""

import asyncio
import os
import threading
from collections import Counter

import pytest

from rope_protocol import frames as F
from rope_protocol.hub import Hub
from rope_protocol.linksim import Profile
from rope_protocol.messages import Mailbox, MessageStore
from rope_protocol.source import Source
from test_relay import BLOCK, HARSH, PARTICIPANTS, RelayBench
from test_transfer import FAST, SRC, _file, _until


def test_store_custody_duplicates_and_restart(tmp_path):
    st = MessageStore(tmp_path)
    meta = {"id": "a" * 32, "from": "source-1", "to": ["r-1"], "t": 1, "kind": "note"}
    assert st.put(meta, b"body", {"pending": ["r-1"]}) is True
    assert st.put(meta, b"other", {"pending": []}) is False            # duplicate id: kept as it was
    st.modify("a" * 32, lambda s: s["pending"].remove("r-1"))
    (tmp_path / "x.json.tmp").write_text("{broken")                    # crash in the middle of an atomic write
    again = MessageStore(tmp_path)
    assert again.get("a" * 32)["state"] == {"pending": []}
    assert again.body("a" * 32) == b"body"
    assert not (tmp_path / "x.json.tmp").exists()


def test_leftover_still_being_written_does_not_stop_the_start(tmp_path):
    """Found by the test bench on Windows: a hub restarted inside the same program found a .tmp file that the old
    instance was still writing; removing it failed and the new hub did not start."""
    MessageStore(tmp_path)
    with open(tmp_path / "y.json.tmp", "w") as busy:                     # open: Windows refuses to remove it
        busy.write("{")
        again = MessageStore(tmp_path)
    assert again.select(lambda r: True) == []


def test_stored_but_untold_message_is_told_after_restart(tmp_path):
    """The connection broke after the message was stored and acknowledged, before the application was told."""
    got = []
    box = MessageStore(tmp_path / "recipient-r-1" / "messages")
    meta = {"id": "b" * 32, "from": "source-1", "to": ["r-1"], "t": 1, "kind": "note"}
    box.put(meta, b"stored, not told", {"box": "in", "told": False})

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            r = await b.recipient(on_message=lambda m, body: got.append((m["id"], body)))
            await _until(lambda: got)
            await asyncio.sleep(0.3)
            assert got == [("b" * 32, b"stored, not told")]                # told once, not twice
            assert len(r.inbox()) == 1
    asyncio.run(run())


def test_session_cancelled_while_recording_told_does_not_tell_twice(tmp_path):
    """Found by the test bench: the session ended (connection cut) after telling the application but before the
    "told" flag was on disk; the next session told the message again, without any restart."""
    got = []
    box = Mailbox(str(tmp_path / "box"), "r-1", on_message=lambda m, body: got.append(m["id"]))
    meta = {"id": "c" * 32, "from": "source-1", "to": ["r-1"], "t": 1, "kind": "note"}
    box.store.put(meta, b"x", {"box": "in", "told": False})
    update = box.store.update
    slow = threading.Event()

    def slow_update(msg_id, **state):
        slow.wait(1.0)                              # the flag reaches the disk only after the session is gone
        update(msg_id, **state)
    box.store.update = slow_update

    async def run():
        first = asyncio.ensure_future(box.notify())
        await _until(lambda: got)
        first.cancel()                              # the session ends while the flag is being written
        try:
            await first
        except asyncio.CancelledError:
            pass
        await box.notify()                          # next session
        slow.set()
    asyncio.run(run())
    assert got == ["c" * 32]


def test_message_to_consumer_and_recipient_exactly_once(tmp_path):
    got = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer(on_message=lambda m, body: got.append(("c-1", m["id"], body)))
            r = await b.recipient(on_message=lambda m, body: got.append(("r-1", m["id"], body)))
            src = b.source({}).start()
            msg_id = src.send_message(["c-1", "r-1"], "task", "Check position 7 again".encode("utf-8"))
            await _until(lambda: len(got) == 2)
            assert sorted(g[0] for g in got) == ["c-1", "r-1"]
            assert all(g[1] == msg_id and g[2] == b"Check position 7 again" for g in got)
            await _until(lambda: src.outbox()[0]["at_hub"])
            await _until(lambda: b.hub.message_status(msg_id)["pending"] == [])
            assert sorted(b.hub.message_status(msg_id)["delivered"]) == ["c-1", "r-1"]
            assert [m["id"] for m in r.inbox()] == [msg_id] and r.message_body(msg_id) == b"Check position 7 again"
            await src.stop()
            assert c.inbox()[0]["from"] == "source-1"
    asyncio.run(run())


def test_custody_while_recipient_offline_and_hub_restarts(tmp_path):
    """The source hands the message over and leaves; the hub restarts; the recipient comes later and still gets it."""
    got = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({}).start()
            msg_id = src.send_message(["r-1"], "note", b"for later")
            await _until(lambda: src.outbox()[0]["at_hub"])
            await src.stop()
            port = b.hub.port
            await b.hub.stop()
            await asyncio.sleep(0.3)
            b.hub = await Hub(os.path.join(b.tmp, "hub"), PARTICIPANTS, "op-1", port=port, block_size=BLOCK,
                              **FAST).start()
            await b.recipient(on_message=lambda m, body: got.append((m["id"], body)))
            await _until(lambda: got)
            assert got == [(msg_id, b"for later")]
    asyncio.run(run())


def test_outbox_survives_a_sender_restart(tmp_path):
    """Written while the hub is unreachable, the message waits in the outbox, also across a restart of the sender."""
    got = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = Source("source-1", "op-1", "127.0.0.1", {}, os.path.join(b.tmp, "src-source-1"), hub_port=1,
                         **SRC).start()                                     # nobody listens there
            msg_id = src.send_message(["r-1"], "note", b"queued")
            await asyncio.sleep(0.3)
            await src.stop()
            assert src.outbox()[0]["at_hub"] is False
            src2 = b.source({}).start()                                     # same state folder, hub reachable
            await b.recipient(on_message=lambda m, body: got.append((m["id"], body)))
            await _until(lambda: got)
            assert got == [(msg_id, b"queued")]
            await src2.stop()
    asyncio.run(run())


def test_many_messages_over_harsh_links_exactly_once(tmp_path, seed=21):
    counts = Counter()

    async def run():
        async with RelayBench(str(tmp_path), HARSH, consumer_profile=HARSH) as b:
            b.proxy.link.rng.seed(seed)
            r = await b.recipient(on_message=lambda m, body: counts.update([m["id"]]))
            b.consumer_proxies[0].link.rng.seed(seed + 1000)
            src = b.source({}, poll_s=0.05).start()
            ids = []
            for i in range(40):
                ids.append(src.send_message(["r-1"], "note", ("message %d" % i).encode()))
                await asyncio.sleep(0.05)
            await _until(lambda: len(counts) == 40, 60)
            await asyncio.sleep(1.0)                                        # late duplicates would show up now
            await src.stop()
            assert set(counts) == set(ids) and set(counts.values()) == {1}  # every message exactly once
            assert len(r.inbox()) == 40
    asyncio.run(run())


def test_recipient_may_answer_and_unknown_recipients_are_reported(tmp_path):
    got = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer(on_message=lambda m, body: got.append((m["from"], body)))
            r = await b.recipient()
            msg_id = r.send_message(["c-1", "nobody"], "reply", b"understood")
            await _until(lambda: got)
            assert got == [("r-1", b"understood")]
            status = b.hub.message_status(msg_id)
            assert status["delivered"] == ["c-1"] and status["unknown"] == ["nobody"]
            assert c.inbox()[0]["to"] == ["c-1", "nobody"]
    asyncio.run(run())


def test_forged_sender_is_refused(tmp_path):
    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({}).start()
            src.mailbox.participant = "source-2"                            # claims to be someone else
            src.send_message(["r-1"], "note", b"forged")
            await _until(lambda: src.sessions >= 2, 10)                     # refused, reconnects, refused again
            await src.stop()
            assert src.outbox()[0]["at_hub"] is False
            assert b.hub.messages.select(lambda r: True) == []
    asyncio.run(run())


def test_bad_input_is_rejected_at_once(tmp_path):
    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({})
            with pytest.raises(F.FrameError):
                src.send_message(["r-1"], "has space", b"x")
            with pytest.raises(F.FrameError):
                src.send_message([], "note", b"x")
            with pytest.raises(ValueError):
                src.send_message(["r-1"], "note", b"x" * (F.MAX_BLOB + 1))
            assert src.outbox() == []
    asyncio.run(run())


def test_messages_and_streams_share_the_link(tmp_path):
    """A message is not stuck behind a large stream backlog."""
    data = os.urandom(3_000_000)
    path = _file(tmp_path, "big.dat", data)
    got = []

    async def run():
        slow = Profile("slow", rate=1_000_000)
        async with RelayBench(str(tmp_path), slow) as b:
            await b.recipient(on_message=lambda m, body: got.append(asyncio.get_running_loop().time()))
            src = b.source({"big": path}).start()
            await _until(lambda: b.received("big").committed > 200_000)
            t0 = asyncio.get_running_loop().time()
            src.send_message(["r-1"], "task", b"urgent")
            await _until(lambda: got, 10)
            assert got[0] - t0 < 2.0                                        # the file needs about 3 s in total
            assert b.received("big").committed < len(data)                  # ... and is still on its way
            await src.stop()
    asyncio.run(run())
