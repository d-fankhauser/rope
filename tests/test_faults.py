"""Test bench (spec 13): one fault at every frame of the procedure in turn - connection cut before or in the middle
of the frame, frame sent twice, frame damaged, restart of the participant or of the hub - on the link of the source
and on the link of a consumer.

The scenario uses every part of the protocol: greeting, segments, a header changed in place (final check, block
sums, repair), relaying to a consumer, messages both ways and the delivery report. Pass criterion after every fault:
the stream is bit for bit identical at hub and consumer, every message was told exactly once, and the sender has
its delivery report.

Every step = every occurrence of every frame type of a clean run, except heartbeats (their number depends on
timing; the first two are included). The normal test run cuts the connection at every step and tries the other
faults at a few steps; ``ROPE_SOAK=1`` runs every fault at every step (several minutes).

A restart stops the participant or the hub the way a program ends (tasks cancelled, nothing more written) and starts
a new one on the same folders - not a power cut, which can lose what the operating system had not written yet.
"""

import asyncio
import os
import random
import threading
import time
from collections import Counter

import pytest

from rope_protocol import frames as F
from rope_protocol.consumer import Consumer
from rope_protocol.hub import Hub
from rope_protocol.recipient import Recipient
from rope_protocol.source import Source
from framefault import DOWN, FAULTS, RESTARTS, UP, FrameFaultProxy
from test_transfer import _file, _patch_file, _until

SOAK = os.environ.get("ROPE_SOAK") == "1"
PARTICIPANTS = {"source-1": "source", "c-1": "consumer", "r-1": "recipient"}
SEG = 64 * 1024
BLOCK = 64 * 1024
FAST = dict(heartbeat_s=0.1, timeout_s=0.6)
PEER = dict(timeout_s=0.6, backoff=(0.02, 0.1))
DATA = random.Random(1).randbytes(3 * SEG + 1000)
HEADER = b"\xff\xfe\xfd\xfc"


async def scenario(tmp, link="source", fault=None, direction=UP, at=None):
    """Run the whole procedure once; return the two proxies (source link, consumer link)."""
    os.makedirs(tmp, exist_ok=True)
    path = _file(tmp, "rec.dat", DATA)
    told = Counter()
    reports = []
    cur = {}                                            # the running hub and participants (replaced on restart)

    def new_hub(port):
        return Hub(os.path.join(tmp, "hub"), PARTICIPANTS, "op-1", port=port, block_size=BLOCK, poll_s=0.05,
                   segment_bytes=SEG, **FAST)

    def new_consumer():
        return Consumer("c-1", "op-1", "127.0.0.1", os.path.join(tmp, "c-1"), hub_port=p_con.port, block_size=BLOCK,
                        on_message=lambda m, body: told.update([("c-1", body)]), **PEER)

    def new_source():
        s = Source("source-1", "op-1", "127.0.0.1", {"rec": path}, os.path.join(tmp, "src"), hub_port=p_src.port,
                   segment_bytes=SEG, poll_s=0.05, on_message=lambda m, body: told.update([("source-1", body)]),
                   on_delivery=lambda i, rep: reports.append(rep), **PEER)
        if cur.get("completed"):
            s.complete("rec")                           # the application asks again after its restart
        return s

    async def restart(what):
        if what == "restart_hub":
            port = cur["hub"].port
            await cur["hub"].stop()                # waits for writes still running (spec 13)
            cur["hub"] = await new_hub(port).start()
            return
        key = "src" if link == "source" else "c"
        await cur[key].stop()
        cur[key] = (new_source if key == "src" else new_consumer)().start()

    cur["hub"] = hub = await new_hub(0).start()
    p_src = await FrameFaultProxy(("127.0.0.1", hub.port), *((fault, direction, at) if link == "source" else ())
                                  ).start()
    p_con = await FrameFaultProxy(("127.0.0.1", hub.port), *((fault, direction, at) if link == "consumer" else ())
                                  ).start()
    p_src.on_restart = p_con.on_restart = restart
    cur["c"] = new_consumer().start()
    r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(tmp, "r-1"), hub_port=hub.port, poll_s=0.05,
                  on_message=lambda m, body: told.update([("r-1", body)]), **PEER).start()
    cur["src"] = new_source().start()

    def state():
        src, c, hub = cur["src"], cur["c"], cur["hub"]
        o = src.streams["rec"].out
        hs = hub.store.get("source-1", "rec", 1)
        return ("source: acked %d final %s end_sent %s result %s rounds %d sums %s repairs %s | hub: committed %d "
                "verified %s | consumer: %s | told %s | outbox %s" % (
                    o.acked, o.final_size, o.end_sent, o.result, o.repair_rounds, o.sums is not None, o.repairs,
                    hs.committed, hs.verified, c.streams(), dict(told), src.outbox()))

    async def wait(what, cond):
        try:
            await _until(cond)
        except AssertionError:
            raise AssertionError("waiting for %s: %s" % (what, state())) from None

    try:
        task = cur["src"].send_message(["c-1", "r-1"], "task", b"check position 7")
        r.send_message(["source-1"], "reply", b"understood")
        cur["c"].send_message(["source-1"], "reply", b"on my way")

        def hub_has_all():
            return cur["hub"].store.get("source-1", "rec", 1).committed == len(DATA)
        await wait("the hub to hold the file", hub_has_all)
        data = bytearray(DATA)
        data[2:6] = HEADER
        _patch_file(path, 2, HEADER)                    # changed in place after sending: block repair
        cur["completed"] = True
        cur["src"].complete("rec")

        def consumer_done():
            return any(s["stream"] == "source-1/rec" and s["verified"] == "ok" for s in cur["c"].streams())
        await wait("the final checks", lambda: cur["src"].streams["rec"].out.result == "ok" and consumer_done())
        await wait("the messages", lambda: len(told) == 4)
        await wait("the delivery report", lambda: (cur["src"].outbox()[0]["report"] or {}).get("pending") == [])
        await asyncio.sleep(0.2)                        # late duplicates would show up now

        src, c, hub = cur["src"], cur["c"], cur["hub"]
        assert src.streams["rec"].gen == 1
        for rs in (hub.store.get("source-1", "rec", 1), c.received("source-1/rec")):
            with open(rs.path, "rb") as f:
                assert f.read() == bytes(data)
        assert told == Counter({("c-1", b"check position 7"): 1, ("r-1", b"check position 7"): 1,
                                ("source-1", b"understood"): 1, ("source-1", b"on my way"): 1})
        assert src.outbox()[0]["id"] == task and sorted(src.outbox()[0]["report"]["delivered"]) == ["c-1", "r-1"]
        assert reports[-1]["pending"] == [] and len(reports) <= 2
        return p_src, p_con
    finally:
        for p in (p_src, p_con):
            if p.restart_task is not None:
                await p.restart_task
        for p in (cur["src"], cur["c"], r):
            await p.stop()
        await cur["hub"].stop()
        for p in (p_src, p_con):
            await p.stop()


def _steps(tmp, link, only=None):
    """[(direction, (frame type, n))] of a clean run on ``link``; ``only``: frame type names to keep."""
    p_src, p_con = asyncio.run(scenario(os.path.join(tmp, "clean")))
    p = p_src if link == "source" else p_con
    out = []
    for direction in (UP, DOWN):
        for ftype, count in sorted(p.seen[direction].items()):
            if ftype == F.FrameType.HEARTBEAT:
                count = min(count, 2)
            if only is None or F.FrameType(ftype).name in only:
                out += [(direction, (ftype, n)) for n in range(count)]
    return out


def _run_all(tmp_path, link, faults, only=None):
    """Every fault in ``faults`` at every step of ``link``; returns what the faults hit."""
    tmp = str(tmp_path)
    steps = _steps(tmp, link, only)
    hit = Counter()
    n = 0
    for fault in faults:
        for direction, at in steps:
            if True:
                n += 1
                where = "%s %s %s #%d %s" % (link, direction, F.FrameType(at[0]).name, at[1], fault)
                t0 = time.monotonic()
                try:
                    p_src, p_con = asyncio.run(scenario(os.path.join(tmp, "run%d" % n), link, fault, direction, at))
                except Exception as e:
                    raise AssertionError("%s: %r" % (where, e)) from e
                p = p_src if link == "source" else p_con
                if p.fired is not None:
                    hit[(fault, direction, F.FrameType(p.fired).name)] += 1
                assert time.monotonic() - t0 < 30, where
    return hit


def test_unexpected_error_in_a_session_reconnects(tmp_path):
    """A bug in a session must not stop the participant for good (found by the duplicate STREAM_END fault: the
    connection loop ended silently and the source never connected again)."""
    async def run():
        hub = await Hub(os.path.join(str(tmp_path), "hub"), PARTICIPANTS, "op-1", port=0, **FAST).start()
        r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(str(tmp_path), "r-1"), hub_port=hub.port, **PEER)
        active = r._active
        calls = []

        async def broken_once(ch, welcome):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("simulated bug")
            return await active(ch, welcome)
        r._active = broken_once
        r.start()
        try:
            await _until(lambda: r.connected.is_set(), 10)
            assert len(calls) >= 2
        finally:
            await r.stop()
            await hub.stop()
    asyncio.run(run())


def test_stop_waits_for_writes_still_running(tmp_path):
    """A cancelled session's write goes on in its worker thread; stop() returns only when it is on disk, so that a
    new instance on the same folders starts from the final state."""
    done = []

    def slow_change(state):
        time.sleep(0.5)
        state["late"] = True
        done.append("written")

    async def run():
        hub = Hub(os.path.join(str(tmp_path), "hub"), PARTICIPANTS, "op-1")
        hub.messages.put({"id": "d" * 32, "from": "r-1", "to": ["c-1"], "t": 1, "kind": "note"}, b"x",
                         {"pending": ["c-1"], "delivered": [], "unknown": []})
        writing = asyncio.ensure_future(asyncio.to_thread(hub.messages.modify, "d" * 32, slow_change))
        await asyncio.sleep(0.1)
        writing.cancel()                                # the session is gone, the thread is not
        await hub.stop()
        assert done == ["written"]
        again = Hub(os.path.join(str(tmp_path), "hub"), PARTICIPANTS, "op-1")
        assert again.messages.get("d" * 32)["state"]["late"] is True

        c = Consumer("c-1", "op-1", "127.0.0.1", os.path.join(str(tmp_path), "c-1"), hub_port=1, **PEER)
        rs = c.store.get("hub", "x", 1)
        threading.Thread(target=lambda: (time.sleep(0.1), rs.write(0, b"y" * 1000))).start()
        await asyncio.sleep(0.05)
        with c.store.activity:                          # a write that is still running
            stopper = asyncio.ensure_future(c.stop())
            await asyncio.sleep(0.3)
            assert not stopper.done()
        await stopper
        assert c.store.get("hub", "x", 1).committed == 1000
    asyncio.run(run())


def test_clean_scenario_passes(tmp_path):
    p_src, p_con = asyncio.run(scenario(str(tmp_path)))
    for p in (p_src, p_con):
        assert all(sum(p.seen[d].values()) > 3 for d in (UP, DOWN))


@pytest.mark.parametrize("link", ["source", "consumer"])
def test_cut_at_every_frame(tmp_path, link):
    hit = _run_all(tmp_path, link, ["cut"] if not SOAK else ["cut", "cut_mid"])
    names = {k[2] for k in hit}
    expected = {"HELLO", "WELCOME", "SEGMENT", "ACK", "STREAM_END", "STREAM_CHECK", "BLOCK_SUMS", "MESSAGE",
                "MESSAGE_ACK"}
    if link == "source":
        expected |= {"DELIVERY", "DELIVERY_ACK"}
    assert expected <= names, expected - names


@pytest.mark.parametrize("link", ["source", "consumer"])
def test_restart_at_every_frame(tmp_path, link):
    """The participant on the link or the hub stops just before a frame and starts again on the same folders."""
    hit = _run_all(tmp_path, link, list(RESTARTS), None if SOAK else {"HELLO", "STREAM_END", "MESSAGE", "DELIVERY"})
    assert {k[0] for k in hit} == set(RESTARTS)


@pytest.mark.parametrize("link", ["source", "consumer"])
def test_cut_mid_frame_duplicate_and_damage(tmp_path, link):
    faults = [f for f in FAULTS if f != "cut"]
    hit = _run_all(tmp_path, link, faults, None if SOAK else {"HELLO", "STREAM_END", "BLOCK_SUMS", "MESSAGE"})
    assert {k[0] for k in hit} == set(faults)
