"""Participant list (spec 7): names, roles, and which streams each consumer receives - over the network and on
offline media; every participant learns the names and keeps them across restarts."""

import asyncio
import os

import pytest

from rope_protocol import frames as F
from rope_protocol import media, roster
from rope_protocol.consumer import Consumer
from rope_protocol.hub import Hub
from rope_protocol.recipient import Recipient
from rope_protocol.source import Source

from test_relay import BLOCK, RelayBench, _content, _verified
from test_transfer import FAST, _file, _until

ROSTER = {
    "source-1": {"role": "source", "name": "Survey boat"},
    "source-2": {"role": "source", "name": "Second boat"},
    "c-1": {"role": "consumer", "name": "Analysis laptop", "streams": ["source-1/*"]},
    "c-2": {"role": "consumer", "name": "Tablet", "streams": ["*/rec-b*"]},
    "c-3": "consumer",
    "r-1": {"role": "recipient", "name": "Team lead"},
}


@pytest.fixture(autouse=True)
def _small_segments(monkeypatch):
    monkeypatch.setattr(media, "SEGMENT_BYTES", 256 * 1024)


def _names(participant):
    return {e["participant"]: (e["name"], e["role"]) for e in participant.participant_list()}


def test_participant_list_is_checked():
    ok = roster.parse(ROSTER, "hub", "Command post")
    assert ok["c-3"] == roster.Entry("consumer", "c-3", None)
    assert ok["c-1"].streams == ("source-1/*",)
    bad = [
        ({"bad id!": "source"}, "identifiers"),
        ({"hub": "source"}, "identifier of the hub"),
        ({"s": "listener"}, "role must be one of"),
        ({"s": {"role": "source", "streams": ["*"]}}, "only a consumer receives streams"),
        ({"c": {"role": "consumer", "streams": "source-1/*"}}, "list of patterns"),
        ({"c": {"role": "consumer", "receives": ["*"]}}, "unknown keys receives"),
        ({"c": {"role": "consumer", "name": ""}}, "name must be"),
        ({"c": {"role": "consumer", "name": "x" * 101}}, "name must be"),
        ({"c": {"role": "consumer", "name": "two\nlines"}}, "name must be"),
    ]
    for participants, reason in bad:
        with pytest.raises(ValueError, match=reason):
            Hub("unused", participants, "op-1")
    with pytest.raises(ValueError, match="hub identifier"):
        roster.parse({}, "the hub")


def test_selection_patterns():
    e = roster.parse(ROSTER, "hub")
    assert roster.receives(e["c-1"], "source-1/rec-a.dat") and roster.receives(e["c-1"], "source-1/sub/x")
    assert not roster.receives(e["c-1"], "source-2/rec-a.dat")
    assert not roster.receives(e["c-1"], "Source-1/rec-a.dat")                  # case matters on every system
    assert roster.receives(e["c-2"], "source-2/rec-b.dat") and not roster.receives(e["c-2"], "source-2/rec-a.dat")
    assert roster.receives(e["c-3"], "anything/at-all")
    assert not roster.receives(e["source-1"], "source-1/rec-a.dat")
    assert not roster.receives(e["r-1"], "source-1/rec-a.dat")
    assert not roster.receives(None, "source-1/rec-a.dat")
    nothing = roster.parse({"c": {"role": "consumer", "streams": []}}, "hub")["c"]
    assert not roster.receives(nothing, "source-1/rec-a.dat")


def test_welcome_participants_are_validated():
    meta = dict(version=1, hub="hub", t=1, t_hello=1, heartbeat_s=2, timeout_s=10, have=[])
    F.validate(F.Frame(F.FrameType.WELCOME, meta))                                    # optional key
    F.validate(F.Frame(F.FrameType.WELCOME, dict(meta, participants=[{"participant": "c-1", "name": "Tablet",
                                                                      "role": "consumer"}])))
    for value in ("c-1", [{"participant": "bad id", "name": "x", "role": "consumer"}], [{"participant": "c-1"}]):
        with pytest.raises(F.FrameError):
            F.validate(F.Frame(F.FrameType.WELCOME, dict(meta, participants=value)))


def test_each_consumer_receives_its_selection_over_the_network(tmp_path):
    """Two sources, three consumers with different selections: each gets exactly its streams, bit for bit; every
    participant learns the names and roles, and still knows them after a restart without network."""
    tmp = str(tmp_path)
    data = {k: os.urandom(300_000 + 1000 * i) for i, k in enumerate(("1a", "1b", "2a", "2b"))}
    files = {k: _file(tmp, "%s.dat" % k, v) for k, v in data.items()}
    want = {
        "c-1": {"source-1/rec-a", "source-1/rec-b"},
        "c-2": {"source-1/rec-b", "source-2/rec-b"},
        "c-3": {"source-1/rec-a", "source-1/rec-b", "source-2/rec-a", "source-2/rec-b"},
    }

    async def run():
        b = RelayBench(tmp, hub_name="Command post")
        b.participants = ROSTER
        async with b:
            cs = {p: await b.consumer(p) for p in want}
            r = await b.recipient("r-1")
            s1 = b.source({"rec-a": files["1a"], "rec-b": files["1b"]}).start()
            s2 = b.source({"rec-a": files["2a"], "rec-b": files["2b"]}, participant="source-2").start()
            for s in (s1, s2):
                s.complete("rec-a")
                s.complete("rec-b")
            for p, streams in want.items():
                for name in streams:
                    await _until(lambda: _verified(cs[p], name))
            await asyncio.sleep(0.3)                                 # nothing else may arrive meanwhile
            for p, streams in want.items():
                got = {s["stream"] for s in cs[p].streams()}
                assert got == streams, p
                for name in streams:
                    src, rec = name.split("/")
                    assert _content(cs[p], name) == data[src[-1] + rec[-1]]
            await _until(lambda: _names(r) and _names(s1))
            for who in list(cs.values()) + [r, s1, s2]:
                names = _names(who)
                assert names["hub"] == ("Command post", "hub")
                assert names["c-1"] == ("Analysis laptop", "consumer") and names["c-3"] == ("c-3", "consumer")
                assert names["source-2"] == ("Second boat", "source")
                assert "streams" not in str(who.participant_list())     # the selection stays at the hub
            for s in (s1, s2):
                await s.stop()
        again = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(tmp, "recipient-r-1"), hub_port=1)
        assert _names(again)["r-1"] == ("Team lead", "recipient")
    asyncio.run(run())


def test_changed_selection_after_a_hub_restart(tmp_path):
    """A wider selection brings the missing streams; a narrower one stops new data, and what the consumer holds
    stays untouched."""
    tmp = str(tmp_path)
    a, b2 = os.urandom(400_000), os.urandom(400_000)
    pa, pb = _file(tmp, "a.dat", a), _file(tmp, "b.dat", b2)

    def roster_with(streams):
        return {"source-1": "source", "c-1": {"role": "consumer", "streams": streams}}

    async def run():
        b = RelayBench(tmp)
        b.participants = roster_with(["source-1/a"])
        async with b:
            c = await b.consumer("c-1")
            src = b.source({"a": pa, "b": pb}).start()
            src.complete("a")
            src.complete("b")
            await _until(lambda: _verified(c, "source-1/a"))
            assert await src.wait_checked("b", 20) == "ok"
            assert {s["stream"] for s in c.streams()} == {"source-1/a"}

            for streams, expect in ((["source-1/*"], {"source-1/a", "source-1/b"}), (["source-1/b"], None)):
                port = b.hub.port
                await b.hub.stop()
                b.hub = await Hub(os.path.join(tmp, "hub"), roster_with(streams), "op-1", port=port,
                                  block_size=BLOCK, **FAST).start()
                if expect:
                    await _until(lambda: _verified(c, "source-1/b"))
                    assert {s["stream"] for s in c.streams()} == expect
                else:
                    await _until(lambda: c.sessions >= 3)
                    await asyncio.sleep(0.3)
            assert _content(c, "source-1/a") == a and _content(c, "source-1/b") == b2
            assert _verified(c, "source-1/a")
            await src.stop()
    asyncio.run(run())


def test_selection_and_names_on_offline_media(tmp_path):
    """The hub's bundle for a consumer carries only its selection and the participant list; an older bundle does not
    replace a newer list."""
    tmp = str(tmp_path)
    stick = os.path.join(tmp, "stick")
    d1, d2 = os.urandom(300_000), os.urandom(300_000)

    async def run():
        src1 = Source("source-1", "op-1", "127.0.0.1", {"rec-a": _file(tmp, "1.dat", d1)},
                      os.path.join(tmp, "s1"), hub_port=1)
        src2 = Source("source-2", "op-1", "127.0.0.1", {"rec-a": _file(tmp, "2.dat", d2)},
                      os.path.join(tmp, "s2"), hub_port=1)
        for s in (src1, src2):
            s.complete("rec-a")
            await s.export_media(stick)
        hub = Hub(os.path.join(tmp, "hub"), ROSTER, "op-1", block_size=BLOCK, hub_name="Command post")
        assert all(x.ok for x in await hub.import_media(stick))

        out = await hub.export_media(stick, "c-1")
        assert out.stream_bytes == len(d1)                           # source-2 is not in its selection
        c = Consumer("c-1", "op-1", "127.0.0.1", os.path.join(tmp, "c-1"), hub_port=1, block_size=BLOCK)
        [rep] = await c.import_media(stick)
        assert rep.ok and {s["stream"] for s in c.streams()} == {"source-1/rec-a"}
        assert _content(c, "source-1/rec-a") == d1 and _verified(c, "source-1/rec-a")
        assert _names(c)["c-1"] == ("Analysis laptop", "consumer")

        # the hub renames the tablet; the old bundle of the stick, imported again, keeps the new name
        hub2 = Hub(os.path.join(tmp, "hub"), dict(ROSTER, **{"c-1": {"role": "consumer", "name": "Laptop 2",
                                                                       "streams": ["source-1/*"]}}),
                   "op-1", block_size=BLOCK)
        await asyncio.sleep(0.01)
        newer = os.path.join(tmp, "stick2")
        await hub2.export_media(newer, "c-1")
        await c.import_media(newer)
        assert _names(c)["c-1"] == ("Laptop 2", "consumer")
        await c.import_media(stick)
        assert _names(c)["c-1"] == ("Laptop 2", "consumer")

        # the source learns the list from its receipt bundle, too
        await hub.export_media(stick, "source-1")
        await src1.import_media(stick)
        assert _names(src1)["source-1"] == ("Survey boat", "source")
    asyncio.run(run())


def test_rejection_names_the_participant(tmp_path):
    async def run():
        b = RelayBench(str(tmp_path))
        b.participants = ROSTER
        async with b:
            r = Recipient("c-1", "op-1", "127.0.0.1", os.path.join(str(tmp_path), "wrong"), hub_port=b.hub.port,
                          timeout_s=0.6, backoff=(0.05, 0.2)).start()
            await _until(lambda: r.rejected is not None)
            assert r.rejected.code == "role"
            assert "Analysis laptop" in r.rejected.text and "listed as consumer" in r.rejected.text
            await r.stop()
    asyncio.run(run())
