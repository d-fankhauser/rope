"""Source -> hub -> consumer(s): relayed streams arrive bit for bit, live while they grow, resume after disconnects
and restarts, and are repaired when the source changed bytes in place (spec 7, 8, 14.7)."""

import asyncio
import hashlib
import os

from rope_protocol.consumer import Consumer
from rope_protocol.hub import Hub
from rope_protocol.linksim import LinkProxy, Profile
from test_transfer import FAST, SRC, Bench, _file, _patch_file, _until

HARSH = Profile("harsh", rate=1_000_000, latency=0.01, jitter=0.02, blackout_every=0.4, blackout_len=(0.05, 0.9),
                disconnect_every=1.0, outage_len=(0.05, 0.3), description="test only")
PARTICIPANTS = {"source-1": "source", "source-2": "source", "c-1": "consumer", "c-2": "consumer"}
BLOCK = 64 * 1024


class RelayBench(Bench):
    """Bench plus consumers, each behind its own simulated link to the hub."""

    def __init__(self, tmp, profile="lan", consumer_profile="lan", **kw):
        super().__init__(tmp, profile, participants=PARTICIPANTS, block_size=BLOCK, **kw)
        self.consumer_profile = consumer_profile
        self.consumer_proxies = []
        self.consumers = []

    async def consumer(self, participant="c-1", **kw):
        proxy = await LinkProxy(("127.0.0.1", 0), ("127.0.0.1", self.hub.port), self.consumer_profile).start()
        self.consumer_proxies.append(proxy)
        c = Consumer(participant, "op-1", "127.0.0.1", os.path.join(self.tmp, "consumer-" + participant),
                     hub_port=proxy.port, timeout_s=SRC["timeout_s"], backoff=SRC["backoff"], block_size=BLOCK, **kw)
        self.consumers.append(c)
        return c.start()

    async def __aexit__(self, *exc):
        for c in self.consumers:
            await c.stop()
        for p in self.consumer_proxies:
            await p.stop()
        await super().__aexit__(*exc)


def _content(consumer, stream, gen=1):
    rs = consumer.received(stream, gen)
    with open(rs.path, "rb") as f:
        return f.read(rs.committed)


def _verified(consumer, stream, gen=1):
    return any(s["stream"] == stream and s["gen"] == gen and s["verified"] == "ok" for s in consumer.streams())


def test_relay_complete_file_bit_for_bit(tmp_path):
    data = os.urandom(800_000)
    path = _file(tmp_path, "rec.dat", data)
    checked = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer(on_checked=lambda *a: checked.append(a))
            src = b.source({"rec": path}).start()
            src.complete("rec")
            assert await src.wait_checked("rec", 20) == "ok"
            await _until(lambda: _verified(c, "source-1/rec"))
            await src.stop()
            assert _content(c, "source-1/rec") == data
            assert checked and checked[-1][:3] == ("source-1/rec", 1, "ok")
    asyncio.run(run())


def test_consumer_follows_a_growing_file_live(tmp_path):
    path = _file(tmp_path, "live.dat", b"")
    written = []

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer(on_written=lambda *a: written.append(a))
            src = b.source({"live": path}).start()
            parts = [os.urandom(40_000) for _ in range(6)]
            for i, p in enumerate(parts):
                with open(path, "ab") as f:
                    f.write(p)
                total = sum(len(x) for x in parts[:i + 1])
                await _until(lambda: c.received("source-1/live").committed == total)   # before the file is complete
            src.complete("live")
            await _until(lambda: _verified(c, "source-1/live"))
            await src.stop()
            assert _content(c, "source-1/live") == b"".join(parts)
            assert len({w[2] for w in written}) >= 6                                 # grew step by step
    asyncio.run(run())


def test_late_consumer_and_consumer_restart_resume(tmp_path):
    data = os.urandom(1_200_000)
    path = _file(tmp_path, "r.dat", data)

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({"r": path}).start()
            src.complete("r")
            assert await src.wait_checked("r", 20) == "ok"          # the hub has everything, no consumer yet
            c = await b.consumer()
            await _until(lambda: c.received("source-1/r").committed > 400_000)
            await c.stop()                                          # consumer goes away mid-transfer
            down_before = sum(p.stats.bytes_down for p in b.consumer_proxies)
            c2 = await b.consumer()                                 # same participant, same folder
            await _until(lambda: _verified(c2, "source-1/r"))
            await src.stop()
            assert _content(c2, "source-1/r") == data
            down_after = sum(p.stats.bytes_down for p in b.consumer_proxies) - down_before
            assert down_after < len(data)                           # resumed, not from zero
    asyncio.run(run())


def test_two_sources_two_consumers(tmp_path):
    a, z = os.urandom(300_000), os.urandom(200_000)
    pa, pz = _file(tmp_path, "a.dat", a), _file(tmp_path, "z.dat", z)

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c1, c2 = await b.consumer("c-1"), await b.consumer("c-2")
            s1 = b.source({"rec": pa}, participant="source-1").start()
            s2 = b.source({"rec": pz}, participant="source-2").start()   # same stream name, other source
            s1.complete("rec")
            s2.complete("rec")
            for c in (c1, c2):
                await _until(lambda: _verified(c, "source-1/rec") and _verified(c, "source-2/rec"))
                assert _content(c, "source-1/rec") == a
                assert _content(c, "source-2/rec") == z
            await s1.stop()
            await s2.stop()
    asyncio.run(run())


def test_header_change_is_repaired_at_hub_and_consumer(tmp_path):
    """The source changes its header after everything was relayed: hub and consumer end up with the new bytes."""
    data = bytearray(os.urandom(600_000))
    path = _file(tmp_path, "h.dat", bytes(data))

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            c = await b.consumer()
            src = b.source({"h": path}).start()
            await _until(lambda: c.received("source-1/h").committed == len(data))
            data[0:4] = b"\x01\x02\x03\x04"
            _patch_file(path, 0, data[0:4])
            src.complete("h")
            assert await src.wait_checked("h", 20) == "ok"
            await _until(lambda: _verified(c, "source-1/h"))
            await src.stop()
            assert _content(c, "source-1/h") == bytes(data)
    asyncio.run(run())


def test_relay_over_harsh_links_and_hub_restart(tmp_path, seed=11):
    data = os.urandom(700_000)
    path = _file(tmp_path, "x.dat", data)

    async def run():
        async with RelayBench(str(tmp_path), HARSH, consumer_profile=HARSH) as b:
            b.proxy.link.rng.seed(seed)
            c = await b.consumer()
            b.consumer_proxies[0].link.rng.seed(seed + 1000)
            src = b.source({"x": path}).start()
            await _until(lambda: c.received("source-1/x").committed > 200_000, 60)
            await b.hub.stop()                                      # hub restart in the middle
            await asyncio.sleep(0.3)
            b.hub = await Hub(os.path.join(b.tmp, "hub"), PARTICIPANTS, "op-1", port=b.hub.port, block_size=BLOCK,
                              **FAST).start()
            src.complete("x")
            assert await src.wait_checked("x", 60) == "ok"
            await _until(lambda: _verified(c, "source-1/x"), 60)
            await src.stop()
            assert hashlib.sha256(_content(c, "source-1/x")).digest() == hashlib.sha256(data).digest()
    asyncio.run(run())


def test_role_not_served_yet_is_rejected(tmp_path):
    class Recipient(Consumer):
        role = "recipient"

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            b.hub.participants["r-1"] = "recipient"
            r = Recipient("r-1", "op-1", "127.0.0.1", os.path.join(b.tmp, "r"), hub_port=b.hub.port,
                          timeout_s=SRC["timeout_s"], backoff=SRC["backoff"]).start()
            await _until(lambda: r.rejected is not None, 5)
            assert r.rejected.code == "role" and "recipient" in r.rejected.text
            await r.stop()
    asyncio.run(run())


def test_stream_name_too_long_to_relay_is_refused(tmp_path):
    path = _file(tmp_path, "n.dat", b"data")

    async def run():
        async with RelayBench(str(tmp_path)) as b:
            src = b.source({"n" * 250: path}).start()           # 250 + len("source-1/") > 255 bytes
            await _until(lambda: src.sessions >= 2, 10)         # refused, reconnects, refused again
            await src.stop()
            assert b.hub.store.have("source-1") == []
    asyncio.run(run())
