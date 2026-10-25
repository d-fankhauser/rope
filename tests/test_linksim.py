"""Link simulator: data passes bit for bit; bandwidth, latency, blackouts and disconnects behave as configured."""

import asyncio
import hashlib
import os
import time

import pytest

from rope_protocol import linksim
from rope_protocol.linksim import LinkProxy, Profile

LOCAL = ("127.0.0.1", 0)


async def _echo_server():
    async def handle(r, w):
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            w.close()
    return await asyncio.start_server(handle, *LOCAL)


async def _sink_server(received):
    async def handle(r, w):
        h = hashlib.sha256()
        n = 0
        try:
            while data := await r.read(65536):
                h.update(data)
                n += len(data)
        except (ConnectionError, OSError):
            pass
        received.append((n, h.hexdigest(), time.monotonic()))
        w.close()
    return await asyncio.start_server(handle, *LOCAL)


def _port(server):
    return server.sockets[0].getsockname()[1]


async def _echo_through(proxy, payload):
    r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
    got = bytearray()

    async def read_all():
        while len(got) < len(payload):
            data = await r.read(65536)
            if not data:
                break
            got.extend(data)
    reader = asyncio.ensure_future(read_all())
    w.write(payload)
    await w.drain()
    await asyncio.wait_for(reader, 30)
    w.close()
    return bytes(got)


def test_lan_passes_bit_for_bit():
    async def run():
        server = await _echo_server()
        payload = os.urandom(2 * 1024 * 1024)
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), "lan") as proxy:
            assert await _echo_through(proxy, payload) == payload
            assert proxy.stats.bytes_up == proxy.stats.bytes_down == len(payload)
        server.close()
    asyncio.run(run())


def test_bandwidth_limit():
    async def run():
        received = []
        server = await _sink_server(received)
        payload = os.urandom(100_000)
        prof = Profile("slow", rate=200_000)
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), prof) as proxy:
            t0 = time.monotonic()
            _r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
            w.write(payload)
            await w.drain()
            w.write_eof()
            while not received:
                await asyncio.sleep(0.02)
            elapsed = received[0][2] - t0
        server.close()
        assert received[0][:2] == (len(payload), hashlib.sha256(payload).hexdigest())
        assert 0.4 <= elapsed < 3.0, elapsed           # 100 kB at 200 kB/s = 0.5 s
    asyncio.run(run())


def test_latency_each_way():
    async def run():
        server = await _echo_server()
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), Profile("far", latency=0.2)) as proxy:
            t0 = time.monotonic()
            assert await _echo_through(proxy, b"ping") == b"ping"
            rtt = time.monotonic() - t0
        server.close()
        assert 0.4 <= rtt < 2.0, rtt
    asyncio.run(run())


def test_blackout_delays_but_keeps_connection_and_data():
    async def run():
        server = await _echo_server()
        events = []
        script = [(0.1, "blackout", 0.6)]
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), "lan", script=script,
                             on_event=lambda t, k, n: events.append(k)) as proxy:
            await asyncio.sleep(0.2)                    # inside the blackout
            payload = os.urandom(300_000)
            t0 = time.monotonic()
            assert await _echo_through(proxy, payload) == payload
            waited = time.monotonic() - t0
            assert proxy.stats.blackouts == 1 and proxy.stats.disconnects == 0
            assert proxy.stats.connections == 1
        server.close()
        assert waited >= 0.4, waited
        assert events == ["blackout"]
    asyncio.run(run())


def test_disconnect_aborts_then_link_comes_back():
    async def run():
        server = await _echo_server()
        script = [(0.2, "disconnect", 0.4)]
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), "lan", script=script) as proxy:
            r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
            w.write(b"a")
            await w.drain()
            assert await r.read(1) == b"a"
            try:                                         # the open connection is cut
                data = await asyncio.wait_for(r.read(1), 2)
            except (ConnectionError, OSError):
                data = b""
            assert data == b""
            w.close()
            t_cut = time.monotonic()
            assert await _echo_through(proxy, b"again") == b"again"      # new connection waits for the link
            assert time.monotonic() - t_cut >= 0.25
            assert proxy.stats.disconnects == 1 and proxy.stats.connections == 2
        server.close()
    asyncio.run(run())


def test_unreachable_target_closes_client():
    async def run():
        server = await _echo_server()
        port = _port(server)
        server.close()
        await server.wait_closed()
        async with LinkProxy(LOCAL, ("127.0.0.1", port), "lan") as proxy:
            r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
            try:
                data = await asyncio.wait_for(r.read(1), 5)
            except (ConnectionError, OSError):
                data = b""
            assert data == b""
            w.close()
    asyncio.run(run())


def test_random_schedule_is_reproducible():
    """Profile-driven faults: they happen, and the same seed gives the same schedule."""
    fast = Profile("fast", blackout_every=0.05, blackout_len=(0.01, 0.03), disconnect_every=0.2, outage_len=(0.01, 0.02),
                   description="test only")

    async def run(seed):
        server = await _echo_server()
        async with LinkProxy(LOCAL, ("127.0.0.1", _port(server)), fast, seed=seed) as proxy:
            await asyncio.sleep(0.8)
            events = [(k, n) for _t, k, n in proxy.stats.events]
        server.close()
        return events

    a, b, c = asyncio.run(run(1)), asyncio.run(run(1)), asyncio.run(run(2))
    assert len(a) >= 5 and {k for k, _n in a} <= {"blackout", "disconnect"}
    n = min(len(a), len(b))
    assert a[:n - 1] == b[:n - 1]                      # last event may be cut by the end of the run
    assert a[:3] != c[:3]


@pytest.mark.parametrize("name", sorted(linksim.PROFILES))
def test_profiles_are_consistent(name):
    p = linksim.PROFILES[name]
    assert p.name == name and p.description
    assert p.blackout_len[0] <= p.blackout_len[1] and p.outage_len[0] <= p.outage_len[1]
    assert (p.blackout_every is None) == (p.blackout_len == (0.0, 0.0))
    assert (p.disconnect_every is None) == (p.outage_len == (0.0, 0.0))


def test_command_line(capsys):
    async def target():
        return await _echo_server()
    loop = asyncio.new_event_loop()
    server = loop.run_until_complete(target())
    port = _port(server)
    loop.run_until_complete(asyncio.sleep(0))
    # the proxy runs in its own event loop; the target only needs to exist
    assert linksim.main(["--listen", "127.0.0.1:0", "--target", "127.0.0.1:%d" % port, "--profile", "lan",
                         "--duration", "0.2"]) == 0
    out = capsys.readouterr().out
    assert "link lan" in out and "disconnects 0" in out
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
