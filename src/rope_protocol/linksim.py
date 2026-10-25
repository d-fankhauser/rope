"""Link simulator: a TCP proxy that behaves like an unreliable radio link (spec section 13, test bench).

Put it between two participants and it adds, per direction:

- **bandwidth limit** (bytes per second),
- **latency** and jitter (one way),
- **blackouts**: no data flows, but the TCP connections stay open - like shadowing or antenna movement on a
  directional radio link. Data in flight waits; the sender is slowed down by TCP back-pressure, nothing is lost.
- **disconnects**: all connections through the link are aborted, followed by an outage (new connections wait until
  the link is back).

Faults come either from a random schedule (``Profile``, reproducible with a seed) or from an exact script for tests.
All profile values are **assumed** until real radio links have been measured (spec section 9 and open points).

Command line::

    python -m rope_protocol.linksim --listen 127.0.0.1:47311 --target 127.0.0.1:47310 --profile radio

Standard library only. Nothing in here knows ROPE frames: it moves bytes.
"""

import argparse
import asyncio
import random
import sys
import time
from dataclasses import dataclass, field

CHUNK = 16 * 1024
QUEUE_CHUNKS = 64          # bounded buffer per direction: back-pressure instead of unbounded memory


@dataclass(frozen=True)
class Profile:
    name: str
    rate: float = None                  # bytes per second per direction; None = unlimited
    latency: float = 0.0                # one way, seconds
    jitter: float = 0.0                 # added uniformly 0..jitter, order is kept
    blackout_every: float = None        # mean seconds between blackouts (exponential); None = never
    blackout_len: tuple = (0.0, 0.0)    # min, max seconds
    disconnect_every: float = None      # mean seconds between disconnects; None = never
    outage_len: tuple = (0.0, 0.0)      # min, max seconds the link stays down after a disconnect
    description: str = ""


#: Starting profiles. All values are assumptions, to be replaced by measurements on real links.
PROFILES = {
    "lan": Profile("lan", description="no faults, no limit (reference)"),
    "wifi": Profile("wifi", rate=2_500_000, latency=0.005, jitter=0.01, blackout_every=120, blackout_len=(0.2, 2),
                    description="usable Wi-Fi with short dropouts"),
    "radio": Profile("radio", rate=500_000, latency=0.01, jitter=0.02, blackout_every=60, blackout_len=(0.5, 20),
                     disconnect_every=900, outage_len=(5, 60),
                     description="directional radio, ~4 Mbit/s, dropouts up to 20 s, rare disconnects"),
    "radio-bad": Profile("radio-bad", rate=125_000, latency=0.03, jitter=0.05, blackout_every=20, blackout_len=(1, 60),
                         disconnect_every=180, outage_len=(10, 300),
                         description="poor radio, ~1 Mbit/s, dropouts up to 60 s, disconnects every few minutes"),
}


@dataclass
class Stats:
    bytes_up: int = 0                   # client -> target
    bytes_down: int = 0                 # target -> client
    connections: int = 0
    blackouts: int = 0
    disconnects: int = 0
    down_seconds: float = 0.0
    events: list = field(default_factory=list)   # (seconds since start, kind, length)


class Link:
    """State of the simulated link, shared by all connections through one proxy (one radio link)."""

    def __init__(self, profile, seed=None, script=None, on_event=None):
        self.profile = profile
        self.rng = random.Random(seed)
        self.script = sorted(script or [])       # [(at_s, "blackout" | "disconnect", length_s)]
        self.on_event = on_event
        self.stats = Stats()
        self.up = asyncio.Event()
        self.up.set()
        self.connections = set()                 # transports to abort on disconnect
        self.t0 = time.monotonic()

    def _log(self, kind, length):
        t = time.monotonic() - self.t0
        self.stats.events.append((round(t, 3), kind, round(length, 3)))
        if self.on_event:
            self.on_event(t, kind, length)

    async def _down(self, length):
        self.up.clear()
        await asyncio.sleep(length)
        self.stats.down_seconds += length
        self.up.set()

    async def blackout(self, length):
        self.stats.blackouts += 1
        self._log("blackout", length)
        await self._down(length)

    async def disconnect(self, length):
        self.stats.disconnects += 1
        self._log("disconnect", length)
        for tr in list(self.connections):
            tr.abort()
        self.connections.clear()
        await self._down(length)

    async def run(self):
        """Drive the faults until cancelled."""
        if self.script:
            for at, kind, length in self.script:
                await asyncio.sleep(max(0.0, at - (time.monotonic() - self.t0)))
                await (self.blackout if kind == "blackout" else self.disconnect)(length)
            return
        p = self.profile
        if not p.blackout_every and not p.disconnect_every:
            return
        while True:
            waits = []
            if p.blackout_every:
                waits.append((self.rng.expovariate(1 / p.blackout_every), "blackout"))
            if p.disconnect_every:
                waits.append((self.rng.expovariate(1 / p.disconnect_every), "disconnect"))
            wait, kind = min(waits)
            await asyncio.sleep(wait)
            if kind == "blackout":
                await self.blackout(self.rng.uniform(*p.blackout_len))
            else:
                await self.disconnect(self.rng.uniform(*p.outage_len))


async def _pump(reader, writer, link, counter):
    """One direction: read -> bounded queue -> (latency, link up, rate) -> write."""
    p = link.profile
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue(QUEUE_CHUNKS)

    async def receive():
        try:
            while True:
                data = await reader.read(CHUNK)
                if not data:
                    break
                due = loop.time() + p.latency + (link.rng.uniform(0, p.jitter) if p.jitter else 0.0)
                await queue.put((due, data))
        except (ConnectionError, OSError):
            pass
        await queue.put(None)

    async def deliver():
        free_at = loop.time()
        last_due = 0.0
        try:
            while True:
                item = await queue.get()
                if item is None:
                    if writer.can_write_eof():
                        writer.write_eof()
                    return
                due, data = item
                due = max(due, last_due)               # jitter never reorders bytes
                last_due = due
                await link.up.wait()
                if due > loop.time():
                    await asyncio.sleep(due - loop.time())
                if p.rate:
                    free_at = max(free_at, loop.time()) + len(data) / p.rate
                    await asyncio.sleep(max(0.0, free_at - loop.time()))
                await link.up.wait()                   # a blackout may have started meanwhile
                writer.write(data)
                await writer.drain()
                setattr(link.stats, counter, getattr(link.stats, counter) + len(data))
        except (ConnectionError, OSError):
            pass

    rx = asyncio.ensure_future(receive())
    try:
        await deliver()
    finally:
        rx.cancel()


class LinkProxy:
    """TCP proxy listening on ``listen`` and forwarding to ``target`` through a simulated ``Link``."""

    def __init__(self, listen, target, profile="lan", seed=None, script=None, on_event=None):
        self.listen = listen
        self.target = target
        prof = PROFILES[profile] if isinstance(profile, str) else profile
        self.link = Link(prof, seed, script, on_event)
        self._server = None
        self._faults = None
        self._tasks = set()

    @property
    def stats(self):
        return self.link.stats

    @property
    def port(self):
        return self._server.sockets[0].getsockname()[1]

    async def start(self):
        self._server = await asyncio.start_server(self._handle, *self.listen)
        self._faults = asyncio.ensure_future(self.link.run())
        return self

    async def stop(self):
        self._faults.cancel()
        self._server.close()
        for tr in list(self.link.connections):
            tr.abort()
        for t in list(self._tasks):
            t.cancel()
        await self._server.wait_closed()

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    async def _handle(self, c_reader, c_writer):
        task = asyncio.current_task()
        self._tasks.add(task)
        link = self.link
        t_writer = None
        try:
            await link.up.wait()                       # while the link is down, new connections wait
            try:
                t_reader, t_writer = await asyncio.open_connection(*self.target)
            except OSError:
                c_writer.transport.abort()
                return
            link.stats.connections += 1
            link.connections.update((c_writer.transport, t_writer.transport))
            await asyncio.gather(_pump(c_reader, t_writer, link, "bytes_up"),
                                 _pump(t_reader, c_writer, link, "bytes_down"))
        except asyncio.CancelledError:
            pass
        finally:
            for w in (c_writer, t_writer):
                if w is not None:
                    link.connections.discard(w.transport)
                    w.transport.abort()
            self._tasks.discard(task)


def _address(text):
    host, _, port = text.rpartition(":")
    return host or "127.0.0.1", int(port)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m rope_protocol.linksim", description=__doc__.split("\n\n")[0])
    ap.add_argument("--listen", required=True, type=_address, help="host:port to listen on")
    ap.add_argument("--target", required=True, type=_address, help="host:port to forward to")
    ap.add_argument("--profile", default="radio", choices=sorted(PROFILES))
    ap.add_argument("--seed", type=int, help="seed for a reproducible fault schedule")
    ap.add_argument("--duration", type=float, help="stop after this many seconds")
    args = ap.parse_args(argv)

    def show(t, kind, length):
        print("%8.1f s  %-10s %.1f s" % (t, kind, length), flush=True)

    async def run():
        proxy = LinkProxy(args.listen, args.target, args.profile, args.seed, on_event=show)
        await proxy.start()
        prof = proxy.link.profile
        print("link %s (%s): %s -> %s" % (prof.name, prof.description, args.listen, args.target), flush=True)
        try:
            await asyncio.sleep(args.duration if args.duration else 10 ** 9)
        finally:
            await proxy.stop()
            s = proxy.stats
            print("up %d B, down %d B, connections %d, blackouts %d, disconnects %d, down %.1f s" % (
                s.bytes_up, s.bytes_down, s.connections, s.blackouts, s.disconnects, s.down_seconds))

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
