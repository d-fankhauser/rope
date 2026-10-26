"""Helpers for asyncio tasks: wake-ups and bounded cleanup must never make a participant hang."""

import asyncio
import time

from rope_protocol.channel import Wakeup, reap


def test_wakeup_timeout_set_and_cancel():
    async def run():
        w = Wakeup()
        t = time.monotonic()
        await w.wait(0.05)                              # timeout
        assert time.monotonic() - t >= 0.04
        w.set()
        await w.wait(10)                                # already set: returns at once
        w.clear()
        waiter = asyncio.ensure_future(w.wait(10))
        await asyncio.sleep(0)
        w.set()                                         # wakes the waiter
        await asyncio.wait_for(waiter, 1)
        w.clear()
        waiter = asyncio.ensure_future(w.wait(10))
        await asyncio.sleep(0)
        waiter.cancel()                                 # a cancellation always ends the wait
        await asyncio.wait([waiter], timeout=1)
        assert waiter.cancelled()
    asyncio.run(run())


def test_reap_is_bounded_and_collects_failures():
    async def stubborn():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:                  # ignores the first cancellation
            pass
        await asyncio.sleep(10)

    async def failing():
        raise ConnectionError("late failure")

    async def run():
        s = asyncio.ensure_future(stubborn())
        f = asyncio.ensure_future(failing())
        await asyncio.sleep(0)
        t = time.monotonic()
        await reap([s, f, None], timeout_s=0.2)
        assert time.monotonic() - t < 1.0
        assert f.done() and not s.done()
        s.cancel()
        await asyncio.wait([s], timeout=1)
        assert s.cancelled()
    asyncio.run(run())
