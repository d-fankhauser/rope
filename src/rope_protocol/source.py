"""Source participant: sends growing files to the hub and resumes from the committed offset (spec 8.2, 9, 10).

The files are only read: opened briefly, read, closed - no locks, so the recording program is never disturbed.
Sending, final check and block repair: ``rope_protocol.sender``.
"""

import asyncio
import json
import logging
import os

from . import DEFAULT_PORT
from .participant import Participant, Rejected
from .sender import OutStream, StreamSender
from .store import write_json_atomic

__all__ = ["Source", "SourceStream", "Rejected"]

log = logging.getLogger("rope.source")


def _size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return None


class SourceStream:
    """One file at the source. ``out`` is the generation being sent. A new generation starts when the file became
    shorter, or when the final check finds most of its blocks changed (spec 14.4, 14.7). Bytes changed in place (e.g.
    a header updated at the end of a recording) are repaired block by block instead."""

    def __init__(self, name, path, saved=None):
        self.name = name
        self.path = path
        saved = saved or {}
        self.seen = saved.get("seen", 0)
        self.complete_requested = False
        self.out = OutStream(name, saved.get("gen", 1), path)

    @property
    def gen(self):
        return self.out.gen

    def saved(self):
        return dict(gen=self.gen, seen=self.seen)

    def new_generation(self, why):
        log.warning("%s %s: new generation %d", self.name, why, self.gen + 1)
        self.out = OutStream(self.name, self.gen + 1, self.path)
        self.seen = 0
        self.complete_requested = False

    def apply(self, size):
        """Apply the file size seen now. Returns True if the persistent state changed."""
        if size is None:
            return False
        changed = False
        if size < self.seen:
            self.new_generation("became shorter")
            changed = True
        if size > self.seen:
            self.seen = size
            changed = True
        self.out.size = size
        if self.complete_requested and self.out.final_size is None:
            self.out.final_size = max(size, self.out.sent)      # never less than what the hub may already hold
        return changed


class Source(Participant):
    """``streams``: {stream name: file path}. Runs until ``stop()``; reconnects forever with back-off 1-10 s."""

    role = "source"

    def __init__(self, participant, operation, hub_host, streams, state_dir, hub_port=DEFAULT_PORT,
                 segment_bytes=256 * 1024, window=8, poll_s=0.5, timeout_s=10.0, backoff=(1.0, 10.0),
                 zlib_level=1, on_message=None):
        os.makedirs(state_dir, exist_ok=True)
        super().__init__(participant, operation, hub_host, hub_port, timeout_s, backoff,
                         mailbox_root=os.path.join(state_dir, "messages"), on_message=on_message, poll_s=poll_s)
        self._state_path = os.path.join(state_dir, "source_state.json")
        saved = {}
        if os.path.exists(self._state_path):
            with open(self._state_path, encoding="utf-8") as f:
                saved = json.load(f)
        self.streams = {n: SourceStream(n, p, saved.get(n)) for n, p in streams.items()}
        self.sender = StreamSender(self._refresh, segment_bytes, window, poll_s, zlib_level,
                                   on_replaced=self._replaced, peer="hub")

    # --- application interface -------------------------------------------------------------------------------
    def complete(self, name):
        """The application declares the stream finished (e.g. file unchanged for a while): send the final check."""
        self.streams[name].complete_requested = True
        self.sender.wake.set()

    async def wait_checked(self, name, timeout=None):
        """Result of the final check of the stream's current generation ("ok" or "mismatch")."""
        loop = asyncio.get_running_loop()
        end = None if timeout is None else loop.time() + timeout
        while True:
            out = self.streams[name].out
            if out.checked.is_set():
                return out.result
            if end is not None and loop.time() > end:
                raise asyncio.TimeoutError("no final check for %s within %s s" % (name, timeout))
            await asyncio.sleep(0.05)

    # --- session ---------------------------------------------------------------------------------------------------
    def _stopping(self):
        self.sender.wake.set()

    async def _active(self, ch, welcome):
        have = {(h["stream"], h["gen"]): h["committed"] for h in welcome["have"]}
        await self._refresh()
        self.sender.start_session([s.out for s in self.streams.values()], have)

        async def handle(f):
            return self.sender.handle(f)
        return [self._serve(ch, handle), asyncio.ensure_future(self.sender.run(ch))]

    async def _refresh(self):
        changed = False
        for s in self.streams.values():
            changed |= s.apply(await asyncio.to_thread(_size, s.path))
        if changed:
            await self._save()
        return [s.out for s in self.streams.values()]

    async def _save(self):
        await asyncio.to_thread(write_json_atomic, self._state_path, {n: x.saved() for n, x in self.streams.items()})

    # --- offline media (spec 8.6) ------------------------------------------------------------------------------------
    async def _media_streams(self):
        """Every stream not yet verified at the hub, from what the hub acknowledged (over the network or in a bundle
        from the hub; 0 after a restart - the hub ignores what it already holds)."""
        await self._refresh()
        out = []
        for s in self.streams.values():
            o = s.out
            if o.result == "ok":
                continue
            limit = o.final_size if o.final_size is not None else o.size
            out.append((o.name, o.gen, o.path, min(o.acked, limit), limit, o.final_size))
        return out

    async def _media_imported(self, report):
        for stream, gen, committed in report.acks:      # what the hub holds: the next bundle starts there
            s = self.streams.get(stream)
            if s is not None and s.out.gen == gen:
                s.out.acked = max(s.out.acked, committed)
        if report.acks:
            self.sender.wake.set()

    async def _replaced(self, out):
        """Most blocks differ at the hub: the file was replaced, not edited - keep the hub's version, start over."""
        for s in self.streams.values():
            if s.out is out:
                s.new_generation("changed in most of its blocks")
                await self._save()
                return True
        return False
