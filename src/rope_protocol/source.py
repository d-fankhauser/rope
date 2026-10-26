"""Source participant: sends growing files to the hub and resumes from the committed offset (spec 8.2, 9, 10).

The files are only read: opened briefly, read, closed - no locks, so the recording program is never disturbed. The
file on disk is the buffer during an outage; in memory there are at most ``window`` unacknowledged segments.
"""

import asyncio
import hashlib
import json
import logging
import os
import random

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from .channel import Channel, Wakeup, now_ms, reap
from .store import write_json_atomic

log = logging.getLogger("rope.source")

HEAD_BYTES = 4096


class Rejected(Exception):
    """The hub refused this participant (REJECT); ``code`` and ``text`` say why, in plain language."""

    def __init__(self, code, text):
        super().__init__(text)
        self.code = code
        self.text = text


def _read(path, off, n):
    with open(path, "rb") as f:
        f.seek(off)
        return f.read(n)


def _size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _sha256_prefix(path, size):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        left = size
        while left > 0:
            chunk = f.read(min(left, 1024 * 1024))
            if not chunk:
                break
            h.update(chunk)
            left -= len(chunk)
    return h.hexdigest(), size - left


class SourceStream:
    """State of one stream at the source. ``gen`` changes when the file is not a continuation (spec 14.4)."""

    def __init__(self, name, path, saved=None):
        self.name = name
        self.path = path
        saved = saved or {}
        self.gen = saved.get("gen", 1)
        self.head_len = saved.get("head_len", 0)
        self.head_sha = saved.get("head_sha", hashlib.sha256(b"").hexdigest())
        self.seen = saved.get("seen", 0)
        self.size = 0
        self.acked = 0
        self.sent = 0
        self.final_size = None          # set by Source.complete()
        self.end_sent = False
        self.result = None              # "ok" / "mismatch" from STREAM_CHECK
        self.checked = asyncio.Event()

    def saved(self):
        return dict(gen=self.gen, head_len=self.head_len, head_sha=self.head_sha, seen=self.seen)

    def look(self):
        """Read-only look at the file, safe in a worker thread: (size, sha256 of the first head_len bytes, sha256 of
        the first HEAD_BYTES bytes) or None if the file does not exist (yet)."""
        size = _size(self.path)
        if size is None:
            return None
        head = _read(self.path, 0, max(self.head_len, min(size, HEAD_BYTES)))
        return size, hashlib.sha256(head[:self.head_len]).hexdigest(), hashlib.sha256(head[:HEAD_BYTES]).hexdigest(), \
            len(head[:HEAD_BYTES])

    def apply(self, seen):
        """Apply a ``look()`` result in the event loop. Returns True if the persistent state changed."""
        if seen is None:
            return False
        size, old_head_sha, new_head_sha, new_head_len = seen
        changed = False
        replaced = size < self.seen or (self.head_len and old_head_sha != self.head_sha)
        if replaced:
            log.warning("%s is not a continuation (shorter or beginning changed): new generation %d",
                        self.name, self.gen + 1)
            self.gen += 1
            self.head_len, self.head_sha, self.seen = 0, hashlib.sha256(b"").hexdigest(), 0
            self.acked = self.sent = 0
            self.final_size = None
            self.end_sent = False
            self.result = None
            changed = True
        if self.head_len < HEAD_BYTES and new_head_len > self.head_len:
            self.head_sha, self.head_len = new_head_sha, new_head_len
            changed = True
        if size > self.seen:
            self.seen = size
            changed = True
        self.size = size
        return changed


class Source:
    """``streams``: {stream name: file path}. Runs until ``stop()``; reconnects forever with back-off 1-10 s."""

    def __init__(self, participant, operation, hub_host, streams, state_dir, hub_port=DEFAULT_PORT,
                 segment_bytes=256 * 1024, window=8, poll_s=0.5, timeout_s=10.0, backoff=(1.0, 10.0),
                 zlib_level=1):
        self.participant = participant
        self.operation = operation
        self.hub = (hub_host, hub_port)
        self.segment_bytes = segment_bytes
        self.window = window
        self.poll_s = poll_s
        self.timeout_s = timeout_s
        self.backoff = backoff
        self.zlib_level = zlib_level
        os.makedirs(state_dir, exist_ok=True)
        self._state_path = os.path.join(state_dir, "source_state.json")
        saved = {}
        if os.path.exists(self._state_path):
            with open(self._state_path, encoding="utf-8") as f:
                saved = json.load(f)
        self.streams = {n: SourceStream(n, p, saved.get(n)) for n, p in streams.items()}
        self.connected = asyncio.Event()
        self.clock_offset_ms = None
        self.sessions = 0
        self.rejected = None
        self._stop = asyncio.Event()
        self._wake = Wakeup()
        self._task = None

    # --- application interface -------------------------------------------------------------------------------
    def complete(self, name):
        """The application declares the stream finished (e.g. file unchanged for a while): send the final check."""
        self._wake.set()
        self.streams[name].final_size = -1          # resolved to the size at the next look at the file

    async def wait_checked(self, name, timeout=None):
        await asyncio.wait_for(self.streams[name].checked.wait(), timeout)
        return self.streams[name].result

    def start(self):
        self._task = asyncio.ensure_future(self.run())
        return self

    async def stop(self):
        self._stop.set()
        self._wake.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    # --- connection loop ---------------------------------------------------------------------------------------
    async def run(self):
        delay = self.backoff[0]
        while not self._stop.is_set():
            try:
                await self._session()
                delay = self.backoff[0]
            except Rejected as e:
                self.rejected = e
                log.error("hub rejected %s: %s", self.participant, e.text)
                delay = self.backoff[1]
            except (ConnectionError, OSError, asyncio.TimeoutError, F.FrameError) as e:
                log.info("%s: connection lost (%s), retrying in %.1f s", self.participant, e, delay)
            finally:
                self.connected.clear()
            if self._stop.is_set():
                return
            await asyncio.sleep(delay * random.uniform(0.8, 1.0))
            delay = min(delay * 2, self.backoff[1])

    async def _session(self):
        reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.hub), self.timeout_s)
        ch = Channel(reader, writer, send_timeout_s=self.timeout_s)
        tasks = []
        try:
            t_hello = now_ms()
            await ch.send(F.Frame(F.FrameType.HELLO, dict(participant=self.participant, operation=self.operation,
                                                          role="source", versions=[PROTOCOL_VERSION], t=t_hello,
                                                          have=[])))
            first = await ch.recv(self.timeout_s)
            if first.type == F.FrameType.REJECT:
                raise Rejected(first.meta["code"], first.meta["text"])
            if first.type != F.FrameType.WELCOME:
                raise F.FrameError("bad_frame", "expected WELCOME, got 0x%02x" % first.type)
            w = first.meta
            ch.version = w["version"]
            self.clock_offset_ms = w["t"] - (t_hello + now_ms()) / 2
            self.timeout_s = float(w["timeout_s"])
            have = {(h["stream"], h["gen"]): h["committed"] for h in w["have"]}
            for s in self.streams.values():
                s.acked = s.sent = have.get((s.name, s.gen), 0)
                s.end_sent = False
            self._inflight = []                     # [(stream, gen, end offset)]
            self.sessions += 1
            self.connected.set()
            self.rejected = None
            tasks = [asyncio.ensure_future(ch.heartbeat(float(w["heartbeat_s"]))),
                     asyncio.ensure_future(self._receive(ch)),
                     asyncio.ensure_future(self._send(ch))]
            stopper = asyncio.ensure_future(self._stop.wait())
            done, _ = await asyncio.wait(tasks + [stopper], return_when=asyncio.FIRST_COMPLETED)
            stopper.cancel()
            for t in done:
                if t is not stopper:
                    t.result()                      # re-raise the reason the session ended
            if self._stop.is_set():
                try:
                    await asyncio.wait_for(ch.send(F.Frame(F.FrameType.BYE, {"reason": "stop"})), 1.0)
                except Exception:
                    pass
        finally:
            ch.abort()
            await reap(tasks)

    async def _receive(self, ch):
        while True:
            f = await ch.recv(self.timeout_s)
            m = f.meta
            s = self.streams.get(m.get("stream"))
            if f.type == F.FrameType.ACK:
                if s and s.gen == m["gen"]:
                    s.acked = max(s.acked, m["committed"])
                    self._inflight = [x for x in self._inflight if not (x[0] == s.name and x[1] == s.gen
                                                                         and x[2] <= s.acked)]
                    self._wake.set()
            elif f.type == F.FrameType.RESEND:
                if s and s.gen == m["gen"]:
                    s.sent = min(s.sent, m["off"])
                    self._inflight = [x for x in self._inflight if not (x[0] == s.name and x[1] == s.gen)]
                    self._wake.set()
            elif f.type == F.FrameType.STREAM_CHECK:
                if s and s.gen == m["gen"]:
                    s.result = m["result"]
                    s.checked.set()
                    if s.result != "ok":
                        log.error("%s generation %d: final check at the hub failed (%s)", s.name, s.gen, s.result)
            elif f.type == F.FrameType.ERROR:
                raise ConnectionError("hub reported %s: %s" % (m["code"], m["text"]))
            elif f.type == F.FrameType.BYE:
                raise ConnectionError("hub closed the session: %s" % m["reason"])

    async def _send(self, ch):
        while not ch.closed:
            self._wake.clear()
            progressed = False
            for s in self.streams.values():
                if s.apply(await asyncio.to_thread(s.look)):
                    await asyncio.to_thread(write_json_atomic, self._state_path,
                                            {n: x.saved() for n, x in self.streams.items()})
                if s.final_size == -1:
                    s.final_size = max(s.size, s.sent)     # never less than what the hub may already hold
                limit = s.final_size if s.final_size is not None else s.size
                while len(self._inflight) < self.window and s.sent < limit:
                    n = min(self.segment_bytes, limit - s.sent)
                    data = await asyncio.to_thread(_read, s.path, s.sent, n)
                    if not data:
                        break
                    frame = await asyncio.to_thread(F.segment, s.name, s.gen, s.sent, data, now_ms(),
                                                    self.zlib_level)
                    await ch.send(frame)
                    s.sent += len(data)
                    self._inflight.append((s.name, s.gen, s.sent))
                    progressed = True
                if s.final_size is not None and not s.end_sent and s.result != "ok" and s.acked >= s.final_size:
                    digest, n = await asyncio.to_thread(_sha256_prefix, s.path, s.final_size)
                    await ch.send(F.Frame(F.FrameType.STREAM_END, dict(stream=s.name, gen=s.gen, size=n,
                                                                       sha256=digest)))
                    s.end_sent = True
            if not progressed:
                await self._wake.wait(self.poll_s)
        raise ConnectionError("connection closed")
