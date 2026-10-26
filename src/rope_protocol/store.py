"""Durable storage of received streams (spec 8.3, 8.4): custody before acknowledgement.

A receiver writes a segment, flushes and fsyncs it, records the new committed offset atomically, and only then may it
acknowledge. After a crash the data file is cut back to the committed offset, so bytes that were never acknowledged
never count.

Layout below ``root``::

    streams/<key>/data          the stream bytes, exactly ``committed`` long after opening
    streams/<key>/state.json    participant, stream, gen, committed, complete, verified

``key`` is derived from a hash of participant and stream name plus the generation: stream names chosen by a remote
application are never used as paths (spec 14.4).
"""

import asyncio
import hashlib
import json
import os
import threading

READ_CHUNK = 1024 * 1024


class GapError(Exception):
    """A segment starts beyond the committed offset; the sender must resend from ``committed``."""

    def __init__(self, committed):
        super().__init__("segment beyond committed offset %d" % committed)
        self.committed = committed


def _key(participant, stream, gen):
    h = hashlib.sha256(("%s\0%s" % (participant, stream)).encode("utf-8")).hexdigest()[:32]
    return "%s-g%d" % (h, gen)


def write_json_atomic(path, obj):
    """Write ``obj`` as JSON so that a crash leaves either the old or the new file, never a mix."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path))


def _hash_file(path, size):
    """Running SHA-256 over the first ``size`` bytes of ``path``."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        left = size
        while left > 0 and (chunk := f.read(min(READ_CHUNK, left))):
            h.update(chunk)
            left -= len(chunk)
    return h


def _fsync_dir(path):
    """Make a rename durable where the platform allows it (POSIX); Windows has no directory handles for this."""
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Activity:
    """Counts disk writes in progress (``with activity:`` around each). A stop waits until there are none, so that a
    new instance started in the same program afterwards finds the final state on disk - a worker thread keeps writing
    after the task that started it was cancelled."""

    def __init__(self):
        self._n = 0
        self._cond = threading.Condition()

    def __enter__(self):
        with self._cond:
            self._n += 1

    def __exit__(self, *exc):
        with self._cond:
            self._n -= 1
            self._cond.notify_all()

    async def run(self, fn, *args, **kwargs):
        """Like ``asyncio.to_thread(fn, *args)``, but a write once started always completes, and it counts from this
        call on. Cancelling ``asyncio.to_thread`` drops a job that is still queued in the thread pool - a session
        ending at that moment lost e.g. the "told" flag of a message, and the next session told it again (found by
        the test bench on CI, spec 13). The caller still sees the cancellation at once; the write goes on."""
        self.__enter__()

        def job():
            try:
                return fn(*args, **kwargs)
            finally:
                self.__exit__()
        try:
            inner = asyncio.ensure_future(asyncio.to_thread(job))
        except BaseException:
            self.__exit__()
            raise
        return await asyncio.shield(inner)

    def wait_idle(self, timeout_s):
        """True once no write is in progress, False after ``timeout_s``."""
        with self._cond:
            return self._cond.wait_for(lambda: self._n == 0, timeout_s)


def wait_idle(activities, timeout_s=5.0):
    """Wait for several Activity objects; True if all became idle in time."""
    return all([a.wait_idle(timeout_s) for a in activities])


class ReceivedStream:
    """One generation of one stream at the receiver. ``StreamStore`` hands out one instance per stream; ``write`` and
    ``finish`` are serialised by a lock, because a replaced session's write may still be running in its worker thread
    while the new session writes the same segment."""

    def __init__(self, folder, participant, stream, gen, activity=None):
        self.folder = folder
        self._activity = activity or Activity()
        self.participant = participant
        self.stream = stream
        self.gen = gen
        self.path = os.path.join(folder, "data")
        self._state_path = os.path.join(folder, "state.json")
        self._lock = threading.Lock()
        os.makedirs(folder, exist_ok=True)
        state = {}
        if os.path.exists(self._state_path):
            with open(self._state_path, encoding="utf-8") as f:
                state = json.load(f)
        self.committed = int(state.get("committed", 0))
        self.complete = bool(state.get("complete", False))
        self.verified = state.get("verified")            # None, "ok" or "mismatch"
        # cut off anything written but never committed (crash between write and state update)
        with open(self.path, "ab"):
            pass
        if os.path.getsize(self.path) != self.committed:
            with open(self.path, "r+b") as f:
                f.truncate(self.committed)
                f.flush()
                os.fsync(f.fileno())
        self._sha = _hash_file(self.path, self.committed)
        if not state:
            self._save()

    def _save(self):
        write_json_atomic(self._state_path, dict(participant=self.participant, stream=self.stream, gen=self.gen,
                                                 committed=self.committed, complete=self.complete,
                                                 verified=self.verified))

    def write(self, off, data, repair=False):
        """Store ``data`` at ``off`` durably and return the new committed offset.

        Already stored bytes are not written again (duplicates are acknowledged, spec 8.3) - unless ``repair`` is set
        (spec 14.7): then the part below the committed offset overwrites what is stored. A segment that starts beyond
        the committed offset raises GapError.
        """
        with self._activity, self._lock:
            if off > self.committed:
                raise GapError(self.committed)
            end = off + len(data)
            if self.complete:
                return self.committed
            if repair and off < self.committed:
                with open(self.path, "r+b") as f:
                    f.seek(off)
                    f.write(data[:self.committed - off])
                    f.flush()
                    os.fsync(f.fileno())
                self._sha = None                        # recomputed from the file when needed
            if end <= self.committed:
                return self.committed
            tail = data[self.committed - off:]
            with open(self.path, "r+b") as f:
                f.seek(self.committed)
                f.write(tail)
                f.flush()
                os.fsync(f.fileno())
            if self._sha is not None:
                self._sha.update(tail)
            self.committed = end
            self._save()
            return self.committed

    def finish(self, size, sha256):
        """Final check (spec 8.4): returns "ok" or "mismatch" and records it. Bytes are kept either way."""
        with self._activity, self._lock:
            ok = size == self.committed and sha256 == self._digest()
            self.complete = ok
            self.verified = "ok" if ok else "mismatch"
            self._save()
            return self.verified

    def block_sums(self, block_size):
        """SHA-256 of every block of ``block_size`` bytes of the stored data (the last one may be shorter)."""
        with self._lock:
            sums = []
            with open(self.path, "rb") as f:
                left = self.committed
                while left > 0:
                    chunk = f.read(min(block_size, left))
                    if not chunk:
                        break
                    sums.append(hashlib.sha256(chunk).hexdigest())
                    left -= len(chunk)
            return sums

    def sha256(self):
        with self._lock:
            return self._digest()

    def _digest(self):
        if self._sha is None:
            self._sha = _hash_file(self.path, self.committed)
        return self._sha.hexdigest()


class StreamStore:
    """All received streams below ``root``."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self._folder = os.path.join(self.root, "streams")
        os.makedirs(self._folder, exist_ok=True)
        self._open = {}
        self._index = None            # state of the streams on disk at the first entries() call
        self._lock = threading.Lock()
        self.activity = Activity()

    def get(self, participant, stream, gen):
        k = _key(participant, stream, gen)
        with self._lock:
            if k not in self._open:
                self._open[k] = ReceivedStream(os.path.join(self._folder, k), participant, stream, gen,
                                               self.activity)
            return self._open[k]

    def entries(self):
        """Every stored stream as {participant, stream, gen, committed, complete, verified, path}.

        Streams not opened since the start are read from their state file once; those never change unless opened.
        """
        with self._lock:
            if self._index is None:
                self._index = {}
                for name in sorted(os.listdir(self._folder)):
                    p = os.path.join(self._folder, name, "state.json")
                    if os.path.isfile(p):
                        with open(p, encoding="utf-8") as f:
                            self._index[name] = json.load(f)
            out = []
            for k, s in self._index.items():
                if k not in self._open:
                    out.append(dict(participant=s["participant"], stream=s["stream"], gen=s["gen"],
                                    committed=s["committed"], complete=s.get("complete", False),
                                    verified=s.get("verified"), path=os.path.join(self._folder, k, "data")))
            for rs in self._open.values():
                out.append(dict(participant=rs.participant, stream=rs.stream, gen=rs.gen, committed=rs.committed,
                                complete=rs.complete, verified=rs.verified, path=rs.path))
            return out

    def have(self, participant):
        """[{stream, gen, committed}] of everything stored from ``participant`` (for WELCOME/HELLO, spec 14.5)."""
        return [dict(stream=e["stream"], gen=e["gen"], committed=e["committed"])
                for e in sorted(self.entries(), key=lambda e: (e["stream"], e["gen"]))
                if e["participant"] == participant]
