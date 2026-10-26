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


def _fsync_dir(path):
    """Make a rename durable where the platform allows it (POSIX); Windows has no directory handles for this."""
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ReceivedStream:
    """One generation of one stream at the receiver. ``StreamStore`` hands out one instance per stream; ``write`` and
    ``finish`` are serialised by a lock, because a replaced session's write may still be running in its worker thread
    while the new session writes the same segment."""

    def __init__(self, folder, participant, stream, gen):
        self.folder = folder
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
        self._sha = hashlib.sha256()
        with open(self.path, "rb") as f:
            while chunk := f.read(READ_CHUNK):
                self._sha.update(chunk)
        if not state:
            self._save()

    def _save(self):
        write_json_atomic(self._state_path, dict(participant=self.participant, stream=self.stream, gen=self.gen,
                                                 committed=self.committed, complete=self.complete,
                                                 verified=self.verified))

    def write(self, off, data):
        """Store ``data`` at ``off`` durably and return the new committed offset.

        Already stored bytes are not written again (duplicates are acknowledged, spec 8.3). A segment that starts
        beyond the committed offset raises GapError.
        """
        with self._lock:
            if off > self.committed:
                raise GapError(self.committed)
            end = off + len(data)
            if end <= self.committed or self.complete:
                return self.committed
            tail = data[self.committed - off:]
            with open(self.path, "r+b") as f:
                f.seek(self.committed)
                f.write(tail)
                f.flush()
                os.fsync(f.fileno())
            self._sha.update(tail)
            self.committed = end
            self._save()
            return self.committed

    def finish(self, size, sha256):
        """Final check (spec 8.4): returns "ok" or "mismatch" and records it. Bytes are kept either way."""
        with self._lock:
            ok = size == self.committed and sha256 == self._sha.hexdigest()
            self.complete = ok
            self.verified = "ok" if ok else "mismatch"
            self._save()
            return self.verified

    def sha256(self):
        with self._lock:
            return self._sha.hexdigest()


class StreamStore:
    """All received streams below ``root``."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self._folder = os.path.join(self.root, "streams")
        os.makedirs(self._folder, exist_ok=True)
        self._open = {}
        self._lock = threading.Lock()

    def get(self, participant, stream, gen):
        k = _key(participant, stream, gen)
        with self._lock:
            if k not in self._open:
                self._open[k] = ReceivedStream(os.path.join(self._folder, k), participant, stream, gen)
            return self._open[k]

    def have(self, participant):
        """[{stream, gen, committed}] of everything stored from ``participant`` (for WELCOME/HELLO, spec 14.5)."""
        out = []
        for name in sorted(os.listdir(self._folder)):
            p = os.path.join(self._folder, name, "state.json")
            if not os.path.isfile(p):
                continue
            with open(p, encoding="utf-8") as f:
                s = json.load(f)
            if s.get("participant") == participant:
                k = _key(participant, s["stream"], s["gen"])
                committed = self._open[k].committed if k in self._open else s["committed"]
                out.append(dict(stream=s["stream"], gen=s["gen"], committed=committed))
        return out
