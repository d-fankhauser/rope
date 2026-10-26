"""Offline media as a second path (spec 8.6): segments and messages on removable media, e.g. a USB stick.

A **bundle** carries the frames the exporting participant would send over the network, in the same wire format
(spec 14): ``SEGMENT``, ``STREAM_END`` and ``MESSAGE``, plus ``ACK`` and ``MESSAGE_ACK`` for what it holds from the
other side. Import follows the network rules: store durably, never beyond the committed offset, duplicates ignored,
damaged items reported. Network and media may be mixed in any order.

Layout on the medium::

    rope/<operation>/<from>-to-<to>-<created ms>-<8 hex>/
        frames-0001.rope ...    frames, at most 1 GiB per file (FAT32 limit is 4 GiB)
        bundle.json             written last: format, operation, from, to, created, files, complete, ...

A bundle is written below a name ending in ``.partial`` and renamed when complete, so a medium pulled out during the
export never holds a half bundle under a valid name. Importing the same bundle again changes nothing.
"""

import json
import os
import re
import shutil
import uuid

from . import frames as F
from .channel import now_ms
from .receiver import SUMS_PER_FRAME
from .sender import differing_blocks, read_range, sha256_prefix
from .store import GapError, _fsync_dir, write_json_atomic

#: Format of the bundle folder (not the wire format, which is the frames' version byte).
FORMAT = 1
MEDIA_DIR = "rope"
MANIFEST = "bundle.json"
PARTIAL = ".partial"
#: Raw bytes per segment on a medium: one frame per MiB keeps the frame count low; the blob limit is 1 MiB (spec 14.2).
SEGMENT_BYTES = F.MAX_BLOB
#: Bytes per frames file; below the 4 GiB file limit of FAT32 sticks.
FILE_BYTES = 1024 * 1024 * 1024

_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")


def _check_identifier(what, value):
    if not (isinstance(value, str) and _IDENTIFIER.match(value)) or value in (".", ".."):
        raise ValueError("%s %r is not a valid identifier (spec 14.4)" % (what, value))


class ExportReport:
    """What an export wrote: ``path`` of the bundle, ``bytes`` on the medium, ``segments``/``messages`` counts,
    ``complete`` False if ``max_bytes`` stopped it before everything fitted (the rest goes with the next one)."""

    def __init__(self):
        self.path = None
        self.bytes = 0
        self.frames = 0
        self.segments = 0
        self.stream_bytes = 0
        self.messages = 0
        self.repair_requests = 0          # streams for which this bundle asks the sender to repair
        self.repair_bytes = 0             # bytes resent as repair
        self.complete = True

    def __repr__(self):
        return "<ExportReport %s: %d frames, %d bytes, %d messages, complete=%s>" % (
            self.path, self.frames, self.bytes, self.messages, self.complete)


class ImportReport:
    """What an import did with one bundle. ``problems`` are plain-language sentences for the operator; an empty list
    means the bundle was read completely and every item was accepted or was already there."""

    def __init__(self, path, manifest):
        manifest = manifest or {}
        self.path = path
        self.sender = manifest.get("from")
        self.created = manifest.get("created")
        self.frames = 0
        self.new_bytes = 0                  # stream bytes stored for the first time
        self.written = []                   # [(owner, stream, gen, committed)] after new bytes
        self.checked = []                   # [(owner, stream, gen, result, path)] final checks done
        self.messages = []                  # [meta] new messages taken into custody
        self.acks = []                      # [(stream, gen, committed)] the sender holds
        self.message_acks = []              # [id] the sender holds
        self.repair_requests = {}           # {(stream, gen): {size, block_size, sums}} a receiver asks to repair
        self.deliveries = []                # [id] of own messages with a new delivery report
        self.delivery_acks = []             # [id] of delivery reports the sender holds
        self.problems = []

    @property
    def ok(self):
        return not self.problems

    def __repr__(self):
        return "<ImportReport %s from %s: %d frames, %d new bytes, %d new messages, %d problems>" % (
            self.path, self.sender, self.frames, self.new_bytes, len(self.messages), len(self.problems))


class BundleWriter:
    """Writes one bundle. Use ``add(frame)`` (False if ``max_bytes`` would be exceeded), then ``close()``; on an
    error ``abort()`` removes the partial folder."""

    def __init__(self, medium, operation, sender, to, max_bytes=None):
        for what, v in (("operation", operation), ("sender", sender), ("addressee", to)):
            _check_identifier(what, v)
        self.operation = operation
        self.sender = sender
        self.to = to
        self.max_bytes = max_bytes
        self.created = now_ms()
        folder = os.path.join(os.path.abspath(medium), MEDIA_DIR, operation)
        name = "%s-to-%s-%d-%s" % (sender, to, self.created, uuid.uuid4().hex[:8])
        self.path = os.path.join(folder, name)
        self.tmp = self.path + PARTIAL
        os.makedirs(self.tmp)
        self.files = []                     # [{name, size, frames}]
        self._f = None
        self.report = ExportReport()

    def room(self, n):
        return self.max_bytes is None or self.report.bytes + n <= self.max_bytes

    def add(self, frame):
        raw = F.encode(frame)
        if not self.room(len(raw)):
            self.report.complete = False
            return False
        if self._f is None or self.files[-1]["size"] + len(raw) > FILE_BYTES:
            self._next_file()
        self._f.write(raw)
        self.files[-1]["size"] += len(raw)
        self.files[-1]["frames"] += 1
        self.report.bytes += len(raw)
        self.report.frames += 1
        return True

    def _next_file(self):
        self._close_file()
        name = "frames-%04d.rope" % (len(self.files) + 1)
        self.files.append(dict(name=name, size=0, frames=0))
        self._f = open(os.path.join(self.tmp, name), "wb")

    def _close_file(self):
        if self._f is not None:
            self._f.flush()
            os.fsync(self._f.fileno())
            self._f.close()
            self._f = None

    def close(self, **extra):
        """Make the bundle durable and give it its final name. Returns the ExportReport."""
        self._close_file()
        manifest = dict(extra, format=FORMAT, operation=self.operation, to=self.to, created=self.created,
                        files=self.files, frames=self.report.frames, complete=self.report.complete)
        manifest["from"] = self.sender
        write_json_atomic(os.path.join(self.tmp, MANIFEST), manifest)
        os.replace(self.tmp, self.path)
        _fsync_dir(os.path.dirname(self.path))
        self.report.path = self.path
        return self.report

    def abort(self):
        try:
            if self._f is not None:
                self._f.close()
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)


def find_bundles(medium, operation, to):
    """Complete bundles on ``medium`` for ``to`` in ``operation``, oldest first: [(path, manifest)].

    Partial folders (export cut off) and bundles for others are skipped. A bundle whose manifest cannot be read is
    returned with ``manifest`` None, so that the importer reports it (nobody can tell for whom it was).
    """
    folder = os.path.join(os.path.abspath(medium), MEDIA_DIR, operation)
    if not os.path.isdir(folder):
        return []
    out = []
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if name.endswith(PARTIAL) or not os.path.isfile(os.path.join(path, MANIFEST)):
            continue
        try:
            with open(os.path.join(path, MANIFEST), encoding="utf-8") as f:
                manifest = json.load(f)
        except (OSError, ValueError):
            manifest = None
        if not isinstance(manifest, dict):
            out.append((path, None))
        elif manifest.get("to") == to and manifest.get("operation") == operation:
            out.append((path, manifest))
    return sorted(out, key=lambda b: (_created(b[1]), b[0]))


def _created(manifest):
    t = (manifest or {}).get("created")
    return t if isinstance(t, int) else 0


def check_manifest(manifest, operation, to):
    """Plain-language reason why the bundle cannot be imported, or None."""
    if manifest is None:
        return "the bundle's %s cannot be read (damaged medium?) - the bundle was skipped" % MANIFEST
    if manifest.get("format") != FORMAT:
        return "bundle format %r is not supported (this program reads format %d)" % (manifest.get("format"), FORMAT)
    if manifest.get("operation") != operation or manifest.get("to") != to:
        return "bundle is for %s in operation %s" % (manifest.get("to"), manifest.get("operation"))
    sender = manifest.get("from")
    if not (isinstance(sender, str) and _IDENTIFIER.match(sender)):
        return "bundle names no valid sender"
    files = manifest.get("files")
    if not (isinstance(files, list) and all(isinstance(x, dict) and isinstance(x.get("name"), str)
                                            and re.match(r"frames-\d{4}\.rope\Z", x["name"]) for x in files)):
        return "bundle has no valid list of frame files"
    return None


def read_frames(path, manifest, report):
    """Yield the validated frames of a bundle in order, reading one frame at a time. Damage is recorded in
    ``report.problems``: a frame with a wrong CRC is skipped (its length is known, so reading goes on); a damaged
    header ends the reading of its file, because the position of the next frame is lost. A missing or cut-off file is
    reported as well."""
    for entry in manifest["files"]:
        name = entry["name"]
        count = 0
        try:
            with open(os.path.join(path, name), "rb") as f:
                while head := f.read(F.HEADER.size):
                    count += 1
                    if len(head) < F.HEADER.size:
                        report.problems.append("%s ends in the middle of frame %d (cut off)" % (name, count))
                        break
                    magic, _version, _type, m, b = F.HEADER.unpack(head)
                    if magic != F.MAGIC or m > F.MAX_META or b > F.MAX_BLOB:
                        report.problems.append("%s is damaged at frame %d; the rest of this file was not read"
                                               % (name, count))
                        break
                    rest = f.read(m + b + F.TRAILER.size)
                    if len(rest) < m + b + F.TRAILER.size:
                        report.problems.append("%s ends in the middle of frame %d (cut off)" % (name, count))
                        break
                    try:
                        frame = F.decode(head + rest)
                        F.validate(frame)
                    except F.FrameError as e:
                        report.problems.append("%s, frame %d is damaged (%s) - skipped" % (name, count, e.text))
                        continue
                    report.frames += 1
                    yield frame
                else:
                    if count != entry.get("frames"):
                        report.problems.append("%s holds %d frames, the bundle lists %s"
                                               % (name, count, entry.get("frames")))
        except OSError as e:
            report.problems.append("%s cannot be read: %s" % (name, e.strerror or e))


# --- building blocks for exporters ------------------------------------------------------------------------------------

def add_stream(writer, name, gen, path, start, limit, final_size=None, zlib_level=1):
    """Add the bytes ``start``..``limit`` of a file as SEGMENT frames, and STREAM_END if ``final_size`` is set and
    the stream is complete up to it. Returns False when the medium is full (the writer marked itself incomplete)."""
    off = start
    while off < limit:
        data = read_range(path, off, min(SEGMENT_BYTES, limit - off))
        if not data:
            break
        if not writer.add(F.segment(name, gen, off, data, now_ms(), zlib_level)):
            return False
        writer.report.segments += 1
        writer.report.stream_bytes += len(data)
        off += len(data)
    if final_size is not None and off >= final_size:
        digest, n = sha256_prefix(path, final_size)
        if n == final_size and not writer.add(F.Frame(F.FrameType.STREAM_END, dict(stream=name, gen=gen, size=n,
                                                                                    sha256=digest))):
            return False
    return True


def add_messages(writer, store, records, zlib_level=1):
    """Add MESSAGE frames for message records of ``store``. Returns False when the medium is full."""
    for rec in records:
        m = rec["meta"]
        body = store.body(m["id"])
        if not writer.add(F.message(m["id"], m["from"], m["to"], m["t"], m["kind"], body, zlib_level)):
            return False
        writer.report.messages += 1
    return True


def add_check_request(writer, name, gen, rs, block_size):
    """Ask the sender to repair a stream whose final check failed here (spec 8.6): STREAM_CHECK ``mismatch`` and the
    BLOCK_SUMS of what is stored, the same frames as on the network (spec 14.7). False when the medium is full."""
    if not writer.add(F.Frame(F.FrameType.STREAM_CHECK, dict(stream=name, gen=gen, result="mismatch",
                                                             size=rs.committed))):
        return False
    sums = rs.block_sums(block_size)
    for first in range(0, max(len(sums), 1), SUMS_PER_FRAME):
        if not writer.add(F.Frame(F.FrameType.BLOCK_SUMS, dict(stream=name, gen=gen, block_size=block_size,
                                                               first=first, sums=sums[first:first + SUMS_PER_FRAME]))):
            return False
    writer.report.repair_requests += 1
    return True


def collect_repair_frame(f, requests):
    """Collect STREAM_CHECK ``mismatch`` and BLOCK_SUMS from a bundle into ``requests``. True if ``f`` was one."""
    m = f.meta
    key = (m["stream"], m["gen"])
    if f.type == F.FrameType.STREAM_CHECK:
        if m["result"] == "mismatch":
            requests[key] = dict(size=m["size"], block_size=None, sums={})
        return True
    if f.type == F.FrameType.BLOCK_SUMS:
        req = requests.get(key)
        if req is not None:
            req["block_size"] = m["block_size"]
            for i, digest in enumerate(m["sums"]):
                req["sums"][m["first"] + i] = digest
        return True
    return False


def complete_requests(requests):
    """The requests whose block sums all arrived, as {"<stream>\\0<gen>": {size, block_size, sums: [...]}} for a JSON
    file; a request cut off by damage is dropped (the receiver asks again with its next bundle)."""
    out = {}
    for (stream, gen), req in requests.items():
        if req["block_size"] is None:
            continue
        count = -(-req["size"] // req["block_size"])
        if all(i in req["sums"] for i in range(count)):
            out["%s\0%d" % (stream, gen)] = dict(size=req["size"], block_size=req["block_size"],
                                                  sums=[req["sums"][i] for i in range(count)])
    return out


def split_key(key):
    stream, _, gen = key.rpartition("\0")
    return stream, int(gen)


def add_repair(writer, name, gen, path, req, zlib_level=1):
    """Resend the blocks of the file that differ from the receiver's sums as SEGMENT ``repair`` (spec 14.7). Returns
    (ok, differing blocks, all blocks); ok is False when the medium is full."""
    diff = differing_blocks(path, req["block_size"], req["sums"], req["size"])
    for off, end in diff:
        while off < end:
            data = read_range(path, off, min(SEGMENT_BYTES, end - off))
            if not data:
                break
            if not writer.add(F.segment(name, gen, off, data, now_ms(), zlib_level, repair=True)):
                return False, len(diff), len(req["sums"])
            writer.report.repair_bytes += len(data)
            off += len(data)
    return True, len(diff), len(req["sums"])


# --- building blocks for importers ------------------------------------------------------------------------------------

def import_segment(store, owner, f, report, skipped):
    """Store a SEGMENT like a network receiver (spec 8.3): never beyond the committed offset, duplicates ignored.
    ``skipped``: set of (stream, gen) with a gap in this bundle; later segments of those are skipped as well."""
    m = f.meta
    key = (m["stream"], m["gen"])
    if key in skipped:
        return
    try:
        data = F.segment_data(f)
    except F.FrameError as e:
        report.problems.append("stream %s generation %d at byte %d: %s - skipped" % (m["stream"], m["gen"], m["off"],
                                                                                   e.text))
        skipped.add(key)
        return
    rs = store.get(owner, m["stream"], m["gen"])
    before = rs.committed
    try:
        # a repair segment overwrites below the committed offset (spec 14.7); a verified stream ignores it, so an
        # old bundle never changes a stream that passed its final check
        committed = rs.write(m["off"], data, m.get("repair") is True)
    except GapError as e:
        report.problems.append("stream %s generation %d: bytes %d to %d are missing here; this bundle starts at "
                               "byte %d - an earlier bundle or the network must bring them first"
                               % (m["stream"], m["gen"], e.committed, m["off"], m["off"]))
        skipped.add(key)
        return
    if committed > before:
        report.new_bytes += committed - before
        report.written.append((owner, m["stream"], m["gen"], committed))


def import_stream_end(store, owner, f, report, skipped):
    """Final check from a medium. It never overrides a stream already verified ok: a bundle may be older than a
    repair that happened over the network since."""
    m = f.meta
    if (m["stream"], m["gen"]) in skipped:
        return
    rs = store.get(owner, m["stream"], m["gen"])
    if rs.complete:
        return
    if rs.committed < m["size"]:
        report.problems.append("stream %s generation %d: final check skipped, %d of %d bytes are here"
                               % (m["stream"], m["gen"], rs.committed, m["size"]))
        return
    result = rs.finish(m["size"], m["sha256"])
    report.checked.append((owner, m["stream"], m["gen"], result, rs.path))
    if result != "ok":
        report.problems.append("stream %s generation %d differs from the sender's file; the next bundle asks the "
                               "sender for the differing blocks (or the network repairs it, spec 14.7)"
                               % (m["stream"], m["gen"]))


def import_message(store, f, report, state, sender=None):
    """Take custody of a MESSAGE; with ``sender``, only one written by the bundle's sender (the hub's check, spec 8.5).
    Returns (meta, body) if it was new."""
    m = f.meta
    if sender is not None and m["from"] != sender:
        report.problems.append("message %s claims to be from %s, the bundle is from %s - skipped"
                               % (m["id"], m["from"], sender))
        return None
    try:
        body = F.message_body(f)
    except F.FrameError as e:
        report.problems.append("message %s: %s - skipped" % (m["id"], e.text))
        return None
    meta = {k: m[k] for k in ("id", "from", "to", "t", "kind")}
    if store.put(meta, body, state):
        report.messages.append(meta)
        return meta, body
    return None
