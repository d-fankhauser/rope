"""Frame codec of the ROPE wire format (spec section 14, draft 1).

A frame is a fixed 12-byte header, a JSON object ("meta"), raw bytes ("blob") and a CRC-32 over everything before it.
This module only encodes and decodes frames; it does no I/O. ``FrameDecoder`` is incremental: feed it whatever the
socket returned and it yields complete frames. It checks the length limits before buffering a frame, so a damaged or
hostile length field cannot make it allocate more than the limits allow.
"""

import enum
import json
import re
import struct
import zlib
from dataclasses import dataclass, field

from . import roster

MAGIC = b"RP"
VERSION = 1
HEADER = struct.Struct(">2sBBII")
TRAILER = struct.Struct(">I")
MAX_META = 64 * 1024
MAX_BLOB = 1024 * 1024
EXTENSION_FIRST = 0x80

_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")
_HEX32 = re.compile(r"[0-9a-f]{32}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


class FrameType(enum.IntEnum):
    HELLO = 0x01
    WELCOME = 0x02
    REJECT = 0x03
    HEARTBEAT = 0x04
    BYE = 0x05
    ERROR = 0x06
    SEGMENT = 0x10
    ACK = 0x11
    RESEND = 0x12
    STREAM_END = 0x13
    STREAM_CHECK = 0x14
    BLOCK_SUMS = 0x15
    MESSAGE = 0x20
    MESSAGE_ACK = 0x21
    DELIVERY = 0x22
    DELIVERY_ACK = 0x23


#: Required meta keys per core frame type (spec 14.3). Unknown keys are allowed and ignored.
REQUIRED = {
    FrameType.HELLO: ("participant", "operation", "role", "versions", "t", "have"),
    FrameType.WELCOME: ("version", "hub", "t", "t_hello", "heartbeat_s", "timeout_s", "have"),
    FrameType.REJECT: ("code", "text"),
    FrameType.HEARTBEAT: ("t",),
    FrameType.BYE: ("reason",),
    FrameType.ERROR: ("code", "text"),
    FrameType.SEGMENT: ("stream", "gen", "off", "len", "crc", "t", "enc"),
    FrameType.ACK: ("stream", "gen", "committed"),
    FrameType.RESEND: ("stream", "gen", "off", "len"),
    FrameType.STREAM_END: ("stream", "gen", "size", "sha256"),
    FrameType.STREAM_CHECK: ("stream", "gen", "result", "size"),
    FrameType.BLOCK_SUMS: ("stream", "gen", "block_size", "first", "sums"),
    FrameType.MESSAGE: ("id", "from", "to", "t", "kind", "len", "crc", "enc"),
    FrameType.MESSAGE_ACK: ("id",),
    FrameType.DELIVERY: ("id", "delivered", "pending", "unknown"),
    FrameType.DELIVERY_ACK: ("id", "delivered"),
}

ROLES = ("source", "consumer", "recipient")
ENCODINGS = ("none", "zlib")
CODES = ("unknown_participant", "wrong_operation", "version", "role", "busy", "bad_frame", "too_large", "unknown_type",
         "storage", "internal")


class FrameError(Exception):
    """A frame violates the wire format. ``code`` is the error code to send (spec 14.4) before closing."""

    def __init__(self, code, text):
        super().__init__(text)
        self.code = code
        self.text = text


@dataclass
class Frame:
    type: int
    meta: dict = field(default_factory=dict)
    blob: bytes = b""
    version: int = VERSION

    @property
    def is_extension(self):
        return self.type >= EXTENSION_FIRST


def encode_meta(meta):
    """Compact, key-sorted UTF-8 JSON (spec 14.2) - reproducible, so test vectors are exact."""
    return json.dumps(meta, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")


def encode(frame):
    """Frame -> bytes. Raises FrameError if a limit is exceeded or the type is out of range."""
    if not 0 <= frame.type <= 0xFF or not 0 <= frame.version <= 0xFF:
        raise FrameError("bad_frame", "frame type or version out of range")
    meta = encode_meta(frame.meta)
    blob = bytes(frame.blob)
    if len(meta) > MAX_META:
        raise FrameError("too_large", "meta is %d bytes, limit %d" % (len(meta), MAX_META))
    if len(blob) > MAX_BLOB:
        raise FrameError("too_large", "blob is %d bytes, limit %d" % (len(blob), MAX_BLOB))
    body = HEADER.pack(MAGIC, frame.version, frame.type, len(meta), len(blob)) + meta + blob
    return body + TRAILER.pack(zlib.crc32(body))


class FrameDecoder:
    """Incremental decoder. ``feed(data)`` returns the frames completed by ``data``.

    After a FrameError the stream position is lost; the caller must send ERROR and close the connection (spec 14.2).
    Further calls then raise the same error again.
    """

    def __init__(self, max_meta=MAX_META, max_blob=MAX_BLOB):
        self.max_meta = max_meta
        self.max_blob = max_blob
        self._buf = bytearray()
        self._need = None          # total frame length once the header is known
        self._error = None

    @property
    def buffered(self):
        return len(self._buf)

    def feed(self, data):
        if self._error:
            raise self._error
        self._buf += data
        out = []
        try:
            while True:
                frame = self._next()
                if frame is None:
                    return out
                out.append(frame)
        except FrameError as e:
            self._error = e
            self._buf.clear()
            raise

    def _next(self):
        if self._need is None:
            if len(self._buf) < HEADER.size:
                # reject a wrong magic as early as possible
                if self._buf[:2] and not MAGIC.startswith(bytes(self._buf[:2])):
                    raise FrameError("bad_frame", "wrong magic")
                return None
            magic, _version, _type, m, b = HEADER.unpack_from(self._buf)
            if magic != MAGIC:
                raise FrameError("bad_frame", "wrong magic")
            if m > self.max_meta or b > self.max_blob:
                raise FrameError("too_large", "frame announces meta %d / blob %d bytes" % (m, b))
            self._need = HEADER.size + m + b + TRAILER.size
        if len(self._buf) < self._need:
            return None
        raw = bytes(self._buf[:self._need])
        del self._buf[:self._need]
        self._need = None
        return decode(raw)


def decode(raw):
    """Exactly one complete frame (bytes) -> Frame. Checks magic, lengths, CRC and that meta is a JSON object."""
    if len(raw) < HEADER.size + TRAILER.size:
        raise FrameError("bad_frame", "frame shorter than header and CRC")
    magic, version, ftype, m, b = HEADER.unpack_from(raw)
    if magic != MAGIC:
        raise FrameError("bad_frame", "wrong magic")
    if len(raw) != HEADER.size + m + b + TRAILER.size:
        raise FrameError("bad_frame", "frame length does not match header")
    (crc,) = TRAILER.unpack_from(raw, len(raw) - TRAILER.size)
    if zlib.crc32(raw[:-TRAILER.size]) != crc:
        raise FrameError("bad_frame", "CRC mismatch")
    try:
        meta = json.loads(raw[HEADER.size:HEADER.size + m].decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        raise FrameError("bad_frame", "meta is not valid UTF-8 JSON") from None
    if not isinstance(meta, dict):
        raise FrameError("bad_frame", "meta is not a JSON object")
    return Frame(ftype, meta, raw[HEADER.size + m:HEADER.size + m + b], version)


def _is_int(v, minimum=0):
    return isinstance(v, int) and not isinstance(v, bool) and v >= minimum


def _check_stream(meta):
    s = meta["stream"]
    if not isinstance(s, str) or not 1 <= len(s.encode("utf-8")) <= 255:
        raise FrameError("bad_frame", "stream name must be 1-255 bytes UTF-8")
    if not _is_int(meta["gen"], 1):
        raise FrameError("bad_frame", "generation must be an integer >= 1")


def validate(frame):
    """Check a decoded core frame against spec 14.3/14.4. Extensions pass unchecked; an unknown core type raises
    FrameError('unknown_type'). Returns the FrameType."""
    if frame.is_extension:
        return None
    try:
        ftype = FrameType(frame.type)
    except ValueError:
        raise FrameError("unknown_type", "unknown core frame type 0x%02x" % frame.type) from None
    meta = frame.meta
    missing = [k for k in REQUIRED[ftype] if k not in meta]
    if missing:
        raise FrameError("bad_frame", "%s without %s" % (ftype.name, ", ".join(missing)))
    for k in ("t", "t_hello"):
        if k in REQUIRED[ftype] and not _is_int(meta[k]):
            raise FrameError("bad_frame", "%s must be milliseconds since 1970 UTC" % k)
    for k in ("participant", "hub", "operation", "from"):
        if k in REQUIRED[ftype] and not (isinstance(meta[k], str) and _IDENTIFIER.match(meta[k])):
            raise FrameError("bad_frame", "%s is not a valid identifier" % k)
    if "stream" in REQUIRED[ftype]:
        _check_stream(meta)
    for k in ("off", "len", "size", "committed", "first", "crc"):
        if k in REQUIRED[ftype] and not _is_int(meta[k]):
            raise FrameError("bad_frame", "%s must be a non-negative integer" % k)
    if ftype in (FrameType.REJECT, FrameType.ERROR) and not isinstance(meta["text"], str):
        raise FrameError("bad_frame", "text must be a string")
    if ftype is FrameType.HELLO:
        if meta["role"] not in ROLES:
            raise FrameError("bad_frame", "unknown role %r" % (meta["role"],))
        if not (isinstance(meta["versions"], list) and meta["versions"] and all(_is_int(v, 1) for v in meta["versions"])):
            raise FrameError("bad_frame", "versions must be a non-empty list of integers >= 1")
    if ftype in (FrameType.HELLO, FrameType.WELCOME):
        if not isinstance(meta["have"], list):
            raise FrameError("bad_frame", "have must be a list")
        for h in meta["have"]:
            if not (isinstance(h, dict) and "stream" in h and "gen" in h and _is_int(h.get("committed"))):
                raise FrameError("bad_frame", "have entries need stream, gen, committed")
            _check_stream(h)
    if ftype is FrameType.WELCOME:
        if not _is_int(meta["version"], 1):
            raise FrameError("bad_frame", "version must be an integer >= 1")
        for k in ("heartbeat_s", "timeout_s"):
            if isinstance(meta[k], bool) or not isinstance(meta[k], (int, float)) or meta[k] <= 0:
                raise FrameError("bad_frame", "%s must be a positive number" % k)
        if "participants" in meta:
            reason = roster.check_public(meta["participants"])
            if reason:
                raise FrameError("bad_frame", reason)
    if ftype in (FrameType.SEGMENT, FrameType.MESSAGE) and meta["enc"] not in ENCODINGS:
        raise FrameError("bad_frame", "unknown encoding %r" % (meta["enc"],))
    if ftype is FrameType.SEGMENT and not isinstance(meta.get("repair", False), bool):
        raise FrameError("bad_frame", "repair must be true or false")
    if ftype is FrameType.STREAM_END and not (isinstance(meta["sha256"], str) and _HEX64.match(meta["sha256"])):
        raise FrameError("bad_frame", "sha256 must be 64 lower-case hex characters")
    if ftype is FrameType.STREAM_CHECK and meta["result"] not in ("ok", "mismatch"):
        raise FrameError("bad_frame", "result must be ok or mismatch")
    if ftype is FrameType.BLOCK_SUMS:
        if not _is_int(meta["block_size"], 1):
            raise FrameError("bad_frame", "block_size must be an integer >= 1")
        if not (isinstance(meta["sums"], list) and all(isinstance(s, str) and _HEX64.match(s) for s in meta["sums"])):
            raise FrameError("bad_frame", "sums must be SHA-256 hex digests")
    if ftype in (FrameType.MESSAGE, FrameType.MESSAGE_ACK, FrameType.DELIVERY, FrameType.DELIVERY_ACK) and not (isinstance(meta["id"], str) and _HEX32.match(meta["id"])):
        raise FrameError("bad_frame", "message id must be 32 lower-case hex characters")
    if ftype is FrameType.MESSAGE:
        if not (isinstance(meta["to"], list) and meta["to"] and all(isinstance(t, str) and _IDENTIFIER.match(t) for t in meta["to"])):
            raise FrameError("bad_frame", "to must be a non-empty list of identifiers")
        if not (isinstance(meta["kind"], str) and _IDENTIFIER.match(meta["kind"])):
            raise FrameError("bad_frame", "kind is not a valid identifier")
    if ftype is FrameType.DELIVERY:
        for k in ("delivered", "pending", "unknown"):
            if not (isinstance(meta[k], list) and all(isinstance(t, str) and _IDENTIFIER.match(t) for t in meta[k])):
                raise FrameError("bad_frame", "%s must be a list of identifiers" % k)
    if ftype is FrameType.DELIVERY_ACK and not _is_int(meta["delivered"]):
        raise FrameError("bad_frame", "delivered must be a non-negative integer")
    if ftype in (FrameType.REJECT, FrameType.ERROR) and meta["code"] not in CODES:
        # unknown codes from newer versions are allowed but must be strings
        if not isinstance(meta["code"], str):
            raise FrameError("bad_frame", "code must be a string")
    return ftype


def encode_payload(data, level=1):
    """Raw bytes -> (enc, blob): zlib only when it makes the blob smaller (spec 14.4)."""
    packed = zlib.compress(data, level)
    return ("zlib", packed) if len(packed) < len(data) else ("none", bytes(data))


def decode_payload(enc, blob, length):
    """(enc, blob) -> raw bytes of exactly ``length``; FrameError on anything else (never more than ``length``)."""
    if enc == "none":
        data = bytes(blob)
    elif enc == "zlib":
        d = zlib.decompressobj()
        try:
            data = d.decompress(blob, length + 1)
        except zlib.error:
            raise FrameError("bad_frame", "zlib data damaged") from None
        if not d.eof or d.unused_data:
            raise FrameError("bad_frame", "zlib data incomplete or followed by extra bytes")
    else:
        raise FrameError("bad_frame", "unknown encoding %r" % (enc,))
    if len(data) != length:
        raise FrameError("bad_frame", "decoded length %d, expected %d" % (len(data), length))
    return data


def segment(stream, gen, off, data, t, level=1, **extra):
    """Build a SEGMENT frame for raw ``data`` at offset ``off``."""
    enc, blob = encode_payload(data, level)
    meta = dict(extra, stream=stream, gen=gen, off=off, len=len(data), crc=zlib.crc32(data), t=t, enc=enc)
    return Frame(FrameType.SEGMENT, meta, blob)


def message(msg_id, sender, to, t, kind, body, level=1):
    """Build a MESSAGE frame with the raw ``body`` (spec 8.5, 14.3)."""
    enc, blob = encode_payload(body, level)
    meta = dict(id=msg_id, to=list(to), t=t, kind=kind, len=len(body), crc=zlib.crc32(body), enc=enc)
    meta["from"] = sender
    return Frame(FrameType.MESSAGE, meta, blob)


def message_body(frame):
    """Decoded, CRC-checked body of a MESSAGE frame."""
    m = frame.meta
    body = decode_payload(m["enc"], frame.blob, m["len"])
    if zlib.crc32(body) != m["crc"]:
        raise FrameError("bad_frame", "message %s damaged (CRC mismatch)" % m["id"])
    return body


def segment_data(frame):
    """Decoded, CRC-checked raw data of a SEGMENT frame."""
    m = frame.meta
    data = decode_payload(m["enc"], frame.blob, m["len"])
    if zlib.crc32(data) != m["crc"]:
        raise FrameError("bad_frame", "segment CRC mismatch")
    return data
