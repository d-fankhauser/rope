"""Wire format (spec section 14): encoding, incremental decoding, limits, damage, validation."""

import hashlib
import os
import random
import struct
import zlib

import pytest

from rope_protocol import frames as F
from rope_protocol.frames import Frame, FrameError, FrameType

T = 1767225600000          # 2026-01-01 00:00 UTC
ID = "0123456789abcdef0123456789abcdef"
SHA = hashlib.sha256(b"").hexdigest()

EXAMPLES = {
    FrameType.HELLO: dict(participant="source-1", operation="op-1", role="source", versions=[1], t=T, have=[]),
    FrameType.WELCOME: dict(version=1, hub="hub", t=T + 5, t_hello=T, heartbeat_s=2, timeout_s=10.5,
                            have=[dict(stream="source-1/rec-1.dat", gen=1, committed=4096)]),
    FrameType.REJECT: dict(code="unknown_participant", text="source-9 is not on the participant list"),
    FrameType.HEARTBEAT: dict(t=T),
    FrameType.BYE: dict(reason="shutdown"),
    FrameType.ERROR: dict(code="bad_frame", text="CRC mismatch"),
    FrameType.ACK: dict(stream="s", gen=1, committed=10),
    FrameType.RESEND: dict(stream="s", gen=1, off=0, len=10),
    FrameType.STREAM_END: dict(stream="s", gen=2, size=0, sha256=SHA),
    FrameType.STREAM_CHECK: dict(stream="s", gen=2, result="mismatch", size=0),
    FrameType.BLOCK_SUMS: dict(stream="s", gen=2, block_size=1 << 20, first=0, sums=[SHA, SHA]),
    FrameType.MESSAGE: dict(id=ID, to=["hub", "tablet"], t=T, kind="note", enc="none", **{"from": "source-1"}),
    FrameType.MESSAGE_ACK: dict(id=ID),
}


def all_frames():
    out = [Frame(t, m) for t, m in EXAMPLES.items()]
    out.append(F.segment("source-1/rec-1.dat", 1, 0, b"ping" * 1000, T))
    out.append(F.segment("source-1/rec-1.dat", 1, 4000, os.urandom(300), T))     # incompressible -> enc none
    out.append(Frame(FrameType.MESSAGE, EXAMPLES[FrameType.MESSAGE], "Grüße – ✓".encode("utf-8")))
    return out


def test_every_core_type_has_an_example():
    assert set(EXAMPLES) | {FrameType.SEGMENT} == set(FrameType)


def test_heartbeat_test_vector():
    """Spec 14.6: exact bytes."""
    raw = F.encode(Frame(FrameType.HEARTBEAT, {"t": T}))
    head = bytes.fromhex("52 50 01 04 00 00 00 13 00 00 00 00")
    meta = b'{"t":1767225600000}'
    assert raw[:12] == head and raw[12:31] == meta and len(raw) == 35
    assert raw[31:] == struct.pack(">I", zlib.crc32(head + meta))
    assert raw[31:] == bytes.fromhex("45 28 16 84")


@pytest.mark.parametrize("frame", all_frames(), ids=lambda f: hex(f.type))
def test_roundtrip_and_valid(frame):
    raw = F.encode(frame)
    (back,) = F.FrameDecoder().feed(raw)
    assert back.type == frame.type and back.meta == frame.meta and back.blob == bytes(frame.blob)
    assert F.validate(back) == frame.type


def test_byte_by_byte_and_many_in_one_chunk():
    raws = [F.encode(f) for f in all_frames()]
    stream = b"".join(raws)
    dec = F.FrameDecoder()
    got = []
    for i in range(len(stream)):
        got += dec.feed(stream[i:i + 1])
    assert [F.encode(f) for f in got] == raws
    assert dec.buffered == 0
    got = F.FrameDecoder().feed(stream)
    assert len(got) == len(raws)


def test_random_chunking():
    rng = random.Random(7)
    raws = [F.encode(f) for f in all_frames()] * 3
    stream = b"".join(raws)
    dec = F.FrameDecoder()
    got, i = [], 0
    while i < len(stream):
        n = rng.randint(1, 5000)
        got += dec.feed(stream[i:i + n])
        i += n
    assert [F.encode(f) for f in got] == raws


def test_every_single_bit_flip_is_detected():
    """Damage anywhere never produces a different valid frame (CRC-32 catches every single-bit error)."""
    raw = F.encode(F.segment("s", 1, 0, b"abc" * 20, T))
    original = F.FrameDecoder().feed(raw)[0]
    for i in range(len(raw)):
        for bit in range(8):
            bad = bytearray(raw)
            bad[i] ^= 1 << bit
            try:
                got = F.FrameDecoder().feed(bytes(bad) + raw)
            except FrameError:
                continue
            # a flip in a length field may only make the decoder wait for more data - never a wrong frame
            assert all(g.meta == original.meta and g.blob == original.blob for g in got)


def test_wrong_magic_rejected_at_first_byte():
    with pytest.raises(FrameError) as e:
        F.FrameDecoder().feed(b"G")            # e.g. an HTTP client: "GET /"
    assert e.value.code == "bad_frame"


def test_limits_checked_before_buffering():
    dec = F.FrameDecoder()
    header = F.HEADER.pack(F.MAGIC, 1, FrameType.SEGMENT, 10, F.MAX_BLOB + 1)
    with pytest.raises(FrameError) as e:
        dec.feed(header)
    assert e.value.code == "too_large"
    with pytest.raises(FrameError):
        dec.feed(b"more")                       # decoder stays failed: the connection must be closed
    with pytest.raises(FrameError) as e:
        F.FrameDecoder().feed(F.HEADER.pack(F.MAGIC, 1, 4, F.MAX_META + 1, 0))
    assert e.value.code == "too_large"


def test_encode_refuses_too_large():
    with pytest.raises(FrameError):
        F.encode(Frame(FrameType.MESSAGE, EXAMPLES[FrameType.MESSAGE], b"x" * (F.MAX_BLOB + 1)))
    with pytest.raises(FrameError):
        F.encode(Frame(FrameType.BYE, {"reason": "x" * (F.MAX_META + 1)}))


def _raw(ftype, meta_bytes, blob=b""):
    body = F.HEADER.pack(F.MAGIC, 1, ftype, len(meta_bytes), len(blob)) + meta_bytes + blob
    return body + struct.pack(">I", zlib.crc32(body))


@pytest.mark.parametrize("meta", [b"[1,2]", b"not json", b"\xff\xfe", b'"text"'])
def test_meta_must_be_json_object(meta):
    with pytest.raises(FrameError):
        F.FrameDecoder().feed(_raw(FrameType.BYE, meta))


def test_extension_types_pass_unknown_core_types_fail():
    (ext,) = F.FrameDecoder().feed(_raw(0x80, b'{"x":1}', b"data"))
    assert F.validate(ext) is None
    (unknown,) = F.FrameDecoder().feed(_raw(0x7F, b"{}"))
    with pytest.raises(FrameError) as e:
        F.validate(unknown)
    assert e.value.code == "unknown_type"


def test_unknown_meta_keys_are_ignored():
    f = Frame(FrameType.HEARTBEAT, {"t": T, "future_key": {"a": 1}})
    (back,) = F.FrameDecoder().feed(F.encode(f))
    assert F.validate(back) is FrameType.HEARTBEAT


@pytest.mark.parametrize("ftype,change", [
    (FrameType.HELLO, {"role": "admin"}),
    (FrameType.HELLO, {"versions": []}),
    (FrameType.HELLO, {"participant": "has space"}),
    (FrameType.HELLO, {"t": "yesterday"}),
    (FrameType.HELLO, {"have": [{"stream": "s", "gen": 0, "committed": 1}]}),
    (FrameType.WELCOME, {"timeout_s": 0}),
    (FrameType.WELCOME, {"heartbeat_s": True}),
    (FrameType.ACK, {"gen": 0}),
    (FrameType.ACK, {"committed": -1}),
    (FrameType.ACK, {"stream": ""}),
    (FrameType.ACK, {"stream": "x" * 256}),
    (FrameType.RESEND, {"len": 1.5}),
    (FrameType.STREAM_END, {"sha256": SHA.upper()}),
    (FrameType.STREAM_CHECK, {"result": "maybe"}),
    (FrameType.BLOCK_SUMS, {"sums": ["abc"]}),
    (FrameType.MESSAGE, {"id": "not-hex"}),
    (FrameType.MESSAGE, {"to": []}),
    (FrameType.MESSAGE, {"enc": "lzma"}),
    (FrameType.MESSAGE_ACK, {"id": ID.upper()}),
])
def test_validate_rejects(ftype, change):
    meta = dict(EXAMPLES[ftype], **change)
    with pytest.raises(FrameError):
        F.validate(Frame(ftype, meta))


@pytest.mark.parametrize("ftype", list(EXAMPLES))
def test_validate_requires_every_key(ftype):
    for k in F.REQUIRED[ftype]:
        meta = dict(EXAMPLES[ftype])
        del meta[k]
        with pytest.raises(FrameError):
            F.validate(Frame(ftype, meta))


def test_segment_payload():
    data = b"repeated test data " * 500
    f = F.segment("s", 1, 0, data, T)
    assert f.meta["enc"] == "zlib" and len(f.blob) < len(data)
    assert F.segment_data(f) == data
    rnd = os.urandom(1000)
    g = F.segment("s", 1, 0, rnd, T)
    assert g.meta["enc"] == "none" and F.segment_data(g) == rnd
    assert F.segment_data(F.segment("s", 1, 0, b"", T)) == b""


def test_segment_damage_detected():
    data = b"abcdef" * 100
    f = F.segment("s", 1, 0, data, T)
    with pytest.raises(FrameError):                       # wrong CRC in meta
        F.segment_data(Frame(f.type, dict(f.meta, crc=f.meta["crc"] ^ 1), f.blob))
    with pytest.raises(FrameError):                       # truncated zlib
        F.segment_data(Frame(f.type, f.meta, f.blob[:-3]))
    with pytest.raises(FrameError):                       # length lie: never returns more than announced
        F.segment_data(Frame(f.type, dict(f.meta, len=10), f.blob))


def test_zip_bomb_is_bounded():
    """A small blob that would expand hugely is cut off at len + 1 bytes."""
    bomb = zlib.compress(b"\0" * (50 * 1024 * 1024), 9)
    with pytest.raises(FrameError):
        F.decode_payload("zlib", bomb, 100)
