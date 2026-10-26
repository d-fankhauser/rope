"""Durable receiver storage: committed offset, duplicates, gaps, crash recovery, final check (spec 8.3, 8.4)."""

import hashlib
import json
import os

import pytest

from rope_protocol.store import GapError, StreamStore


def test_write_sequence_duplicates_and_gap(tmp_path):
    rs = StreamStore(tmp_path).get("source-1", "a/b.dat", 1)
    assert rs.committed == 0
    assert rs.write(0, b"hello ") == 6
    assert rs.write(0, b"hello ") == 6                  # duplicate: acknowledged, not written again
    assert rs.write(3, b"lo world") == 11               # overlap: only the new tail is written
    with pytest.raises(GapError) as e:
        rs.write(20, b"x")
    assert e.value.committed == 11
    assert open(rs.path, "rb").read() == b"hello world"


def test_state_survives_restart_and_uncommitted_tail_is_cut(tmp_path):
    rs = StreamStore(tmp_path).get("source-1", "s", 1)
    rs.write(0, b"0123456789")
    with open(rs.path, "ab") as f:                      # crash after writing, before the state update
        f.write(b"garbage")
    again = StreamStore(tmp_path).get("source-1", "s", 1)
    assert again.committed == 10
    assert open(again.path, "rb").read() == b"0123456789"
    assert again.sha256() == hashlib.sha256(b"0123456789").hexdigest()
    assert again.write(10, b"AB") == 12


def test_final_check(tmp_path):
    rs = StreamStore(tmp_path).get("p", "s", 1)
    rs.write(0, b"data")
    assert rs.finish(4, hashlib.sha256(b"data").hexdigest()) == "ok"
    rs2 = StreamStore(tmp_path).get("p", "t", 1)
    rs2.write(0, b"data")
    assert rs2.finish(4, hashlib.sha256(b"DATA").hexdigest()) == "mismatch"
    assert open(rs2.path, "rb").read() == b"data"       # kept for inspection


def test_generations_are_separate_and_names_are_not_paths(tmp_path):
    st = StreamStore(tmp_path)
    st.get("p", "../../evil", 1).write(0, b"one")
    st.get("p", "../../evil", 2).write(0, b"two!")
    st.get("q", "../../evil", 1).write(0, b"other")
    assert sorted((h["gen"], h["committed"]) for h in st.have("p")) == [(1, 3), (2, 4)]
    assert [h["committed"] for h in StreamStore(tmp_path).have("q")] == [5]
    for root, dirs, files in os.walk(tmp_path):
        assert "evil" not in root and not any("evil" in d for d in dirs)
    # every state file is readable JSON with the original name
    names = set()
    for root, _d, files in os.walk(tmp_path):
        if "state.json" in files:
            names.add(json.load(open(os.path.join(root, "state.json")))["stream"])
    assert names == {"../../evil"}
