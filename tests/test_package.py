"""Package-level checks: importable, versioned, standard library only."""

import ast
import sys
from pathlib import Path

import rope_protocol

SRC = Path(__file__).resolve().parent.parent / "src" / "rope_protocol"


def test_version():
    assert isinstance(rope_protocol.__version__, str) and rope_protocol.__version__
    assert isinstance(rope_protocol.PROTOCOL_VERSION, int)


def _imported_top_level_modules(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            yield node.module.split(".")[0]


def test_standard_library_only():
    allowed = set(sys.stdlib_module_names) | {"rope_protocol"}
    found = {}
    for path in SRC.rglob("*.py"):
        for name in _imported_top_level_modules(path):
            if name not in allowed:
                found.setdefault(name, []).append(path.name)
    assert not found, f"third-party imports in the runtime package: {found}"
