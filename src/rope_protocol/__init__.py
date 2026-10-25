"""ROPE - Resumable Offline Protocol for Emergencies.

Specification: docs/spec.md. Implemented so far: the frame codec of the wire format (``rope_protocol.frames``).
"""

__version__ = "0.0.1.dev0"

#: Protocol version announced in the greeting (spec 14.5). Increase on incompatible changes.
PROTOCOL_VERSION = 1
