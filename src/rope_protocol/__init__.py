"""ROPE - Resumable Offline Protocol for Emergencies.

Specification: docs/spec.md. Implemented so far: the frame codec of the wire format (``rope_protocol.frames``), the
link simulator for tests (``rope_protocol.linksim``) and streams from sources via a hub to consumers
(``rope_protocol.source``, ``rope_protocol.hub``, ``rope_protocol.consumer``; shared parts in ``sender``,
``receiver``, ``participant``, ``store``).
"""

__version__ = "0.0.1.dev0"

#: Protocol version announced in the greeting (spec 14.5). Increase on incompatible changes.
PROTOCOL_VERSION = 1

#: Default TCP port of the hub (spec 14.1). Applications must allow overriding it.
DEFAULT_PORT = 47310
