<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/rope-logo-dark.svg">
    <img alt="ROPE" src="docs/images/rope-logo.svg" height="72">
  </picture>
</h1>

**Resumable Offline Protocol for Emergencies**

[![Tests](https://github.com/d-fankhauser/rope/actions/workflows/tests.yml/badge.svg)](https://github.com/d-fankhauser/rope/actions/workflows/tests.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)
![Status: early development](https://img.shields.io/badge/status-early_development-orange.svg)

ROPE transfers growing files and small messages between a small, closed group of devices over unreliable local
networks – radio links, Wi-Fi, no internet. Like a rescue line it holds the connection even when it is pulled:
an interruption costs time, never data.

> **Status:** early development. The [specification](docs/spec.md) is draft 1 with a proposed wire format. Streams
> from sources via the hub to consumers work, including resume after disconnects and restarts and block repair, and
> so do messages with custody and offline media (e.g. a USB stick) as a second path. Interfaces will change.

## Why

Field teams record large files (for example sonar recordings) on one device and want to evaluate them somewhere else
while they are still being recorded. The link in between drops out regularly, and there is no internet. Plain TCP does
not resume across connections and acknowledges data before it is safely stored; UDP loses data during outages. ROPE
adds the missing pieces on top of TCP, borrowing ideas – not code – from space communication (CFDP class 2, DTN custody
transfer).

## Key properties

- **Resumable** – after any interruption, transfer continues from the last durably stored byte, never from zero.
- **Custody** – a receiver acknowledges only after the data is on disk (flush + fsync). The sender of a message
  learns which recipients have it.
- **End-to-end verified** – checksums per segment and per complete file; bit-for-bit identical results. If bytes
  changed after they were sent (e.g. a header updated at the end of a recording), only the differing blocks travel
  again.
- **Never blocks the producer** – separate process, read-only file access without locks, bounded memory.
- **Offline** – no internet, no cloud, no external services.
- **Small groups** – star topology with a hub and a closed participant list: source, consumer and recipient roles,
  names for the operator, and per consumer a choice of which streams it receives.
- **Application-agnostic core** – the core knows nothing about the data; applications define streams and messages.
- **Standard library only** – no third-party runtime dependencies.
- **Two paths** – the same segments and messages can also travel on removable media and mix with the network.

ROPE has **no encryption** by design and is meant for closed networks – see
[Security considerations](docs/spec.md#11-security-considerations).

## How it works (in short)

```
 source ──TCP──▶ hub ──TCP──▶ consumer(s)
                  │
                  └──TCP──▶ recipient(s)   (messages only)
```

1. Every participant connects to the hub and greets it (identifier, role, protocol version). The hub checks its
   participant list and tells everybody the names and roles of the group.
2. The hub reports how far it has stored each stream (*committed offset*).
3. The source sends segments from there; the hub writes, syncs, then acknowledges.
4. When a stream is complete, a final checksum confirms the whole file; on a difference, only the differing blocks
   are resent.
5. On any disconnect: reconnect, greet, continue from the committed offset.
6. Without a network, a participant and the hub exchange the same frames as bundles on removable media
   (`export_media`, `import_media`); network and media mix freely.

Details: [docs/spec.md](docs/spec.md).

## Requirements

- Python 3.11 or newer, on Windows or Linux
- Nothing else at runtime

## Installation

Not published yet. The package will be named `rope-protocol` (import `rope_protocol`), because the name `rope` on PyPI
belongs to a well-known refactoring library.

For development:

```
git clone https://github.com/d-fankhauser/rope.git
cd rope
python -m pip install pytest pyflakes
python -m pytest
```

### Simulating an unreliable link

No radio link at hand? The link simulator sits between two programs and adds bandwidth limits, latency, blackouts
and disconnects:

```
python -m rope_protocol.linksim --listen 127.0.0.1:47311 --target 127.0.0.1:47310 --profile radio
```

Profiles are `lan`, `wifi`, `radio` and `radio-bad`; `--seed` makes the fault schedule reproducible. All profile
values are assumptions until measured on real links (see spec §13).

ROPE's default port is **47310**; applications can override it.

## Repository layout

| Path | Content |
|---|---|
| `docs/spec.md` | Protocol specification (draft) |
| `docs/decisions.md` | Decisions with date and reason |
| `docs/ideas.md` | Ideas not yet planned |
| `docs/images/` | Logo and symbol (SVG; light and dark variant) |
| `TASKS.md` | Task board |
| `src/rope_protocol/` | Package: frame codec (`frames.py`), link simulator (`linksim.py`), durable store (`store.py`), sending and receiving side of streams (`sender.py`, `receiver.py`), messages (`messages.py`), offline media (`media.py`), participant list (`roster.py`), participants (`hub.py`, `source.py`, `consumer.py`, `recipient.py`, base `participant.py`) |
| `tests/` | Unit tests and, later, the fault-injection test bench |
| `AGENTS.md` | Guidelines for contributors and coding agents |

## Testing

Every change runs `pyflakes` and `pytest` on Windows and Linux (Python 3.11–3.13). The planned test bench injects
faults – disconnects at every step, delay, throttling, restarts, duplicate and damaged segments – and checks that every
file arrives bit-for-bit identical and every message exactly once.

## Limitations

- Early development: streams, messages and offline media work. Interfaces and the wire format may still change.
- No block repair over offline media: a stream that differs after an import is repaired over the network.
- Not a general file synchronisation tool and not meant for the internet.
- No encryption, no authentication beyond a participant list.
- Not compatible with CFDP or DTN software; only the ideas are borrowed.

## Contributing

Contributions are welcome – please read [CONTRIBUTING.md](CONTRIBUTING.md). Questions and ideas: open an issue.

## License

Copyright 2026 Daniel Fankhauser. Licensed under the [Apache License, Version 2.0](LICENSE); see also [NOTICE](NOTICE).
The name "ROPE" in connection with this protocol is not covered by the licence (Apache-2.0, section 6).
