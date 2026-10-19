# Decisions (newest first)

Each entry: date, status (**decided** / **proposed**), reason.

### 2026-10-19 – Repository and process (decided)
- Public repository, **Apache-2.0** licence: permissive, proprietary applications may use the core, patent grant,
  contributions under the same licence (section 5); the name is not licensed (section 6).
- Package name `rope-protocol`, import `rope_protocol` – `rope` on PyPI is an established refactoring library.
- Documentation in **English**. `AGENTS.md` is the source of instructions, `CLAUDE.md` includes it.
- `main` protected: pull requests only, green checks, maintainer approval.
- CI on GitHub-hosted runners only, Windows and Linux. Never self-hosted runners (fork pull requests would run
  untrusted code on that machine).
- Development tools (`pytest`, `pyflakes`) only for testing; runtime uses the standard library only.

### 2026-10-18 – Protocol concept (decided unless marked otherwise)
- Own protocol on top of **TCP**; adopt ideas from CFDP class 2 / DTN (offset instead of packet number,
  acknowledgement after durable storage, resume from committed offset, final checksum), not the standards themselves.
- **Star topology** with a hub; every participant connects to the hub.
- **Closed participant list** prepared in advance; several participants from the start.
- **No encryption**, as simple as possible.
- **Python standard library only.**
- **Application-agnostic core**; applications define streams and messages.
- **Never block the producer**; work fully **offline**.
- Name: **ROPE – Resumable Offline Protocol for Emergencies** – like a rescue line it holds even when pulled.
- Roles, segment size, window, heartbeat and timeouts: **proposed** starting values, see `spec.md`.
