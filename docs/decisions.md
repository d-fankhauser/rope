# Decisions (newest first)

Each entry: date, status (**decided** / **proposed**), reason.

### 2026-10-26 – Messages with custody (proposed)
- **Every role may send and receive messages**; a recipient only receives no streams. Answers ("understood") come
  from the same devices that receive tasks.
- **`MESSAGE` gets `len` and `crc`** (wire format draft 1, changed before any release): `len` bounds the
  decompression like for segments, `crc` detects damage of the body.
- **The hub checks `from`** against the connection: no participant can send in another's name by mistake.
- **"Told" flag in the inbox**: found with 40 messages over the harsh simulated link - if the connection breaks
  between storing and telling the application, the resent copy is a duplicate and the application would never hear
  of the message. Untold messages are now told after each received message and at session start (at least once).
- **Delivery report to the sender** is left open; the sender sees "at the hub".

### 2026-10-26 – Relaying to consumers (proposed)
- **The hub sends like a source.** One sender implementation for both links (source → hub, hub → consumer):
  resume, final check and block repair behave the same everywhere and are tested once.
- **No delivery state per consumer at the hub.** The consumer reports what it holds in its greeting. Nothing to keep
  consistent at the hub, and restarts on either side lose nothing.
- **Relayed names `<source>/<stream>`** instead of a new meta key: no change to the frames, and two sources may use
  the same stream name.
- **Final check to consumers only after the hub's own check passed**, so a consumer never confirms bytes that the
  hub might still repair.
- **Every consumer receives every stream** of the operation for now; selection per consumer is an open point.

### 2026-10-26 – Block repair instead of a new generation for in-place changes (proposed)
- **A changed beginning is no longer a new generation.** Recording programs may update a header after the data
  was written (to be confirmed on real devices). With the earlier rule, that would have meant a full retransfer of
  every recording over a slow link. Now only a shorter file starts a new generation right away; bytes changed in
  place are found by the final check and only the differing blocks are resent (spec §14.7).
- **More than half of the blocks differ → new generation.** Then the file was evidently replaced, not edited. The
  receiver keeps the old version, as before.
- **At most 3 repairs**, then the mismatch is reported to the application, never retried forever.
- Block size 1 MiB, the half rule and 3 rounds are **assumed** values until measured.

### 2026-10-26 – Default port and link simulator
- **Default port 47310, overridable by every application (decided).** The maintainer wants a default that is
  overridable and not used by other software by chance; the exact number does not matter. When chosen, 47310 was not
  assigned in the IANA registry and no software was known to use it. It also lies below the dynamic port range
  (49152+) that operating systems use for outgoing connections. No silent fallback to another port.
- **Link simulator instead of a real radio link (decided)** for now: no radio link is available for measurements.
  A TCP proxy simulates bandwidth, latency, blackouts (connection stays open) and disconnects. Its profiles are
  **assumed** until real links are measured.

### 2026-10-25 – Wire format draft 1 (proposed)
- One frame layout for everything: 12-byte header, JSON meta, binary blob, CRC-32 (spec §14.2). One parser, readable
  captures, extendable without breaking older participants (unknown keys ignored, extension types `0x80`–`0xFF`).
- Limits checked before buffering (meta 64 KiB, blob 1 MiB): bounded memory, never block (§10).
- CRC-32 per frame and per segment, SHA-256 per whole stream: all in the standard library; CRC is enough against
  transmission damage, SHA-256 for the final end-to-end check.
- Streams carry a generation: a shortened or replaced source becomes a new generation, never an overwrite.
- Clock offset estimated in the greeting, reported to the application, never applied to the system clock.
- TLS is an optional layer below ROPE, chosen by the application; frames do not change.
- Proposed until the first release; may change after the first end-to-end implementation and measurements.

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
