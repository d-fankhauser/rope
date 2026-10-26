# Tasks – ROPE

Flow: new idea → `docs/ideas.md`; planned → **Backlog**; started → **In progress** (with branch);
finished → **Done** (date, short description; details in `docs/`).

## Ideas inbox
_(see `docs/ideas.md`)_

## Backlog
- [ ] **Test bench** – the link simulator exists (bandwidth, latency, blackouts, disconnects). Still missing:
      disconnect at every protocol step, restarts, duplicate/damaged/reordered segments.
- [ ] **Messages** with custody at the hub and per-recipient delivery state.
- [ ] **Participant list and roles** (source, consumer, recipient), greeting with plain-language rejection reasons.
- [ ] **Offline media** as a second path (export/import of segments and messages).
- [ ] **Packaging** as `rope-protocol` on PyPI (check name availability first).
- [ ] **Heartbeat and timeout for radio links:** the assumed 2 s / ~10 s may be too short for directional radio links
      (longer dropouts are normal there). Make both configurable; measure on a real radio network before fixing
      defaults (spec §9).
- [ ] Resolve the open points listed at the end of `docs/spec.md`.

## In progress
- [ ] **Core: streams from one source to the hub** (`feature/core-stream`): durable store (fsync before ACK, crash
      cut-back), hub with participant list and plain-language REJECT, source following growing files without locks,
      resume from the committed offset, generations for replaced files, final SHA-256 check. Tests over the link
      simulator: disconnects, blackouts, hub and source restarts. Still missing: per-block repair after a mismatch
      (`BLOCK_SUMS`), relaying to consumers.
- [ ] **Wire format draft 1** (`feature/wire-format`): spec §14 and frame codec `rope_protocol.frames` with tests;
      default port 47310; link simulator `rope_protocol.linksim` (first part of the test bench).

## Done
- 2026-10-19 Repository created: specification draft 0, licence, contribution guidelines, CI (pyflakes + pytest on
  Windows and Linux), package skeleton with a standard-library-only check.
