# Tasks – ROPE

Flow: new idea → `docs/ideas.md`; planned → **Backlog**; started → **In progress** (with branch);
finished → **Done** (date, short description; details in `docs/`).

## Ideas inbox
_(see `docs/ideas.md`)_

## Backlog
- [ ] **Wire format** in `docs/spec.md`: framing, control messages, checksum algorithm, version negotiation.
- [ ] **Core: single stream, one source, one hub** – send from committed offset, durable write + fsync before
      acknowledgement, resume after reconnect, final checksum.
- [ ] **Test bench** – fault-injecting TCP proxy (disconnect at every step, delay, throttling, loss), restarts,
      duplicate/damaged/reordered segments; runs on Windows and Linux in CI.
- [ ] **Messages** with custody at the hub and per-recipient delivery state.
- [ ] **Participant list and roles** (source, consumer, recipient), greeting with plain-language rejection reasons.
- [ ] **Offline media** as a second path (export/import of segments and messages).
- [ ] **Packaging** as `rope-protocol` on PyPI (check name availability first).
- [ ] **Heartbeat and timeout for radio links:** the assumed 2 s / ~10 s may be too short for directional radio links
      (longer dropouts are normal there). Make both configurable; measure on a real radio network before fixing
      defaults (spec §9).
- [ ] Resolve the open points listed at the end of `docs/spec.md`.

## In progress

## Done
- 2026-10-19 Repository created: specification draft 0, licence, contribution guidelines, CI (pyflakes + pytest on
  Windows and Linux), package skeleton with a standard-library-only check.
