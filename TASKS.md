# Tasks – ROPE

Flow: new idea → `docs/ideas.md`; planned → **Backlog**; started → **In progress** (with branch);
finished → **Done** (date, short description; details in `docs/`).

## Ideas inbox
_(see `docs/ideas.md`)_

## Backlog
- [ ] **Test bench** – the link simulator exists (bandwidth, latency, blackouts, disconnects). Still missing:
      disconnect at every protocol step, restarts, duplicate/damaged/reordered segments, and a long soak run over
      many seeds (opt-in, outside the normal test run – 40 seeds found a race the single-seed soak test missed).
- [ ] **Messages** with custody at the hub and per-recipient delivery state.
- [ ] **Participant list and roles** (source, consumer, recipient), greeting with plain-language rejection reasons.
- [ ] **Offline media** as a second path (export/import of segments and messages).
- [ ] **Packaging** as `rope-protocol` on PyPI (check name availability first).
- [ ] **Heartbeat and timeout for radio links:** the assumed 2 s / ~10 s may be too short for directional radio links
      (longer dropouts are normal there). Make both configurable; measure on a real radio network before fixing
      defaults (spec §9).
- [ ] Resolve the open points listed at the end of `docs/spec.md`.


## In progress
- [ ] **Relaying to consumers** (`feature/relay`, spec §7): the hub sends every stored stream on to every consumer,
      named `<source>/<stream>`, with the same sender as the source. Shared building blocks: `sender.py`,
      `receiver.py`, `participant.py`.

## Done
- 2026-10-26 Block repair after a mismatch (#8, spec §14.7): `BLOCK_SUMS`, repair segments, new generation only for
  shorter or mostly different files, at most 3 rounds.
- 2026-10-26 Core: streams from one source to the hub (#7): durable store (fsync before ACK, crash cut-back), hub with
  participant list and plain-language REJECT, source following growing files without locks, resume from the
  committed offset, generations, final SHA-256 check; tests over the link simulator.
- 2026-10-26 Wire format draft 1: spec §14 and frame codec `rope_protocol.frames` with test vectors (#5); default
  port 47310 and link simulator `rope_protocol.linksim`, the first part of the test bench (#6).
- 2026-10-19 Repository created: specification draft 0, licence, contribution guidelines, CI (pyflakes + pytest on
  Windows and Linux), package skeleton with a standard-library-only check.
