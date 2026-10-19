# Guidelines for contributors and coding agents

This file is the single source of instructions for humans and coding agents (`CLAUDE.md` includes it).

## Project

ROPE (*Resumable Offline Protocol for Emergencies*) – resumable transfer of growing files and messages over TCP in
small, closed, offline groups. Specification: `docs/spec.md`. Package `rope-protocol`, import `rope_protocol`.

## Rules

- **English** in code, comments, docs, commits, issues.
- **Python standard library only** at runtime. `pytest` and `pyflakes` are for development only.
  `tests/test_package.py` enforces this.
- **Application-agnostic core:** no knowledge of sonar or any other application in `src/rope_protocol/`.
- **Generic content only:** no references to specific organisations, people, sites, deployments, private data or
  internal planning. Examples use neutral names (`source-1`, `hub`, `tablet`).
- **Spec first:** behaviour changes update `docs/spec.md` in the same pull request. Mark statements as
  **decided**, **proposed** or **assumed**.
- **Tests with the code:** every feature comes with tests; protocol behaviour is tested under faults
  (disconnect, delay, restart, duplicates, damage). Pass criterion: bit-for-bit identical streams, every message exactly
  once.
- **Never block the producer:** no unbounded buffers, timeouts on every network call, no file locks on source files.
- Decisions go to `docs/decisions.md` (date, decided/proposed, reason); ideas to `docs/ideas.md`; tasks to `TASKS.md`.
- Files use LF line endings and UTF-8 without BOM.

## Workflow

- `main` is protected: changes only via pull request with green checks and maintainer approval.
- Branch names: `feature/...`, `fix/...`, `docs/...`.
- CI runs on GitHub-hosted runners only (never self-hosted – pull requests from forks run untrusted code).

## Commands

```
python -m pyflakes src tests
python -m pytest
```
