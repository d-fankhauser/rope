# Contributing to ROPE

Thank you for your interest. ROPE is at the concept stage, so discussion of the specification is just as valuable as
code.

## Before you start

- For anything larger than a typo, please **open an issue first** so we can agree on the approach.
- Read [AGENTS.md](AGENTS.md) – the rules apply to everyone, human or agent.
- Behaviour changes need a matching update of [docs/spec.md](docs/spec.md).

## Pull requests

1. Fork the repository and create a branch (`feature/...`, `fix/...`, `docs/...`).
2. Keep the runtime free of third-party dependencies (Python standard library only).
3. Add or update tests; run `python -m pyflakes src tests` and `python -m pytest` locally.
4. Open a pull request against `main` and describe what changes and why.

All checks must pass on Windows and Linux, and the maintainer approves every merge.

## Licence of contributions

By submitting a contribution you agree that it is licensed under the
[Apache License, Version 2.0](LICENSE) (section 5 of the licence). No separate contributor agreement is required.

## Reporting security issues

See [SECURITY.md](SECURITY.md).
