# Security

## Scope

ROPE has **no encryption and no authentication** beyond a participant list, by design. It is intended for closed,
physically controlled networks. Reading or spoofing traffic on the same network is therefore expected behaviour, not a
vulnerability. See [Security considerations](docs/spec.md#11-security-considerations).

Relevant reports are, for example: crashes or hangs caused by malformed input, unbounded memory or disk use, writing
outside the configured storage directory, or data corruption that passes verification.

## Reporting

Please report vulnerabilities privately via GitHub: **Security → Report a vulnerability** in this repository. Do not
open a public issue for them.
