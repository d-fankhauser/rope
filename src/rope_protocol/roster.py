"""Participant list of a hub (spec 7): identifier, name, role and, for a consumer, which streams it receives.

The hub is configured with ``{identifier: role}`` or ``{identifier: {"role", "name", "streams"}}``:

- ``name``: plain language for the operator, e.g. "Tablet at the command post" (default: the identifier).
- ``streams`` (consumers only): patterns for the relayed stream names ``<source>/<stream>``, e.g. ``["source-1/*"]``.
  ``*`` matches any characters including ``/``, ``?`` one character; case matters. Default: every stream.

The hub tells every participant the identifiers, names and roles (``WELCOME.participants``, bundle manifest), so
that an application can show names and address messages. The stream selection stays at the hub.
"""

import fnmatch
import re
from collections import namedtuple

ROLES = ("source", "consumer", "recipient")
MAX_NAME = 100
_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")

Entry = namedtuple("Entry", "role name streams")


def _check_name(what, name):
    if not isinstance(name, str) or not name.strip() or len(name) > MAX_NAME or not name.isprintable():
        raise ValueError("%s: the name must be 1-%d printable characters, got %r" % (what, MAX_NAME, name))


def parse(participants, hub_id, hub_name=None):
    """Check a participant list and return ``{identifier: Entry}``. Raises ValueError with a plain-language reason."""
    if not _IDENTIFIER.match(hub_id or ""):
        raise ValueError("hub identifier %r: 1-64 characters from A-Z a-z 0-9 . _ -" % (hub_id,))
    if hub_name is not None:
        _check_name("the hub", hub_name)
    out = {}
    for ident, spec in dict(participants).items():
        if not isinstance(ident, str) or not _IDENTIFIER.match(ident):
            raise ValueError("participant %r: identifiers are 1-64 characters from A-Z a-z 0-9 . _ -" % (ident,))
        if ident == hub_id:
            raise ValueError("participant %s has the identifier of the hub" % ident)
        if isinstance(spec, str):
            spec = {"role": spec}
        if not isinstance(spec, dict):
            raise ValueError("participant %s: expected a role or a dict with role, name, streams" % ident)
        unknown = set(spec) - {"role", "name", "streams"}
        if unknown:
            raise ValueError("participant %s: unknown keys %s" % (ident, ", ".join(sorted(unknown))))
        role = spec.get("role")
        if role not in ROLES:
            raise ValueError("participant %s: role must be one of %s, got %r" % (ident, ", ".join(ROLES), role))
        name = spec.get("name", ident)
        _check_name("participant %s" % ident, name)
        streams = spec.get("streams")
        if streams is not None:
            if role != "consumer":
                raise ValueError("participant %s: only a consumer receives streams, a %s cannot select any"
                                 % (ident, role))
            if isinstance(streams, str) or not all(isinstance(p, str) and p for p in streams):
                raise ValueError("participant %s: streams must be a list of patterns like \"source-1/*\"" % ident)
            streams = tuple(streams)
        out[ident] = Entry(role, name, streams)
    return out


def receives(entry, relayed_name):
    """True if a consumer with this entry receives the relayed stream ``<source>/<stream>``."""
    if entry is None or entry.role != "consumer":
        return False
    if entry.streams is None:
        return True
    return any(fnmatch.fnmatchcase(relayed_name, p) for p in entry.streams)


def public(entries, hub_id, hub_name=None):
    """What every participant may know: ``[{participant, name, role}]``, the hub first (role ``hub``)."""
    return [dict(participant=hub_id, name=hub_name or hub_id, role="hub")] + \
        [dict(participant=p, name=e.name, role=e.role) for p, e in sorted(entries.items())]


def check_public(value):
    """Plain-language reason why ``value`` is not a valid public participant list, or None."""
    if not isinstance(value, list):
        return "participants must be a list"
    for e in value:
        if not (isinstance(e, dict) and isinstance(e.get("participant"), str) and _IDENTIFIER.match(e["participant"])
                and isinstance(e.get("name"), str) and isinstance(e.get("role"), str)):
            return "participants entries need participant, name and role"
    return None
