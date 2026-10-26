"""Recipient participant: exchanges messages only, receives no streams (spec 7, 8.5)."""

import os

from . import DEFAULT_PORT
from .participant import Participant


class Recipient(Participant):
    """``root``: folder for the mailbox. ``on_message(meta, body)`` is called once per new message."""

    role = "recipient"

    def __init__(self, participant, operation, hub_host, root, hub_port=DEFAULT_PORT, timeout_s=10.0,
                 backoff=(1.0, 10.0), on_message=None, poll_s=0.5):
        super().__init__(participant, operation, hub_host, hub_port, timeout_s, backoff,
                         mailbox_root=os.path.join(root, "messages"), on_message=on_message, poll_s=poll_s,
                         state_dir=root)

    async def _active(self, ch, welcome):
        async def handle(f):
            return False
        return [self._serve(ch, handle)]
