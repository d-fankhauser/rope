"""Consumer participant: receives the streams of all sources from the hub, stores them durably and checks them
(spec 7, 8). Streams are named ``<source>/<stream>``.

The application reads the received bytes from ``path`` up to ``committed`` while a stream is still growing;
``on_written`` and ``on_checked`` tell it when there is more and when a stream is complete and verified.
"""

import asyncio
import os

from . import DEFAULT_PORT
from . import frames as F
from . import media
from .participant import Participant
from .receiver import BLOCK_SIZE, StreamReceiver
from .store import StreamStore

#: Key under which a consumer stores what it receives (it has exactly one sender: the hub).
FROM_HUB = "hub"


class Consumer(Participant):
    """``root``: folder for the received streams and the mailbox. ``on_written(stream, gen, committed)`` after every
    durable write, ``on_checked(stream, gen, result, path)`` after every final check, ``on_message(meta, body)`` once
    per new message - all called in the event loop."""

    role = "consumer"

    def __init__(self, participant, operation, hub_host, root, hub_port=DEFAULT_PORT, timeout_s=10.0,
                 backoff=(1.0, 10.0), block_size=BLOCK_SIZE, on_written=None, on_checked=None, on_message=None):
        super().__init__(participant, operation, hub_host, hub_port, timeout_s, backoff,
                         mailbox_root=os.path.join(root, "messages"), on_message=on_message,
                         state_dir=root)
        self.store = StreamStore(root)
        self.on_written = on_written
        self.on_checked = on_checked
        self.receiver = StreamReceiver(self.store, FROM_HUB, block_size, on_written=self._written,
                                       on_checked=self._checked, me="consumer")

    # --- application interface -------------------------------------------------------------------------------
    def streams(self):
        """Everything received so far: [{stream, gen, committed, complete, verified, path}]."""
        return [dict(stream=e["stream"], gen=e["gen"], committed=e["committed"], complete=e["complete"],
                     verified=e["verified"], path=e["path"])
                for e in sorted(self.store.entries(), key=lambda e: (e["stream"], e["gen"]))
                if e["participant"] == FROM_HUB]

    def received(self, stream, gen=1):
        """The stored stream (``path``, ``committed``, ``verified``)."""
        return self.store.get(FROM_HUB, stream, gen)

    # --- session ---------------------------------------------------------------------------------------------------
    async def _have(self):
        return await asyncio.to_thread(self.store.have, FROM_HUB)

    async def _active(self, ch, welcome):
        async def handle(f):
            return await self.receiver.handle(ch, f)
        return [self._serve(ch, handle)]

    # --- offline media (spec 8.6) ------------------------------------------------------------------------------------
    async def _media_acks(self):
        """What this consumer holds: the hub's next bundle for it starts there."""
        return [(h["stream"], h["gen"], h["committed"]) for h in await self._have()]

    def _media_frame(self, f, report, skipped):
        if f.type == F.FrameType.SEGMENT:
            media.import_segment(self.store, FROM_HUB, f, report, skipped)
        elif f.type == F.FrameType.STREAM_END:
            media.import_stream_end(self.store, FROM_HUB, f, report, skipped)
        else:
            return False
        return True

    async def _media_imported(self, report):
        for owner, stream, gen, committed in report.written:
            self._written(owner, stream, gen, committed)
        for owner, stream, gen, result, path in report.checked:
            self._checked(owner, stream, gen, result, path)

    def _written(self, owner, stream, gen, committed):
        if self.on_written:
            self.on_written(stream, gen, committed)

    def _checked(self, owner, stream, gen, result, path):
        if self.on_checked:
            self.on_checked(stream, gen, result, path)
