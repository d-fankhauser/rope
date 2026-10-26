"""Connection loop shared by the participants that connect to the hub (source, consumer, recipient): connect, greet,
run the session, reconnect with back-off (spec 8.1, 9); export to and import from offline media (spec 8.6)."""

import asyncio
import logging
import random

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from . import media
from .channel import Channel, now_ms, reap, serve_frames
from .messages import Mailbox

log = logging.getLogger("rope.participant")


class Rejected(Exception):
    """The hub refused this participant (REJECT); ``code`` and ``text`` say why, in plain language."""

    def __init__(self, code, text):
        super().__init__(text)
        self.code = code
        self.text = text


class Participant:
    """Base class. Subclasses set ``role`` and implement ``_have()`` and ``_active(ch, welcome)``, which returns the
    session's tasks (receiving through ``_serve``). Runs until ``stop()``; reconnects forever with back-off.

    Every participant has a mailbox below ``mailbox_root`` (spec 8.5): ``send_message``, ``outbox``, ``inbox``,
    ``message_body``; ``on_message(meta, body)`` is called once per new message.

    Offline media (spec 8.6): ``export_media`` writes a bundle for the hub, ``import_media`` reads the bundles the hub
    wrote for this participant. ``hub_id`` addresses the bundles; it is taken from every WELCOME.
    """

    role = None

    def __init__(self, participant, operation, hub_host, hub_port=DEFAULT_PORT, timeout_s=10.0, backoff=(1.0, 10.0),
                 mailbox_root=None, on_message=None, poll_s=0.5):
        self.participant = participant
        self.hub_id = "hub"
        self.mailbox = Mailbox(mailbox_root, participant, on_message, poll_s)
        self.operation = operation
        self.hub = (hub_host, hub_port)
        self.timeout_s = timeout_s
        self.backoff = backoff
        self.connected = asyncio.Event()
        self.clock_offset_ms = None
        self.sessions = 0
        self.rejected = None
        self._stop = asyncio.Event()
        self._task = None

    # --- messages ------------------------------------------------------------------------------------------------
    def send_message(self, to, kind, body):
        """Store a message for the participants ``to`` durably and return its id; it leaves when connected."""
        return self.mailbox.send(to, kind, body)

    def outbox(self):
        return self.mailbox.outbox()

    def inbox(self):
        return self.mailbox.inbox()

    def message_body(self, msg_id):
        return self.mailbox.body(msg_id)

    # --- connection ------------------------------------------------------------------------------------------------
    def start(self):
        self._task = asyncio.ensure_future(self.run())
        return self

    async def stop(self):
        self._stop.set()
        self.mailbox.sender.wake.set()
        self._stopping()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass

    def _stopping(self):
        """Hook: wake up loops when stopping."""

    async def run(self):
        delay = self.backoff[0]
        while not self._stop.is_set():
            try:
                await self._session()
                delay = self.backoff[0]
            except Rejected as e:
                self.rejected = e
                log.error("hub rejected %s: %s", self.participant, e.text)
                delay = self.backoff[1]
            except (ConnectionError, OSError, asyncio.TimeoutError, F.FrameError) as e:
                log.info("%s: connection lost (%s), retrying in %.1f s", self.participant, e, delay)
            finally:
                self.connected.clear()
            if self._stop.is_set():
                return
            await asyncio.sleep(delay * random.uniform(0.8, 1.0))
            delay = min(delay * 2, self.backoff[1])

    async def _session(self):
        reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.hub), self.timeout_s)
        ch = Channel(reader, writer, send_timeout_s=self.timeout_s)
        tasks = []
        try:
            t_hello = now_ms()
            have = await self._have()
            await ch.send(F.Frame(F.FrameType.HELLO, dict(participant=self.participant, operation=self.operation,
                                                          role=self.role, versions=[PROTOCOL_VERSION], t=t_hello,
                                                          have=have)))
            first = await ch.recv(self.timeout_s)
            if first.type == F.FrameType.REJECT:
                raise Rejected(first.meta["code"], first.meta["text"])
            if first.type != F.FrameType.WELCOME:
                raise F.FrameError("bad_frame", "expected WELCOME, got 0x%02x" % first.type)
            w = first.meta
            ch.version = w["version"]
            self.hub_id = w["hub"]
            self.clock_offset_ms = w["t"] - (t_hello + now_ms()) / 2
            self.timeout_s = float(w["timeout_s"])
            tasks = [asyncio.ensure_future(ch.heartbeat(float(w["heartbeat_s"])))]
            self.mailbox.sender.start_session()
            await self.mailbox.notify()             # messages stored but untold before a break or restart
            tasks.append(asyncio.ensure_future(self.mailbox.sender.run(ch)))
            tasks += await self._active(ch, w)
            self.sessions += 1
            self.connected.set()
            self.rejected = None
            stopper = asyncio.ensure_future(self._stop.wait())
            done, _ = await asyncio.wait(tasks + [stopper], return_when=asyncio.FIRST_COMPLETED)
            stopper.cancel()
            for t in done:
                if t is not stopper:
                    t.result()                      # re-raise the reason the session ended
            if self._stop.is_set():
                try:
                    await asyncio.wait_for(ch.send(F.Frame(F.FrameType.BYE, {"reason": "stop"})), 1.0)
                except Exception:
                    pass
        finally:
            ch.abort()
            await reap(tasks)

    # --- offline media (spec 8.6) ------------------------------------------------------------------------------------
    async def export_media(self, medium, max_bytes=None):
        """Write a bundle for the hub to the folder ``medium`` (e.g. a USB stick): receipts for messages received from
        a medium, the outbox messages not yet at the hub, and what the role adds (a source its streams from what the
        hub acknowledged, a consumer what it holds). ``max_bytes`` limits the bundle; the report says whether
        everything fitted. Returns an ExportReport."""
        acks = await self._media_acks()
        streams = await self._media_streams()
        return await asyncio.to_thread(self._export_media, medium, max_bytes, acks, streams)

    def _export_media(self, medium, max_bytes, acks, streams):
        box = self.mailbox.store
        w = media.BundleWriter(medium, self.operation, self.participant, self.hub_id, max_bytes)
        try:
            receipts = box.select(lambda r: r["state"].get("box") == "in" and r["state"].get("via") == "media")
            frames = [F.Frame(F.FrameType.MESSAGE_ACK, {"id": r["meta"]["id"]}) for r in receipts]
            frames += [F.Frame(F.FrameType.ACK, dict(stream=s, gen=g, committed=c)) for s, g, c in acks]
            ok = all(w.add(f) for f in frames)
            ok = ok and media.add_messages(w, box, self.mailbox.pending())
            for name, gen, path, start, limit, final_size in streams:
                ok = ok and media.add_stream(w, name, gen, path, start, limit, final_size)
            return w.close()
        except BaseException:
            w.abort()
            raise

    async def import_media(self, medium):
        """Import every bundle on ``medium`` the hub wrote for this participant, oldest first. Importing a bundle
        again changes nothing. Returns one ImportReport per bundle; ``problems`` lists what was skipped and why."""
        reports = []
        for path, manifest in await asyncio.to_thread(media.find_bundles, medium, self.operation, self.participant):
            report = await asyncio.to_thread(self._import_media, path, manifest)
            await self._media_imported(report)
            reports.append(report)
        await self.mailbox.notify()
        return reports

    def _import_media(self, path, manifest):
        report = media.ImportReport(path, manifest)
        reason = media.check_manifest(manifest, self.operation, self.participant)
        if reason:
            report.problems.append(reason)
            return report
        box = self.mailbox.store
        skipped = set()
        for f in media.read_frames(path, manifest, report):
            m = f.meta
            if f.type == F.FrameType.MESSAGE:
                media.import_message(box, f, report, {"box": "in", "told": False, "via": "media"})
            elif f.type == F.FrameType.MESSAGE_ACK:
                rec = box.get(m["id"])
                if rec is not None and rec["state"].get("box") == "out":
                    box.update(m["id"], at_hub=True)
                    report.message_acks.append(m["id"])
            elif f.type == F.FrameType.ACK:
                report.acks.append((m["stream"], m["gen"], m["committed"]))
            elif not self._media_frame(f, report, skipped):
                report.problems.append("unexpected %s for a %s - skipped" % (F.FrameType(f.type).name, self.role))
        return report

    async def _media_acks(self):
        """[(stream, gen, committed)] this participant holds as a receiver."""
        return []

    async def _media_streams(self):
        """[(name, gen, path, start, limit, final_size)] to export."""
        return []

    def _media_frame(self, f, report, skipped):
        """Import a role-specific frame (in a worker thread). False if the role does not take it."""
        return False

    async def _media_imported(self, report):
        """Hook in the event loop after a bundle was imported."""

    def _serve(self, ch, handle):
        """Task receiving frames from the hub: messages first, then the subclass's ``handle(frame)``."""
        async def both(f):
            return await self.mailbox.handle(ch, f) or await handle(f)
        return asyncio.ensure_future(serve_frames(ch, self.timeout_s, both, "hub"))

    async def _have(self):
        return []

    async def _active(self, ch, welcome):
        raise NotImplementedError
