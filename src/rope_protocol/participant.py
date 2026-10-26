"""Connection loop shared by the participants that connect to the hub (source, consumer, recipient): connect, greet,
run the session, reconnect with back-off (spec 8.1, 9); export to and import from offline media (spec 8.6)."""

import asyncio
import json
import logging
import os
import random
import threading

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from . import media
from . import roster
from .channel import Channel, now_ms, reap, serve_frames
from .messages import Mailbox
from .store import Activity, wait_idle, write_json_atomic

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
    ``message_body``; ``on_message(meta, body)`` is called once per new message, ``on_delivery(msg_id, report)``
    once per new delivery report of a sent message (``outbox()`` holds the latest report).

    Offline media (spec 8.6): ``export_media`` writes a bundle for the hub, ``import_media`` reads the bundles the hub
    wrote for this participant. ``hub_id`` addresses the bundles; it is taken from every WELCOME.

    ``participant_list()`` returns the hub's participant list with names and roles (spec 7), as last learnt from a
    WELCOME or a bundle of the hub; it is kept in ``state_dir``, so it is known after a restart without network.
    """

    role = None

    def __init__(self, participant, operation, hub_host, hub_port=DEFAULT_PORT, timeout_s=10.0, backoff=(1.0, 10.0),
                 mailbox_root=None, on_message=None, poll_s=0.5, state_dir=None, on_delivery=None):
        self.participant = participant
        self._roster_path = os.path.join(state_dir, "participants.json") if state_dir else None
        self._roster_lock = threading.Lock()    # a greeting and a medium import may learn the list at the same time
        self._activity = Activity()             # own disk writes (participant list, a source's state)
        self._roster = self._load_roster()
        self.hub_id = "hub"
        self.mailbox = Mailbox(mailbox_root, participant, on_message, poll_s, on_delivery)
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

    # --- participant list (spec 7) ----------------------------------------------------------------------------------
    def participant_list(self):
        """[{participant, name, role}] of the operation, the hub first (role ``hub``); empty until learnt."""
        return [dict(e) for e in self._roster.get("participants", [])]

    def _load_roster(self):
        if self._roster_path:
            try:
                with open(self._roster_path, encoding="utf-8") as f:
                    saved = json.load(f)
                if roster.check_public(saved.get("participants")) is None:
                    return saved
            except FileNotFoundError:
                pass
            except (OSError, ValueError, AttributeError) as e:
                log.warning("%s: participant list %s unreadable (%s), waiting for the hub", self.participant,
                            self._roster_path, e)
        return {}

    def _learn_roster(self, participants, t):
        """Keep the list the hub sent at hub time ``t`` (ms) unless a newer one is known (in a worker thread)."""
        if participants is None or roster.check_public(participants) is not None:
            return
        with self._activity, self._roster_lock:
            if self._roster and self._roster.get("t", 0) > t:
                return                              # e.g. an old stick after a newer greeting
            new = {"t": t, "participants": participants}
            if new == self._roster:
                return
            if self._roster_path:
                write_json_atomic(self._roster_path, new)
            self._roster = new

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
        # as for the hub: when stop() returns, no write of this participant is still running
        if not await asyncio.to_thread(wait_idle, self._activities()):
            log.warning("%s stop: disk writes still running after 5 s", self.participant)

    def _activities(self):
        return [self.mailbox.store.activity, self._activity]

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
            except Exception:
                # a bug must not end the loop for good: the participant would never connect again
                log.exception("%s: unexpected error in the session, reconnecting in %.1f s", self.participant, delay)
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
            await self._activity.run(self._learn_roster, w.get("participants"), w["t"])
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
        checks = await self._media_checks()
        repairs = await self._media_repairs()
        streams = await self._media_streams()
        report = await asyncio.to_thread(self._export_media, medium, max_bytes, acks, streams, checks, repairs)
        await self._media_exported(report)
        return report

    def _export_media(self, medium, max_bytes, acks, streams, checks=(), repairs=()):
        box = self.mailbox.store
        w = media.BundleWriter(medium, self.operation, self.participant, self.hub_id, max_bytes)
        try:
            receipts = box.select(lambda r: r["state"].get("box") == "in" and r["state"].get("via") == "media")
            frames = [F.Frame(F.FrameType.MESSAGE_ACK, {"id": r["meta"]["id"]}) for r in receipts]
            frames += self.mailbox.report_acks()
            frames += [F.Frame(F.FrameType.ACK, dict(stream=s, gen=g, committed=c)) for s, g, c in acks]
            ok = all(w.add(f) for f in frames)
            for name, gen, rs, block_size in checks:
                ok = ok and media.add_check_request(w, name, gen, rs, block_size)
            ok = ok and media.add_messages(w, box, self.mailbox.pending())
            w.report.repaired = []
            for name, gen, path, req in repairs:
                if not ok:
                    break
                ok, n, count = media.add_repair(w, name, gen, path, req)
                if ok:
                    w.report.repaired.append((name, gen, n, count))
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
        if isinstance(manifest.get("created"), int):
            self._learn_roster(manifest.get("participants"), manifest["created"])
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
            elif f.type == F.FrameType.DELIVERY:
                if self.mailbox.take_report(m):
                    report.deliveries.append(m["id"])
            elif media.collect_repair_frame(f, report.repair_requests):
                pass                                # a receiver asks this sender for a block repair
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

    async def _media_checks(self):
        """[(name, gen, ReceivedStream, block_size)] of received streams whose final check failed: the bundle asks
        the sender to repair them (STREAM_CHECK + BLOCK_SUMS)."""
        return []

    async def _media_repairs(self):
        """[(name, gen, path, request)] of block repairs a receiver asked this sender for."""
        return []

    async def _media_exported(self, report):
        """Hook after a bundle was written (``report.repaired``: repairs that fitted)."""

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
