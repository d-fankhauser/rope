"""Hub: accepts participants from the participant list, stores the sources' streams durably and relays them to the
consumers; takes custody of messages and delivers them to each recipient (spec 7, 8).

To a consumer, the hub is a sender like a source is to the hub: same window, resume, final check and block repair
(``rope_protocol.sender``). A relayed stream is named ``<source>/<stream>`` so that streams of different sources never
mix. The hub keeps no state per consumer: the consumer reports what it holds when it greets (spec 14.5). The
participant list (``rope_protocol.roster``) may restrict which streams a consumer receives.

Offline media (spec 8.6): ``import_media`` reads the bundles of all participants, ``export_media`` writes one for a
participant. For media only, the hub remembers what a consumer last reported in a bundle (``media_have.json``), so
that its next bundle starts there.
"""

import asyncio
import json
import logging
import os
import threading

from . import PROTOCOL_VERSION, DEFAULT_PORT
from . import frames as F
from . import media
from . import messages as M
from . import roster
from .channel import Channel, now_ms, reap, serve_frames
from .receiver import BLOCK_SIZE, StreamReceiver
from .sender import OutStream, StreamSender
from .store import StreamStore, write_json_atomic

log = logging.getLogger("rope.hub")

#: Longest stream name in bytes (spec 14.4); a relayed name adds the source identifier and "/".
MAX_STREAM_BYTES = 255


def relayed_name(source, stream):
    return "%s/%s" % (source, stream)


class Hub:
    """``participants``: {identifier: role} or {identifier: {"role", "name", "streams"}} (``rope_protocol.roster``,
    spec 7); ValueError with a plain-language reason if the list is not valid. ``hub_name`` is the hub's name for the
    operator. ``on_stream_checked(participant, stream, gen, result, path)`` is called after the final check of a
    source's stream (spec 8.4)."""

    def __init__(self, root, participants, operation, hub_id="hub", host="127.0.0.1", port=DEFAULT_PORT,
                 heartbeat_s=2.0, timeout_s=10.0, on_stream_checked=None, block_size=BLOCK_SIZE,
                 segment_bytes=256 * 1024, window=8, poll_s=0.5, zlib_level=1, hub_name=None):
        self.roster = roster.parse(participants, hub_id, hub_name)
        self.store = StreamStore(root)
        self._media_have_path = os.path.join(os.path.abspath(root), "media_have.json")
        self._media_lock = threading.Lock()
        self.messages = M.MessageStore(os.path.join(root, "messages"))
        self.participants = {p: e.role for p, e in self.roster.items()}       # {identifier: role}
        self.operation = operation
        self.hub_id = hub_id
        self.hub_name = hub_name
        self.host = host
        self.port = port
        self.heartbeat_s = heartbeat_s
        self.timeout_s = timeout_s
        self.on_stream_checked = on_stream_checked
        self.block_size = block_size
        self.segment_bytes = segment_bytes
        self.window = window
        self.poll_s = poll_s
        self.zlib_level = zlib_level
        self._server = None
        self._sessions = {}           # participant -> task of its current session
        self._connections = set()     # every connection task, also those still greeting
        self._relays = set()          # StreamSender of every connected consumer
        self._mail = {}               # participant -> MessageSender of its current session
        self.sessions_started = 0

    async def start(self):
        try:
            self._server = await asyncio.start_server(self._accept, self.host, self.port)
        except OSError as e:
            raise OSError(e.errno, "ROPE hub cannot listen on %s port %d: %s" % (self.host, self.port, e.strerror)) \
                from None
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self._server:
            self._server.close()
        tasks = list(self._connections)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=5)
        if self._server:
            try:
                await asyncio.wait_for(self._server.wait_closed(), 5)
            except asyncio.TimeoutError:
                log.warning("hub stop: connections did not close within 5 s")

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    async def _accept(self, reader, writer):
        ch = Channel(reader, writer, send_timeout_s=self.timeout_s)
        participant = None
        me = asyncio.current_task()
        self._connections.add(me)
        try:
            hello = await ch.recv(self.timeout_s)
            if hello.type != F.FrameType.HELLO:
                await ch.error_and_close("bad_frame", "the first frame must be HELLO")
                return
            m = hello.meta
            reason = self._check(m)
            if reason:
                code, text = reason
                await ch.send(F.Frame(F.FrameType.REJECT, {"code": code, "text": text}))
                ch.abort()
                return
            participant = m["participant"]
            # a participant has one session: a new greeting replaces a stale one (e.g. after a radio dropout)
            old = self._sessions.get(participant)
            if old is not None and old is not asyncio.current_task():
                old.cancel()
                try:
                    await old
                except BaseException:
                    pass
            self._sessions[participant] = asyncio.current_task()
            self.sessions_started += 1
            ch.version = max(set(m["versions"]) & {PROTOCOL_VERSION})
            have = await asyncio.to_thread(self.store.have, participant)
            await ch.send(F.Frame(F.FrameType.WELCOME, dict(
                version=ch.version, hub=self.hub_id, t=now_ms(), t_hello=m["t"], heartbeat_s=self.heartbeat_s,
                timeout_s=self.timeout_s, have=have, participants=self.participant_list())))
            mail = M.MessageSender(self.messages, lambda: self._mail_for(participant),
                                   lambda msg_id: self._delivered(msg_id, participant), self.poll_s, self.zlib_level)
            mail.reports = M.ReportSender(lambda: self._reports_for(participant),
                                          lambda msg_id, n: self._report_acked(msg_id, n, participant), self.poll_s)
            self._mail[participant] = mail
            try:
                serve = dict(consumer=self._serve_consumer, source=self._serve_source,
                             recipient=self._serve_recipient)[m["role"]]
                await serve(ch, participant, m, mail)
            finally:
                if self._mail.get(participant) is mail:
                    del self._mail[participant]
        except asyncio.CancelledError:
            ch.abort()
        except F.FrameError as e:
            await ch.error_and_close(e.code, e.text)
        except (ConnectionError, OSError) as e:
            log.info("connection of %s ended: %s", participant or "unknown participant", e)
            ch.abort()
        finally:
            ch.abort()                                  # every way out closes the connection
            self._connections.discard(me)
            if participant and self._sessions.get(participant) is me:
                del self._sessions[participant]

    def _check(self, m):
        p = m["participant"]
        if p not in self.participants:
            return "unknown_participant", "%s is not on the participant list of this hub" % p
        if m["operation"] != self.operation:
            return "wrong_operation", "%s belongs to operation %s, this hub serves %s" % (p, m["operation"],
                                                                                          self.operation)
        if PROTOCOL_VERSION not in m["versions"]:
            return "version", "%s speaks protocol versions %s, this hub speaks %d" % (p, m["versions"],
                                                                                     PROTOCOL_VERSION)
        if m["role"] != self.participants[p]:
            return "role", "%s (%s) greets as %s but is listed as %s" % (p, self.roster[p].name, m["role"],
                                                                         self.participants[p])
        if m["role"] not in ("source", "consumer", "recipient"):
            return "role", "this hub does not serve the role %s" % m["role"]
        return None

    def participant_list(self):
        """[{participant, name, role}] as every participant learns it, the hub first (role ``hub``)."""
        return roster.public(self.roster, self.hub_id, self.hub_name)

    def receives(self, participant, stream):
        """True if ``participant`` receives the relayed stream ``<source>/<stream>`` (spec 7)."""
        return roster.receives(self.roster.get(participant), stream)

    async def _run_session(self, ch, participant, mail, handle, tasks=()):
        """Receive frames (messages first, then ``handle``) and run the session's tasks, plus heartbeat and message
        delivery, until one ends; re-raise why it ended."""
        async def both(f):
            return await self._mail_frame(ch, participant, mail, f) or await handle(f)
        mail.start_session()
        mail.reports.start_session()
        tasks = [asyncio.ensure_future(ch.heartbeat(self.heartbeat_s)),
                 asyncio.ensure_future(mail.run(ch)),
                 asyncio.ensure_future(mail.reports.run(ch)),
                 asyncio.ensure_future(serve_frames(ch, self.timeout_s, both, self.participants[participant]))] \
            + list(tasks)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                t.result()
        finally:
            await reap(tasks)

    # --- from a source -------------------------------------------------------------------------------------------
    async def _serve_source(self, ch, participant, hello, mail):
        receiver = StreamReceiver(self.store, participant, self.block_size, on_written=self._written,
                                  on_checked=self.on_stream_checked, me="hub")

        async def handle(f):
            if f.type in (F.FrameType.SEGMENT, F.FrameType.STREAM_END):
                name = relayed_name(participant, f.meta["stream"])
                if len(name.encode("utf-8")) > MAX_STREAM_BYTES:
                    raise F.FrameError("bad_frame", "stream name %r is too long to relay: at most %d bytes "
                                       "including the source identifier and '/'" % (f.meta["stream"],
                                                                                   MAX_STREAM_BYTES))
            return await receiver.handle(ch, f)
        await self._run_session(ch, participant, mail, handle)

    def _written(self, *_):
        for relay in self._relays:
            relay.wake.set()

    # --- to a consumer -------------------------------------------------------------------------------------------
    async def _serve_consumer(self, ch, participant, hello, mail):
        outs = {}                                       # (relayed name, gen) -> OutStream, this session only

        async def refresh():
            entries = await asyncio.to_thread(self.store.entries)
            for e in entries:
                if self.participants.get(e["participant"]) != "source":
                    continue
                key = (relayed_name(e["participant"], e["stream"]), e["gen"])
                if not self.receives(participant, key[0]):
                    continue
                out = outs.get(key)
                if out is None:
                    out = outs[key] = OutStream(key[0], key[1], e["path"])
                out.size = e["committed"]
                if e["complete"] and e["verified"] == "ok":
                    out.final_size = e["committed"]     # verified at the hub: the consumer may check it, too
            return [outs[k] for k in sorted(outs)]

        sender = StreamSender(refresh, self.segment_bytes, self.window, self.poll_s, self.zlib_level,
                              peer=participant)
        have = {(h["stream"], h["gen"]): h["committed"] for h in hello["have"]}
        sender.start_session(await refresh(), have)

        async def handle(f):
            return sender.handle(f)
        self._relays.add(sender)
        try:
            await self._run_session(ch, participant, mail, handle, [asyncio.ensure_future(sender.run(ch))])
        finally:
            self._relays.discard(sender)

    # --- recipients and messages ---------------------------------------------------------------------------------
    async def _serve_recipient(self, ch, participant, hello, mail):
        async def handle(f):
            return False
        await self._run_session(ch, participant, mail, handle)

    async def _mail_frame(self, ch, participant, mail, f):
        """MESSAGE from the participant (custody) or MESSAGE_ACK for a delivered one. False for other frames."""
        if f.type == F.FrameType.MESSAGE:
            if f.meta["from"] != participant:
                raise F.FrameError("bad_frame", "%s sent a message from %s" % (participant, f.meta["from"]))
            state = self._custody_state(participant, f.meta["to"])
            if await M.receive(self.messages, ch, f, state):
                self._new_message(f.meta, state)
            return True
        if f.type == F.FrameType.MESSAGE_ACK:
            await mail.handle_ack(f.meta["id"])
            self._wake_report(f.meta["id"])
            return True
        if f.type == F.FrameType.DELIVERY_ACK:
            await mail.reports.handle_ack(f.meta["id"], f.meta["delivered"])
            return True
        return False

    def _custody_state(self, sender, to):
        """State of a new message at the hub: known recipients pending, the others unknown (spec 8.5)."""
        return dict(pending=[p for p in dict.fromkeys(to) if p in self.participants and p != sender], delivered=[],
                    unknown=[p for p in dict.fromkeys(to) if p not in self.participants])

    def _new_message(self, meta, state):
        if state["unknown"]:
            log.warning("message %s from %s: %s not on the participant list, not delivered",
                        meta["id"], meta["from"], ", ".join(state["unknown"]))
        for p in state["pending"]:
            if p in self._mail:
                self._mail[p].wake.set()
        if M.needs_report(state):
            self._wake_report(meta["id"])

    def _mail_for(self, participant):
        return self.messages.select(lambda r: participant in r["state"].get("pending", ()))

    def _delivered(self, msg_id, participant):
        def change(state):
            if participant in state["pending"]:
                state["pending"].remove(participant)
                state["delivered"].append(participant)
        self.messages.modify(msg_id, change)

    # --- delivery reports to the sender (spec 8.5) ------------------------------------------------------------------
    def _reports_for(self, participant):
        """Records of the messages from ``participant`` whose delivery report it has not acknowledged yet."""
        return self.messages.select(lambda r: r["meta"]["from"] == participant and M.needs_report(r["state"]))

    def _report_acked(self, msg_id, delivered, participant):
        rec = self.messages.get(msg_id)
        if rec is None or rec["meta"]["from"] != participant:
            return

        def change(state):
            state["reported"] = max(state.get("reported", -1), delivered)
        self.messages.modify(msg_id, change)

    def _wake_report(self, msg_id):
        """In the event loop: the report of ``msg_id`` may have changed - wake its sender's session, if connected."""
        rec = self.messages.get(msg_id)
        mail = rec and self._mail.get(rec["meta"]["from"])
        if mail is not None:
            mail.reports.wake.set()

    def message_status(self, msg_id):
        """{pending, delivered, unknown} of a message the hub holds, or None."""
        rec = self.messages.get(msg_id)
        return None if rec is None else rec["state"]

    # --- offline media (spec 8.6) --------------------------------------------------------------------------------
    async def import_media(self, medium):
        """Import every bundle on ``medium`` addressed to this hub, oldest first, as if it had arrived over the
        network: streams from sources, messages from everyone, receipts. Importing a bundle again changes nothing.
        Returns one ImportReport per bundle; ``problems`` lists what was skipped and why."""
        reports = []
        for path, manifest in await asyncio.to_thread(media.find_bundles, medium, self.operation, self.hub_id):
            report = await asyncio.to_thread(self._import_media, path, manifest)
            if report.written:
                self._written()
            if self.on_stream_checked:
                for owner, stream, gen, result, data_path in report.checked:
                    self.on_stream_checked(owner, stream, gen, result, data_path)
            for meta in report.messages:
                self._new_message(meta, self.messages.get(meta["id"])["state"])
            for msg_id in report.message_acks:
                self._wake_report(msg_id)
            if report.problems:
                log.warning("bundle %s: %s", path, "; ".join(report.problems))
            reports.append(report)
        return reports

    def _import_media(self, path, manifest):
        report = media.ImportReport(path, manifest)
        reason = media.check_manifest(manifest, self.operation, self.hub_id)
        sender = manifest and manifest.get("from")
        if reason is None and sender not in self.participants:
            reason = "the bundle is from %s, who is not on the participant list of this hub" % sender
        if reason:
            report.problems.append(reason)
            return report
        role = self.participants[sender]
        skipped = set()
        for f in media.read_frames(path, manifest, report):
            m = f.meta
            if f.type in (F.FrameType.SEGMENT, F.FrameType.STREAM_END) and role == "source":
                if len(relayed_name(sender, m["stream"]).encode("utf-8")) > MAX_STREAM_BYTES:
                    if (m["stream"], m["gen"]) not in skipped:
                        report.problems.append("stream name %r is too long to relay - skipped" % m["stream"])
                        skipped.add((m["stream"], m["gen"]))
                elif f.type == F.FrameType.SEGMENT:
                    media.import_segment(self.store, sender, f, report, skipped)
                else:
                    media.import_stream_end(self.store, sender, f, report, skipped)
            elif f.type == F.FrameType.MESSAGE:
                media.import_message(self.messages, f, report, self._custody_state(sender, m["to"]), sender)
            elif f.type == F.FrameType.MESSAGE_ACK:
                self._delivered(m["id"], sender)
                report.message_acks.append(m["id"])
            elif f.type == F.FrameType.DELIVERY_ACK:
                self._report_acked(m["id"], m["delivered"], sender)
                report.delivery_acks.append(m["id"])
            elif f.type == F.FrameType.ACK and role == "consumer":
                report.acks.append((m["stream"], m["gen"], m["committed"]))
            else:
                report.problems.append("unexpected %s from a %s - skipped" % (F.FrameType(f.type).name, role))
        if report.acks:
            self._remember_media_have(sender, report.acks)
        return report

    async def export_media(self, medium, participant, max_bytes=None):
        """Write a bundle for ``participant`` to the folder ``medium``: messages pending for it, receipts and delivery
        reports for the messages it sent, and what the role needs (a source: what the hub holds of its streams; a consumer: the
        relayed streams from what it last reported in a bundle, final checks only for streams verified here).
        ``max_bytes`` limits the bundle; the report says whether everything fitted. Returns an ExportReport."""
        role = self.participants.get(participant)
        if role is None:
            raise ValueError("%s is not on the participant list of this hub" % participant)
        return await asyncio.to_thread(self._export_media, medium, participant, role, max_bytes)

    def _export_media(self, medium, participant, role, max_bytes):
        w = media.BundleWriter(medium, self.operation, self.hub_id, participant, max_bytes)
        try:
            receipts = self.messages.select(lambda r: r["meta"]["from"] == participant)
            frames = [F.Frame(F.FrameType.MESSAGE_ACK, {"id": r["meta"]["id"]}) for r in receipts]
            frames += [M.report_frame(r) for r in self._reports_for(participant)]
            if role == "source":
                frames += [F.Frame(F.FrameType.ACK, h) for h in self.store.have(participant)]
            ok = all(w.add(f) for f in frames)
            ok = ok and media.add_messages(w, self.messages, self._mail_for(participant), self.zlib_level)
            if role == "consumer":
                have = self._media_have().get(participant, {})
                for e in sorted(self.store.entries(), key=lambda e: (e["participant"], e["stream"], e["gen"])):
                    if self.participants.get(e["participant"]) != "source":
                        continue
                    name = relayed_name(e["participant"], e["stream"])
                    if not self.receives(participant, name):
                        continue
                    start = min(have.get("%s\0%d" % (name, e["gen"]), 0), e["committed"])
                    final = e["committed"] if e["complete"] and e["verified"] == "ok" else None
                    ok = ok and media.add_stream(w, name, e["gen"], e["path"], start, e["committed"], final,
                                                 self.zlib_level)
            return w.close(participants=self.participant_list())
        except BaseException:
            w.abort()
            raise

    def _media_have(self):
        """{consumer: {"<stream>\\0<gen>": committed}} as last reported in bundles."""
        try:
            with open(self._media_have_path, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _remember_media_have(self, participant, acks):
        with self._media_lock:
            all_have = self._media_have()
            mine = all_have.setdefault(participant, {})
            for stream, gen, committed in acks:
                k = "%s\0%d" % (stream, gen)
                mine[k] = max(mine.get(k, 0), committed)
            write_json_atomic(self._media_have_path, all_have)
