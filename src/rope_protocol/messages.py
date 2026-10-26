"""Messages with custody (spec 8.5): small, self-contained items (e.g. a task or a note) with a unique id.

Every holder stores a message durably before it acknowledges it: the sender in its outbox, the hub, each recipient
in its inbox. Until acknowledged, a message is sent again after every reconnect; duplicates are recognised by id.

Delivery report: the hub tells the sender which recipients have a message (``DELIVERY``) whenever that grows, and
once at the start if some recipients are unknown or there is nobody to deliver to. The sender stores the report
durably and acknowledges it (``DELIVERY_ACK`` with the number of recipients it now knows as delivered).

Layout below ``root``::

    <id>.body    the raw body, written and synced first
    <id>.json    meta (id, from, to, t, kind), the holder's state, written atomically - its presence means "held"
"""

import asyncio
import json
import logging
import os
import threading
import uuid

from . import frames as F
from .channel import Wakeup, now_ms
from .store import write_json_atomic

log = logging.getLogger("rope.messages")

#: Unacknowledged MESSAGE frames in flight per connection.
WINDOW = 16


def new_id():
    return uuid.uuid4().hex


class MessageStore:
    """Durable messages of one holder. Thread-safe; meta and state are kept in memory, bodies are read on demand."""

    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self._lock = threading.Lock()
        self._items = {}
        for name in sorted(os.listdir(self.root)):
            if name.endswith(".json"):
                with open(os.path.join(self.root, name), encoding="utf-8") as f:
                    rec = json.load(f)
                self._items[rec["meta"]["id"]] = rec
            elif name.endswith(".tmp"):
                os.remove(os.path.join(self.root, name))            # crash during an atomic write

    def put(self, meta, body, state):
        """Take custody: store ``body`` and then ``meta``/``state`` durably. Returns False for a known id."""
        with self._lock:
            if meta["id"] in self._items:
                return False
            path = os.path.join(self.root, meta["id"] + ".body")
            with open(path, "wb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
            rec = dict(meta=dict(meta), state=dict(state))
            write_json_atomic(os.path.join(self.root, meta["id"] + ".json"), rec)
            self._items[meta["id"]] = rec
            return True

    def body(self, msg_id):
        with open(os.path.join(self.root, msg_id + ".body"), "rb") as f:
            return f.read()

    def update(self, msg_id, **state):
        self.modify(msg_id, lambda st: st.update(state))

    def modify(self, msg_id, change):
        """Apply ``change(state)`` and store the result durably, as one step (no lost updates between threads)."""
        with self._lock:
            rec = self._items.get(msg_id)
            if rec is None:
                return
            change(rec["state"])
            write_json_atomic(os.path.join(self.root, msg_id + ".json"), rec)

    def get(self, msg_id):
        with self._lock:
            rec = self._items.get(msg_id)
            return None if rec is None else json.loads(json.dumps(rec))

    def select(self, predicate):
        """Copies of the records for which ``predicate(record)`` is true, oldest first."""
        with self._lock:
            recs = [json.loads(json.dumps(r)) for r in self._items.values() if predicate(r)]
        return sorted(recs, key=lambda r: (r["meta"]["t"], r["meta"]["id"]))


class MessageSender:
    """Sends the messages ``pending()`` returns over one connection and reports acknowledgements to ``on_acked``.

    ``pending``: callable returning record copies to deliver to this peer. ``on_acked(msg_id)``: the peer has taken
    custody (called in a worker thread, so it may write to disk).
    """

    def __init__(self, store, pending, on_acked, poll_s=0.5, zlib_level=1):
        self.store = store
        self.pending = pending
        self.on_acked = on_acked
        self.poll_s = poll_s
        self.zlib_level = zlib_level
        self.wake = Wakeup()
        self._inflight = set()

    def start_session(self):
        self._inflight = set()

    async def handle_ack(self, msg_id):
        if msg_id in self._inflight:
            self._inflight.discard(msg_id)
            await asyncio.to_thread(self.on_acked, msg_id)
            self.wake.set()

    async def run(self, ch):
        while not ch.closed:
            self.wake.clear()
            for rec in self.pending():
                if len(self._inflight) >= WINDOW:
                    break
                m = rec["meta"]
                if m["id"] in self._inflight:
                    continue
                body = await asyncio.to_thread(self.store.body, m["id"])
                frame = await asyncio.to_thread(F.message, m["id"], m["from"], m["to"], m["t"], m["kind"], body,
                                                self.zlib_level)
                await ch.send(frame)
                self._inflight.add(m["id"])
            await self.wake.wait(self.poll_s)
        raise ConnectionError("connection closed")


def needs_report(state):
    """True if the hub owes the sender a delivery report for a message with this custody state."""
    n = len(state.get("delivered", ()))
    return n > state.get("reported", -1) and (n > 0 or bool(state.get("unknown")) or not state.get("pending"))


def report_frame(rec):
    st = rec["state"]
    return F.Frame(F.FrameType.DELIVERY, dict(id=rec["meta"]["id"], delivered=list(st["delivered"]),
                                              pending=list(st["pending"]), unknown=list(st["unknown"])))


class ReportSender:
    """At the hub: sends the delivery reports ``pending()`` returns to the sender of the messages over one connection
    and records acknowledgements with ``on_acked(msg_id, delivered)`` (called in a worker thread)."""

    def __init__(self, pending, on_acked, poll_s=0.5):
        self.pending = pending
        self.on_acked = on_acked
        self.poll_s = poll_s
        self.wake = Wakeup()
        self._inflight = set()

    def start_session(self):
        self._inflight = set()

    async def handle_ack(self, msg_id, delivered):
        self._inflight.discard(msg_id)
        await asyncio.to_thread(self.on_acked, msg_id, delivered)
        self.wake.set()

    async def run(self, ch):
        while not ch.closed:
            self.wake.clear()
            for rec in await asyncio.to_thread(self.pending):
                if len(self._inflight) >= WINDOW:
                    break
                if rec["meta"]["id"] in self._inflight:
                    continue
                await ch.send(report_frame(rec))
                self._inflight.add(rec["meta"]["id"])
            await self.wake.wait(self.poll_s)
        raise ConnectionError("connection closed")


async def receive(store, ch, f, state):
    """Store an incoming MESSAGE durably (new ids only), then acknowledge. Returns (meta, body) if it was new."""
    body = F.message_body(f)                            # FrameError on damage -> ERROR, connection closed
    meta = {k: f.meta[k] for k in ("id", "from", "to", "t", "kind")}
    new = await asyncio.to_thread(store.put, meta, body, state)
    await ch.send(F.Frame(F.FrameType.MESSAGE_ACK, {"id": meta["id"]}))
    return (meta, body) if new else None


class Mailbox:
    """Outbox and inbox of a participant (spec 8.5). ``on_message(meta, body)`` is called once per new message,
    ``on_delivery(msg_id, report)`` once per new delivery report of a sent message (``report``: delivered, pending,
    unknown - lists of participants).

    The inbox records whether the application was told about a message. If the connection breaks between storing
    and telling, the message is stored but untold; a resent copy is a duplicate. ``notify()`` therefore tells the
    application about every untold message - after each received message and when a session starts.
    """

    def __init__(self, root, participant, on_message=None, poll_s=0.5, on_delivery=None):
        self.participant = participant
        self.on_message = on_message
        self.on_delivery = on_delivery
        self.store = MessageStore(root)
        self.sender = MessageSender(self.store, self.pending, self._acked, poll_s)
        self._notify_lock = asyncio.Lock()     # a medium import and a network session may notify at the same time
        # told in this run, noted without a pause after telling: a session cancelled while the "told" flag is being
        # written must not lead the next session to tell again (found by the test bench, spec 13)
        self._told = set()

    def send(self, to, kind, body):
        """Put a message into the outbox (durably) and return its id. It leaves with the next connection."""
        meta = {"id": new_id(), "from": self.participant, "to": list(to), "t": now_ms(), "kind": kind}
        F.validate(F.message(meta["id"], meta["from"], meta["to"], meta["t"], kind, b""))    # reject bad input now
        if len(body) > F.MAX_BLOB:
            raise ValueError("message body larger than %d bytes" % F.MAX_BLOB)
        self.store.put(meta, bytes(body), {"box": "out", "at_hub": False})
        self.sender.wake.set()
        return meta["id"]

    def outbox(self):
        """[{id, to, kind, t, at_hub, report}] of the messages sent from here. ``report`` is the hub's latest delivery
        report ({delivered, pending, unknown}) or None; the message reached everybody when ``pending`` is empty."""
        return [dict(rec["meta"], at_hub=rec["state"]["at_hub"], report=rec["state"].get("report"))
                for rec in self.store.select(lambda r: r["state"].get("box") == "out")]

    def inbox(self):
        """[{id, from, to, t, kind}] of the messages received, oldest first; ``body(id)`` for the content."""
        return [rec["meta"] for rec in self.store.select(lambda r: r["state"].get("box") == "in")]

    def body(self, msg_id):
        return self.store.body(msg_id)

    async def notify(self):
        """Tell the application about every stored message it was not told about yet (at least once)."""
        async with self._notify_lock:
            for rec in self.store.select(lambda r: r["state"].get("box") == "in" and not r["state"].get("told", True)):
                msg_id = rec["meta"]["id"]
                if msg_id in self._told:
                    await asyncio.to_thread(self.store.update, msg_id, told=True)
                    continue
                body = await asyncio.to_thread(self.store.body, msg_id) if self.on_message else None
                if self.on_message:
                    self.on_message(rec["meta"], body)
                self._told.add(msg_id)
                await asyncio.to_thread(self.store.update, msg_id, told=True)
            for rec in self.store.select(lambda r: r["state"].get("box") == "out"
                                         and r["state"].get("report_told") is False):
                msg_id = rec["meta"]["id"]
                key = ("report", msg_id, len(rec["state"]["report"]["delivered"]))
                if key not in self._told:
                    if self.on_delivery:
                        self.on_delivery(msg_id, rec["state"]["report"])
                    self._told.add(key)
                await asyncio.to_thread(self.store.update, msg_id, report_told=True)

    def pending(self):
        """Records of the outbox messages the hub has not taken into custody yet."""
        return self.store.select(lambda r: r["state"].get("box") == "out" and not r["state"]["at_hub"])

    def _acked(self, msg_id):
        self.store.update(msg_id, at_hub=True)

    def take_report(self, meta):
        """Store a delivery report of an own message durably unless an equal or newer one is known (e.g. an old stick).
        Returns True if it was new."""
        rec = self.store.get(meta["id"])
        if rec is None or rec["state"].get("box") != "out":
            return False
        new = {k: list(meta[k]) for k in ("delivered", "pending", "unknown")}
        changed = []

        def change(state):
            old = state.get("report")
            if old is None or len(new["delivered"]) > len(old["delivered"]):
                state.update(report=new, report_told=False, at_hub=True)
                changed.append(True)
        self.store.modify(meta["id"], change)
        return bool(changed)

    def report_acks(self):
        """DELIVERY_ACK frames for every report held (for a bundle: the hub ignores what it already knows)."""
        return [F.Frame(F.FrameType.DELIVERY_ACK, dict(id=rec["meta"]["id"],
                                                       delivered=len(rec["state"]["report"]["delivered"])))
                for rec in self.store.select(lambda r: r["state"].get("box") == "out" and r["state"].get("report"))]

    async def handle(self, ch, f):
        """MESSAGE from the hub or MESSAGE_ACK for an own message. Returns False for other frames."""
        if f.type == F.FrameType.MESSAGE:
            await receive(self.store, ch, f, {"box": "in", "told": False})
            await self.notify()
            return True
        if f.type == F.FrameType.MESSAGE_ACK:
            await self.sender.handle_ack(f.meta["id"])
            return True
        if f.type == F.FrameType.DELIVERY:
            await asyncio.to_thread(self.take_report, f.meta)
            await ch.send(F.Frame(F.FrameType.DELIVERY_ACK, dict(id=f.meta["id"],
                                                                 delivered=len(f.meta["delivered"]))))
            await self.notify()
            return True
        return False

