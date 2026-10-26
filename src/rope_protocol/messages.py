"""Messages with custody (spec 8.5): small, self-contained items (e.g. a task or a note) with a unique id.

Every holder stores a message durably before it acknowledges it: the sender in its outbox, the hub, each recipient
in its inbox. Until acknowledged, a message is sent again after every reconnect; duplicates are recognised by id.

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


async def receive(store, ch, f, state):
    """Store an incoming MESSAGE durably (new ids only), then acknowledge. Returns (meta, body) if it was new."""
    body = F.message_body(f)                            # FrameError on damage -> ERROR, connection closed
    meta = {k: f.meta[k] for k in ("id", "from", "to", "t", "kind")}
    new = await asyncio.to_thread(store.put, meta, body, state)
    await ch.send(F.Frame(F.FrameType.MESSAGE_ACK, {"id": meta["id"]}))
    return (meta, body) if new else None


class Mailbox:
    """Outbox and inbox of a participant (spec 8.5). ``on_message(meta, body)`` is called once per new message.

    The inbox records whether the application was told about a message. If the connection breaks between storing
    and telling, the message is stored but untold; a resent copy is a duplicate. ``notify()`` therefore tells the
    application about every untold message - after each received message and when a session starts.
    """

    def __init__(self, root, participant, on_message=None, poll_s=0.5):
        self.participant = participant
        self.on_message = on_message
        self.store = MessageStore(root)
        self.sender = MessageSender(self.store, self._pending, self._acked, poll_s)

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
        """[{id, to, kind, t, at_hub}] of the messages sent from here."""
        return [dict(rec["meta"], at_hub=rec["state"]["at_hub"])
                for rec in self.store.select(lambda r: r["state"].get("box") == "out")]

    def inbox(self):
        """[{id, from, to, t, kind}] of the messages received, oldest first; ``body(id)`` for the content."""
        return [rec["meta"] for rec in self.store.select(lambda r: r["state"].get("box") == "in")]

    def body(self, msg_id):
        return self.store.body(msg_id)

    async def notify(self):
        """Tell the application about every stored message it was not told about yet (at least once)."""
        for rec in self.store.select(lambda r: r["state"].get("box") == "in" and not r["state"].get("told", True)):
            msg_id = rec["meta"]["id"]
            if self.on_message:
                body = await asyncio.to_thread(self.store.body, msg_id)
                self.on_message(rec["meta"], body)
            await asyncio.to_thread(self.store.update, msg_id, told=True)

    def _pending(self):
        return self.store.select(lambda r: r["state"].get("box") == "out" and not r["state"]["at_hub"])

    def _acked(self, msg_id):
        self.store.update(msg_id, at_hub=True)

    async def handle(self, ch, f):
        """MESSAGE from the hub or MESSAGE_ACK for an own message. Returns False for other frames."""
        if f.type == F.FrameType.MESSAGE:
            await receive(self.store, ch, f, {"box": "in", "told": False})
            await self.notify()
            return True
        if f.type == F.FrameType.MESSAGE_ACK:
            await self.sender.handle_ack(f.meta["id"])
            return True
        return False

