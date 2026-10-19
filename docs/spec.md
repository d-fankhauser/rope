<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/rope-logo-dark.svg">
  <img alt="ROPE" src="images/rope-logo.svg" height="56">
</picture>

# ROPE – Protocol Specification

**Status: draft 0 (concept).** This document describes the goals, model and behaviour of ROPE. The wire format
(framing, message encoding, version negotiation details) is not yet defined – see [Open points](#open-points).

Every statement is marked as one of:

- **decided** – fixed by the maintainer,
- **proposed** – a starting point, may change after measurements or review,
- **assumed** – an estimate that has not been measured yet.

## 1. Purpose

ROPE (*Resumable Offline Protocol for Emergencies*) transfers growing files ("streams") and small messages between a
small, closed group of devices over an unreliable local network – for example radio links or Wi-Fi between a boat,
a command post and a tablet – without any internet access.

The motivating application is the transfer of side-scan sonar recordings while they are being recorded, but the
protocol core knows nothing about sonar (**decided**). An application defines which streams and messages exist.

## 2. Requirements (decided)

1. **Survive interruptions.** If the link drops, e.g. for 0.5 s or for an hour, nothing is lost and nothing restarts
   from zero.
2. **Never block the producer.** The device that records data must never hang or slow down noticeably because of the
   transfer.
3. **Offline.** Works completely without internet; being on the same network does not imply internet access.
4. **More than two devices.** Several participants (e.g. a data source, a hub, a display device) – but always a
   closed, pre-selected group.
5. **No encryption,** as simple as possible (see [Security considerations](#11-security-considerations)).
6. **Built for the future.** Multiple participants, a protocol version and an application-agnostic core are part of
   the design from the start, even if the first deployment has only two participants.

## 3. Why not plain TCP or UDP?

| Problem | TCP alone | UDP alone |
|---|---|---|
| Longer outage | Connection breaks; afterwards TCP does not know where it was. Without extra logic the transfer starts over. | Packets sent during the outage are gone; gaps appear. |
| Acknowledgement | Means "arrived in the network buffer", not "safely on disk". If the receiver crashes, data counted as delivered is missing. | Does not exist. |
| Order, completeness | Guaranteed, but only within one connection. | Not guaranteed. |
| Files and growing files | Unknown – TCP only carries a byte stream. | Same. |
| End-to-end check of a whole file | None. | None. |
| Several devices, store-and-forward | Always exactly two endpoints; nothing waits for a participant that is currently away. | Same. |
| Congestion | Built-in rate control. | None – can saturate a radio link. |
| Hanging | A program waiting on the network stalls unless built otherwise. | Same. |

**Conclusion:** TCP is the better foundation – ordering, retransmission and rate control already exist. ROPE adds what
is missing on top. A UDP-based design would have to rebuild retransmission, ordering and congestion control, which only
pays off with very long round-trip times (space links).

## 4. Prior art

The ideas – not the code or the formats – come from space communication, where links are interrupted routinely:
CCSDS File Delivery Protocol (CFDP) class 2 (acknowledged mode: the receiver reports missing parts, which are resent
selectively) and Delay/Disruption Tolerant Networking (DTN, custody transfer). ESA's Euclid mission, for example,
delivers its files with CFDP class 2 over a K-band link.

Not adopted: the standards themselves. They target dedicated hardware, radio frames and round-trip times of seconds to
minutes. ROPE stays small and manageable and is therefore **not compatible** with CFDP or DTN software (**decided**).

## 5. Design decisions

| Topic | Decision | Reason |
|---|---|---|
| Transport | TCP (**decided**) | Short round-trip times on local radio networks (**assumed**). TCP bridges short dropouts by itself. |
| Own protocol | Yes, as a separate package (**decided**) | TCP cannot resume across connections and does not acknowledge persistence. Existing solutions are too large or need internet. |
| Standards | Adopt ideas, not standards (**decided**) | Small and controllable. |
| Topology | Star with a hub (**decided**) | Simple responsibilities and troubleshooting; every device has exactly one peer. |
| Participants | Only from a list prepared in advance (**decided**) | Prevents mix-ups on the network. Without encryption no protection against intent. |
| Encryption | None (**decided**) | As simple as possible in the field. |
| Dependencies | Python standard library only (**decided**) | Offline installation, small programs, runs on old hardware. |
| Network file shares (SMB etc.) | Not used (**decided**) | Programs hang on file access when the link is unstable. |
| Who connects | Every participant connects to the hub (**decided**) | The hub has a known address; participants may come and go. |
| Licence | Apache-2.0 (**decided**) | Permissive; proprietary applications may use the core. |

## 6. Architecture and terms

Two layers (**decided:** separate package; **proposed:** this split):

```
+--------------------------------------------+
| Application  (defines streams + messages)  |
+--------------------------------------------+
| ROPE core    (resume, custody, delivery)   |
+--------------------------------------------+
| TCP                                        |
+--------------------------------------------+
```

The core does not rebuild what TCP provides. It adds what TCP lacks: resuming across connections, acknowledgement only
after persistence, end-to-end verification, and delivery to participants that are currently unreachable.

**Terms**

- **Participant** – a device of the group with a unique identifier.
- **Hub** – the participant all others connect to; relays streams and messages.
- **Stream** – a sequence of bytes that only grows at the end (an append-only file).
- **Segment** – a piece of a stream with offset, length, checksum and timestamp. The application may choose segment
  boundaries (e.g. at record boundaries).
- **Committed offset** – up to which byte a receiver has stored a stream durably.
- **Message** – a small, self-contained item with a unique identifier (e.g. a task or a note).
- **Custody** – a participant takes over data or a message only once it is stored durably on disk; only then does it
  acknowledge.

## 7. Star topology

All participants connect only to the hub (**decided**). Data and messages between two participants always travel via
the hub.

- **Participant list (proposed):** identifier, name, role and what the participant receives. Anyone not on the list is
  rejected during the greeting with a plain-language reason.
- **Roles (proposed):**
  - *source* – provides streams,
  - *consumer* – receives streams and may send messages,
  - *recipient* – receives messages only.
- **Need-to-know:** raw streams only to consumers; messages also to recipients.
- **Several consumers:** a second consumer receives the same streams via the hub.
- **Group size:** a few devices, no open membership (**decided**).
- **Hub down:** all participants keep collecting and send as soon as the hub is back. A standby hub is an open point.

## 8. Procedures

All numbers are **proposed** starting values until measured.

### 8.1 Connect and greet

1. The participant connects to the hub. The address comes from the prepared configuration, optionally discovered by
   broadcast on the local network.
2. It sends its identifier, role, protocol version and an operation identifier (separates independent operations).
3. The hub checks the participant list and the version. On mismatch it replies with a plain-language reason and closes.
4. The hub reports, per stream, its committed offset and, per message, what it already holds. The connection is then
   active.

### 8.2 Sending a stream

- The source sends each stream starting at the reported committed offset.
- Segments hold roughly one second of data, at most 256 KiB (**assumed** suitable), and are compressed individually.
- Every segment carries stream id, offset, raw and compressed length, checksum and timestamp.
- The source keeps only a bounded number of unacknowledged segments in flight, e.g. 8. This bounds memory and backlog.

### 8.3 Acknowledging

- The receiver verifies the checksum, writes the segment at its offset and makes it durable (flush + fsync).
- Only then does it acknowledge with the new committed offset. Duplicate or already present segments are acknowledged
  without writing.
- Gaps cannot arise because sending always starts at the committed offset. If a segment arrives damaged, the receiver
  requests exactly that range again.

### 8.4 Completing a stream

- When the stream is complete (application decides, e.g. file unchanged for 10 s), the source sends a checksum of the
  whole stream.
- The receiver compares. On mismatch both sides exchange per-block checksums and only differing blocks are resent.
- The stream is then marked complete and verified.

### 8.5 Messages

- Every message has a unique id, sender, recipient(s) and timestamp.
- The sender stores it durably in its outbox. The hub takes custody and acknowledges, then delivers it to each recipient
  individually.
- Each recipient acknowledges after storing it. The hub tracks "delivered" or "pending" per recipient.
- Messages take priority over stream segments.
- Duplicates are detected by id and shown only once.

### 8.6 Offline media as a second path

Removable media (e.g. a USB stick) can carry the same segments and messages. Import follows the same rules: check the
committed offset, ignore duplicates, report damaged items. Network and media may be mixed.

## 9. Interruptions and resumption

Core idea: the data lives on the source's disk and the receiver knows its committed offset. An interruption costs time,
never data.

Connection states (**proposed**): *disconnected* → *connecting* → *greeting* → *active* → back to *disconnected* on
any failure. Reconnect back-off grows from 1 s to at most 10 s and never gives up.

| Outage | What happens |
|---|---|
| Up to a few seconds, e.g. 0.5 s | TCP retransmits; the connection stays up. The core notices nothing. |
| No sign of life for about 10 s | Connection considered lost (heartbeat every 2 s, **assumed** suitable). Reconnect, greet, continue from the committed offset; unacknowledged segments are resent. |
| Minutes to hours | The source keeps recording; nothing piles up in memory. After reconnecting the backlog is transferred. |
| Device restart | Committed offsets and outbox are on disk; work continues from the last acknowledged point. |

**Catch-up order (proposed, open):** messages first, then the tail of the currently growing stream (so the consumer is
up to date), then the backlog in order.

## 10. Never block

The producer must never hang because of the transfer (**decided**). Measures (**proposed**):

- **Separate process,** independent of the recording software and of any UI. If it crashes, recording continues; it
  restarts itself (service or scheduled task).
- **Low priority** for CPU and disk I/O.
- **Read-only, no locks:** open files briefly and close them; watch growing files by size every 0.5–1 s.
- **Network never in the read path:** reading, packing and sending are decoupled; all network calls have timeouts. If
  the network hangs, only sending waits.
- **Hard limits** for unacknowledged segments, memory and bandwidth, configurable.
- **Fast compression,** e.g. zlib at a low level (**assumed**; to be measured on target hardware).
- **No in-memory buffer for outages:** the file on disk is the buffer.
- **Receiver likewise:** writing and syncing are decoupled from display and processing.

## 11. Security considerations

ROPE deliberately has **no encryption and no authentication** beyond the participant list (**decided**). It is meant
for closed, physically controlled networks. Anyone on the same network can read the traffic and could impersonate a
participant. Do not use ROPE over the internet or on shared networks without an additional protection layer (e.g. a
VPN). The participant list prevents accidental mix-ups, not attacks.

## 12. Failure cases

| Case | Detection | Reaction |
|---|---|---|
| Short dropout | none needed | TCP bridges it |
| Dead connection | no heartbeat for about 10 s | disconnect, reconnect, continue from committed offset |
| Damaged segment | checksum mismatch | request that range again, keep the rest |
| Duplicate segment | offset already stored | acknowledge, do not write |
| Whole stream differs | final checksum | compare per-block checksums, resend only differing blocks |
| Source file shortened or replaced | file smaller than committed offset | never overwrite; report in plain language; keep as a new version |
| Unknown participant | not on the list | reject with a plain-language reason |
| Different protocol version | greeting | reject or operate on the common subset, with a plain-language reason |
| Wrong operation | operation identifier | store separately, never mix |
| Receiver disk full | write fails | do not acknowledge, report; source waits |
| Recipient offline | no acknowledgement | hub keeps custody and delivers later |
| Hub offline | no connection | participants keep collecting; offline media as fallback |

## 13. Test bench (proposed)

A custom protocol brings custom bugs, so faults are produced in tests rather than discovered in the field. The test
bench runs on Windows and Linux in CI (**decided**):

- drop the connection at every point of the procedure, including mid-segment and mid-greeting,
- delay, throttled bandwidth, packet loss,
- restart of source or hub during a transfer,
- duplicate, damaged and reordered segments,
- offline media and network at the same time.

**Pass criterion of every test:** at the end the stream at the receiver is bit-for-bit identical to the original, and
every message has arrived exactly once.

## Open points

- [ ] Wire format: framing, encoding of control messages, checksum algorithm, version negotiation.
- [ ] Network: fixed hub address or discovery; other users on the same network.
- [ ] Bandwidth and typical outages of the radio links (how often, how long).
- [ ] Catch-up order: current stream first, or strictly in order.
- [ ] Hub failure: standby hub or offline media only.

**To measure**

- [ ] Data rate of typical streams.
- [ ] Compression ratio and CPU time on target hardware.
- [ ] Heartbeat and timeout on a real radio network, so that short dropouts do not count as disconnects.
