<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/rope-logo-dark.svg">
  <img alt="ROPE" src="images/rope-logo.svg" height="56">
</picture>

# ROPE – Protocol Specification

**Status: draft 1.** This document describes the goals, model and behaviour of ROPE, and since draft 1 the wire format
([§14](#14-wire-format)). The wire format is **proposed**: it may change until the first release, after measurements
and the first implementation – see [Open points](#open-points).

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
  - *recipient* – exchanges messages only, receives no streams.
- **Need-to-know:** raw streams only to consumers; messages also to recipients.
- **Several consumers:** a second consumer receives the same streams via the hub.
- **Relaying (proposed, first implementation 2026-10-26):**
  - To a consumer, the hub is a sender exactly as a source is to the hub: same window, resume from the consumer's
    committed offset, final check and block repair (§8.2–8.4, §14.7).
  - A consumer receives every stream of every source of the operation, while the stream still grows.
  - A relayed stream is named `<source>/<stream>`, so that streams of different sources never mix.
  - The hub sends the final check to a consumer only after its own copy passed the check.
  - The hub keeps no delivery state per consumer. The consumer reports what it holds when it greets (`HELLO.have`,
    §14.5), so a restart of the hub or of the consumer loses nothing.
  - Order: streams in name order, each from its committed offset (see open point "catch-up order").
- **Participant list (proposed, first implementation 2026-10-26):**
  - Per participant: identifier, role, a **name** in plain language for the operator (default: the identifier) and,
    for a consumer, a **selection** of streams. Anything else in the list is refused when the hub starts, with a
    plain-language reason.
  - **Selection:** patterns over the relayed names `<source>/<stream>`, e.g. `source-1/*` (all streams of one source)
    or `*/rec-*`. `*` matches any characters including `/`, `?` exactly one; upper and lower case are different on
    every system. Without a selection a consumer receives every stream (as before); an empty selection means none.
    Only consumers have a selection: sources and recipients receive no streams.
  - The selection applies to the network and to media in the same way. The hub checks it whenever it looks for
    streams to send, so a changed list takes effect when the hub restarts with it. A consumer keeps what it already
    holds when its selection becomes narrower; nothing is deleted.
  - **Every participant learns the list** – identifier, name and role of everybody, the hub first with role
    `hub` – in `WELCOME` (`participants`, §14.3) and in every bundle the hub writes (§8.6). An application can thus
    show names and address messages also offline. The participant keeps the newest list it has seen (by the hub's
    time), so an older stick never replaces a newer greeting. The selections stay at the hub: a participant does not
    need to know what others receive.
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
5. A participant has at most one session. A new greeting from the same participant replaces the old session
   (**decided** with the first implementation). After a radio dropout, the hub often still holds the old, dead
   connection; it must not block the new one.

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
  whole stream. The final size is never smaller than what the source has already sent.
- The receiver compares. On mismatch both sides exchange per-block checksums and only differing blocks are resent
  (§14.7). Bytes the source changed in place, e.g. a header updated at the end of a recording, are fixed this way
  without a full retransfer.
- The stream is then marked complete and verified.

### 8.5 Messages

- Every message has a unique id, sender, recipient(s) and timestamp.
- The sender stores it durably in its outbox. The hub takes custody and acknowledges, then delivers it to each recipient
  individually.
- Each recipient acknowledges after storing it. The hub tracks "delivered" or "pending" per recipient.
- Messages take priority over stream segments.
- Duplicates are detected by id and shown only once.

**First implementation (2026-10-26, proposed):**

- **Who:** every role may send and receive messages. A *recipient* differs from the others only in that it
  receives no streams.
- **Sender check:** the hub accepts a message only if `from` is the participant on that connection. Otherwise it
  answers `ERROR` `bad_frame`.
- **Unknown recipients:** recipients not on the participant list are recorded as "unknown" and logged. The
  message is delivered to the known ones.
- **Custody at every step:** a holder writes the body, then the meta, both durably. Only then does it send
  `MESSAGE_ACK`.
  - Until it is acknowledged, a message is sent again after every reconnect.
  - At most 16 unacknowledged messages are in flight per connection.
  - The sender's outbox shows "at the hub" once the hub has taken custody.
- **Delivery report to the sender (proposed, 2026-10-26):**
  - The hub sends the sender a `DELIVERY` frame with the lists `delivered`, `pending` and `unknown`
    (§14.3) whenever another recipient has acknowledged the message.
  - It also sends one at once if there is nobody to wait for: all recipients unknown, or only the sender itself.
    With unknown recipients next to known ones, a first report goes out at once as well, so the sender learns early
    that some recipients do not exist.
  - The sender stores the report durably, then answers `DELIVERY_ACK` with the number of recipients it now knows as
    delivered. The hub keeps that number per message (`reported`) and sends again, after every reconnect, until a
    report with all delivered recipients is acknowledged. A report that is older than the stored one (e.g. from
    an old stick) is acknowledged but not stored.
  - The application is told once per new report (`on_delivery`), with the same "told" flag as for received
    messages: a report stored but not yet told is told when the next session starts. The outbox always holds the
    latest report; the message reached everybody when `pending` is empty.
  - The report is about custody at each recipient, not about a person reading the message.
- **Telling the application:** the inbox records whether the application was told about a message. If the
  connection breaks between storing and telling, a resent copy is a duplicate and would not tell it again. So every
  untold message is told after each received message and when a session starts. The application is told at least
  once; in practice exactly once (it is told twice only if the program stops between telling and recording that).
- **Priority:** messages travel in their own task on the same connection. A message waits behind at most the
  segment being written, not behind the stream backlog. The test bench checks this on a simulated 1 MB/s link
  with a 3 MB file in progress: the message must arrive within 2 s, while the file is still on its way.

### 8.6 Offline media as a second path

Removable media (e.g. a USB stick) can carry the same segments and messages. Import follows the same rules: check the
committed offset, ignore duplicates, report damaged items. Network and media may be mixed.

**First implementation (2026-10-26, proposed):**

- **Bundle:** one export writes one bundle. It holds the frames the exporter would send over the network, in the
  wire format of §14:
  - `SEGMENT`, `STREAM_END` and `MESSAGE`,
  - plus receipts for what it holds from the other side: `ACK` (committed offsets) and `MESSAGE_ACK`.
  - The core decides nothing new for media; only the way the frames travel is different.
- **Who exchanges with whom:** as on the network, always a participant and the hub (star, §7).
  - A participant exports for the hub and imports what the hub exported for it.
  - The hub imports bundles from all participants and exports one bundle per participant.
- **Layout on the medium:** `rope/<operation>/<from>-to-<to>-<created ms>-<random>/`, with the frames in
  `frames-0001.rope`, … and a manifest `bundle.json` (`format`, `operation`, `from`, `to`, `created`, `files`,
  `frames`, `complete`; a bundle from the hub also carries `participants`, §7).
  - At most 1 GiB per frames file, because many sticks use FAT32 (limit 4 GiB per file).
  - A bundle is written under a name ending in `.partial` and renamed when it is complete and durable. A medium
    pulled out during the export therefore never holds a half bundle under a valid name.
  - ROPE never deletes a bundle from a medium. The application decides when a stick is cleared.
- **What a bundle carries:**

  | Direction | Content |
  |---|---|
  | source → hub | streams not yet verified at the hub, from the offset the hub last acknowledged (over the network or by a receipt on a medium; from 0 after a restart of the source); `STREAM_END` once the stream is complete and fully in the bundle |
  | consumer → hub | its committed offsets as `ACK` |
  | hub → source | the hub's committed offsets of that source's streams as `ACK`, so that the next bundle of the source is smaller |
  | hub → consumer | the relayed streams (§7), from the offsets the consumer last reported in a bundle (from 0 if it never did); `STREAM_END` only for streams verified at the hub |
  | everybody, both ways | messages for the addressee not yet in custody there, and `MESSAGE_ACK` for messages received from the addressee |
  | hub → participant | `DELIVERY` reports for the participant's messages it has not acknowledged yet (§8.5) |
  | participant → hub | `DELIVERY_ACK` for every report it holds |

  Order inside a bundle: receipts, then messages, then streams. If the medium is too small (`max_bytes`), the export
  stops at a frame boundary and reports `complete: false`; the rest goes with the next bundle.
- **Import:**
  - Bundles are imported oldest first. Importing a bundle again changes nothing.
  - Segments are stored as from the network (§8.3): durably, duplicates ignored. A segment beyond the committed
    offset is a gap: the rest of that stream in the bundle is skipped and reported, the bytes must come first from
    an earlier bundle or the network.
  - Frames are read one at a time. A frame with a wrong CRC is skipped and reported; its length is known, so
    reading goes on (later segments of that stream then usually meet a gap). A damaged frame header ends the reading
    of its file, because the position of the next frame is lost. A cut-off file is reported.
  - A bundle whose manifest cannot be read is reported by every participant that looks at the medium: nobody can
    tell for whom it was.
  - The hub accepts only bundles from participants on its list, and only messages written by the bundle's sender
    (§8.5).
  - A received message is told to the application once, also if the same message arrives by medium and by network
    at the same time.
- **Limits of the first implementation:**
  - No block repair over media. Repair needs the receiver's block sums first, i.e. a round trip. A final check that
    fails after an import is repaired over the network (§14.7).
  - A `STREAM_END` from a medium never overrides a stream that is already verified: the bundle may be older than a
    repair over the network.
  - A participant's message counts as "at the hub" only after a receipt from the hub (network or medium). Until then
    every export carries it again; the hub ignores the duplicates.
  - The hub keeps what a consumer last reported in a bundle (`media_have.json`). That is state per consumer, but for
    media only. On the network the consumer still reports in `HELLO` (§7).

## 9. Interruptions and resumption

Core idea: the data lives on the source's disk and the receiver knows its committed offset. An interruption costs time,
never data.

Connection states (**proposed**): *disconnected* → *connecting* → *greeting* → *active* → back to *disconnected* on
any failure. Reconnect back-off grows from 1 s to at most 10 s and never gives up.

| Outage | What happens |
|---|---|
| Up to a few seconds, e.g. 0.5 s | TCP retransmits; the connection stays up. The core notices nothing. |
| No sign of life for about 10 s | Connection considered lost (heartbeat every 2 s, **assumed** suitable – **may be too short for radio links**, see note below). Reconnect, greet, continue from the committed offset; unacknowledged segments are resent. |
| Minutes to hours | The source keeps recording; nothing piles up in memory. After reconnecting the backlog is transferred. |
| Device restart | Committed offsets and outbox are on disk; work continues from the last acknowledged point. |

**Note on heartbeat and timeout (open, must not get lost):** The values above (2 s heartbeat, ~10 s until a connection
counts as lost) are assumptions. On directional radio links, longer dropouts are normal – shadowing, movement of a
vessel or vehicle, antenna alignment. A timeout that is too short turns every such dropout into a disconnect with an
unnecessary reconnect and greeting. Therefore heartbeat interval and timeout **must be configurable** and are to be
**measured on real radio links** before defaults are fixed. Losing the TCP connection is harmless for data (resume from
the committed offset), but frequent reconnects cost time and bandwidth.

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
| Source changed bytes in place | final checksum | resend only the differing blocks (§14.7) |
| Source file shortened or replaced | file shorter than before, or most blocks differ at the final check | never overwrite; report in plain language; keep as a new version |
| Unknown participant | not on the list | reject with a plain-language reason |
| Different protocol version | greeting | reject or operate on the common subset, with a plain-language reason |
| Wrong operation | operation identifier | store separately, never mix |
| Receiver disk full | write fails | do not acknowledge, report; source waits |
| Recipient offline | no acknowledgement | hub keeps custody and delivers later |
| Hub offline | no connection | participants keep collecting; offline media as fallback (§8.6) |
| Damaged or cut-off medium | frame CRC, frame count | keep everything before the damage, report the rest |
| Bundle without its predecessor | segment beyond the committed offset | skip that stream in the bundle, report the gap |

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

**Link simulator** (`rope_protocol.linksim`, implemented). It is a TCP proxy between two participants and behaves like
an unreliable link, per direction:
- bandwidth limit, latency and jitter
- **blackouts:** no data flows, but the connections stay open, as with shadowing on a directional radio link
- **disconnects:** all connections are aborted, followed by an outage

Faults follow a random schedule from a profile (reproducible with a seed) or an exact script for tests. It can also
be run by hand: `python -m rope_protocol.linksim --listen 127.0.0.1:47311 --target 127.0.0.1:47310 --profile radio`.

The profiles are **assumed** and must be replaced by measurements on real links:

| Profile | Bandwidth | Latency | Blackouts | Disconnects |
|---|---|---|---|---|
| `lan` | unlimited | – | – | – |
| `wifi` | 2.5 MB/s | 5 ms + up to 10 ms | every ~2 min, 0.2–2 s | – |
| `radio` | 500 kB/s (~4 Mbit/s) | 10 ms + up to 20 ms | every ~1 min, 0.5–20 s | every ~15 min, down 5–60 s |
| `radio-bad` | 125 kB/s (~1 Mbit/s) | 30 ms + up to 50 ms | every ~20 s, 1–60 s | every ~3 min, down 10–300 s |

Limits of the simulation:
- It acts on the byte stream, not on radio packets. Packet loss and retransmissions inside TCP appear only as delay.
- During an outage, a new connection is accepted by the proxy and waits. On a real link the connection attempt itself
  would wait or fail.

## 14. Wire format

Everything in this section is **proposed** (draft 1).

### 14.1 Transport

- ROPE runs over one TCP connection per participant (§5).
- **Default port 47310** (TCP). Every application must be able to override it. At the time of choosing (2026-10-26)
  the port was not assigned in the IANA registry, no known software used it, and it lies below the dynamic range that
  operating systems use for outgoing connections (49152 and up). It is not registered with IANA. If the hub cannot
  listen on the port, it reports this in plain language and names the port; it never silently picks another one.
- An application may wrap the connection in TLS, e.g. with certificates on both sides, when the network is not closed
  (§11). The frames inside are unchanged; the core does not know whether TLS is in use.
- All integers are unsigned and big-endian (network byte order).

### 14.2 Frames

Everything on the connection is a sequence of frames:

| Offset | Size | Field |
|---|---|---|
| 0 | 2 | magic `0x52 0x50` (`RP`) |
| 2 | 1 | protocol version |
| 3 | 1 | frame type |
| 4 | 4 | meta length *M* |
| 8 | 4 | blob length *B* |
| 12 | *M* | **meta:** a JSON object, UTF-8 |
| 12 + *M* | *B* | **blob:** raw bytes (segment data, message body), may be empty |
| 12 + *M* + *B* | 4 | CRC-32 (as in zlib/ISO-HDLC) over all preceding bytes of the frame |

- **Limits:** *M* ≤ 64 KiB and *B* ≤ 1 MiB. A receiver checks both lengths **before** reading the rest. A frame that
  is too long, has a wrong magic or a wrong CRC is answered with `ERROR`, and then the connection is closed. Nothing
  is acknowledged, so nothing is lost: after reconnecting, the transfer continues from the committed offset (§9).
- **Why JSON for meta:** readable in network captures, part of every standard library, and extendable. Unknown keys
  are ignored, so a newer participant can add keys without breaking older ones. The cost is about 100–200 bytes per
  segment of up to 256 KiB, below 0.1 %.
- **Meta encoding:** compact (no spaces), keys sorted, non-ASCII characters as UTF-8. Receivers must accept any valid
  JSON object; the rule only makes frames reproducible for test vectors.
- **Why a CRC per frame:** the TCP checksum is weak; the CRC catches damage between the network stack and the
  application. It protects the transfer; stored data is protected separately (segment CRC, stream SHA-256).

### 14.3 Frame types

Types `0x00`–`0x7F` belong to the core. A receiver that does not know a core type answers with `ERROR` and closes the
connection. Types `0x80`–`0xFF` are extensions: a receiver that does not know one ignores it.

| Type | Name | Direction | Meta (required keys) | Blob |
|---|---|---|---|---|
| `0x01` | `HELLO` | participant → hub | `participant`, `operation`, `role`, `versions`, `t`, `have` | – |
| `0x02` | `WELCOME` | hub → participant | `version`, `hub`, `t`, `t_hello`, `heartbeat_s`, `timeout_s`, `have` | – |
| `0x03` | `REJECT` | hub → participant | `code`, `text` | – |
| `0x04` | `HEARTBEAT` | both | `t` | – |
| `0x05` | `BYE` | both | `reason` | – |
| `0x06` | `ERROR` | both | `code`, `text` | – |
| `0x10` | `SEGMENT` | sender → receiver | `stream`, `gen`, `off`, `len`, `crc`, `t`, `enc` | encoded data |
| `0x11` | `ACK` | receiver → sender | `stream`, `gen`, `committed` | – |
| `0x12` | `RESEND` | receiver → sender | `stream`, `gen`, `off`, `len` | – |
| `0x13` | `STREAM_END` | sender → receiver | `stream`, `gen`, `size`, `sha256` | – |
| `0x14` | `STREAM_CHECK` | receiver → sender | `stream`, `gen`, `result`, `size` | – |
| `0x15` | `BLOCK_SUMS` | receiver → sender | `stream`, `gen`, `block_size`, `first`, `sums` | – |
| `0x20` | `MESSAGE` | both | `id`, `from`, `to`, `t`, `kind`, `len`, `crc`, `enc` | encoded body |
| `0x21` | `MESSAGE_ACK` | both | `id` | – |
| `0x22` | `DELIVERY` | hub → sender of a message | `id`, `delivered`, `pending`, `unknown` | – |
| `0x23` | `DELIVERY_ACK` | sender of a message → hub | `id`, `delivered` | – |

"Sender" and "receiver" are roles per stream: a source sends to the hub, and the hub sends to consumers.

Optional keys:

- `SEGMENT` may carry `repair` (`true` or `false`, default `false`, §14.7).
- `WELCOME` may carry `participants`: the participant list as every participant may know it,
  `[{"participant": …, "name": …, "role": …}]`, the hub first with role `hub` (§7). Added 2026-10-26; a participant
  that does not know the key ignores it.

### 14.4 Values

- **Times** (`t`, `t_hello`): integer milliseconds since 1970-01-01 UTC, from the clock of the frame's sender.
- **Identifiers** (`participant`, `hub`, `operation`, `from`, entries of `to`): 1–64 characters from `A–Z a–z 0–9 . _ -`.
- **Stream name** (`stream`): chosen by the application, 1–255 bytes UTF-8, e.g. `rec-0005.dat`. A receiver
  must **never** use it directly as a file path; it maps names to its own storage. The hub relays a stream as
  `<source>/<stream>` (§7). That name must also fit in 255 bytes; otherwise the hub refuses the stream with `ERROR`
  `bad_frame` and a plain-language reason.
- **Generation** (`gen`): integer ≥ 1. The sender starts a new generation when a stream is not a continuation of the
  previous one: the file became shorter, or the final check finds more than half of its blocks changed (§14.7).
  Receivers keep every generation separately and never overwrite one with another. Bytes changed in place within a
  generation (e.g. a header updated at the end of a recording) are not a new generation; the block repair fixes them.
- **Offsets and lengths** (`off`, `len`, `size`, `committed`): bytes of the raw (decoded) stream.
- **Checksums:** `crc` is the CRC-32 of the raw segment data or message body after decoding; `sha256` and `sums` are
  SHA-256 digests in lower-case hex.
- **Message length** (`len`): bytes of the raw body, at most 1 MiB. Added on 2026-10-26: without it, a receiver could
  not bound the decompression of a `zlib` body (the same rule as for segments, see Encoding).
- **Encoding** (`enc`): `none` or `zlib` (§10: low level). The sender uses `zlib` only when it makes the blob smaller.
  A `zlib` blob is exactly one complete zlib stream. The receiver decodes at most `len` + 1 bytes and rejects anything
  that is not exactly `len` bytes, so a small blob can never expand into a large amount of memory.
- **Message id** (`id`): 32 lower-case hex characters chosen by the original sender, e.g. a random UUID. It stays the
  same across retries and relays.
- **Message kind** (`kind`): chosen by the application, 1–64 characters, same alphabet as identifiers. The core does
  not interpret it.
- **Delivery report:** in `DELIVERY`, `delivered`, `pending` and `unknown` are lists of participant identifiers
  (possibly empty). In `DELIVERY_ACK`, `delivered` is the number of recipients in the report the sender holds.
  Added 2026-10-26.
- **Role** (`role`): `source`, `consumer` or `recipient` (§7).
- **Reject and error codes** (`code`): `unknown_participant`, `wrong_operation`, `version`, `role`, `busy`,
  `bad_frame`, `too_large`, `unknown_type`, `storage`, `internal`. `text` is plain language for the operator.

### 14.5 Greeting and version

1. The participant sends `HELLO`. `versions` lists the protocol versions it supports. `have` lists what it already
   holds as a receiver: `[{"stream": …, "gen": …, "committed": …}]` (empty for a pure source).
2. The hub picks the highest common version and answers `WELCOME` with `version`. `have` lists what the hub holds from
   this participant. Otherwise it answers `REJECT` and closes the connection.
3. Every later frame carries the chosen version in byte 2. The 12-byte header and the frames `HELLO`, `WELCOME` and
   `REJECT` keep their layout in all versions, so that version 1 can always reject a newer participant with a
   readable reason.
4. **Clock offset:** `WELCOME` echoes the participant's `HELLO.t` as `t_hello`. The participant receives `WELCOME` at
   its own time *r* and estimates the hub clock offset as `t − (t_hello + r) / 2`, with an uncertainty of ± half the
   round trip. The core reports the estimate to the application and never changes the system clock.
5. `heartbeat_s` and `timeout_s` from the hub's configuration apply to both sides (§9: configurable, to be measured).

### 14.6 Example (test vector)

A `HEARTBEAT` at `t = 1767225600000` (2026-01-01 00:00 UTC) in version 1, meta `{"t":1767225600000}` (19 bytes),
empty blob:

```
52 50 01 04 00 00 00 13 00 00 00 00
7b 22 74 22 3a 31 37 36 37 32 32 35 36 30 30 30 30 30 7d
45 28 16 84
```

The last four bytes are the CRC-32 of the 31 bytes before them. `tests/test_frames.py` checks this vector.

### 14.7 Final check and block repair

Why: a recording program may change bytes it has already written, typically a header with counters at the end of a
recording. Resending the whole file for that would cost the full transfer again over a slow link.

1. When the application declares the stream complete, the sender sends `STREAM_END` with the final `size` and the
   `sha256` of that many bytes of its file, once everything up to `size` is acknowledged.
2. The receiver compares with what it holds and answers `STREAM_CHECK` (`ok` or `mismatch`, `size` = its committed
   offset). On `mismatch` it then sends `BLOCK_SUMS`: the SHA-256 of every block of `block_size` bytes of its data
   (the last block may be shorter), in index order, several frames if needed; `first` is the index of the frame's
   first block. Draft 1: blocks of 1 MiB and at most 512 sums per frame (about 34 KiB of meta).
3. The sender has all sums when it holds ⌈`size` / `block_size`⌉ of them. It compares each block with its file:
   - If **more than half** of the blocks differ, the file was replaced rather than edited: the sender starts a new
     generation, and the receiver keeps the old one unchanged.
   - Otherwise it resends exactly the differing blocks as `SEGMENT` with `repair: true` and then sends `STREAM_END`
     again. The receiver overwrites those bytes below its committed offset, makes them durable, and acknowledges as
     usual (the committed offset does not change).
4. After **3** repairs that reached the receiver and still end in `mismatch`, the sender stops and reports the
   mismatch to the application in plain language. The receiver keeps all bytes. A repair counts as reached when the
   answer to the `STREAM_END` after it arrives on the same connection (frames keep their order); a repair cut off by
   a disconnect does not count.
5. A disconnect at any point is harmless. After reconnecting, the sender sends `STREAM_END` again, and the check and
   repair start over from what the receiver holds.

Without `repair`, a segment below the committed offset is a duplicate and is not written (§8.3), so a late duplicate
never changes stored data. Streams are kept per participant, so only a stream's own sender can repair it.

Status: **proposed** (block size, the half rule and the 3 rounds are assumptions until measured).

## Open points

- [x] Wire format: framing, encoding of control messages, checksum algorithm, version negotiation (draft 1, §14,
      proposed).
- [x] Default TCP port: 47310, overridable (§14.1).
- [ ] Message body size above 1 MiB: split into several frames or forbid (draft 1: at most one blob of 1 MiB).
- [ ] Network: fixed hub address or discovery; other users on the same network.
- [ ] Bandwidth and typical outages of the radio links (how often, how long).
- [ ] Catch-up order: current stream first, or strictly in order. First implementation: streams in name order,
      each from its committed offset.
- [x] Delivery report to the sender of a message: `DELIVERY` / `DELIVERY_ACK` (§8.5, proposed).
- [x] Which consumer receives which streams: a selection per consumer in the participant list (§7, proposed).
- [ ] Hub failure: standby hub or offline media only.
- [ ] Offline media: block repair over media (needs two trips of the stick), and whether the core should help the
      application decide when a bundle on a stick can be deleted.

**To measure**

- [ ] Data rate of typical streams.
- [ ] Compression ratio and CPU time on target hardware.
- [ ] Heartbeat and timeout on a real radio network, so that short dropouts do not count as disconnects. The current
      assumption (2 s / ~10 s) may be too short for radio links – see the note in §9. Make both configurable.
