# Changelog

All notable changes to `rope-rs` (crate `rope-protocol`) are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).
The wire protocol has its own number (`PROTOCOL_VERSION`, spec 14.5).

## [Unreleased]

## [0.1.0] - 2026-11-10

First version: every role of the protocol, validated against the Python reference with the whole conformance suite
(60 of 60 scenarios). Wire format: `PROTOCOL_VERSION` 1, media format 1. Not published on crates.io; not yet used
outside tests. Not included: TLS, names and stream patterns in the hub's participant list.

### Added
- Frame codec of the wire format (spec 14): encode, incremental decoder with the length limits checked before
  buffering, validation of every core frame type, zlib payloads bounded to `len` + 1 bytes. All test vectors of the
  reference pass, including byte-exact encoding.
- Source role (spec 8.2–8.4, 9, 14.7): greeting with plain-language rejects, resume from the hub's committed offset,
  window of 8 unacknowledged segments, heartbeat and timeouts from `WELCOME`, final check with `STREAM_END`, block
  repair of bytes changed in place, a new generation for a file that became shorter or changed in most of its blocks,
  state on disk across restarts, reconnect with back-off. Bad frames from the hub are answered with `ERROR`.
- Messages with custody for every participant (spec 8.5, `mailbox`): outbox and inbox on disk (body synced, then
  meta), acknowledged only after storing, sent again after every reconnect until the hub holds them, at most 16 in
  flight, before stream data; the application is told once per message (`on_message`) and once per new delivery
  report (`on_delivery`), also across a restart. `Participant::send_message`, `inbox`, `outbox`.
- Recipient role (`recipient::start`): messages only.
- Consumer role (`Consumer::start`, `receiver`): stores relayed streams durably before `ACK`, asks for a gap with
  `RESEND`, reports what it holds in `HELLO.have`, final check with SHA-256, `BLOCK_SUMS` on a mismatch and repair
  segments below the committed offset; after a crash, bytes written but never committed are cut off. A stream name
  is never used as a path.
- Offline media, media format 1 (spec 8.6, `media`): bundles written below `.partial` and renamed when durable,
  frames files of at most 1 GiB, `max_bytes` stops at a frame boundary; import reads one frame at a time, skips a
  frame with a wrong CRC, stops a file at a damaged header, reports cut-off files and gaps, never changes a verified
  stream. `export_media` / `import_media` for source (from the hub's `ACK`, block repairs asked for in a bundle,
  a new generation if most blocks differ), consumer (`ACK`, repair requests) and recipient (messages, receipts,
  delivery reports).
- Hub, first part (`hub::Hub`): listens on the chosen port (refuses in plain language if it is taken, never another
  one), checks every greeting against the participant list (`unknown_participant`, `wrong_operation`, `version`,
  `role`), one session per participant (a new greeting replaces the old one), heartbeat and timeout sent in
  `WELCOME` (timeout at least twice the heartbeat), streams of sources stored per participant with final check and
  block repair, `ERROR` for bad frames, relayed names longer than 255 bytes refused.
- Hub, second part: relays every source's streams to consumers as `<source>/<stream>` from what the consumer
  reported in `HELLO.have`, `STREAM_END` only for streams verified at the hub, block repair from the hub's copy (never
  a new generation). Messages with custody: only from the participant of the connection, stored before
  `MESSAGE_ACK`, delivered to every recipient on the list (`pending` → `delivered`), unknown recipients reported,
  `DELIVERY` to the sender until acknowledged; messages to the hub itself go to `HubOptions::on_message`, told again
  after a restart if not taken (`retell`).
- Hub, third part: offline media (`Hub::import_media`, `Hub::export_media`). Import takes bundles only from
  participants on the list and messages only from the bundle's sender; streams of sources, receipts, delivery
  acknowledgements, what consumers hold (`media_have.json`) and the repairs they ask for (`media_repairs.json`).
  Export per participant: receipts and delivery reports, `ACK` and repair requests for a source, the pending messages,
  and for a consumer the repairs it asked for (from the hub's verified copy) and the relayed streams from what it last
  reported, `STREAM_END` only for streams verified at the hub.
- The sending side (window, final check, block repair) is shared by source and hub (`sender`).
- Conformance adapter (`rope-conformance-adapter`) and `conformance/run.py`: **all 60 scenarios** of the ROPE
  conformance suite are required – every role over TCP and by offline media, rejects, frame level.
- `tests/end_to_end.rs`: source, hub, consumer and recipient all in Rust, over the network and by stick only.
- Dependencies: `serde_json`, `crc32fast`, `sha2`, `flate2` (pure-Rust backend) – JSON, CRC-32, SHA-256 and zlib are
  not in the Rust standard library.
