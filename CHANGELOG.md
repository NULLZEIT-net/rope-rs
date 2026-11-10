# Changelog

All notable changes to `rope-rs` (crate `rope-protocol`) are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).
The wire protocol has its own number (`PROTOCOL_VERSION`, spec 14.5).

## [Unreleased]

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
- Conformance adapter (`rope-conformance-adapter`) and `conformance/run.py`: 21 scenarios of the ROPE conformance
  suite pass (source over TCP, messages both ways also through a breaking link, rejects, frame level).
- Dependencies: `serde_json`, `crc32fast`, `sha2`, `flate2` (pure-Rust backend) – JSON, CRC-32, SHA-256 and zlib are
  not in the Rust standard library.
