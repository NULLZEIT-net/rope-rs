# rope-rs

An independent **Rust implementation of ROPE** – the *Resumable Offline Protocol for Emergencies*: resumable transfer
of growing files and messages over TCP in small, closed, offline groups.

The specification, the test vectors and the conformance suite live in the Python reference implementation,
[NULLZEIT-net/rope](https://github.com/NULLZEIT-net/rope). Two implementations that understand each other are the real
test of a protocol specification; this repository is the second one.

## Status

Early development. **Not yet released.**

| Part | Status |
|---|---|
| Frame codec (spec 14) | done – every test vector of the reference passes, byte-exact |
| Source (sends growing files to a hub) | done – resume, heartbeat, final check, block repair, new generations |
| Messages, consumer, recipient | planned |
| Offline media (sticks) | planned |
| Hub | later – the hub stays the Python reference until the participant roles pass the suite |

Conformance: **17 of the 57 scenarios** of the ROPE conformance suite pass; the list is in
`conformance/expected.txt` and is checked on every change.

## Use

```rust
use rope_protocol::source::{Options, Source};
use std::path::{Path, PathBuf};

let mut source = Source::start(
    "source-1",                                     // identifier on the hub's participant list
    "op-1",                                         // operation
    "192.168.1.10:47310",                           // the hub
    vec![("rec-0001.dat".into(), PathBuf::from("rec-0001.dat"))],
    Path::new("state"),                             // what must survive a restart
    Options::default(),
)?;
// ... the recording program writes the file ...
source.complete("rec-0001.dat");                    // the file is finished: final check
source.stop();
```

The source only reads the files – briefly, without locks – and keeps at most 8 unacknowledged segments in memory.
After a break it continues from what the hub holds.

## Testing

```
cargo test                                         # unit tests and the test vectors
cargo build --release
python conformance/run.py --rope ../rope           # conformance suite, needs a checkout of NULLZEIT-net/rope
```

`conformance/run.py --all` also shows the scenarios that do not pass yet.

## Design

- Blocking I/O and threads, no async runtime: a reader thread per connection, the session in its own thread.
- Dependencies: the standard library plus `serde_json`, `crc32fast`, `sha2` and `flate2` (pure-Rust zlib).

## Licence

Apache License 2.0, see `LICENSE`.
