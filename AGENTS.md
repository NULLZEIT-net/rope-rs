# Guidelines for contributors and coding agents

This file is the single source of instructions for humans and coding agents (`CLAUDE.md` includes it).

## Project

`rope-rs`: an independent Rust implementation of ROPE (*Resumable Offline Protocol for Emergencies*). The
specification, the test vectors and the conformance suite live in the Python reference,
[NULLZEIT-net/rope](https://github.com/NULLZEIT-net/rope) (`docs/spec.md`, `docs/vectors/`, `conformance/`).
Crate `rope-protocol`, library `rope_protocol`.

## Rules

- **English** in code, comments, docs, commits, issues.
- **Spec first:** the specification is changed in the `rope` repository first; this implementation follows. If the
  spec is unclear, open an issue there instead of guessing.
- **Dependencies (decided 2026-11-10):** the Rust standard library plus four small, widely used crates for what it
  lacks: `serde_json` (meta), `crc32fast` (CRC-32), `sha2` (SHA-256), `flate2` with its default pure-Rust backend
  (zlib). Blocking I/O and threads, **no async runtime**. A new dependency needs a reason in `CHANGELOG.md`.
- **Generic content only:** no references to specific organisations, people, sites or deployments.
- **Tests with the code:** the test vectors (`tests/vectors.rs`, a copy of `rope/docs/vectors/frames.json`) and the
  conformance suite (`conformance/run.py`). A scenario that passes goes into `conformance/expected.txt`; it must
  never fail again.
- **Never block the producer:** files are only read, briefly, without locks; bounded memory; time limits on every
  network call.
- Every user-visible change goes under `## [Unreleased]` in `CHANGELOG.md`.
- Files use LF line endings and UTF-8 without BOM; `cargo fmt` and `cargo clippy -- -D warnings` are clean.

## Workflow

- `main` is protected: changes only via pull request with green checks and maintainer approval.
- Branch names: `feature/...`, `fix/...`, `docs/...`.
- CI runs on GitHub-hosted runners only.

## Commands

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
python conformance/run.py --rope ../rope        # needs a checkout of NULLZEIT-net/rope
```
