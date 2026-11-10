//! ROPE – Resumable Offline Protocol for Emergencies, in Rust.
//!
//! An independent implementation of the specification in the `rope` repository (`docs/spec.md`), tested against the
//! Python reference with its conformance suite (`rope/conformance/`). So far: the frame codec ([`frames`]), messages
//! with custody ([`mailbox`], for every participant), the source role ([`source`]) and the recipient
//! ([`recipient`]).

pub mod frames;
pub mod mailbox;
pub mod participant;
pub mod recipient;
pub mod session;
pub mod source;

/// Protocol version announced in the greeting (spec 14.5).
pub const PROTOCOL_VERSION: u64 = 1;

/// Default TCP port of the hub (spec 14.1). Applications must be able to choose another one.
pub const DEFAULT_PORT: u16 = 47310;

/// Milliseconds since 1970-01-01 UTC (spec 14.4).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
