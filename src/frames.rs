//! Frame codec of the ROPE wire format (spec section 14).
//!
//! A frame is a fixed 12-byte header, a JSON object ("meta"), raw bytes ("blob") and a CRC-32 over everything before
//! it. This module only encodes and decodes frames; it does no I/O. [`Decoder`] is incremental and checks the length
//! limits before buffering a frame, so a damaged or hostile length field cannot make it allocate more than the limits.

use serde_json::{Map, Value};
use std::fmt;
use std::io::{Read, Write};

pub const MAGIC: &[u8; 2] = b"RP";
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 12;
pub const TRAILER_LEN: usize = 4;
pub const MAX_META: usize = 64 * 1024;
pub const MAX_BLOB: usize = 1024 * 1024;
pub const EXTENSION_FIRST: u8 = 0x80;
/// Longest announced operation name in a REJECT (spec 14.5).
pub const MAX_OPERATION_NAME: usize = 100;

pub type Meta = Map<String, Value>;

/// Core frame types (spec 14.3).
pub mod kind {
    pub const HELLO: u8 = 0x01;
    pub const WELCOME: u8 = 0x02;
    pub const REJECT: u8 = 0x03;
    pub const HEARTBEAT: u8 = 0x04;
    pub const BYE: u8 = 0x05;
    pub const ERROR: u8 = 0x06;
    pub const SEGMENT: u8 = 0x10;
    pub const ACK: u8 = 0x11;
    pub const RESEND: u8 = 0x12;
    pub const STREAM_END: u8 = 0x13;
    pub const STREAM_CHECK: u8 = 0x14;
    pub const BLOCK_SUMS: u8 = 0x15;
    pub const MESSAGE: u8 = 0x20;
    pub const MESSAGE_ACK: u8 = 0x21;
    pub const DELIVERY: u8 = 0x22;
    pub const DELIVERY_ACK: u8 = 0x23;
}

/// Name of a core frame type, for messages and logs.
pub fn type_name(t: u8) -> String {
    let name = match t {
        kind::HELLO => "HELLO",
        kind::WELCOME => "WELCOME",
        kind::REJECT => "REJECT",
        kind::HEARTBEAT => "HEARTBEAT",
        kind::BYE => "BYE",
        kind::ERROR => "ERROR",
        kind::SEGMENT => "SEGMENT",
        kind::ACK => "ACK",
        kind::RESEND => "RESEND",
        kind::STREAM_END => "STREAM_END",
        kind::STREAM_CHECK => "STREAM_CHECK",
        kind::BLOCK_SUMS => "BLOCK_SUMS",
        kind::MESSAGE => "MESSAGE",
        kind::MESSAGE_ACK => "MESSAGE_ACK",
        kind::DELIVERY => "DELIVERY",
        kind::DELIVERY_ACK => "DELIVERY_ACK",
        _ => return format!("0x{:02x}", t),
    };
    name.to_string()
}

/// Required meta keys per core frame type (spec 14.3). Unknown keys are allowed and ignored.
fn required(t: u8) -> Option<&'static [&'static str]> {
    Some(match t {
        kind::HELLO => &["participant", "operation", "role", "versions", "t", "have"],
        kind::WELCOME => &[
            "version",
            "hub",
            "t",
            "t_hello",
            "heartbeat_s",
            "timeout_s",
            "have",
        ],
        kind::REJECT => &["code", "text"],
        kind::HEARTBEAT => &["t"],
        kind::BYE => &["reason"],
        kind::ERROR => &["code", "text"],
        kind::SEGMENT => &["stream", "gen", "off", "len", "crc", "t", "enc"],
        kind::ACK => &["stream", "gen", "committed"],
        kind::RESEND => &["stream", "gen", "off", "len"],
        kind::STREAM_END => &["stream", "gen", "size", "sha256"],
        kind::STREAM_CHECK => &["stream", "gen", "result", "size"],
        kind::BLOCK_SUMS => &["stream", "gen", "block_size", "first", "sums"],
        kind::MESSAGE => &["id", "from", "to", "t", "kind", "len", "crc", "enc"],
        kind::MESSAGE_ACK => &["id"],
        kind::DELIVERY => &["id", "delivered", "pending", "unknown"],
        kind::DELIVERY_ACK => &["id", "delivered"],
        _ => return None,
    })
}

pub const ROLES: [&str; 3] = ["source", "consumer", "recipient"];
pub const ENCODINGS: [&str; 2] = ["none", "zlib"];

/// A frame violates the wire format. `code` is the error code to send (spec 14.4) before closing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameError {
    pub code: String,
    pub text: String,
}

impl FrameError {
    pub fn new(code: &str, text: impl Into<String>) -> Self {
        FrameError {
            code: code.to_string(),
            text: text.into(),
        }
    }
    fn bad(text: impl Into<String>) -> Self {
        Self::new("bad_frame", text)
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.text)
    }
}

impl std::error::Error for FrameError {}

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub version: u8,
    pub ftype: u8,
    pub meta: Meta,
    pub blob: Vec<u8>,
}

impl Frame {
    pub fn new(ftype: u8, meta: Meta) -> Self {
        Frame {
            version: VERSION,
            ftype,
            meta,
            blob: Vec::new(),
        }
    }

    pub fn with_blob(ftype: u8, meta: Meta, blob: Vec<u8>) -> Self {
        Frame {
            version: VERSION,
            ftype,
            meta,
            blob,
        }
    }

    pub fn is_extension(&self) -> bool {
        self.ftype >= EXTENSION_FIRST
    }

    pub fn str(&self, key: &str) -> &str {
        self.meta.get(key).and_then(Value::as_str).unwrap_or("")
    }

    pub fn u64(&self, key: &str) -> u64 {
        self.meta.get(key).and_then(Value::as_u64).unwrap_or(0)
    }
}

/// Builds a meta object from `(key, value)` pairs.
#[macro_export]
macro_rules! meta {
    ($($k:expr => $v:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut m = $crate::frames::Meta::new();
        $( m.insert($k.to_string(), serde_json::json!($v)); )*
        m
    }};
}

/// Compact, key-sorted UTF-8 JSON (spec 14.2): reproducible, so test vectors are exact. `serde_json::Map` keeps
/// its keys sorted.
pub fn encode_meta(meta: &Meta) -> Vec<u8> {
    serde_json::to_vec(meta).expect("a JSON object always serializes")
}

pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Frame -> bytes. Fails if a limit is exceeded.
pub fn encode(frame: &Frame) -> Result<Vec<u8>, FrameError> {
    let meta = encode_meta(&frame.meta);
    if meta.len() > MAX_META {
        return Err(FrameError::new(
            "too_large",
            format!("meta is {} bytes, limit {}", meta.len(), MAX_META),
        ));
    }
    if frame.blob.len() > MAX_BLOB {
        return Err(FrameError::new(
            "too_large",
            format!("blob is {} bytes, limit {}", frame.blob.len(), MAX_BLOB),
        ));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + meta.len() + frame.blob.len() + TRAILER_LEN);
    out.extend_from_slice(MAGIC);
    out.push(frame.version);
    out.push(frame.ftype);
    out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    out.extend_from_slice(&(frame.blob.len() as u32).to_be_bytes());
    out.extend_from_slice(&meta);
    out.extend_from_slice(&frame.blob);
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Ok(out)
}

/// Exactly one complete frame -> Frame. Checks magic, lengths, CRC and that meta is a JSON object.
pub fn decode(raw: &[u8]) -> Result<Frame, FrameError> {
    if raw.len() < HEADER_LEN + TRAILER_LEN {
        return Err(FrameError::bad("frame shorter than header and CRC"));
    }
    if &raw[0..2] != MAGIC {
        return Err(FrameError::bad("wrong magic"));
    }
    let m = u32::from_be_bytes(raw[4..8].try_into().unwrap()) as usize;
    let b = u32::from_be_bytes(raw[8..12].try_into().unwrap()) as usize;
    if raw.len() != HEADER_LEN + m + b + TRAILER_LEN {
        return Err(FrameError::bad("frame length does not match header"));
    }
    let body = &raw[..raw.len() - TRAILER_LEN];
    let crc = u32::from_be_bytes(raw[raw.len() - TRAILER_LEN..].try_into().unwrap());
    if crc32(body) != crc {
        return Err(FrameError::bad("CRC mismatch"));
    }
    let meta: Value = serde_json::from_slice(&raw[HEADER_LEN..HEADER_LEN + m])
        .map_err(|_| FrameError::bad("meta is not valid UTF-8 JSON"))?;
    let meta = match meta {
        Value::Object(o) => o,
        _ => return Err(FrameError::bad("meta is not a JSON object")),
    };
    Ok(Frame {
        version: raw[2],
        ftype: raw[3],
        meta,
        blob: raw[HEADER_LEN + m..HEADER_LEN + m + b].to_vec(),
    })
}

/// Incremental decoder: `feed` whatever the socket returned and get the frames it completed.
///
/// After an error the stream position is lost; the caller must send ERROR and close the connection (spec 14.2).
/// Further calls return the same error again.
#[derive(Default)]
pub struct Decoder {
    buf: Vec<u8>,
    need: Option<usize>,
    error: Option<FrameError>,
}

impl Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<Frame>, FrameError> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        loop {
            match self.next() {
                Ok(Some(f)) => out.push(f),
                Ok(None) => return Ok(out),
                Err(e) => {
                    self.error = Some(e.clone());
                    self.buf.clear();
                    return Err(e);
                }
            }
        }
    }

    fn next(&mut self) -> Result<Option<Frame>, FrameError> {
        if self.need.is_none() {
            if self.buf.len() < HEADER_LEN {
                // reject a wrong magic as early as possible
                let n = self.buf.len().min(2);
                if n > 0 && self.buf[..n] != MAGIC[..n] {
                    return Err(FrameError::bad("wrong magic"));
                }
                return Ok(None);
            }
            if &self.buf[0..2] != MAGIC {
                return Err(FrameError::bad("wrong magic"));
            }
            let m = u32::from_be_bytes(self.buf[4..8].try_into().unwrap()) as usize;
            let b = u32::from_be_bytes(self.buf[8..12].try_into().unwrap()) as usize;
            if m > MAX_META || b > MAX_BLOB {
                return Err(FrameError::new(
                    "too_large",
                    format!("frame announces meta {} / blob {} bytes", m, b),
                ));
            }
            self.need = Some(HEADER_LEN + m + b + TRAILER_LEN);
        }
        let need = self.need.unwrap();
        if self.buf.len() < need {
            return Ok(None);
        }
        let raw: Vec<u8> = self.buf.drain(..need).collect();
        self.need = None;
        decode(&raw).map(Some)
    }
}

fn is_identifier(v: &Value) -> bool {
    match v.as_str() {
        Some(s) => {
            (1..=64).contains(&s.len())
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
        }
        None => false,
    }
}

fn is_hex(v: &Value, len: usize) -> bool {
    match v.as_str() {
        Some(s) => {
            s.len() == len
                && s.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        }
        None => false,
    }
}

/// A non-negative integer of at least `min` (booleans and floats are not integers).
fn is_int(v: &Value, min: u64) -> bool {
    v.as_u64().is_some_and(|n| n >= min)
}

fn check_stream(meta: &Meta) -> Result<(), FrameError> {
    let ok = meta
        .get("stream")
        .and_then(Value::as_str)
        .is_some_and(|s| (1..=255).contains(&s.len()));
    if !ok {
        return Err(FrameError::bad("stream name must be 1-255 bytes UTF-8"));
    }
    if !meta.get("gen").is_some_and(|g| is_int(g, 1)) {
        return Err(FrameError::bad("generation must be an integer >= 1"));
    }
    Ok(())
}

fn list_of_identifiers(v: &Value) -> bool {
    v.as_array().is_some_and(|a| a.iter().all(is_identifier))
}

/// Plain-language reason why `value` is not a valid public participant list (WELCOME.participants), or None.
pub fn check_public_participants(value: &Value) -> Option<String> {
    let Some(list) = value.as_array() else {
        return Some("participants must be a list".into());
    };
    for e in list {
        let ok = e.get("participant").is_some_and(is_identifier)
            && e.get("name").is_some_and(Value::is_string)
            && e.get("role").is_some_and(Value::is_string);
        if !ok {
            return Some("participants entries need participant, name and role".into());
        }
    }
    None
}

/// Check a decoded core frame against spec 14.3/14.4. Extensions pass unchecked; an unknown core type fails with
/// `unknown_type`.
pub fn validate(frame: &Frame) -> Result<(), FrameError> {
    if frame.is_extension() {
        return Ok(());
    }
    let t = frame.ftype;
    let Some(req) = required(t) else {
        return Err(FrameError::new(
            "unknown_type",
            format!("unknown core frame type 0x{:02x}", t),
        ));
    };
    let meta = &frame.meta;
    let missing: Vec<&str> = req
        .iter()
        .copied()
        .filter(|k| !meta.contains_key(*k))
        .collect();
    if !missing.is_empty() {
        return Err(FrameError::bad(format!(
            "{} without {}",
            type_name(t),
            missing.join(", ")
        )));
    }
    let has = |k: &str| req.contains(&k);
    for k in ["t", "t_hello"] {
        if has(k) && !is_int(&meta[k], 0) {
            return Err(FrameError::bad(format!(
                "{} must be milliseconds since 1970 UTC",
                k
            )));
        }
    }
    for k in ["participant", "hub", "operation", "from"] {
        if has(k) && !is_identifier(&meta[k]) {
            return Err(FrameError::bad(format!("{} is not a valid identifier", k)));
        }
    }
    if has("stream") {
        check_stream(meta)?;
    }
    for k in ["off", "len", "size", "committed", "first", "crc"] {
        if has(k) && !is_int(&meta[k], 0) {
            return Err(FrameError::bad(format!(
                "{} must be a non-negative integer",
                k
            )));
        }
    }
    if (t == kind::REJECT || t == kind::ERROR) && !meta["text"].is_string() {
        return Err(FrameError::bad("text must be a string"));
    }
    if t == kind::REJECT {
        if meta.get("operation").is_some_and(|v| !is_identifier(v)) {
            return Err(FrameError::bad("operation is not a valid identifier"));
        }
        if let Some(v) = meta.get("operation_name") {
            let ok = v
                .as_str()
                .is_some_and(|s| (1..=MAX_OPERATION_NAME).contains(&s.chars().count()));
            if !ok {
                return Err(FrameError::bad(format!(
                    "operation_name must be 1-{} characters",
                    MAX_OPERATION_NAME
                )));
            }
        }
    }
    if t == kind::HELLO {
        if !meta["role"].as_str().is_some_and(|r| ROLES.contains(&r)) {
            return Err(FrameError::bad(format!("unknown role {}", meta["role"])));
        }
        let ok = meta["versions"]
            .as_array()
            .is_some_and(|a| !a.is_empty() && a.iter().all(|v| is_int(v, 1)));
        if !ok {
            return Err(FrameError::bad(
                "versions must be a non-empty list of integers >= 1",
            ));
        }
    }
    if t == kind::HELLO || t == kind::WELCOME {
        let Some(have) = meta["have"].as_array() else {
            return Err(FrameError::bad("have must be a list"));
        };
        for h in have {
            let Some(h) = h.as_object() else {
                return Err(FrameError::bad("have entries need stream, gen, committed"));
            };
            if !(h.contains_key("stream")
                && h.contains_key("gen")
                && h.get("committed").is_some_and(|c| is_int(c, 0)))
            {
                return Err(FrameError::bad("have entries need stream, gen, committed"));
            }
            check_stream(h)?;
        }
    }
    if t == kind::WELCOME {
        if !is_int(&meta["version"], 1) {
            return Err(FrameError::bad("version must be an integer >= 1"));
        }
        for k in ["heartbeat_s", "timeout_s"] {
            if !meta[k].as_f64().is_some_and(|v| v > 0.0) {
                return Err(FrameError::bad(format!("{} must be a positive number", k)));
            }
        }
        if let Some(p) = meta.get("participants") {
            if let Some(reason) = check_public_participants(p) {
                return Err(FrameError::bad(reason));
            }
        }
    }
    if (t == kind::SEGMENT || t == kind::MESSAGE)
        && !meta["enc"].as_str().is_some_and(|e| ENCODINGS.contains(&e))
    {
        return Err(FrameError::bad(format!("unknown encoding {}", meta["enc"])));
    }
    if t == kind::SEGMENT && meta.get("repair").is_some_and(|v| !v.is_boolean()) {
        return Err(FrameError::bad("repair must be true or false"));
    }
    if t == kind::STREAM_END && !is_hex(&meta["sha256"], 64) {
        return Err(FrameError::bad(
            "sha256 must be 64 lower-case hex characters",
        ));
    }
    if t == kind::STREAM_CHECK && !matches!(meta["result"].as_str(), Some("ok") | Some("mismatch"))
    {
        return Err(FrameError::bad("result must be ok or mismatch"));
    }
    if t == kind::BLOCK_SUMS {
        if !is_int(&meta["block_size"], 1) {
            return Err(FrameError::bad("block_size must be an integer >= 1"));
        }
        if !meta["sums"]
            .as_array()
            .is_some_and(|a| a.iter().all(|s| is_hex(s, 64)))
        {
            return Err(FrameError::bad("sums must be SHA-256 hex digests"));
        }
    }
    if matches!(
        t,
        kind::MESSAGE | kind::MESSAGE_ACK | kind::DELIVERY | kind::DELIVERY_ACK
    ) && !is_hex(&meta["id"], 32)
    {
        return Err(FrameError::bad(
            "message id must be 32 lower-case hex characters",
        ));
    }
    if t == kind::MESSAGE {
        if !meta["to"]
            .as_array()
            .is_some_and(|a| !a.is_empty() && a.iter().all(is_identifier))
        {
            return Err(FrameError::bad(
                "to must be a non-empty list of identifiers",
            ));
        }
        if !is_identifier(&meta["kind"]) {
            return Err(FrameError::bad("kind is not a valid identifier"));
        }
    }
    if t == kind::DELIVERY {
        for k in ["delivered", "pending", "unknown"] {
            if !list_of_identifiers(&meta[k]) {
                return Err(FrameError::bad(format!(
                    "{} must be a list of identifiers",
                    k
                )));
            }
        }
    }
    if t == kind::DELIVERY_ACK && !is_int(&meta["delivered"], 0) {
        return Err(FrameError::bad("delivered must be a non-negative integer"));
    }
    // unknown reject and error codes from newer versions are allowed, but must be strings (spec 14.9)
    if (t == kind::REJECT || t == kind::ERROR) && !meta["code"].is_string() {
        return Err(FrameError::bad("code must be a string"));
    }
    Ok(())
}

/// Raw bytes -> (enc, blob): zlib only when it makes the blob smaller (spec 14.4).
pub fn encode_payload(data: &[u8], level: u32) -> (&'static str, Vec<u8>) {
    let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(level));
    z.write_all(data).expect("writing to memory");
    let packed = z.finish().expect("writing to memory");
    if packed.len() < data.len() {
        ("zlib", packed)
    } else {
        ("none", data.to_vec())
    }
}

/// (enc, blob) -> raw bytes of exactly `length`; never decodes more than `length` + 1 bytes (spec 14.4).
pub fn decode_payload(enc: &str, blob: &[u8], length: u64) -> Result<Vec<u8>, FrameError> {
    let data = match enc {
        "none" => blob.to_vec(),
        "zlib" => {
            let mut d = flate2::bufread::ZlibDecoder::new(blob);
            let mut out = Vec::new();
            (&mut d)
                .take(length + 1)
                .read_to_end(&mut out)
                .map_err(|_| FrameError::bad("zlib data damaged"))?;
            // the zlib stream must end exactly here: nothing left in it and no bytes after it
            let mut rest = [0u8; 1];
            let more = d
                .read(&mut rest)
                .map_err(|_| FrameError::bad("zlib data damaged"))?;
            if more != 0 || d.total_in() as usize != blob.len() {
                return Err(FrameError::bad(
                    "zlib data incomplete or followed by extra bytes",
                ));
            }
            out
        }
        other => return Err(FrameError::bad(format!("unknown encoding {:?}", other))),
    };
    if data.len() as u64 != length {
        return Err(FrameError::bad(format!(
            "decoded length {}, expected {}",
            data.len(),
            length
        )));
    }
    Ok(data)
}

/// A SEGMENT frame for raw `data` at offset `off`.
pub fn segment(
    stream: &str,
    gen: u64,
    off: u64,
    data: &[u8],
    t: u64,
    level: u32,
    repair: bool,
) -> Frame {
    let (enc, blob) = encode_payload(data, level);
    let mut m = crate::meta! {"stream" => stream, "gen" => gen, "off" => off, "len" => data.len(),
    "crc" => crc32(data), "t" => t, "enc" => enc};
    if repair {
        m.insert("repair".into(), Value::Bool(true));
    }
    Frame::with_blob(kind::SEGMENT, m, blob)
}

/// Decoded, CRC-checked raw data of a SEGMENT or the body of a MESSAGE.
pub fn payload(frame: &Frame) -> Result<Vec<u8>, FrameError> {
    let data = decode_payload(frame.str("enc"), &frame.blob, frame.u64("len"))?;
    if crc32(&data) as u64 != frame.u64("crc") {
        return Err(FrameError::bad(format!(
            "{} CRC mismatch",
            type_name(frame.ftype)
        )));
    }
    Ok(data)
}
