//! Sending side of streams, shared by the source (to the hub) and the hub (to consumers): window of unacknowledged
//! segments, resume from the receiver's committed offset, final check and block repair (spec 8.2, 8.4, 14.7).
//!
//! Files are only read: opened briefly, read, closed – no locks. The file on disk is the buffer during an outage; in
//! memory there are at most `window` unacknowledged segments.

use crate::frames::{self, kind, Frame};
use crate::participant::{debug, log};
use crate::session::{Conn, End};
use crate::{meta, now_ms};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Rounds of block repair per generation before the mismatch is reported to the application (spec 14.7, 14.8).
pub const MAX_REPAIR_ROUNDS: u32 = 3;

/// Sending state of one generation of one stream. The owner keeps `size` (bytes available) and `final_size` (set
/// once the stream is complete) up to date.
pub(crate) struct OutStream {
    pub gen: u64,
    pub path: PathBuf,
    pub size: u64,
    pub final_size: Option<u64>,
    pub acked: u64,
    pub sent: u64,
    pub end_sent: bool,
    /// STREAM_END sent in this session, its STREAM_CHECK not yet received.
    pub awaiting_check: bool,
    pub result: Option<String>,
    /// Repairs of this generation that reached the receiver and still mismatched.
    pub repair_rounds: u32,
    receiver_size: u64,
    block_size: Option<u64>,
    /// Block index -> SHA-256 while BLOCK_SUMS arrive.
    sums: Option<BTreeMap<u64, String>>,
    /// (off, end) still to resend as repair.
    repairs: VecDeque<(u64, u64)>,
    /// Repair done, its STREAM_END not yet answered in this session.
    repair_sent: bool,
}

impl OutStream {
    pub fn new(gen: u64, path: PathBuf) -> Self {
        OutStream {
            gen,
            path,
            size: 0,
            final_size: None,
            acked: 0,
            sent: 0,
            end_sent: false,
            awaiting_check: false,
            result: None,
            repair_rounds: 0,
            receiver_size: 0,
            block_size: None,
            sums: None,
            repairs: VecDeque::new(),
            repair_sent: false,
        }
    }

    fn reset_repair(&mut self) {
        self.receiver_size = 0;
        self.block_size = None;
        self.sums = None;
        self.repairs.clear();
        self.repair_sent = false;
    }

    /// A new session: continue from what the receiver holds; a new final check starts the repair over.
    pub fn start_session(&mut self, committed: u64) {
        self.acked = committed;
        self.sent = committed;
        self.end_sent = false;
        self.awaiting_check = false;
        self.reset_repair();
    }

    /// All BLOCK_SUMS of a failed final check have arrived.
    pub fn sums_complete(&self) -> bool {
        match (&self.sums, self.block_size) {
            (Some(s), Some(b)) => s.len() as u64 >= self.receiver_size.div_ceil(b),
            _ => false,
        }
    }
}

/// The frames a sender takes from its receiver.
pub(crate) fn is_sender_frame(t: u8) -> bool {
    matches!(
        t,
        kind::ACK | kind::RESEND | kind::STREAM_CHECK | kind::BLOCK_SUMS
    )
}

/// Window and wire settings of one session's sending side.
pub(crate) struct Sender {
    pub segment_bytes: u64,
    pub window: usize,
    pub zlib_level: u32,
    /// For log lines: "source", "hub".
    pub who: &'static str,
    /// (stream, gen, end offset) of segments sent and not yet acknowledged.
    inflight: Vec<(String, u64, u64)>,
}

impl Sender {
    pub fn new(segment_bytes: u64, window: usize, zlib_level: u32, who: &'static str) -> Self {
        Sender {
            segment_bytes,
            window,
            zlib_level,
            who,
            inflight: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.inflight.clear();
    }

    /// Forgets the segments in flight of a stream (e.g. after a new generation).
    pub fn forget(&mut self, name: &str) {
        self.inflight.retain(|(n, _, _)| n != name);
    }

    /// ACK, RESEND, STREAM_CHECK or BLOCK_SUMS for `o` (the caller found it by stream and generation).
    pub fn handle(&mut self, f: &Frame, name: &str, o: &mut OutStream) {
        let gen = o.gen;
        match f.ftype {
            kind::ACK => {
                o.acked = o.acked.max(f.u64("committed"));
                let acked = o.acked;
                self.inflight
                    .retain(|(n, g, end)| !(n == name && *g == gen && *end <= acked));
            }
            kind::RESEND => {
                o.sent = o.sent.min(f.u64("off"));
                self.inflight.retain(|(n, g, _)| !(n == name && *g == gen));
            }
            kind::STREAM_CHECK => {
                if !o.awaiting_check {
                    // no STREAM_END of ours is open: a stray answer, never act on it (spec 14.7)
                    log(
                        self.who,
                        &format!(
                            "{} generation {}: unexpected STREAM_CHECK ignored",
                            name, gen
                        ),
                    );
                    return;
                }
                o.awaiting_check = false;
                if o.repair_sent {
                    o.repair_rounds += 1; // same session: the repair reached the receiver (TCP keeps order)
                }
                let result = f.str("result").to_string();
                if result == "ok" || o.repair_rounds >= MAX_REPAIR_ROUNDS {
                    if result != "ok" {
                        let text = format!(
                            "{} generation {}: final check still fails after {} repairs",
                            name, gen, o.repair_rounds
                        );
                        log(self.who, &text);
                    }
                    o.result = Some(result);
                } else {
                    o.reset_repair();
                    o.receiver_size = f.u64("size");
                    o.sums = Some(BTreeMap::new());
                }
            }
            kind::BLOCK_SUMS => {
                // only while a failed final check waits for them
                if let Some(sums) = o.sums.as_mut() {
                    o.block_size = Some(f.u64("block_size"));
                    let first = f.u64("first");
                    for (i, d) in f.meta["sums"].as_array().into_iter().flatten().enumerate() {
                        sums.insert(first + i as u64, d.as_str().unwrap_or("").to_string());
                    }
                }
            }
            _ => {}
        }
    }

    /// All BLOCK_SUMS arrived: finds the differing blocks and plans their repair. With `allow_new_gen`, returns true
    /// instead if most blocks differ – the file was replaced, the caller starts a new generation (spec 14.7).
    pub fn compare(
        &mut self,
        name: &str,
        o: &mut OutStream,
        allow_new_gen: bool,
    ) -> io::Result<bool> {
        let block = o.block_size.unwrap_or(1);
        let count = o.receiver_size.div_ceil(block);
        let sums: Vec<String> = (0..count)
            .map(|i| {
                o.sums
                    .as_ref()
                    .and_then(|s| s.get(&i))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        o.sums = None;
        let diff = differing_blocks(&o.path, block, &sums, o.receiver_size)?;
        if allow_new_gen && count > 0 && diff.len() as u64 * 2 > count {
            self.forget(name);
            return Ok(true);
        }
        log(
            self.who,
            &format!(
                "{} generation {}: resending {} of {} blocks",
                name,
                o.gen,
                diff.len(),
                count
            ),
        );
        o.repairs = diff.into();
        if o.repairs.is_empty() {
            // the blocks agree (file changed meanwhile?): check again
            o.repair_sent = true;
            o.end_sent = false;
        }
        Ok(false)
    }

    /// Sends what is due for one stream: repairs, new segments within the window, the final check. True if anything
    /// was sent.
    pub fn send(&mut self, c: &mut Conn, name: &str, o: &mut OutStream) -> Result<bool, End> {
        let mut progressed = false;
        while let Some(&(off, end)) = o.repairs.front() {
            let data = read_range(&o.path, off, self.segment_bytes.min(end - off))?;
            if data.is_empty() {
                o.repairs.clear(); // the file became shorter: the owner starts a new generation
                break;
            }
            c.send(&frames::segment(
                name,
                o.gen,
                off,
                &data,
                now_ms(),
                self.zlib_level,
                true,
            ))?;
            progressed = true;
            if off + data.len() as u64 >= end {
                o.repairs.pop_front();
                if o.repairs.is_empty() {
                    o.repair_sent = true;
                    o.end_sent = false; // check again
                }
            } else {
                o.repairs[0] = (off + data.len() as u64, end);
            }
        }
        let limit = o.final_size.unwrap_or(o.size);
        while self.inflight.len() < self.window && o.sent < limit {
            let data = read_range(&o.path, o.sent, self.segment_bytes.min(limit - o.sent))?;
            if data.is_empty() {
                break;
            }
            c.send(&frames::segment(
                name,
                o.gen,
                o.sent,
                &data,
                now_ms(),
                self.zlib_level,
                false,
            ))?;
            o.sent += data.len() as u64;
            self.inflight.push((name.to_string(), o.gen, o.sent));
            progressed = true;
        }
        if let Some(final_size) = o.final_size {
            let due = !o.end_sent && o.result.as_deref() != Some("ok") && o.acked >= final_size;
            if due && o.sums.is_none() && o.repairs.is_empty() {
                let (digest, n) = sha256_prefix(&o.path, final_size)?;
                debug(self.who, || {
                    format!("STREAM_END {} gen {} size {}", name, o.gen, n)
                });
                c.send(&Frame::new(
                    kind::STREAM_END,
                    meta! {"stream" => name, "gen" => o.gen, "size" => n, "sha256" => digest},
                ))?;
                o.end_sent = true;
                o.awaiting_check = true;
                progressed = true;
            }
        }
        Ok(progressed)
    }
}

pub(crate) fn read_range(path: &Path, off: u64, n: u64) -> io::Result<Vec<u8>> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(off))?;
    let mut out = Vec::with_capacity(n as usize);
    f.take(n).read_to_end(&mut out)?;
    Ok(out)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// SHA-256 of the first `size` bytes, and how many bytes were actually there.
pub(crate) fn sha256_prefix(path: &Path, size: u64) -> io::Result<(String, u64)> {
    let mut h = Sha256::new();
    let mut r = File::open(path)?.take(size);
    let mut buf = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        total += n as u64;
    }
    Ok((hex(&h.finalize()), total))
}

/// Ranges (off, end) of the blocks whose SHA-256 in the file differs from the receiver's `sums` (spec 14.7).
pub(crate) fn differing_blocks(
    path: &Path,
    block: u64,
    sums: &[String],
    receiver_size: u64,
) -> io::Result<Vec<(u64, u64)>> {
    let mut out = Vec::new();
    for (i, digest) in sums.iter().enumerate() {
        let off = i as u64 * block;
        let end = (off + block).min(receiver_size);
        let data = read_range(path, off, end - off)?;
        if hex(&Sha256::digest(&data)) != *digest {
            out.push((off, end));
        }
    }
    Ok(out)
}
