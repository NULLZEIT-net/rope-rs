//! Source participant: sends growing files to the hub and resumes from the committed offset (spec 8.2–8.4, 9, 10,
//! 14.7). Messages: see [`crate::participant`].
//!
//! Files are only read: opened briefly, read, closed – no locks, so the recording program is never disturbed. The
//! file on disk is the buffer during an outage; in memory there are at most `window` unacknowledged segments.
//!
//! Not yet: offline media.

use crate::frames::{self, kind, Frame};
use crate::participant::{debug, log, Participant, Role};
use crate::session::{Conn, End};
use crate::{meta, now_ms};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use crate::participant::{Options, Rejected};

/// Rounds of block repair per generation before the mismatch is reported to the application (spec 14.7, 14.8).
pub const MAX_REPAIR_ROUNDS: u32 = 3;

/// Sending state of one generation of one stream.
struct OutStream {
    gen: u64,
    size: u64,
    final_size: Option<u64>,
    acked: u64,
    sent: u64,
    end_sent: bool,
    /// STREAM_END sent in this session, its STREAM_CHECK not yet received.
    awaiting_check: bool,
    result: Option<String>,
    /// Repairs of this generation that reached the receiver and still mismatched.
    repair_rounds: u32,
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
    fn new(gen: u64) -> Self {
        OutStream {
            gen,
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

    fn start_session(&mut self, committed: u64) {
        self.acked = committed;
        self.sent = committed;
        self.end_sent = false;
        self.awaiting_check = false;
        self.reset_repair(); // a new final check starts the repair over
    }

    fn sums_complete(&self) -> bool {
        match (&self.sums, self.block_size) {
            (Some(s), Some(b)) => s.len() as u64 >= self.receiver_size.div_ceil(b),
            _ => false,
        }
    }
}

/// One file at the source. A new generation starts when the file became shorter, or when the final check finds
/// most of its blocks changed (spec 14.4, 14.7); bytes changed in place are repaired block by block instead.
struct SourceStream {
    path: PathBuf,
    /// Largest size seen in this generation.
    seen: u64,
    complete_requested: bool,
    out: OutStream,
    /// The session has set the starting point of this generation from what the hub holds.
    started: bool,
}

impl SourceStream {
    fn new(path: PathBuf, saved: Option<&Value>) -> Self {
        let gen = saved.and_then(|s| s["gen"].as_u64()).unwrap_or(1).max(1);
        let seen = saved.and_then(|s| s["seen"].as_u64()).unwrap_or(0);
        SourceStream {
            path,
            seen,
            complete_requested: false,
            out: OutStream::new(gen),
            started: false,
        }
    }

    fn new_generation(&mut self, name: &str, why: &str) {
        log(
            "source",
            &format!("{} {}: new generation {}", name, why, self.out.gen + 1),
        );
        self.out = OutStream::new(self.out.gen + 1);
        self.seen = 0;
        self.complete_requested = false;
        self.started = true; // the hub holds nothing of a new generation
    }

    /// Applies the file size seen now; true if the persistent state changed.
    fn apply(&mut self, name: &str, size: u64) -> bool {
        let mut changed = false;
        if size < self.seen {
            self.new_generation(name, "became shorter");
            changed = true;
        }
        if size > self.seen {
            self.seen = size;
            changed = true;
        }
        self.out.size = size;
        if self.complete_requested && self.out.final_size.is_none() {
            // never less than what the hub may already hold
            self.out.final_size = Some(size.max(self.out.sent));
        }
        changed
    }
}

struct Shared {
    streams: BTreeMap<String, SourceStream>,
    saved_at_start: Map<String, Value>,
    state_path: PathBuf,
}

impl Shared {
    /// Looks at every file's size; writes the state file when it changed.
    fn refresh(&mut self) -> io::Result<()> {
        let mut changed = false;
        for (name, s) in self.streams.iter_mut() {
            if let Ok(md) = fs::metadata(&s.path) {
                changed |= s.apply(name, md.len());
            }
        }
        if changed {
            self.save()?;
        }
        Ok(())
    }

    fn save(&self) -> io::Result<()> {
        let state: Map<String, Value> = self
            .streams
            .iter()
            .map(|(n, s)| (n.clone(), json!({"gen": s.out.gen, "seen": s.seen})))
            .collect();
        write_json_atomic(&self.state_path, &Value::Object(state))
    }
}

/// A running source. Messages, connection state and `stop` come from [`Participant`] (via `Deref`).
pub struct Source {
    shared: Arc<Mutex<Shared>>,
    participant: Participant,
}

impl Deref for Source {
    type Target = Participant;
    fn deref(&self) -> &Participant {
        &self.participant
    }
}

impl DerefMut for Source {
    fn deref_mut(&mut self) -> &mut Participant {
        &mut self.participant
    }
}

impl Source {
    /// Starts a source. `streams`: (stream name, file path); `state_dir` keeps what must survive a restart.
    pub fn start(
        participant: &str,
        operation: &str,
        hub: &str,
        streams: Vec<(String, PathBuf)>,
        state_dir: &Path,
        opts: Options,
    ) -> io::Result<Source> {
        fs::create_dir_all(state_dir)?;
        let state_path = state_dir.join("source_state.json");
        let saved_at_start = match fs::read(&state_path) {
            Ok(raw) => match serde_json::from_slice(&raw) {
                Ok(Value::Object(m)) => m,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "source state file is damaged",
                    ))
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => Map::new(),
            Err(e) => return Err(e),
        };
        let streams = streams
            .into_iter()
            .map(|(n, p)| {
                let s = SourceStream::new(p, saved_at_start.get(&n));
                (n, s)
            })
            .collect();
        let shared = Arc::new(Mutex::new(Shared {
            streams,
            saved_at_start,
            state_path,
        }));
        let role = SourceRole {
            shared: shared.clone(),
            opts: opts.clone(),
            inflight: Vec::new(),
            have: HashMap::new(),
        };
        let participant =
            Participant::spawn(participant, operation, hub, state_dir, opts, Box::new(role))?;
        Ok(Source {
            shared,
            participant,
        })
    }

    /// Sends one more file from now on. A name known from an earlier run continues where the hub left off; a name
    /// already sent does nothing. True if the stream is new.
    pub fn add_stream(&self, name: &str, path: PathBuf) -> bool {
        let mut sh = self.shared.lock().unwrap();
        if sh.streams.contains_key(name) {
            return false;
        }
        let s = SourceStream::new(path, sh.saved_at_start.get(name));
        sh.streams.insert(name.into(), s);
        true
    }

    /// The application declares the stream finished: send the final check (spec 8.4).
    pub fn complete(&self, name: &str) -> bool {
        match self.shared.lock().unwrap().streams.get_mut(name) {
            Some(s) => {
                s.complete_requested = true;
                true
            }
            None => false,
        }
    }

    /// Results of the final checks of the current generations: {stream: "ok" | "mismatch"}.
    pub fn checked(&self) -> BTreeMap<String, String> {
        let sh = self.shared.lock().unwrap();
        sh.streams
            .iter()
            .filter_map(|(n, s)| s.out.result.clone().map(|r| (n.clone(), r)))
            .collect()
    }
}

/// The sending side in a session: window of unacknowledged segments, final check, block repair.
struct SourceRole {
    shared: Arc<Mutex<Shared>>,
    opts: Options,
    /// (stream, gen, end offset) of segments sent and not yet acknowledged.
    inflight: Vec<(String, u64, u64)>,
    /// What the hub reported in WELCOME.
    have: HashMap<(String, u64), u64>,
}

impl Role for SourceRole {
    fn role(&self) -> &'static str {
        "source"
    }

    fn start_session(&mut self, have: HashMap<(String, u64), u64>) -> Result<(), End> {
        let mut sh = self.shared.lock().unwrap();
        sh.refresh()?;
        for s in sh.streams.values_mut() {
            s.started = false;
        }
        self.have = have;
        self.inflight.clear();
        Ok(())
    }

    fn handle(&mut self, f: &Frame, _c: &mut Conn) -> Result<bool, End> {
        if !matches!(
            f.ftype,
            kind::ACK | kind::RESEND | kind::STREAM_CHECK | kind::BLOCK_SUMS
        ) {
            return Ok(false);
        }
        let name = f.str("stream").to_string();
        let gen = f.u64("gen");
        let mut sh = self.shared.lock().unwrap();
        let Some(s) = sh.streams.get_mut(&name) else {
            return Ok(true);
        };
        let o = &mut s.out;
        if o.gen != gen {
            return Ok(true);
        }
        match f.ftype {
            kind::ACK => {
                o.acked = o.acked.max(f.u64("committed"));
                let acked = o.acked;
                self.inflight
                    .retain(|(n, g, end)| !(*n == name && *g == gen && *end <= acked));
            }
            kind::RESEND => {
                o.sent = o.sent.min(f.u64("off"));
                self.inflight.retain(|(n, g, _)| !(*n == name && *g == gen));
            }
            kind::STREAM_CHECK => {
                if !o.awaiting_check {
                    // no STREAM_END of ours is open: a stray answer, never act on it (spec 14.7)
                    log(
                        "source",
                        &format!(
                            "{} generation {}: unexpected STREAM_CHECK ignored",
                            name, gen
                        ),
                    );
                    return Ok(true);
                }
                o.awaiting_check = false;
                if o.repair_sent {
                    o.repair_rounds += 1; // same session: the repair reached the hub (TCP keeps order)
                }
                let result = f.str("result").to_string();
                if result == "ok" || o.repair_rounds >= MAX_REPAIR_ROUNDS {
                    if result != "ok" {
                        let text = format!(
                            "{} generation {}: final check still fails after {} repairs",
                            name, gen, o.repair_rounds
                        );
                        log("source", &text);
                    }
                    o.result = Some(result);
                } else {
                    o.reset_repair();
                    o.receiver_size = f.u64("size");
                    o.sums = Some(BTreeMap::new());
                }
            }
            _ => {
                // BLOCK_SUMS: only while a failed final check waits for them
                if let Some(sums) = o.sums.as_mut() {
                    o.block_size = Some(f.u64("block_size"));
                    let first = f.u64("first");
                    for (i, d) in f.meta["sums"].as_array().into_iter().flatten().enumerate() {
                        sums.insert(first + i as u64, d.as_str().unwrap_or("").to_string());
                    }
                }
            }
        }
        Ok(true)
    }

    fn work(&mut self, c: &mut Conn) -> Result<bool, End> {
        let shared = self.shared.clone();
        let mut sh = shared.lock().unwrap();
        sh.refresh()?;
        let mut progressed = false;
        let mut save = false;
        let names: Vec<String> = sh.streams.keys().cloned().collect();
        for name in names {
            let s = sh.streams.get_mut(&name).unwrap();
            if !s.started {
                // continue from what the hub holds of this generation (also for streams added in the session)
                s.out.start_session(
                    self.have
                        .get(&(name.clone(), s.out.gen))
                        .copied()
                        .unwrap_or(0),
                );
                s.started = true;
            }
            if s.out.sums_complete() && self.compare(&name, s)? {
                save = true;
                continue; // a new generation: nothing more of the old one
            }
            progressed |= self.send_repairs(c, &name, s)?;
            let o = &mut s.out;
            let limit = o.final_size.unwrap_or(o.size);
            while self.inflight.len() < self.opts.window && o.sent < limit {
                let n = (self.opts.segment_bytes as u64).min(limit - o.sent);
                let data = read_range(&s.path, o.sent, n)?;
                if data.is_empty() {
                    break;
                }
                c.send(&frames::segment(
                    &name,
                    o.gen,
                    o.sent,
                    &data,
                    now_ms(),
                    self.opts.zlib_level,
                    false,
                ))?;
                o.sent += data.len() as u64;
                self.inflight.push((name.clone(), o.gen, o.sent));
                progressed = true;
            }
            if let Some(final_size) = o.final_size {
                let due = !o.end_sent && o.result.as_deref() != Some("ok") && o.acked >= final_size;
                if due && o.sums.is_none() && o.repairs.is_empty() {
                    let (digest, n) = sha256_prefix(&s.path, final_size)?;
                    debug("source", || {
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
        }
        if save {
            sh.save()?;
        }
        Ok(progressed)
    }
}

impl SourceRole {
    /// All BLOCK_SUMS of a failed final check arrived: find the differing blocks. True if most blocks differ and a
    /// new generation was started instead of a repair (spec 14.7).
    fn compare(&mut self, name: &str, s: &mut SourceStream) -> Result<bool, End> {
        let o = &mut s.out;
        let block = o.block_size.unwrap();
        let count = o.receiver_size.div_ceil(block);
        let sums: Vec<String> = (0..count)
            .map(|i| o.sums.as_ref().unwrap()[&i].clone())
            .collect();
        o.sums = None;
        let diff = differing_blocks(&s.path, block, &sums, o.receiver_size)?;
        if count > 0 && diff.len() as u64 * 2 > count {
            s.new_generation(name, "changed in most of its blocks");
            self.inflight.retain(|(n, _, _)| n != name);
            return Ok(true);
        }
        log(
            "source",
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

    fn send_repairs(
        &mut self,
        c: &mut Conn,
        name: &str,
        s: &mut SourceStream,
    ) -> Result<bool, End> {
        let mut progressed = false;
        let o = &mut s.out;
        while let Some(&(off, end)) = o.repairs.front() {
            let data = read_range(
                &s.path,
                off,
                (self.opts.segment_bytes as u64).min(end - off),
            )?;
            if data.is_empty() {
                o.repairs.clear(); // the file became shorter: the next refresh starts a new generation
                break;
            }
            c.send(&frames::segment(
                name,
                o.gen,
                off,
                &data,
                now_ms(),
                self.opts.zlib_level,
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
        Ok(progressed)
    }
}

fn read_range(path: &Path, off: u64, n: u64) -> io::Result<Vec<u8>> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(off))?;
    let mut out = Vec::with_capacity(n as usize);
    f.take(n).read_to_end(&mut out)?;
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// SHA-256 of the first `size` bytes, and how many bytes were actually there.
fn sha256_prefix(path: &Path, size: u64) -> io::Result<(String, u64)> {
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
fn differing_blocks(
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

/// Writes JSON durably: temporary file, flush to disk, rename over the old one.
fn write_json_atomic(path: &Path, value: &Value) -> io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(value)?)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}
