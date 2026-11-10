//! Source participant: sends growing files to the hub and resumes from the committed offset (spec 8.2–8.4, 9, 10,
//! 14.7).
//!
//! Files are only read: opened briefly, read, closed – no locks, so the recording program is never disturbed. The
//! file on disk is the buffer during an outage; in memory there are at most `window` unacknowledged segments.
//!
//! Not yet: messages (the source ignores `MESSAGE` frames, so the hub keeps them) and offline media.

use crate::frames::{self, kind, Frame, FrameError};
use crate::session::{Conn, End};
use crate::{meta, now_ms, PROTOCOL_VERSION};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Rounds of block repair per generation before the mismatch is reported to the application (spec 14.7, 14.8).
pub const MAX_REPAIR_ROUNDS: u32 = 3;

/// Settings of a source; the defaults are the recommended values of spec 14.8.
#[derive(Clone, Debug)]
pub struct Options {
    pub segment_bytes: usize,
    pub window: usize,
    /// How often file sizes are looked at when nothing else is going on.
    pub poll: Duration,
    /// Time limit for connecting and greeting; after `WELCOME` the hub's `timeout_s` applies.
    pub timeout: Duration,
    /// Reconnect back-off: first and longest wait.
    pub backoff: (Duration, Duration),
    pub zlib_level: u32,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            segment_bytes: 256 * 1024,
            window: 8,
            poll: Duration::from_millis(500),
            timeout: Duration::from_secs(10),
            backoff: (Duration::from_secs(1), Duration::from_secs(10)),
            zlib_level: 1,
        }
    }
}

/// The hub refused the greeting (spec 14.5), with its plain-language reason.
#[derive(Clone, Debug, PartialEq)]
pub struct Rejected {
    pub code: String,
    pub text: String,
    /// The hub's operation, if it announces it (`wrong_operation`, spec 14.5). Never joined on its own.
    pub operation: Option<String>,
    pub operation_name: Option<String>,
}

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
        }
    }

    fn new_generation(&mut self, name: &str, why: &str) {
        log(&format!(
            "{} {}: new generation {}",
            name,
            why,
            self.out.gen + 1
        ));
        self.out = OutStream::new(self.out.gen + 1);
        self.seen = 0;
        self.complete_requested = false;
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
    rejected: Option<Rejected>,
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

/// A running source. Reconnects forever with back-off until [`Source::stop`].
pub struct Source {
    shared: Arc<Mutex<Shared>>,
    stop: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

struct Config {
    participant: String,
    operation: String,
    hub: String,
    opts: Options,
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
            rejected: None,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let connected = Arc::new(AtomicBool::new(false));
        let cfg = Config {
            participant: participant.into(),
            operation: operation.into(),
            hub: hub.into(),
            opts,
        };
        let (sh, st, co) = (shared.clone(), stop.clone(), connected.clone());
        let thread = thread::spawn(move || run(cfg, sh, st, co));
        Ok(Source {
            shared,
            stop,
            connected,
            thread: Some(thread),
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

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// The hub's last refusal; cleared by the next session it accepts.
    pub fn rejected(&self) -> Option<Rejected> {
        self.shared.lock().unwrap().rejected.clone()
    }

    /// Ends the session (with `BYE`) and the reconnect loop; returns when the thread has finished.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.stop();
    }
}

fn log(text: &str) {
    eprintln!("rope source {}: {}", crate::now_ms() % 100_000, text);
}

/// Detailed events only with the environment variable `ROPE_DEBUG` set.
fn debug(text: impl FnOnce() -> String) {
    if std::env::var_os("ROPE_DEBUG").is_some() {
        log(&text());
    }
}

enum Ended {
    Rejected(Rejected),
    Other(String),
}

fn run(cfg: Config, shared: Arc<Mutex<Shared>>, stop: Arc<AtomicBool>, connected: Arc<AtomicBool>) {
    let (first, longest) = cfg.opts.backoff;
    let mut delay = first;
    while !stop.load(Ordering::SeqCst) {
        let ended = session(&cfg, &shared, &stop, &connected);
        connected.store(false, Ordering::SeqCst);
        match ended {
            Ok(()) => delay = first,
            Err(Ended::Rejected(r)) => {
                log(&format!("hub rejected {}: {}", cfg.participant, r.text));
                delay = longest;
            }
            Err(Ended::Other(why)) => log(&format!(
                "connection lost ({}), retrying in {:.2} s",
                why,
                delay.as_secs_f64()
            )),
        }
        // back-off with jitter (spec 14.8), in small steps so that stop() is quick
        let wait = delay.mul_f64(0.8 + 0.2 * jitter());
        let end = Instant::now() + wait;
        while Instant::now() < end && !stop.load(Ordering::SeqCst) {
            thread::sleep(
                Duration::from_millis(10).min(end.saturating_duration_since(Instant::now())),
            );
        }
        delay = (delay * 2).min(longest);
    }
}

fn jitter() -> f64 {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (n % 1000) as f64 / 1000.0
}

fn session(
    cfg: &Config,
    shared: &Mutex<Shared>,
    stop: &AtomicBool,
    connected: &AtomicBool,
) -> Result<(), Ended> {
    let mut c =
        Conn::connect(&cfg.hub, cfg.opts.timeout).map_err(|e| Ended::Other(e.to_string()))?;
    let result = greet_and_serve(cfg, &mut c, shared, stop, connected);
    match result {
        Err(SessionEnd::End(End::Bad(e))) => {
            // spec 14.2: a bad frame from the hub is answered with ERROR, then the connection is closed
            c.error_and_close(&e);
            Err(Ended::Other(e.to_string()))
        }
        Err(SessionEnd::End(End::Lost(why))) => Err(Ended::Other(why)),
        Err(SessionEnd::Rejected(r)) => {
            // noted before the connection closes, so that whoever sees the close also sees the reason
            shared.lock().unwrap().rejected = Some(r.clone());
            Err(Ended::Rejected(r))
        }
        Ok(()) => Ok(()),
    }
}

enum SessionEnd {
    End(End),
    Rejected(Rejected),
}

impl<T: Into<End>> From<T> for SessionEnd {
    fn from(e: T) -> Self {
        SessionEnd::End(e.into())
    }
}

fn greet_and_serve(
    cfg: &Config,
    c: &mut Conn,
    shared: &Mutex<Shared>,
    stop: &AtomicBool,
    connected: &AtomicBool,
) -> Result<(), SessionEnd> {
    let t_hello = now_ms();
    c.send(&Frame::new(
        kind::HELLO,
        meta! {"participant" => cfg.participant, "operation" => cfg.operation, "role" => "source",
        "versions" => [PROTOCOL_VERSION], "t" => t_hello, "have" => Vec::<Value>::new()},
    ))?;
    let first = c.recv(cfg.opts.timeout)?;
    if first.ftype == kind::REJECT {
        let opt = |k: &str| first.meta.get(k).and_then(Value::as_str).map(String::from);
        return Err(SessionEnd::Rejected(Rejected {
            code: first.str("code").into(),
            text: first.str("text").into(),
            operation: opt("operation"),
            operation_name: opt("operation_name"),
        }));
    }
    if first.ftype != kind::WELCOME {
        return Err(FrameError::new(
            "bad_frame",
            format!("expected WELCOME, got {}", frames::type_name(first.ftype)),
        )
        .into());
    }
    let w = &first.meta;
    c.version = w["version"].as_u64().unwrap_or(1).min(255) as u8;
    let heartbeat = Duration::from_secs_f64(w["heartbeat_s"].as_f64().unwrap_or(2.0));
    let timeout = Duration::from_secs_f64(w["timeout_s"].as_f64().unwrap_or(10.0));
    c.send_timeout = timeout; // the hub's timeout for sending too, not only for receiving (spec 9)
    let have: HashMap<(String, u64), u64> = w["have"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|h| {
            (
                (
                    h["stream"].as_str().unwrap_or("").to_string(),
                    h["gen"].as_u64().unwrap_or(0),
                ),
                h["committed"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    {
        let mut sh = shared.lock().unwrap();
        sh.refresh()?;
        for (name, s) in sh.streams.iter_mut() {
            s.out
                .start_session(have.get(&(name.clone(), s.out.gen)).copied().unwrap_or(0));
        }
        sh.rejected = None;
    }
    connected.store(true, Ordering::SeqCst);
    let mut sender = Sender {
        opts: &cfg.opts,
        inflight: Vec::new(),
        started: have,
    };
    loop {
        if stop.load(Ordering::SeqCst) {
            c.send_timeout = c.send_timeout.min(Duration::from_secs(1));
            let _ = c.send(&Frame::new(kind::BYE, meta! {"reason" => "stop"}));
            return Ok(());
        }
        while let Some(f) = c.try_recv()? {
            sender.handle(&f, shared)?;
        }
        if c.silent_for() > timeout {
            return Err(End::Lost(format!(
                "no sign of life for {:.1} s",
                timeout.as_secs_f64()
            ))
            .into());
        }
        let progressed = sender.work(c, shared)?;
        if c.last_sent.elapsed() >= heartbeat {
            c.send(&Frame::new(kind::HEARTBEAT, meta! {"t" => now_ms()}))?;
        }
        if !progressed {
            let until_beat = heartbeat.saturating_sub(c.last_sent.elapsed());
            if let Some(f) =
                c.recv_wait(cfg.opts.poll.min(until_beat).max(Duration::from_millis(1)))?
            {
                sender.handle(&f, shared)?;
            }
        }
    }
}

/// The sending side of one session: window of unacknowledged segments, final check, block repair.
struct Sender<'a> {
    opts: &'a Options,
    /// (stream, gen, end offset) of segments sent and not yet acknowledged.
    inflight: Vec<(String, u64, u64)>,
    /// What the hub reported in WELCOME, for streams added during the session.
    started: HashMap<(String, u64), u64>,
}

impl Sender<'_> {
    /// Processes a frame from the hub.
    fn handle(&mut self, f: &Frame, shared: &Mutex<Shared>) -> Result<(), End> {
        if f.ftype != kind::HEARTBEAT && f.ftype != kind::ACK {
            debug(|| {
                format!(
                    "from hub {} {}",
                    frames::type_name(f.ftype),
                    Value::Object(f.meta.clone())
                )
            });
        }
        match f.ftype {
            kind::ACK | kind::RESEND | kind::STREAM_CHECK | kind::BLOCK_SUMS => {}
            kind::HEARTBEAT => return Ok(()),
            kind::BYE => {
                return Err(End::Lost(format!(
                    "the hub closed the session: {}",
                    f.str("reason")
                )))
            }
            kind::ERROR => {
                return Err(End::Lost(format!(
                    "the hub reported {}: {}",
                    f.str("code"),
                    f.str("text")
                )))
            }
            // messages are not implemented yet: not acknowledged, so the hub keeps them for later
            kind::MESSAGE | kind::MESSAGE_ACK | kind::DELIVERY => return Ok(()),
            _ if f.is_extension() => return Ok(()), // unknown extensions are ignored (spec 14.3)
            t => {
                return Err(End::Bad(FrameError::new(
                    "bad_frame",
                    format!("unexpected {} from the hub", frames::type_name(t)),
                )))
            }
        }
        let name = f.str("stream").to_string();
        let gen = f.u64("gen");
        let mut sh = shared.lock().unwrap();
        let Some(s) = sh.streams.get_mut(&name) else {
            return Ok(());
        };
        let o = &mut s.out;
        if o.gen != gen {
            return Ok(());
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
                    log(&format!(
                        "{} generation {}: unexpected STREAM_CHECK ignored",
                        name, gen
                    ));
                    return Ok(());
                }
                o.awaiting_check = false;
                if o.repair_sent {
                    o.repair_rounds += 1; // same session: the repair reached the hub (TCP keeps order)
                }
                let result = f.str("result").to_string();
                if result == "ok" || o.repair_rounds >= MAX_REPAIR_ROUNDS {
                    if result != "ok" {
                        log(&format!(
                            "{} generation {}: final check still fails after {} repairs",
                            name, gen, o.repair_rounds
                        ));
                    }
                    o.result = Some(result);
                } else {
                    o.reset_repair();
                    o.receiver_size = f.u64("size");
                    o.sums = Some(BTreeMap::new());
                }
            }
            kind::BLOCK_SUMS => {
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
        Ok(())
    }

    /// Sends what is due: repairs, new segments within the window, the final check. True if anything was sent.
    fn work(&mut self, c: &mut Conn, shared: &Mutex<Shared>) -> Result<bool, End> {
        let mut sh = shared.lock().unwrap();
        sh.refresh()?;
        let mut progressed = false;
        let mut save = false;
        let names: Vec<String> = sh.streams.keys().cloned().collect();
        for name in names {
            let s = sh.streams.get_mut(&name).unwrap();
            if s.out.sent == 0 && s.out.acked == 0 && !s.out.end_sent {
                // a stream added during the session, or a new generation: start from what the hub reported
                if let Some(c0) = self.started.remove(&(name.clone(), s.out.gen)) {
                    s.out.start_session(c0);
                }
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
                if !o.end_sent
                    && o.result.as_deref() != Some("ok")
                    && o.acked >= final_size
                    && o.sums.is_none()
                    && o.repairs.is_empty()
                {
                    let (digest, n) = sha256_prefix(&s.path, final_size)?;
                    debug(|| format!("STREAM_END {} gen {} size {}", name, o.gen, n));
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
        log(&format!(
            "{} generation {}: resending {} of {} blocks",
            name,
            o.gen,
            diff.len(),
            count
        ));
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
    let f = File::open(path)?;
    let mut r = f.take(size);
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
