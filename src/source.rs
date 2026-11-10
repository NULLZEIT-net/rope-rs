//! Source participant: sends growing files to the hub and resumes from the committed offset (spec 8.2–8.4, 9, 10,
//! 14.7). Messages: see [`crate::participant`].
//!
//! Files are only read: opened briefly, read, closed – no locks, so the recording program is never disturbed. The
//! file on disk is the buffer during an outage; in memory there are at most `window` unacknowledged segments.
//!
//! Not yet: offline media.

use crate::frames::{self, kind, Frame};
use crate::media::{collect_repair_frame, ExportReport, ImportReport};
use crate::now_ms;
use crate::participant::{log, Participant, Role};
use crate::sender::{differing_blocks, is_sender_frame, read_range, OutStream, Sender};
use crate::session::{Conn, End};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub use crate::participant::{Options, Rejected};

pub use crate::sender::MAX_REPAIR_ROUNDS;

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
            seen,
            complete_requested: false,
            out: OutStream::new(gen, path.clone()),
            path,
            started: false,
        }
    }

    fn new_generation(&mut self, name: &str, why: &str) {
        log(
            "source",
            &format!("{} {}: new generation {}", name, why, self.out.gen + 1),
        );
        self.out = OutStream::new(self.out.gen + 1, self.path.clone());
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

/// Block repairs asked for in bundles: (stream, gen) -> (receiver size, block size, block sums).
type Repairs = BTreeMap<(String, u64), (u64, u64, Vec<String>)>;

struct Shared {
    streams: BTreeMap<String, SourceStream>,
    saved_at_start: Map<String, Value>,
    state_path: PathBuf,
    /// Block repairs the hub asked for in a bundle (spec 8.6), kept durably until a bundle carried them:
    /// (stream, gen) -> (receiver size, block size, block sums).
    repairs: Repairs,
    repairs_path: PathBuf,
}

impl Shared {
    fn save_repairs(&self) -> io::Result<()> {
        let list: Vec<Value> = self
            .repairs
            .iter()
            .map(|((n, g), (size, block, sums))| json!({"stream": n, "gen": g, "size": size, "block_size": block, "sums": sums}))
            .collect();
        write_json_atomic(&self.repairs_path, &Value::Array(list))
    }

    fn load_repairs(path: &Path) -> io::Result<Repairs> {
        let raw = match fs::read(path) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(e),
        };
        let v: Value = serde_json::from_slice(&raw)?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|r| {
                let sums = r["sums"]
                    .as_array()?
                    .iter()
                    .map(|s| s.as_str().map(String::from))
                    .collect::<Option<Vec<_>>>()?;
                Some((
                    (r["stream"].as_str()?.to_string(), r["gen"].as_u64()?),
                    (r["size"].as_u64()?, r["block_size"].as_u64()?, sums),
                ))
            })
            .collect())
    }

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
    zlib_level: u32,
}

use crate::media::{add_stream, SEGMENT_BYTES as MEDIA_SEGMENT};

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
        let repairs_path = state_dir.join("media_repairs.json");
        let shared = Arc::new(Mutex::new(Shared {
            streams,
            saved_at_start,
            state_path,
            repairs: Shared::load_repairs(&repairs_path)?,
            repairs_path,
        }));
        let zlib_level = opts.zlib_level;
        let role = SourceRole {
            shared: shared.clone(),
            sender: Sender::new(
                opts.segment_bytes as u64,
                opts.window,
                opts.zlib_level,
                "source",
            ),
            have: HashMap::new(),
        };
        let participant =
            Participant::spawn(participant, operation, hub, state_dir, opts, Box::new(role))?;
        Ok(Source {
            shared,
            participant,
            zlib_level,
        })
    }

    /// Writes a bundle for the hub (spec 8.6): the outbox, block repairs the hub asked for, and every stream not yet
    /// verified at the hub from what the hub acknowledged (over the network or in a bundle; from 0 after a restart –
    /// the hub ignores what it holds), with `STREAM_END` once complete. `max_bytes` limits the bundle.
    pub fn export_media(&self, medium: &Path, max_bytes: Option<u64>) -> io::Result<ExportReport> {
        let shared = &self.shared;
        let level = self.zlib_level;
        let mut repaired = Vec::new();
        let report =
            self.participant
                .export_bundle(medium, max_bytes, &mut |_| Ok(true), &mut |w| {
                    let mut sh = shared.lock().unwrap();
                    sh.refresh()?;
                    let mut ok = true;
                    let mut dropped = false;
                    for (key, (size, block, sums)) in sh.repairs.clone() {
                        let Some(s) = sh.streams.get_mut(&key.0).filter(|s| s.out.gen == key.1)
                        else {
                            sh.repairs.remove(&key);
                            dropped = true;
                            continue;
                        };
                        let diff = differing_blocks(&s.path, block, &sums, size)?;
                        if !sums.is_empty() && diff.len() * 2 > sums.len() {
                            // the file was replaced, not edited: a new generation, as on the network (spec 14.7)
                            s.new_generation(
                                &key.0,
                                "changed in most of its blocks (reported in a bundle)",
                            );
                            sh.repairs.remove(&key);
                            sh.save()?;
                            dropped = true;
                            continue;
                        }
                        for (mut off, end) in diff {
                            while ok && off < end {
                                let data = read_range(&s.path, off, MEDIA_SEGMENT.min(end - off))?;
                                if data.is_empty() {
                                    break;
                                }
                                ok = w.add(&frames::segment(
                                    &key.0,
                                    key.1,
                                    off,
                                    &data,
                                    now_ms(),
                                    level,
                                    true,
                                ))?;
                                if ok {
                                    w.report.repair_bytes += data.len() as u64;
                                    off += data.len() as u64;
                                }
                            }
                        }
                        if !ok {
                            break;
                        }
                        repaired.push(key);
                    }
                    if dropped {
                        sh.save_repairs()?;
                    }
                    for (name, s) in sh.streams.iter() {
                        let o = &s.out;
                        if !ok || o.result.as_deref() == Some("ok") {
                            continue;
                        }
                        let limit = o.final_size.unwrap_or(o.size);
                        ok = add_stream(
                            w,
                            name,
                            o.gen,
                            &s.path,
                            o.acked.min(limit),
                            limit,
                            o.final_size,
                            level,
                        )?;
                    }
                    Ok(ok)
                })?;
        if !repaired.is_empty() {
            // carried by a bundle: forget them; if that bundle never arrives, the hub asks again
            let mut sh = self.shared.lock().unwrap();
            for k in repaired {
                sh.repairs.remove(&k);
            }
            sh.save_repairs()?;
        }
        Ok(report)
    }

    /// Imports the hub's bundles for this source: what the hub holds (`ACK`: the next bundle starts there), repair
    /// requests, messages and delivery reports.
    pub fn import_media(&self, medium: &Path) -> io::Result<Vec<ImportReport>> {
        let shared = &self.shared;
        let mut requests = BTreeMap::new();
        let reports = self.participant.import_bundles(medium, &mut |f, rep, _| {
            if f.ftype == kind::ACK {
                let (name, gen, committed) = (
                    f.str("stream").to_string(),
                    f.u64("gen"),
                    f.u64("committed"),
                );
                if let Some(s) = shared
                    .lock()
                    .unwrap()
                    .streams
                    .get_mut(&name)
                    .filter(|s| s.out.gen == gen)
                {
                    s.out.acked = s.out.acked.max(committed);
                }
                rep.acks.push((name, gen, committed));
                return Ok(true);
            }
            Ok(collect_repair_frame(f, &mut requests))
        })?;
        let complete: Vec<_> = requests
            .into_iter()
            .filter_map(|(k, r)| r.complete_sums().map(|s| (k, (r.size, r.block_size, s))))
            .collect();
        if !complete.is_empty() {
            let mut sh = self.shared.lock().unwrap();
            sh.repairs.extend(complete);
            sh.save_repairs()?;
        }
        Ok(reports)
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

/// The sending side in a session (window, final check, block repair: [`crate::sender`]).
struct SourceRole {
    shared: Arc<Mutex<Shared>>,
    sender: Sender,
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
        self.sender.clear();
        Ok(())
    }

    fn handle(&mut self, f: &Frame, _c: &mut Conn) -> Result<bool, End> {
        if !is_sender_frame(f.ftype) {
            return Ok(false);
        }
        let name = f.str("stream").to_string();
        let mut sh = self.shared.lock().unwrap();
        if let Some(s) = sh
            .streams
            .get_mut(&name)
            .filter(|s| s.out.gen == f.u64("gen"))
        {
            self.sender.handle(f, &name, &mut s.out);
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
                let committed = self
                    .have
                    .get(&(name.clone(), s.out.gen))
                    .copied()
                    .unwrap_or(0);
                s.out.start_session(committed);
                s.started = true;
            }
            if s.out.sums_complete() && self.sender.compare(&name, &mut s.out, true)? {
                s.new_generation(&name, "changed in most of its blocks");
                save = true;
                continue; // a new generation: nothing more of the old one
            }
            progressed |= self.sender.send(c, &name, &mut s.out)?;
        }
        if save {
            sh.save()?;
        }
        Ok(progressed)
    }
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
