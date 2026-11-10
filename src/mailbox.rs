//! Messages with custody (spec 8.5): outbox and inbox of a participant.
//!
//! Every holder stores a message durably before it acknowledges it. Until the hub has taken custody, a message is
//! sent again after every reconnect; duplicates are recognised by id. The application is told about each received
//! message (`on_message`) and each new delivery report of an own message (`on_delivery`) – at least once, in practice
//! exactly once.
//!
//! Layout below the mailbox folder: `<id>.body` (raw body, written and synced first) and `<id>.json` (meta and the
//! holder's state, written atomically – its presence means "held").

use crate::frames::{self, kind, Frame, Meta};
use crate::session::{Conn, End};
use crate::{meta, now_ms};
use serde_json::{json, Map, Value};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Unacknowledged MESSAGE frames in flight per connection (spec 8.5).
pub const WINDOW: usize = 16;

/// Called once per new message: (meta with id, from, to, t, kind; body).
pub type OnMessage = Arc<dyn Fn(&Meta, &[u8]) + Send + Sync>;
/// Called once per new delivery report of an own message: (id, report with delivered, pending, unknown).
pub type OnDelivery = Arc<dyn Fn(&str, &Value) + Send + Sync>;

/// A new message id: 32 lower-case hex characters (spec 14.4). The standard library's randomly keyed hasher, fed
/// with the time and a counter, serves as the random source.
pub fn new_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        let mut h = RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        h.write_u32(std::process::id());
        out.push_str(&format!("{:016x}", h.finish()));
    }
    out
}

#[derive(Clone, Debug)]
struct Record {
    meta: Meta,
    state: Map<String, Value>,
}

impl Record {
    fn t(&self) -> u64 {
        self.meta.get("t").and_then(Value::as_u64).unwrap_or(0)
    }
    fn is(&self, box_: &str) -> bool {
        self.state.get("box").and_then(Value::as_str) == Some(box_)
    }
    fn flag(&self, key: &str) -> Option<bool> {
        self.state.get(key).and_then(Value::as_bool)
    }
}

/// A message as the application sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub id: String,
    pub from: String,
    pub to: Vec<String>,
    pub t: u64,
    pub kind: String,
}

impl Message {
    fn of(meta: &Meta) -> Message {
        let s = |k: &str| {
            meta.get(k)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        Message {
            id: s("id"),
            from: s("from"),
            to: meta
                .get("to")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            t: meta.get("t").and_then(Value::as_u64).unwrap_or(0),
            kind: s("kind"),
        }
    }
}

/// An outbox entry with its custody state.
#[derive(Clone, Debug, PartialEq)]
pub struct Sent {
    pub message: Message,
    /// The hub has taken custody.
    pub at_hub: bool,
    /// The hub's latest delivery report ({delivered, pending, unknown}); the message reached everybody when
    /// `pending` is empty.
    pub report: Option<Value>,
}

pub struct Mailbox {
    dir: PathBuf,
    participant: String,
    items: Mutex<BTreeMap<String, Record>>,
    /// Told in this run, noted without a pause after telling: a session that ends while the "told" flag is being
    /// written must not lead the next session to tell again.
    told: Mutex<HashSet<String>>,
    on_message: Option<OnMessage>,
    on_delivery: Option<OnDelivery>,
}

fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

impl Mailbox {
    pub fn open(
        dir: &Path,
        participant: &str,
        on_message: Option<OnMessage>,
        on_delivery: Option<OnDelivery>,
    ) -> io::Result<Mailbox> {
        fs::create_dir_all(dir)?;
        let mut items = BTreeMap::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("json") => {
                    let v: Value = serde_json::from_slice(&fs::read(&path)?)?;
                    let (Some(meta), Some(state)) = (v["meta"].as_object(), v["state"].as_object())
                    else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("damaged {}", path.display()),
                        ));
                    };
                    let id = meta
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    items.insert(
                        id,
                        Record {
                            meta: meta.clone(),
                            state: state.clone(),
                        },
                    );
                }
                Some("tmp") => {
                    let _ = fs::remove_file(&path); // a crash during an atomic write
                }
                _ => {}
            }
        }
        Ok(Mailbox {
            dir: dir.to_path_buf(),
            participant: participant.into(),
            items: Mutex::new(items),
            told: Mutex::new(HashSet::new()),
            on_message,
            on_delivery,
        })
    }

    fn persist(&self, rec: &Record) -> io::Result<()> {
        let id = rec.meta["id"].as_str().unwrap_or("");
        let v = json!({"meta": rec.meta, "state": rec.state});
        write_atomic(
            &self.dir.join(format!("{}.json", id)),
            &serde_json::to_vec(&v)?,
        )
    }

    /// Takes custody: body, then meta and state, durably. False for a known id (nothing written).
    fn put(&self, meta: Meta, body: &[u8], state: Map<String, Value>) -> io::Result<bool> {
        let mut items = self.items.lock().unwrap();
        let id = meta["id"].as_str().unwrap_or("").to_string();
        if items.contains_key(&id) {
            return Ok(false);
        }
        {
            let mut f = File::create(self.dir.join(format!("{}.body", id)))?;
            f.write_all(body)?;
            f.sync_all()?;
        }
        let rec = Record { meta, state };
        self.persist(&rec)?;
        items.insert(id, rec);
        Ok(true)
    }

    /// Changes a record's state and stores it durably, as one step.
    fn modify(
        &self,
        id: &str,
        change: impl FnOnce(&mut Map<String, Value>) -> bool,
    ) -> io::Result<bool> {
        let mut items = self.items.lock().unwrap();
        let Some(rec) = items.get_mut(id) else {
            return Ok(false);
        };
        let mut copy = rec.clone();
        if !change(&mut copy.state) {
            return Ok(false);
        }
        self.persist(&copy)?;
        *rec = copy;
        Ok(true)
    }

    fn select(&self, pred: impl Fn(&Record) -> bool) -> Vec<Record> {
        let items = self.items.lock().unwrap();
        let mut out: Vec<Record> = items.values().filter(|r| pred(r)).cloned().collect();
        out.sort_by(|a, b| (a.t(), a.meta["id"].as_str()).cmp(&(b.t(), b.meta["id"].as_str())));
        out
    }

    /// Puts a message into the outbox (durably) and returns its id. It leaves with the next connection.
    pub fn send(&self, to: &[String], kind: &str, body: &[u8]) -> io::Result<String> {
        let id = new_id();
        let m = meta! {"id" => id, "from" => self.participant, "to" => to, "t" => now_ms(), "kind" => kind};
        // reject bad input now, not at the hub
        let probe = Frame::new(kind::MESSAGE, {
            let mut p = m.clone();
            p.insert("len".into(), json!(0));
            p.insert("crc".into(), json!(0));
            p.insert("enc".into(), json!("none"));
            p
        });
        frames::validate(&probe)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.text))?;
        if body.len() > frames::MAX_BLOB {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("message body larger than {} bytes", frames::MAX_BLOB),
            ));
        }
        let mut state = Map::new();
        state.insert("box".into(), json!("out"));
        state.insert("at_hub".into(), json!(false));
        self.put(m, body, state)?;
        Ok(id)
    }

    pub fn body(&self, id: &str) -> io::Result<Vec<u8>> {
        fs::read(self.dir.join(format!("{}.body", id)))
    }

    /// Received messages, oldest first.
    pub fn inbox(&self) -> Vec<Message> {
        self.select(|r| r.is("in"))
            .iter()
            .map(|r| Message::of(&r.meta))
            .collect()
    }

    /// Sent messages with their custody state, oldest first.
    pub fn outbox(&self) -> Vec<Sent> {
        self.select(|r| r.is("out"))
            .iter()
            .map(|r| Sent {
                message: Message::of(&r.meta),
                at_hub: r.flag("at_hub") == Some(true),
                report: r.state.get("report").cloned(),
            })
            .collect()
    }

    /// Tells the application about every stored message and report it was not told about yet.
    pub(crate) fn notify(&self) -> io::Result<()> {
        for rec in self.select(|r| r.is("in") && r.flag("told") == Some(false)) {
            let id = rec.meta["id"].as_str().unwrap_or("").to_string();
            let fresh = !self.told.lock().unwrap().contains(&id);
            if fresh {
                if let Some(cb) = &self.on_message {
                    cb(&rec.meta, &self.body(&id)?);
                }
                self.told.lock().unwrap().insert(id.clone());
            }
            self.modify(&id, |st| {
                st.insert("told".into(), json!(true));
                true
            })?;
        }
        for rec in self.select(|r| r.is("out") && r.flag("report_told") == Some(false)) {
            let id = rec.meta["id"].as_str().unwrap_or("").to_string();
            let report = rec.state.get("report").cloned().unwrap_or(Value::Null);
            let n = report["delivered"].as_array().map_or(0, Vec::len);
            let key = format!("report\0{}\0{}", id, n);
            if !self.told.lock().unwrap().contains(&key) {
                if let Some(cb) = &self.on_delivery {
                    cb(&id, &report);
                }
                self.told.lock().unwrap().insert(key);
            }
            self.modify(&id, |st| {
                st.insert("report_told".into(), json!(true));
                true
            })?;
        }
        Ok(())
    }

    /// Stores a delivery report of an own message unless an equal or newer one is known. True if it was new.
    fn take_report(&self, f: &Frame) -> io::Result<bool> {
        let id = f.str("id").to_string();
        let new = json!({"delivered": f.meta["delivered"], "pending": f.meta["pending"], "unknown": f.meta["unknown"]});
        let n_new = f.meta["delivered"].as_array().map_or(0, Vec::len);
        let is_out = self
            .items
            .lock()
            .unwrap()
            .get(&id)
            .is_some_and(|r| r.is("out"));
        if !is_out {
            return Ok(false);
        }
        self.modify(&id, |st| {
            let n_old = st
                .get("report")
                .and_then(|r| r["delivered"].as_array())
                .map(Vec::len);
            if n_old.is_some_and(|o| o >= n_new) {
                return false;
            }
            st.insert("report".into(), new);
            st.insert("report_told".into(), json!(false));
            st.insert("at_hub".into(), json!(true));
            true
        })
    }
}

/// The message side of one session: sends the outbox, receives messages and reports.
pub(crate) struct MailSession<'a> {
    mailbox: &'a Mailbox,
    inflight: HashSet<String>,
    zlib_level: u32,
}

impl<'a> MailSession<'a> {
    pub fn start(mailbox: &'a Mailbox, zlib_level: u32) -> Result<Self, End> {
        mailbox.notify()?; // stored but untold before a break or a restart
        Ok(MailSession {
            mailbox,
            inflight: HashSet::new(),
            zlib_level,
        })
    }

    /// Sends outbox messages the hub does not hold yet, at most `WINDOW` unacknowledged. Messages go before stream
    /// data (spec 8.5): the session calls this first.
    pub fn work(&mut self, c: &mut Conn) -> Result<bool, End> {
        let mut progressed = false;
        for rec in self
            .mailbox
            .select(|r| r.is("out") && r.flag("at_hub") != Some(true))
        {
            if self.inflight.len() >= WINDOW {
                break;
            }
            let id = rec.meta["id"].as_str().unwrap_or("").to_string();
            if self.inflight.contains(&id) {
                continue;
            }
            let body = self.mailbox.body(&id)?;
            let (enc, blob) = frames::encode_payload(&body, self.zlib_level);
            let mut m = rec.meta.clone();
            m.insert("len".into(), json!(body.len()));
            m.insert("crc".into(), json!(frames::crc32(&body)));
            m.insert("enc".into(), json!(enc));
            c.send(&Frame::with_blob(kind::MESSAGE, m, blob))?;
            self.inflight.insert(id);
            progressed = true;
        }
        Ok(progressed)
    }

    /// MESSAGE, MESSAGE_ACK or DELIVERY from the hub. False for other frames.
    pub fn handle(&mut self, f: &Frame, c: &mut Conn) -> Result<bool, End> {
        match f.ftype {
            kind::MESSAGE => {
                let body = frames::payload(f)?; // damaged: ERROR, connection closed
                let m: Meta = ["id", "from", "to", "t", "kind"]
                    .iter()
                    .map(|k| (k.to_string(), f.meta[*k].clone()))
                    .collect();
                let mut state = Map::new();
                state.insert("box".into(), json!("in"));
                state.insert("told".into(), json!(false));
                self.mailbox.put(m, &body, state)?; // durably, before the acknowledgement (spec 14.10)
                c.send(&Frame::new(kind::MESSAGE_ACK, meta! {"id" => f.str("id")}))?;
                self.mailbox.notify()?;
            }
            kind::MESSAGE_ACK => {
                let id = f.str("id").to_string();
                if self.inflight.remove(&id) {
                    self.mailbox.modify(&id, |st| {
                        st.insert("at_hub".into(), json!(true));
                        true
                    })?;
                }
            }
            kind::DELIVERY => {
                self.mailbox.take_report(f)?;
                let n = f.meta["delivered"].as_array().map_or(0, Vec::len);
                c.send(&Frame::new(
                    kind::DELIVERY_ACK,
                    meta! {"id" => f.str("id"), "delivered" => n},
                ))?;
                self.mailbox.notify()?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rope-rs-test-{}-{}", name, new_id()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn ids_are_32_hex_and_differ() {
        let a = new_id();
        let b = new_id();
        assert_eq!(a.len(), 32);
        assert!(a
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(a, b);
    }

    #[test]
    fn outbox_survives_a_restart_and_bad_input_is_refused() {
        let dir = temp_dir("outbox");
        let id = {
            let m = Mailbox::open(&dir, "r-1", None, None).unwrap();
            assert!(m.send(&["no spaces allowed".into()], "note", b"x").is_err());
            assert!(m
                .send(&["r-2".into()], "note", &vec![0u8; frames::MAX_BLOB + 1])
                .is_err());
            m.send(&["r-2".into(), "hub".into()], "note", b"hello")
                .unwrap()
        };
        let m = Mailbox::open(&dir, "r-1", None, None).unwrap();
        let out = m.outbox();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].message.id, id);
        assert_eq!(
            out[0].message.to,
            vec!["r-2".to_string(), "hub".to_string()]
        );
        assert!(!out[0].at_hub);
        assert_eq!(m.body(&id).unwrap(), b"hello");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_stored_message_is_told_once_also_after_a_restart() {
        let dir = temp_dir("inbox");
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let cb: OnMessage = Arc::new(move |_m, body| {
            assert_eq!(body, b"task");
            c.fetch_add(1, Ordering::SeqCst);
        });
        {
            let m = Mailbox::open(&dir, "r-1", Some(cb.clone()), None).unwrap();
            let meta = meta! {"id" => new_id(), "from" => "source-1", "to" => ["r-1"], "t" => 1, "kind" => "note"};
            let mut state = Map::new();
            state.insert("box".into(), json!("in"));
            state.insert("told".into(), json!(false));
            assert!(m.put(meta.clone(), b"task", state.clone()).unwrap());
            assert!(!m.put(meta, b"task", state).unwrap()); // a duplicate is not stored again
            m.notify().unwrap();
            m.notify().unwrap();
        }
        let m = Mailbox::open(&dir, "r-1", Some(cb), None).unwrap();
        m.notify().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(m.inbox().len(), 1);
        fs::remove_dir_all(dir).unwrap();
    }
}
