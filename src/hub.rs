//! The hub (spec 7, 8.1, 14.5, 14.10): checks every greeting against its participant list, keeps one session per
//! participant, stores the streams of sources durably and checks them, relays them to consumers and takes custody
//! of messages (spec 8.5): delivers them to every recipient on its list, reports delivery to the sender, and hands
//! messages addressed to the hub itself to its application.
//!
//! Offline media (spec 8.6): [`Hub::import_media`] and [`Hub::export_media`] carry the same frames by stick.

use crate::frames::{self, kind, Frame, FrameError, Meta};
use crate::mailbox::{Mailbox, OnMessage, WINDOW};
use crate::media::{
    add_repair, add_stream, check_manifest, collect_repair_frame, find_bundles, read_frames,
    BundleWriter, ExportReport, ImportReport, Skipped,
};
use crate::participant::{debug, log};
use crate::receiver::{Store, StreamInfo};
use crate::sender::{is_sender_frame, OutStream, Sender};
use crate::session::{Conn, End};
use crate::{meta, now_ms, PROTOCOL_VERSION};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Called after the final check of a source's stream: (source, stream, gen, "ok" | "mismatch").
pub type OnStreamChecked = Arc<dyn Fn(&str, &str, u64, &str) + Send + Sync>;

/// Settings of a hub; the defaults are the recommended values of spec 14.8.
#[derive(Clone)]
pub struct HubOptions {
    /// The hub's identifier on the wire (1–64 characters, spec 14.4).
    pub hub_id: String,
    /// Address to listen on, e.g. "0.0.0.0" for every network.
    pub host: String,
    /// TCP port; 0 picks a free one ([`Hub::port`] tells which).
    pub port: u16,
    /// Sent in `WELCOME`; every participant uses both (spec 9). `timeout` must be at least twice `heartbeat`.
    pub heartbeat: Duration,
    pub timeout: Duration,
    pub on_stream_checked: Option<OnStreamChecked>,
    /// Messages addressed to the hub itself (`to` contains its identifier) go to its application (spec 8.5).
    pub on_message: Option<OnMessage>,
    /// A message to the hub that was not told yet (a restart in between) is told again after this time.
    pub retell: Duration,
    /// Relay to consumers: stream data per segment, unacknowledged segments in flight, zlib level.
    pub segment_bytes: u64,
    pub window: usize,
    pub zlib_level: u32,
}

impl Default for HubOptions {
    fn default() -> Self {
        HubOptions {
            hub_id: "hub".into(),
            host: "127.0.0.1".into(),
            port: crate::DEFAULT_PORT,
            heartbeat: Duration::from_secs(2),
            timeout: Duration::from_secs(10),
            on_stream_checked: None,
            on_message: None,
            retell: Duration::from_secs(10),
            segment_bytes: 256 * 1024,
            window: 8,
            zlib_level: 1,
        }
    }
}

/// A stored stream at the hub, with the source it came from.
#[derive(Clone, Debug, PartialEq)]
pub struct HubStream {
    pub participant: String,
    pub info: StreamInfo,
}

struct SessionHandle {
    number: u64,
    socket: TcpStream,
}

struct Inner {
    operation: String,
    /// identifier -> role
    participants: BTreeMap<String, String>,
    root: PathBuf,
    opts: HubOptions,
    stores: Mutex<BTreeMap<String, Arc<Mutex<Store>>>>,
    sessions: Mutex<HashMap<String, SessionHandle>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    next_session: AtomicU64,
    stop: AtomicBool,
    /// Messages in the hub's custody, with the state of every recipient.
    messages: Mailbox,
    /// Messages to the hub told in this run (noted at once; the flag on disk follows).
    hub_told: Mutex<HashSet<String>>,
    media: Mutex<MediaState>,
}

/// A running hub. Stops with [`Hub::stop`] (or when dropped); its state stays on disk, so a hub started again on the
/// same folder continues.
pub struct Hub {
    inner: Arc<Inner>,
    port: u16,
    accept: Option<JoinHandle<()>>,
}

fn is_identifier(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

impl Hub {
    /// Starts a hub. `participants`: identifier -> role (`source`, `consumer`, `recipient`); `root` keeps everything
    /// it receives. Fails with a plain-language reason if the list or the timing is not valid, or the port is taken
    /// (never silently another port, spec 14.1).
    pub fn start(
        root: &Path,
        participants: BTreeMap<String, String>,
        operation: &str,
        opts: HubOptions,
    ) -> io::Result<Hub> {
        let bad = |t: String| io::Error::new(io::ErrorKind::InvalidInput, t);
        if !is_identifier(&opts.hub_id) || !is_identifier(operation) {
            return Err(bad(
                "hub identifier and operation: 1-64 characters from A-Z a-z 0-9 . _ -".into(),
            ));
        }
        for (p, role) in &participants {
            if !is_identifier(p) || p == &opts.hub_id {
                return Err(bad(format!(
                    "participant {:?}: not a valid identifier, or the hub's own",
                    p
                )));
            }
            if !frames::ROLES.contains(&role.as_str()) {
                return Err(bad(format!(
                    "participant {}: role must be source, consumer or recipient, got {:?}",
                    p, role
                )));
            }
        }
        if opts.heartbeat.is_zero() || opts.timeout < opts.heartbeat * 2 {
            return Err(bad("timeout must be at least twice the heartbeat (one lost heartbeat is no disconnect)".into()));
        }
        let listener = TcpListener::bind((opts.host.as_str(), opts.port)).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "ROPE hub cannot listen on {} port {}: {}",
                    opts.host, opts.port, e
                ),
            )
        })?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let inner = Arc::new(Inner {
            operation: operation.into(),
            participants,
            root: root.to_path_buf(),
            opts,
            stores: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(HashMap::new()),
            threads: Mutex::new(Vec::new()),
            next_session: AtomicU64::new(1),
            stop: AtomicBool::new(false),
            messages: Mailbox::open(&root.join("messages"), "hub", None, None)?,
            hub_told: Mutex::new(HashSet::new()),
            media: Mutex::new(MediaState::load(root)?),
        });
        inner.retell(); // messages to the hub not taken before a restart
                        // open what earlier runs stored, so that the hub reports it from the start
        for (p, role) in &inner.participants {
            if role == "source" {
                inner.store(p)?;
            }
        }
        let i = inner.clone();
        let accept = thread::spawn(move || accept_loop(listener, i));
        Ok(Hub {
            inner,
            port,
            accept: Some(accept),
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Participants with a session right now.
    pub fn sessions(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        v.sort();
        v
    }

    /// Every stream stored at the hub, by source, stream and generation.
    pub fn streams(&self) -> Vec<HubStream> {
        let stores = self.inner.stores.lock().unwrap();
        stores
            .iter()
            .flat_map(|(p, s)| {
                s.lock()
                    .unwrap()
                    .streams()
                    .into_iter()
                    .map(|info| HubStream {
                        participant: p.clone(),
                        info,
                    })
            })
            .collect()
    }

    /// Stops listening, ends every session and waits for their threads.
    pub fn stop(&mut self) {
        self.inner.stop.store(true, Ordering::SeqCst);
        if let Some(a) = self.accept.take() {
            let _ = a.join();
        }
        for s in self.inner.sessions.lock().unwrap().values() {
            let _ = s.socket.shutdown(Shutdown::Both);
        }
        let threads: Vec<JoinHandle<()>> = self.inner.threads.lock().unwrap().drain(..).collect();
        for t in threads {
            let _ = t.join();
        }
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Inner {
    /// The store of one source (a folder per participant; the identifier never becomes a path on its own).
    fn store(&self, participant: &str) -> io::Result<Arc<Mutex<Store>>> {
        let mut stores = self.stores.lock().unwrap();
        if let Some(s) = stores.get(participant) {
            return Ok(s.clone());
        }
        let s = Arc::new(Mutex::new(Store::open(
            &self.root.join("from").join(format!("p-{}", participant)),
        )?));
        stores.insert(participant.into(), s.clone());
        Ok(s)
    }

    /// Plain-language reason (code, text) why a greeting is refused, or None (spec 14.10, rule 8).
    fn check(&self, hello: &Frame) -> Option<(&'static str, String)> {
        let p = hello.str("participant");
        let Some(role) = self.participants.get(p) else {
            return Some((
                "unknown_participant",
                format!("{} is not on the participant list of this hub", p),
            ));
        };
        if hello.str("operation") != self.operation {
            return Some((
                "wrong_operation",
                format!(
                    "{} belongs to operation {}, this hub serves {}",
                    p,
                    hello.str("operation"),
                    self.operation
                ),
            ));
        }
        let versions = hello.meta["versions"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if !versions
            .iter()
            .any(|v| v.as_u64() == Some(PROTOCOL_VERSION))
        {
            return Some((
                "version",
                format!(
                    "{} speaks protocol versions {}, this hub speaks {}",
                    p, hello.meta["versions"], PROTOCOL_VERSION
                ),
            ));
        }
        if hello.str("role") != role {
            return Some((
                "role",
                format!(
                    "{} greets as {} but is listed as {}",
                    p,
                    hello.str("role"),
                    role
                ),
            ));
        }
        None
    }

    /// State of a new message at the hub: known recipients pending, the hub itself delivered (its application
    /// gets the message), the others unknown (spec 8.5).
    fn custody_state(&self, sender: &str, to: &[String]) -> Map<String, Value> {
        let mut seen = HashSet::new();
        let to: Vec<&String> = to.iter().filter(|t| seen.insert(t.as_str())).collect();
        let hub = &self.opts.hub_id;
        let pending: Vec<&String> = to
            .iter()
            .copied()
            .filter(|t| self.participants.contains_key(*t) && *t != sender)
            .collect();
        let unknown: Vec<&String> = to
            .iter()
            .copied()
            .filter(|t| !self.participants.contains_key(*t) && *t != hub)
            .collect();
        let to_hub = to.contains(&hub);
        let mut st = Map::new();
        st.insert("pending".into(), json!(pending));
        st.insert(
            "delivered".into(),
            json!(if to_hub { vec![hub.clone()] } else { vec![] }),
        );
        st.insert("unknown".into(), json!(unknown));
        if to_hub {
            st.insert("hub_told".into(), json!(false)); // until the application has taken it
        }
        st
    }

    /// Hands a message addressed to the hub to the application, once per run; noted as told afterwards (at least
    /// once across restarts: the application treats messages by their id, spec 8.5).
    fn to_hub(&self, meta: &Meta) {
        let hub = self.opts.hub_id.as_str();
        let addressed = meta["to"]
            .as_array()
            .is_some_and(|a| a.iter().any(|x| x.as_str() == Some(hub)));
        let id = meta["id"].as_str().unwrap_or("").to_string();
        let Some(cb) = &self.opts.on_message else {
            return;
        };
        if !addressed || self.hub_told.lock().unwrap().contains(&id) {
            return;
        }
        let Ok(body) = self.messages.body(&id) else {
            return;
        };
        cb(meta, &body);
        self.hub_told.lock().unwrap().insert(id.clone());
        let _ = self.messages.modify(&id, |st| {
            st.insert("hub_told".into(), json!(true));
            true
        });
    }

    /// Tells the application every message to the hub it has not taken yet.
    fn retell(&self) {
        for rec in self
            .messages
            .select(|r| r.state.get("hub_told").and_then(Value::as_bool) == Some(false))
        {
            self.to_hub(&rec.meta);
        }
    }

    fn participant_list(&self) -> Value {
        let mut list =
            vec![json!({"participant": self.opts.hub_id, "name": self.opts.hub_id, "role": "hub"})];
        list.extend(
            self.participants
                .iter()
                .map(|(p, r)| json!({"participant": p, "name": p, "role": r})),
        );
        Value::Array(list)
    }
}

fn accept_loop(listener: TcpListener, inner: Arc<Inner>) {
    let mut last_retell = Instant::now();
    while !inner.stop.load(Ordering::SeqCst) {
        if last_retell.elapsed() >= inner.opts.retell {
            inner.retell();
            last_retell = Instant::now();
        }
        match listener.accept() {
            Ok((socket, _)) => {
                let i = inner.clone();
                let t = thread::spawn(move || serve(socket, i));
                let mut threads = inner.threads.lock().unwrap();
                threads.retain(|t| !t.is_finished());
                threads.push(t);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20))
            }
            Err(e) => {
                log("hub", &format!("accepting a connection failed: {}", e));
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// One connection: greeting, then the session until it ends. Every way out closes the connection.
fn serve(socket: TcpStream, inner: Arc<Inner>) {
    let _ = socket.set_nonblocking(false);
    let Ok(handle) = socket.try_clone() else {
        return;
    };
    let Ok(mut c) = Conn::from_stream(socket, inner.opts.timeout) else {
        return;
    };
    let mut who = None;
    let number = inner.next_session.fetch_add(1, Ordering::SeqCst);
    let result = greet_and_serve(&mut c, &inner, handle, number, &mut who);
    match result {
        Ok(()) => {}
        Err(End::Bad(e)) => {
            debug("hub", || {
                format!(
                    "bad frame from {}: {}",
                    who.as_deref().unwrap_or("unknown participant"),
                    e
                )
            });
            c.error_and_close(&e);
        }
        Err(End::Lost(why)) => debug("hub", || {
            format!(
                "connection of {} ended: {}",
                who.as_deref().unwrap_or("unknown participant"),
                why
            )
        }),
    }
    c.close();
    if let Some(p) = who {
        let mut sessions = inner.sessions.lock().unwrap();
        if sessions.get(&p).is_some_and(|s| s.number == number) {
            sessions.remove(&p);
        }
    }
}

fn greet_and_serve(
    c: &mut Conn,
    inner: &Inner,
    handle: TcpStream,
    number: u64,
    who: &mut Option<String>,
) -> Result<(), End> {
    let hello = c.recv(inner.opts.timeout)?;
    if hello.ftype != kind::HELLO {
        return Err(End::Bad(FrameError::new(
            "bad_frame",
            "the first frame must be HELLO",
        )));
    }
    if let Some((code, text)) = inner.check(&hello) {
        c.send(&Frame::new(
            kind::REJECT,
            meta! {"code" => code, "text" => text},
        ))?;
        return Ok(());
    }
    let participant = hello.str("participant").to_string();
    let role = inner.participants[&participant].clone();
    // a participant has one session: a new greeting replaces a stale one (spec 14.10, rule 4)
    if let Some(old) = inner.sessions.lock().unwrap().insert(
        participant.clone(),
        SessionHandle {
            number,
            socket: handle,
        },
    ) {
        let _ = old.socket.shutdown(Shutdown::Both);
    }
    *who = Some(participant.clone());
    let store = if role == "source" {
        Some(inner.store(&participant)?)
    } else {
        None
    };
    let have = store.as_ref().map_or(Value::Array(vec![]), |s| {
        Value::Array(s.lock().unwrap().have())
    });
    c.send(&Frame::new(
        kind::WELCOME,
        meta! {"version" => PROTOCOL_VERSION, "hub" => inner.opts.hub_id, "t" => now_ms(), "t_hello" => hello.u64("t"),
               "heartbeat_s" => inner.opts.heartbeat.as_secs_f64(), "timeout_s" => inner.opts.timeout.as_secs_f64(),
               "have" => have, "participants" => inner.participant_list()},
    ))?;
    let (heartbeat, timeout) = (inner.opts.heartbeat, inner.opts.timeout);
    let mut session = Session {
        participant: participant.clone(),
        store,
        relay: (role == "consumer").then(|| Relay::new(&hello, &inner.opts)),
        mail_inflight: HashSet::new(),
        report_inflight: HashSet::new(),
    };
    loop {
        if inner.stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        if c.silent_for() > timeout {
            return Err(End::Lost(format!(
                "no sign of life for {:.1} s",
                timeout.as_secs_f64()
            )));
        }
        while let Some(f) = c.try_recv()? {
            session.handle(&f, c, inner)?;
        }
        // messages before stream data (spec 8.5)
        let mut progressed = session.send_mail(c, inner)?;
        if let Some(r) = session.relay.as_mut() {
            progressed |= r.work(c, inner)?;
        }
        if c.last_sent.elapsed() >= heartbeat {
            c.send(&Frame::new(kind::HEARTBEAT, meta! {"t" => now_ms()}))?;
        }
        if !progressed {
            let until_beat = heartbeat.saturating_sub(c.last_sent.elapsed());
            if let Some(f) =
                c.recv_wait(until_beat.clamp(Duration::from_millis(1), Duration::from_millis(50)))?
            {
                session.handle(&f, c, inner)?;
            }
        }
    }
}

/// The hub's side of one participant's session.
struct Session {
    participant: String,
    /// The store of a source.
    store: Option<Arc<Mutex<Store>>>,
    /// The streams relayed to a consumer.
    relay: Option<Relay>,
    /// Messages sent to this participant and not yet acknowledged.
    mail_inflight: HashSet<String>,
    /// Delivery reports sent to this participant and not yet acknowledged.
    report_inflight: HashSet<String>,
}

impl Session {
    fn handle(&mut self, f: &Frame, c: &mut Conn, inner: &Inner) -> Result<(), End> {
        let participant = self.participant.as_str();
        match f.ftype {
            kind::SEGMENT | kind::STREAM_END if self.store.is_some() => {
                let name = format!("{}/{}", participant, f.str("stream"));
                if name.len() > 255 {
                    return Err(End::Bad(FrameError::new(
                        "bad_frame",
                        format!(
                            "stream name {:?} is too long to relay: at most 255 bytes including the source identifier and '/'",
                            f.str("stream")
                        ),
                    )));
                }
                let mut s = self.store.as_ref().unwrap().lock().unwrap();
                s.handle(f, c)?;
                if f.ftype == kind::STREAM_END {
                    let result = s
                        .streams()
                        .into_iter()
                        .find(|i| i.stream == f.str("stream") && i.gen == f.u64("gen"))
                        .and_then(|i| i.verified);
                    drop(s);
                    if let (Some(cb), Some(r)) = (&inner.opts.on_stream_checked, result) {
                        cb(participant, f.str("stream"), f.u64("gen"), &r);
                    }
                }
                Ok(())
            }
            t if is_sender_frame(t) && self.relay.is_some() => {
                self.relay.as_mut().unwrap().handle(f);
                Ok(())
            }
            kind::MESSAGE => {
                // the hub takes custody only of messages written by the participant of this connection (spec 14.10)
                if f.str("from") != participant {
                    return Err(End::Bad(FrameError::new(
                        "bad_frame",
                        format!("{} sent a message from {}", participant, f.str("from")),
                    )));
                }
                let body = frames::payload(f)?;
                let to: Vec<String> = f.meta["to"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                let state = inner.custody_state(participant, &to);
                let m: Meta = ["id", "from", "to", "t", "kind"]
                    .iter()
                    .map(|k| (k.to_string(), f.meta[*k].clone()))
                    .collect();
                let new = inner
                    .messages
                    .put(m.clone(), &body, state.clone())
                    .map_err(storage)?;
                c.send(&Frame::new(kind::MESSAGE_ACK, meta! {"id" => f.str("id")}))?; // only after storing
                if new {
                    let unknown = state["unknown"].as_array().map_or(0, Vec::len);
                    if unknown > 0 {
                        log(
                            "hub",
                            &format!(
                                "message {} from {}: {} not on the participant list, not delivered",
                                f.str("id"),
                                participant,
                                state["unknown"]
                            ),
                        );
                    }
                    inner.to_hub(&m);
                }
                Ok(())
            }
            kind::MESSAGE_ACK => {
                let id = f.str("id").to_string();
                if self.mail_inflight.remove(&id) {
                    let p = participant.to_string();
                    inner
                        .messages
                        .modify(&id, |st| move_to_delivered(st, &p))
                        .map_err(storage)?;
                }
                Ok(())
            }
            kind::DELIVERY_ACK => {
                let id = f.str("id").to_string();
                self.report_inflight.remove(&id);
                let n = f.u64("delivered");
                let from_this = inner
                    .messages
                    .select(|r| r.meta["id"].as_str() == Some(id.as_str()))
                    .first()
                    .is_some_and(|r| r.meta["from"].as_str() == Some(participant));
                if from_this {
                    inner
                        .messages
                        .modify(&id, |st| {
                            let old = st.get("reported").and_then(Value::as_i64).unwrap_or(-1);
                            st.insert("reported".into(), json!(old.max(n as i64)));
                            true
                        })
                        .map_err(storage)?;
                }
                Ok(())
            }
            kind::HEARTBEAT => Ok(()),
            kind::BYE => Err(End::Lost(format!(
                "{} closed the session: {}",
                participant,
                f.str("reason")
            ))),
            kind::ERROR => Err(End::Lost(format!(
                "{} reported {}: {}",
                participant,
                f.str("code"),
                f.str("text")
            ))),
            _ if f.is_extension() => Ok(()),
            t => Err(End::Bad(FrameError::new(
                "bad_frame",
                format!("unexpected {} from {}", frames::type_name(t), participant),
            ))),
        }
    }

    /// Messages pending for this participant and delivery reports owed to it (spec 8.5), at most `WINDOW` each in
    /// flight. True if anything was sent.
    fn send_mail(&mut self, c: &mut Conn, inner: &Inner) -> Result<bool, End> {
        let p = self.participant.clone();
        let mut progressed = false;
        let pending = inner.messages.select(|r| {
            r.state["pending"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x.as_str() == Some(p.as_str())))
        });
        for rec in pending {
            if self.mail_inflight.len() >= WINDOW {
                break;
            }
            let id = rec.meta["id"].as_str().unwrap_or("").to_string();
            if self.mail_inflight.contains(&id) {
                continue;
            }
            c.send(
                &inner
                    .messages
                    .message_frame(&rec, inner.opts.zlib_level)
                    .map_err(storage)?,
            )?;
            self.mail_inflight.insert(id);
            progressed = true;
        }
        let reports = inner
            .messages
            .select(|r| r.meta["from"].as_str() == Some(p.as_str()) && needs_report(&r.state));
        for rec in reports {
            if self.report_inflight.len() >= WINDOW {
                break;
            }
            let id = rec.meta["id"].as_str().unwrap_or("").to_string();
            if self.report_inflight.contains(&id) {
                continue;
            }
            let st = &rec.state;
            c.send(&Frame::new(
                kind::DELIVERY,
                meta! {"id" => id, "delivered" => st["delivered"], "pending" => st["pending"], "unknown" => st["unknown"]},
            ))?;
            self.report_inflight.insert(id);
            progressed = true;
        }
        Ok(progressed)
    }
}

/// A recipient took custody: move it from `pending` to `delivered`.
fn move_to_delivered(st: &mut Map<String, Value>, participant: &str) -> bool {
    let pending: Vec<Value> = st["pending"].as_array().cloned().unwrap_or_default();
    if !pending.iter().any(|x| x.as_str() == Some(participant)) {
        return false;
    }
    st.insert(
        "pending".into(),
        Value::Array(
            pending
                .into_iter()
                .filter(|x| x.as_str() != Some(participant))
                .collect(),
        ),
    );
    if let Some(d) = st.get_mut("delivered").and_then(Value::as_array_mut) {
        d.push(json!(participant));
    }
    true
}

/// True if the hub owes the sender a delivery report: more recipients delivered than reported, and something to
/// say – somebody delivered, somebody unknown, or nobody left to wait for (spec 8.5).
fn needs_report(st: &Map<String, Value>) -> bool {
    let len = |k: &str| st.get(k).and_then(Value::as_array).map_or(0, Vec::len);
    let n = len("delivered") as i64;
    n > st.get("reported").and_then(Value::as_i64).unwrap_or(-1)
        && (n > 0 || len("unknown") > 0 || len("pending") == 0)
}

/// A disk error at the hub: nothing is acknowledged (spec 14.10); ERROR `storage`, connection closed.
fn storage(e: io::Error) -> End {
    End::Bad(FrameError::new(
        "storage",
        format!("the hub cannot store data: {}", e),
    ))
}

/// The streams of every source, relayed to one consumer as `<source>/<stream>` (spec 7): from what the consumer
/// reported in its greeting, `STREAM_END` only for streams verified at the hub, block repair from the hub's copy.
struct Relay {
    sender: Sender,
    outs: BTreeMap<(String, u64), OutStream>,
    have: HashMap<(String, u64), u64>,
}

impl Relay {
    fn new(hello: &Frame, opts: &HubOptions) -> Relay {
        let have = hello.meta["have"]
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
        Relay {
            sender: Sender::new(opts.segment_bytes, opts.window, opts.zlib_level, "hub"),
            outs: BTreeMap::new(),
            have,
        }
    }

    fn refresh(&mut self, inner: &Inner) {
        let stores: Vec<(String, Arc<Mutex<Store>>)> = inner
            .stores
            .lock()
            .unwrap()
            .iter()
            .map(|(p, s)| (p.clone(), s.clone()))
            .collect();
        for (p, store) in stores {
            if inner.participants.get(&p).map(String::as_str) != Some("source") {
                continue;
            }
            for info in store.lock().unwrap().streams() {
                let key = (format!("{}/{}", p, info.stream), info.gen);
                let have = self.have.get(&key).copied().unwrap_or(0);
                let o = self.outs.entry(key).or_insert_with(|| {
                    let mut o = OutStream::new(info.gen, info.path.clone());
                    o.start_session(have);
                    o
                });
                o.size = info.committed;
                if info.complete && info.verified.as_deref() == Some("ok") {
                    o.final_size = Some(info.committed); // verified at the hub: the consumer may check it too
                }
            }
        }
    }

    fn handle(&mut self, f: &Frame) {
        if let Some(o) = self
            .outs
            .get_mut(&(f.str("stream").to_string(), f.u64("gen")))
        {
            self.sender.handle(f, f.str("stream"), o);
        }
    }

    fn work(&mut self, c: &mut Conn, inner: &Inner) -> Result<bool, End> {
        self.refresh(inner);
        let mut progressed = false;
        for ((name, _gen), o) in self.outs.iter_mut() {
            if o.sums_complete() {
                // the hub repairs a consumer only from its own verified copy, never with a new generation
                self.sender.compare(name, o, false)?;
            }
            progressed |= self.sender.send(c, name, o)?;
        }
        Ok(progressed)
    }
}

// --- offline media (spec 8.6) ----------------------------------------------------------------------------------------

/// What consumers last reported in bundles, and the block repairs they asked for; kept durably below the hub's
/// folder (`media_have.json`, `media_repairs.json`). State per consumer, for media only: on the network a consumer
/// reports in `HELLO`.
/// (relayed stream, gen) -> (size, block size, block sums): block repairs a consumer asked for.
type Requests = BTreeMap<(String, u64), (u64, u64, Vec<String>)>;

#[derive(Default)]
struct MediaState {
    /// consumer -> (relayed stream, gen) -> committed
    have: BTreeMap<String, BTreeMap<(String, u64), u64>>,
    /// consumer -> (relayed stream, gen) -> (size, block size, block sums)
    repairs: BTreeMap<String, Requests>,
}

impl MediaState {
    fn load(root: &Path) -> io::Result<MediaState> {
        let read = |name: &str| -> io::Result<Value> {
            match std::fs::read(root.join(name)) {
                Ok(raw) => Ok(serde_json::from_slice(&raw)?),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Value::Null),
                Err(e) => Err(e),
            }
        };
        let mut st = MediaState::default();
        for (consumer, list) in read("media_have.json")?.as_object().into_iter().flatten() {
            let mine = st.have.entry(consumer.clone()).or_default();
            for e in list.as_array().into_iter().flatten() {
                if let (Some(s), Some(g), Some(c)) = (
                    e["stream"].as_str(),
                    e["gen"].as_u64(),
                    e["committed"].as_u64(),
                ) {
                    mine.insert((s.to_string(), g), c);
                }
            }
        }
        for (consumer, list) in read("media_repairs.json")?
            .as_object()
            .into_iter()
            .flatten()
        {
            let mine = st.repairs.entry(consumer.clone()).or_default();
            for e in list.as_array().into_iter().flatten() {
                let sums: Option<Vec<String>> = e["sums"].as_array().map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                });
                if let (Some(s), Some(g), Some(size), Some(b), Some(sums)) = (
                    e["stream"].as_str(),
                    e["gen"].as_u64(),
                    e["size"].as_u64(),
                    e["block_size"].as_u64(),
                    sums,
                ) {
                    mine.insert((s.to_string(), g), (size, b, sums));
                }
            }
        }
        Ok(st)
    }

    fn save(&self, root: &Path) -> io::Result<()> {
        let have: Map<String, Value> = self
            .have
            .iter()
            .map(|(c, m)| {
                (
                    c.clone(),
                    Value::Array(
                        m.iter()
                            .map(|((s, g), n)| json!({"stream": s, "gen": g, "committed": n}))
                            .collect(),
                    ),
                )
            })
            .collect();
        let repairs: Map<String, Value> = self
            .repairs
            .iter()
            .map(|(c, m)| {
                let list = m.iter().map(|((s, g), (size, b, sums))| json!({"stream": s, "gen": g, "size": size, "block_size": b, "sums": sums}));
                (c.clone(), Value::Array(list.collect()))
            })
            .collect();
        write_json(&root.join("media_have.json"), &Value::Object(have))?;
        write_json(&root.join("media_repairs.json"), &Value::Object(repairs))
    }
}

fn write_json(path: &Path, v: &Value) -> io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&serde_json::to_vec(v)?)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

impl Hub {
    /// Imports every bundle on `medium` addressed to this hub, oldest first, as if it had arrived over the network:
    /// streams from sources, messages from everyone, receipts, what consumers hold and the repairs they ask for.
    /// Importing a bundle again changes nothing. Only bundles from participants on the list are taken, and only
    /// messages written by the bundle's sender (spec 8.6). One report per bundle.
    pub fn import_media(&self, medium: &Path) -> io::Result<Vec<ImportReport>> {
        let inner = &self.inner;
        let mut reports = Vec::new();
        for (path, manifest) in find_bundles(medium, &inner.operation, &inner.opts.hub_id)? {
            let sender = manifest
                .as_ref()
                .and_then(|m| m.get("from"))
                .and_then(Value::as_str)
                .map(String::from);
            let mut rep = ImportReport {
                path: path.clone(),
                sender: sender.clone(),
                ..Default::default()
            };
            let mut reason =
                check_manifest(manifest.as_ref(), &inner.operation, &inner.opts.hub_id);
            let role = sender
                .as_ref()
                .and_then(|s| inner.participants.get(s))
                .cloned();
            if reason.is_none() && role.is_none() {
                reason = Some(format!(
                    "the bundle is from {}, who is not on the participant list of this hub",
                    sender.clone().unwrap_or_default()
                ));
            }
            if let Some(r) = reason {
                rep.problems.push(r);
                reports.push(rep);
                continue;
            }
            let (sender, role) = (sender.unwrap(), role.unwrap());
            let store = if role == "source" {
                Some(inner.store(&sender)?)
            } else {
                None
            };
            let mut skipped = Skipped::new();
            let mut requests = BTreeMap::new();
            read_frames(
                &path,
                manifest.as_ref().unwrap(),
                &mut rep,
                &mut |f, rep| {
                    match f.ftype {
                        kind::SEGMENT | kind::STREAM_END if store.is_some() => {
                            let key = (f.str("stream").to_string(), f.u64("gen"));
                            if format!("{}/{}", sender, key.0).len() > 255 {
                                if skipped.insert(key.clone()) {
                                    rep.problems.push(format!(
                                        "stream name {:?} is too long to relay - skipped",
                                        key.0
                                    ));
                                }
                            } else {
                                store.as_ref().unwrap().lock().unwrap().media_frame(
                                    &f,
                                    rep,
                                    &mut skipped,
                                )?;
                            }
                        }
                        kind::MESSAGE => {
                            if f.str("from") != sender {
                                rep.problems.push(format!("message {} claims to be from {}, the bundle is from {} - skipped", f.str("id"), f.str("from"), sender));
                                return Ok(());
                            }
                            let body = match frames::payload(&f) {
                                Ok(b) => b,
                                Err(e) => {
                                    rep.problems.push(format!(
                                        "message {}: {} - skipped",
                                        f.str("id"),
                                        e.text
                                    ));
                                    return Ok(());
                                }
                            };
                            let to: Vec<String> = f.meta["to"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect();
                            let m: Meta = ["id", "from", "to", "t", "kind"]
                                .iter()
                                .map(|k| (k.to_string(), f.meta[*k].clone()))
                                .collect();
                            if inner.messages.put(
                                m.clone(),
                                &body,
                                inner.custody_state(&sender, &to),
                            )? {
                                rep.messages.push(f.str("id").to_string());
                                inner.to_hub(&m);
                            }
                        }
                        kind::MESSAGE_ACK => {
                            let s = sender.clone();
                            inner
                                .messages
                                .modify(f.str("id"), |st| move_to_delivered(st, &s))?;
                        }
                        kind::DELIVERY_ACK => {
                            let id = f.str("id").to_string();
                            let n = f.u64("delivered") as i64;
                            let from_sender = inner
                                .messages
                                .select(|r| r.meta["id"].as_str() == Some(id.as_str()))
                                .first()
                                .is_some_and(|r| r.meta["from"].as_str() == Some(sender.as_str()));
                            if from_sender {
                                inner.messages.modify(&id, |st| {
                                    let old =
                                        st.get("reported").and_then(Value::as_i64).unwrap_or(-1);
                                    st.insert("reported".into(), json!(old.max(n)));
                                    true
                                })?;
                            }
                        }
                        kind::ACK if role == "consumer" => rep.acks.push((
                            f.str("stream").to_string(),
                            f.u64("gen"),
                            f.u64("committed"),
                        )),
                        _ if role == "consumer" && collect_repair_frame(&f, &mut requests) => {}
                        t => rep.problems.push(format!(
                            "unexpected {} from a {} - skipped",
                            frames::type_name(t),
                            role
                        )),
                    }
                    Ok(())
                },
            )?;
            if let Some(cb) = &inner.opts.on_stream_checked {
                for (stream, gen, result) in &rep.checked {
                    cb(&sender, stream, *gen, result);
                }
            }
            let complete: Vec<_> = requests
                .into_iter()
                .filter_map(|(k, r)| r.complete_sums().map(|s| (k, (r.size, r.block_size, s))))
                .collect();
            if !rep.acks.is_empty() || !complete.is_empty() {
                let mut media = inner.media.lock().unwrap();
                let mine = media.have.entry(sender.clone()).or_default();
                for (s, g, c) in &rep.acks {
                    let e = mine.entry((s.clone(), *g)).or_insert(0);
                    *e = (*e).max(*c);
                }
                media
                    .repairs
                    .entry(sender.clone())
                    .or_default()
                    .extend(complete);
                media.save(&inner.root)?;
            }
            reports.push(rep);
        }
        Ok(reports)
    }

    /// Writes a bundle for `participant` to the folder `medium`: receipts and delivery reports for the messages it
    /// sent, messages pending for it, and what its role needs – a source: what the hub holds of its streams (`ACK`)
    /// and repair requests for streams whose final check failed here; a consumer: repairs it asked for (from the
    /// hub's verified copy) and the relayed streams from what it last reported in a bundle, `STREAM_END` only for
    /// streams verified here. `max_bytes` limits the bundle.
    pub fn export_media(
        &self,
        medium: &Path,
        participant: &str,
        max_bytes: Option<u64>,
    ) -> io::Result<ExportReport> {
        let inner = &self.inner;
        let Some(role) = inner.participants.get(participant).cloned() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not on the participant list of this hub", participant),
            ));
        };
        let level = inner.opts.zlib_level;
        let mut w = BundleWriter::new(
            medium,
            &inner.operation,
            &inner.opts.hub_id,
            participant,
            max_bytes,
        )?;
        let mut ok = true;
        // receipts first: messages the participant sent, and the delivery reports it is owed
        for rec in inner
            .messages
            .select(|r| r.meta["from"].as_str() == Some(participant))
        {
            ok = ok
                && w.add(&Frame::new(
                    kind::MESSAGE_ACK,
                    meta! {"id" => rec.meta["id"]},
                ))?;
        }
        for rec in inner
            .messages
            .select(|r| r.meta["from"].as_str() == Some(participant) && needs_report(&r.state))
        {
            let st = &rec.state;
            let report = meta! {"id" => rec.meta["id"], "delivered" => st["delivered"], "pending" => st["pending"], "unknown" => st["unknown"]};
            ok = ok && w.add(&Frame::new(kind::DELIVERY, report))?;
        }
        if role == "source" && ok {
            ok = inner
                .store(participant)?
                .lock()
                .unwrap()
                .media_receipts(&mut w)?;
        }
        let pending = inner.messages.select(|r| {
            r.state["pending"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x.as_str() == Some(participant)))
        });
        for rec in pending {
            if !ok {
                break;
            }
            ok = w.add(&inner.messages.message_frame(&rec, level)?)?;
            if ok {
                w.report.messages += 1;
            }
        }
        let mut repaired = Vec::new();
        if role == "consumer" {
            let streams = self.streams();
            let find = |name: &str, gen: u64| {
                streams.iter().find(|s| {
                    format!("{}/{}", s.participant, s.info.stream) == name && s.info.gen == gen
                })
            };
            let asked = inner
                .media
                .lock()
                .unwrap()
                .repairs
                .get(participant)
                .cloned()
                .unwrap_or_default();
            for ((name, gen), (size, block, sums)) in asked {
                let Some(s) = find(&name, gen).filter(|s| s.info.verified.as_deref() == Some("ok"))
                else {
                    continue;
                };
                if !ok {
                    break;
                }
                let (fitted, _n) = add_repair(
                    &mut w,
                    &name,
                    gen,
                    &s.info.path,
                    (size, block, &sums),
                    level,
                )?;
                ok = fitted;
                if ok {
                    repaired.push((name, gen));
                }
            }
            let have = inner
                .media
                .lock()
                .unwrap()
                .have
                .get(participant)
                .cloned()
                .unwrap_or_default();
            for s in &streams {
                if !ok {
                    break;
                }
                if inner.participants.get(&s.participant).map(String::as_str) != Some("source") {
                    continue;
                }
                let name = format!("{}/{}", s.participant, s.info.stream);
                let start = have
                    .get(&(name.clone(), s.info.gen))
                    .copied()
                    .unwrap_or(0)
                    .min(s.info.committed);
                let fin = (s.info.complete && s.info.verified.as_deref() == Some("ok"))
                    .then_some(s.info.committed);
                ok = add_stream(
                    &mut w,
                    &name,
                    s.info.gen,
                    &s.info.path,
                    start,
                    s.info.committed,
                    fin,
                    level,
                )?;
            }
        }
        let report = w.close()?;
        if !repaired.is_empty() {
            // carried by a bundle: forget them; the consumer asks again if the stick never arrives
            let mut media = inner.media.lock().unwrap();
            if let Some(mine) = media.repairs.get_mut(participant) {
                for k in &repaired {
                    mine.remove(k);
                }
            }
            media.save(&inner.root)?;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(p, r)| (p.to_string(), r.to_string()))
            .collect()
    }

    #[test]
    fn refuses_bad_settings_in_plain_language() {
        let dir =
            std::env::temp_dir().join(format!("rope-rs-test-hub-{}", crate::mailbox::new_id()));
        let free = || HubOptions {
            port: 0,
            ..HubOptions::default()
        };
        let err = |r: io::Result<Hub>| r.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(
            err(Hub::start(&dir, list(&[("s 1", "source")]), "op-1", free()))
                .contains("not a valid identifier")
        );
        assert!(
            err(Hub::start(&dir, list(&[("hub", "source")]), "op-1", free()))
                .contains("the hub's own")
        );
        assert!(
            err(Hub::start(&dir, list(&[("s1", "admin")]), "op-1", free()))
                .contains("role must be")
        );
        let slow = HubOptions {
            heartbeat: Duration::from_secs(2),
            timeout: Duration::from_secs(3),
            ..free()
        };
        assert!(
            err(Hub::start(&dir, list(&[("s1", "source")]), "op-1", slow))
                .contains("twice the heartbeat")
        );
        let mut a = Hub::start(&dir, list(&[("s1", "source")]), "op-1", free()).unwrap();
        let taken = HubOptions {
            port: a.port(),
            ..HubOptions::default()
        };
        assert!(
            err(Hub::start(&dir, list(&[("s1", "source")]), "op-1", taken))
                .contains(&format!("port {}", a.port()))
        );
        a.stop();
        let _ = std::fs::remove_dir_all(dir);
    }
}
