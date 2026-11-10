//! The hub (spec 7, 8.1, 14.5, 14.10): checks every greeting against its participant list, keeps one session per
//! participant, stores the streams of sources durably and checks them.
//!
//! So far: sources over TCP. Not yet: relay to consumers, messages, offline media.

use crate::frames::{self, kind, Frame, FrameError};
use crate::participant::{debug, log};
use crate::receiver::{Store, StreamInfo};
use crate::session::{Conn, End};
use crate::{meta, now_ms, PROTOCOL_VERSION};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
        });
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
    while !inner.stop.load(Ordering::SeqCst) {
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
        let until_beat = heartbeat.saturating_sub(c.last_sent.elapsed());
        if let Some(f) =
            c.recv_wait(until_beat.clamp(Duration::from_millis(1), Duration::from_millis(100)))?
        {
            handle_frame(&f, c, inner, &participant, store.as_ref())?;
        }
        if c.last_sent.elapsed() >= heartbeat {
            c.send(&Frame::new(kind::HEARTBEAT, meta! {"t" => now_ms()}))?;
        }
    }
}

fn handle_frame(
    f: &Frame,
    c: &mut Conn,
    inner: &Inner,
    participant: &str,
    store: Option<&Arc<Mutex<Store>>>,
) -> Result<(), End> {
    match f.ftype {
        kind::SEGMENT | kind::STREAM_END if store.is_some() => {
            let name = format!("{}/{}", participant, f.str("stream"));
            if name.len() > 255 {
                return Err(End::Bad(FrameError::new(
                    "bad_frame",
                    format!("stream name {:?} is too long to relay: at most 255 bytes including the source identifier and '/'", f.str("stream")),
                )));
            }
            let store = store.unwrap();
            let mut s = store.lock().unwrap();
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
        // messages follow in the next step: not acknowledged, so the sender keeps them
        kind::MESSAGE | kind::MESSAGE_ACK | kind::DELIVERY_ACK => Ok(()),
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
