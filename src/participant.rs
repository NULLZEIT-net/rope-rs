//! What every participant shares (spec 8.1, 8.5, 9, 14.5, 14.10): greeting, heartbeat and timeouts from `WELCOME`,
//! messages with custody, reconnect with back-off. The role (source, recipient, …) adds its own frames.

use crate::frames::{self, kind, Frame, FrameError};
use crate::mailbox::{MailSession, Mailbox, Message, OnDelivery, OnMessage, Sent};
use crate::media::{
    check_manifest, find_bundles, read_frames, BundleWriter, ExportReport, ImportReport, Skipped,
};
use crate::session::{Conn, End};
use crate::{meta, now_ms, PROTOCOL_VERSION};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Settings of a participant; the defaults are the recommended values of spec 14.8.
#[derive(Clone)]
pub struct Options {
    /// Stream data per segment (sources).
    pub segment_bytes: usize,
    /// Unacknowledged segments in flight (sources).
    pub window: usize,
    /// How often files are looked at when nothing else is going on.
    pub poll: Duration,
    /// Time limit for connecting and greeting; after `WELCOME` the hub's `timeout_s` applies.
    pub timeout: Duration,
    /// Reconnect back-off: first and longest wait.
    pub backoff: (Duration, Duration),
    pub zlib_level: u32,
    /// Called once per received message.
    pub on_message: Option<OnMessage>,
    /// Called once per new delivery report of a sent message.
    pub on_delivery: Option<OnDelivery>,
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
            on_message: None,
            on_delivery: None,
        }
    }
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("segment_bytes", &self.segment_bytes)
            .field("window", &self.window)
            .field("poll", &self.poll)
            .field("timeout", &self.timeout)
            .field("backoff", &self.backoff)
            .field("zlib_level", &self.zlib_level)
            .field("on_message", &self.on_message.is_some())
            .field("on_delivery", &self.on_delivery.is_some())
            .finish()
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

/// The role-specific part of a session.
pub(crate) trait Role: Send + 'static {
    fn role(&self) -> &'static str;
    /// What this participant holds as a receiver, for `HELLO.have` (spec 14.5); empty for a pure source.
    fn have(&mut self) -> Vec<Value> {
        Vec::new()
    }
    /// The session is active; `have`: what the hub holds from this participant (`WELCOME.have`).
    fn start_session(&mut self, have: HashMap<(String, u64), u64>) -> Result<(), End>;
    /// A frame from the hub; false if the role does not take it.
    fn handle(&mut self, f: &Frame, c: &mut Conn) -> Result<bool, End>;
    /// Sends what is due; true if anything was sent.
    fn work(&mut self, c: &mut Conn) -> Result<bool, End>;
}

pub(crate) fn log(what: &str, text: &str) {
    eprintln!("rope {} {}: {}", what, now_ms() % 100_000, text);
}

/// Detailed events only with the environment variable `ROPE_DEBUG` set.
pub(crate) fn debug(what: &str, text: impl FnOnce() -> String) {
    if std::env::var_os("ROPE_DEBUG").is_some() {
        log(what, &text());
    }
}

/// A running participant: reconnects forever with back-off until [`Participant::stop`].
pub struct Participant {
    id: String,
    operation: String,
    /// The hub's identifier, from the last WELCOME ("hub" until then); bundles for the hub are addressed to it.
    hub_id: Arc<Mutex<String>>,
    zlib_level: u32,
    mailbox: Arc<Mailbox>,
    rejected: Arc<Mutex<Option<Rejected>>>,
    connected: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

struct Config {
    participant: String,
    operation: String,
    hub: String,
    opts: Options,
}

struct Running {
    cfg: Config,
    hub_id: Arc<Mutex<String>>,
    mailbox: Arc<Mailbox>,
    rejected: Arc<Mutex<Option<Rejected>>>,
    connected: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl Participant {
    pub(crate) fn spawn(
        participant: &str,
        operation: &str,
        hub: &str,
        state_dir: &Path,
        opts: Options,
        role: Box<dyn Role>,
    ) -> io::Result<Participant> {
        let mailbox = Arc::new(Mailbox::open(
            &state_dir.join("messages"),
            participant,
            opts.on_message.clone(),
            opts.on_delivery.clone(),
        )?);
        let zlib_level = opts.zlib_level;
        let hub_id = Arc::new(Mutex::new("hub".to_string()));
        let run = Running {
            hub_id: hub_id.clone(),
            cfg: Config {
                participant: participant.into(),
                operation: operation.into(),
                hub: hub.into(),
                opts,
            },
            mailbox: mailbox.clone(),
            rejected: Arc::new(Mutex::new(None)),
            connected: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
        };
        let (rejected, connected, stop) = (
            run.rejected.clone(),
            run.connected.clone(),
            run.stop.clone(),
        );
        let thread = thread::spawn(move || run.run(role));
        Ok(Participant {
            id: participant.into(),
            operation: operation.into(),
            hub_id,
            zlib_level,
            mailbox,
            rejected,
            connected,
            stop,
            thread: Some(thread),
        })
    }

    /// Puts a message into the outbox (durably) and returns its id; it leaves with the next connection. `to` may
    /// name the hub itself.
    pub fn send_message(&self, to: &[String], kind: &str, body: &[u8]) -> io::Result<String> {
        self.mailbox.send(to, kind, body)
    }

    /// Received messages, oldest first.
    pub fn inbox(&self) -> Vec<Message> {
        self.mailbox.inbox()
    }

    /// Sent messages with custody state and delivery report.
    pub fn outbox(&self) -> Vec<Sent> {
        self.mailbox.outbox()
    }

    pub fn message_body(&self, id: &str) -> io::Result<Vec<u8>> {
        self.mailbox.body(id)
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// The hub's last refusal; cleared by the next session it accepts.
    pub fn rejected(&self) -> Option<Rejected> {
        self.rejected.lock().unwrap().clone()
    }

    // --- offline media (spec 8.6) ------------------------------------------------------------------------------

    /// Writes a bundle for the hub to the folder `medium` (e.g. a USB stick): receipts, then the messages the hub
    /// does not hold yet. `max_bytes` limits the bundle; the report says whether everything fitted.
    pub fn export_media(&self, medium: &Path, max_bytes: Option<u64>) -> io::Result<ExportReport> {
        self.export_bundle(medium, max_bytes, &mut |_| Ok(true), &mut |_| Ok(true))
    }

    /// Imports every bundle on `medium` the hub wrote for this participant, oldest first. Importing a bundle again
    /// changes nothing. One report per bundle; `problems` lists what was skipped and why.
    pub fn import_media(&self, medium: &Path) -> io::Result<Vec<ImportReport>> {
        self.import_bundles(medium, &mut |_, _, _| Ok(false))
    }

    /// Export with the role's parts: `receipts` (after the message receipts) and `data` (after the messages).
    pub(crate) fn export_bundle(
        &self,
        medium: &Path,
        max_bytes: Option<u64>,
        receipts: &mut dyn FnMut(&mut BundleWriter) -> io::Result<bool>,
        data: &mut dyn FnMut(&mut BundleWriter) -> io::Result<bool>,
    ) -> io::Result<ExportReport> {
        let hub = self.hub_id.lock().unwrap().clone();
        let mut w = BundleWriter::new(medium, &self.operation, &self.id, &hub, max_bytes)?;
        // order inside a bundle: receipts, then messages, then streams (spec 8.6)
        let mut ok = true;
        for f in self.mailbox.media_receipts() {
            if !w.add(&f)? {
                ok = false;
                break;
            }
        }
        ok = ok && receipts(&mut w)?;
        ok = ok && self.mailbox.media_messages(&mut w, self.zlib_level)?;
        if ok {
            data(&mut w)?;
        }
        w.close()
    }

    /// Import with the role's part: `frame` takes the frames the mailbox does not take (false: not for this role).
    pub(crate) fn import_bundles(
        &self,
        medium: &Path,
        frame: &mut dyn FnMut(&Frame, &mut ImportReport, &mut Skipped) -> io::Result<bool>,
    ) -> io::Result<Vec<ImportReport>> {
        let mut reports = Vec::new();
        for (path, manifest) in find_bundles(medium, &self.operation, &self.id)? {
            let mut rep = ImportReport {
                path: path.clone(),
                sender: manifest
                    .as_ref()
                    .and_then(|m| m.get("from"))
                    .and_then(Value::as_str)
                    .map(String::from),
                ..Default::default()
            };
            if let Some(reason) = check_manifest(manifest.as_ref(), &self.operation, &self.id) {
                rep.problems.push(reason);
                reports.push(rep);
                continue;
            }
            let mut skipped = Skipped::new();
            let mailbox = &self.mailbox;
            read_frames(
                &path,
                manifest.as_ref().unwrap(),
                &mut rep,
                &mut |f, rep| {
                    if mailbox.media_import(&f, rep)? || frame(&f, rep, &mut skipped)? {
                        return Ok(());
                    }
                    rep.problems.push(format!(
                        "unexpected {} in a bundle from the hub - skipped",
                        frames::type_name(f.ftype)
                    ));
                    Ok(())
                },
            )?;
            reports.push(rep);
        }
        self.mailbox.notify()?; // tell the application about what came on the medium
        Ok(reports)
    }

    /// Ends the session (with `BYE`) and the reconnect loop; returns when the thread has finished.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for Participant {
    fn drop(&mut self) {
        self.stop();
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

fn jitter() -> f64 {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (n % 1000) as f64 / 1000.0
}

impl Running {
    fn run(self, mut role: Box<dyn Role>) {
        let who = role.role();
        let (first, longest) = self.cfg.opts.backoff;
        let mut delay = first;
        while !self.stop.load(Ordering::SeqCst) {
            let ended = self.session(role.as_mut());
            self.connected.store(false, Ordering::SeqCst);
            match ended {
                Ok(()) => delay = first,
                Err(why) if why.starts_with("rejected") => {
                    log(who, &why);
                    delay = longest;
                }
                Err(why) => log(
                    who,
                    &format!(
                        "connection lost ({}), retrying in {:.2} s",
                        why,
                        delay.as_secs_f64()
                    ),
                ),
            }
            // back-off with jitter (spec 14.8), in small steps so that stop() is quick
            let end = Instant::now() + delay.mul_f64(0.8 + 0.2 * jitter());
            while Instant::now() < end && !self.stop.load(Ordering::SeqCst) {
                thread::sleep(
                    Duration::from_millis(10).min(end.saturating_duration_since(Instant::now())),
                );
            }
            delay = (delay * 2).min(longest);
        }
    }

    fn session(&self, role: &mut dyn Role) -> Result<(), String> {
        let mut c =
            Conn::connect(&self.cfg.hub, self.cfg.opts.timeout).map_err(|e| e.to_string())?;
        match self.greet_and_serve(&mut c, role) {
            Ok(()) => Ok(()),
            Err(SessionEnd::End(End::Bad(e))) => {
                // spec 14.2: a bad frame from the hub is answered with ERROR, then the connection is closed
                c.error_and_close(&e);
                Err(e.to_string())
            }
            Err(SessionEnd::End(End::Lost(why))) => Err(why),
            Err(SessionEnd::Rejected(r)) => {
                // noted before the connection closes, so that whoever sees the close also sees the reason
                let why = format!("rejected by the hub: {}", r.text);
                *self.rejected.lock().unwrap() = Some(r);
                Err(why)
            }
        }
    }

    fn greet_and_serve(&self, c: &mut Conn, role: &mut dyn Role) -> Result<(), SessionEnd> {
        let cfg = &self.cfg;
        c.send(&Frame::new(
            kind::HELLO,
            meta! {"participant" => cfg.participant, "operation" => cfg.operation, "role" => role.role(),
                   "versions" => [PROTOCOL_VERSION], "t" => now_ms(), "have" => role.have()},
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
            let text = format!("expected WELCOME, got {}", frames::type_name(first.ftype));
            return Err(FrameError::new("bad_frame", text).into());
        }
        let w = &first.meta;
        *self.hub_id.lock().unwrap() = first.str("hub").to_string();
        c.version = w["version"].as_u64().unwrap_or(1).min(255) as u8;
        let heartbeat = Duration::from_secs_f64(w["heartbeat_s"].as_f64().unwrap_or(2.0));
        let timeout = Duration::from_secs_f64(w["timeout_s"].as_f64().unwrap_or(10.0));
        c.send_timeout = timeout; // the hub's timeout for sending too, not only for receiving (spec 9)
        let have = w["have"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|h| {
                let key = (
                    h["stream"].as_str().unwrap_or("").to_string(),
                    h["gen"].as_u64().unwrap_or(0),
                );
                (key, h["committed"].as_u64().unwrap_or(0))
            })
            .collect();
        role.start_session(have)?;
        let mut mail = MailSession::start(&self.mailbox, cfg.opts.zlib_level)?;
        *self.rejected.lock().unwrap() = None;
        self.connected.store(true, Ordering::SeqCst);
        loop {
            if self.stop.load(Ordering::SeqCst) {
                c.send_timeout = c.send_timeout.min(Duration::from_secs(1));
                let _ = c.send(&Frame::new(kind::BYE, meta! {"reason" => "stop"}));
                return Ok(());
            }
            while let Some(f) = c.try_recv()? {
                dispatch(&f, c, &mut mail, role)?;
            }
            if c.silent_for() > timeout {
                return Err(End::Lost(format!(
                    "no sign of life for {:.1} s",
                    timeout.as_secs_f64()
                ))
                .into());
            }
            // messages before stream data (spec 8.5)
            let mut progressed = mail.work(c)?;
            progressed |= role.work(c)?;
            if c.last_sent.elapsed() >= heartbeat {
                c.send(&Frame::new(kind::HEARTBEAT, meta! {"t" => now_ms()}))?;
            }
            if !progressed {
                let until_beat = heartbeat.saturating_sub(c.last_sent.elapsed());
                let wait = cfg.opts.poll.min(until_beat).max(Duration::from_millis(1));
                if let Some(f) = c.recv_wait(wait)? {
                    dispatch(&f, c, &mut mail, role)?;
                }
            }
        }
    }
}

fn dispatch(
    f: &Frame,
    c: &mut Conn,
    mail: &mut MailSession,
    role: &mut dyn Role,
) -> Result<(), End> {
    if f.ftype != kind::HEARTBEAT && f.ftype != kind::ACK {
        debug(role.role(), || {
            format!(
                "from hub {} {}",
                frames::type_name(f.ftype),
                Value::Object(f.meta.clone())
            )
        });
    }
    if mail.handle(f, c)? || role.handle(f, c)? {
        return Ok(());
    }
    match f.ftype {
        kind::HEARTBEAT => Ok(()),
        kind::BYE => Err(End::Lost(format!(
            "the hub closed the session: {}",
            f.str("reason")
        ))),
        kind::ERROR => Err(End::Lost(format!(
            "the hub reported {}: {}",
            f.str("code"),
            f.str("text")
        ))),
        _ if f.is_extension() => Ok(()), // unknown extensions are ignored (spec 14.3)
        t => Err(End::Bad(FrameError::new(
            "bad_frame",
            format!("unexpected {} from the hub", frames::type_name(t)),
        ))),
    }
}
