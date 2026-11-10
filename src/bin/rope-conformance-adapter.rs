//! Conformance adapter (the command interface of `rope/conformance/README.md`): one JSON command per line on stdin,
//! one JSON answer per line on stdout, logging on stderr. Lets the ROPE conformance suite test this implementation:
//!
//! ```text
//! python conformance/run.py --rope ../rope
//! ```
//!
//! Implemented so far: source and recipient, messages. Other commands answer `"ok": false`.

use rope_protocol::consumer::Consumer;
use rope_protocol::hub::{Hub, HubOptions};
use rope_protocol::participant::{Options, Participant};
use rope_protocol::source::Source;
use rope_protocol::{mailbox, recipient};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often the application was told about each message (the suite checks "exactly once").
type Told = Arc<Mutex<BTreeMap<String, u64>>>;

/// The harness uses short timeouts to stay quick (conformance/README.md).
fn options(told: &Told) -> Options {
    let t = told.clone();
    let on_message: mailbox::OnMessage = Arc::new(move |meta, _body| {
        let id = meta
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        *t.lock().unwrap().entry(id).or_insert(0) += 1;
    });
    Options {
        segment_bytes: 64 * 1024,
        poll: Duration::from_millis(50),
        timeout: Duration::from_secs(1),
        backoff: (Duration::from_millis(50), Duration::from_millis(300)),
        on_message: Some(on_message),
        ..Options::default()
    }
}

enum Running {
    Source(Source),
    Consumer(Consumer),
    Other(Participant),
}

impl Running {
    fn participant(&self) -> &Participant {
        match self {
            Running::Source(s) => s,
            Running::Consumer(c) => c,
            Running::Other(p) => p,
        }
    }
    fn stop(&mut self) {
        match self {
            Running::Source(s) => s.stop(),
            Running::Consumer(c) => c.stop(),
            Running::Other(p) => p.stop(),
        }
    }
}

/// Final checks a hub reported: "<source>/<stream>" -> result.
type Checked = Arc<Mutex<BTreeMap<String, String>>>;

struct Adapter {
    running: Option<Running>,
    /// The hub under test, with the final checks it reported ("<source>/<stream>" -> result).
    hub: Option<(Hub, Checked)>,
    told: Told,
}

fn text<'a>(c: &'a Value, key: &str) -> Result<&'a str, String> {
    c[key].as_str().ok_or_else(|| format!("missing {}", key))
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd number of hex digits".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

impl Adapter {
    fn stop(&mut self) {
        if let Some(mut r) = self.running.take() {
            r.stop();
        }
        if let Some((mut h, _)) = self.hub.take() {
            h.stop();
        }
    }

    fn start_hub(&mut self, c: &Value) -> Result<Map<String, Value>, String> {
        self.stop();
        let participants: BTreeMap<String, String> = c["participants"]
            .as_object()
            .ok_or("missing participants")?
            .iter()
            .map(|(p, r)| (p.clone(), r.as_str().unwrap_or("").to_string()))
            .collect();
        let checked = Arc::new(Mutex::new(BTreeMap::new()));
        let ck = checked.clone();
        let opts = HubOptions {
            port: c["port"].as_u64().unwrap_or(0) as u16,
            heartbeat: Duration::from_secs_f64(c["heartbeat_s"].as_f64().unwrap_or(0.2)),
            timeout: Duration::from_secs_f64(c["timeout_s"].as_f64().unwrap_or(1.0)),
            on_stream_checked: Some(Arc::new(move |p, s, _g, r| {
                ck.lock()
                    .unwrap()
                    .insert(format!("{}/{}", p, s), r.to_string());
            })),
            ..HubOptions::default()
        };
        let hub = Hub::start(
            &PathBuf::from(text(c, "dir")?),
            participants,
            text(c, "operation")?,
            opts,
        )
        .map_err(|e| e.to_string())?;
        let port = hub.port();
        self.hub = Some((hub, checked));
        Ok(json!({"port": port}).as_object().unwrap().clone())
    }

    fn hub_status(
        &self,
        hub: &Hub,
        checked: &Mutex<BTreeMap<String, String>>,
    ) -> Map<String, Value> {
        let sessions = hub.sessions();
        let streams: Vec<Value> = hub
            .streams()
            .iter()
            .map(|s| {
                json!({"participant": s.participant, "stream": s.info.stream, "gen": s.info.gen,
                       "committed": s.info.committed, "complete": s.info.complete,
                       "verified": s.info.verified, "path": s.info.path})
            })
            .collect();
        let out = json!({"running": true, "connected": !sessions.is_empty(), "sessions": sessions, "rejected": null,
                         "checked": *checked.lock().unwrap(), "told": *self.told.lock().unwrap(), "inbox": [],
                         "streams": streams});
        out.as_object().unwrap().clone()
    }

    fn handle(&mut self, c: &Value) -> Result<Map<String, Value>, String> {
        let cmd = c["cmd"].as_str().unwrap_or("");
        let dir = || text(c, "dir").map(PathBuf::from);
        if cmd == "start_hub" {
            return self.start_hub(c);
        }
        if let (Some((hub, checked)), "status") = (&self.hub, cmd) {
            return Ok(self.hub_status(hub, checked));
        }
        match cmd {
            "start_source" => {
                self.stop();
                let files = c["files"].as_object().ok_or("missing files")?;
                let streams = files
                    .iter()
                    .map(|(n, p)| (n.clone(), PathBuf::from(p.as_str().unwrap_or(""))))
                    .collect();
                let s = Source::start(
                    text(c, "participant")?,
                    text(c, "operation")?,
                    text(c, "hub")?,
                    streams,
                    &dir()?,
                    options(&self.told),
                )
                .map_err(|e| e.to_string())?;
                self.running = Some(Running::Source(s));
            }
            "start_consumer" => {
                self.stop();
                let p = Consumer::start(
                    text(c, "participant")?,
                    text(c, "operation")?,
                    text(c, "hub")?,
                    &dir()?,
                    options(&self.told),
                )
                .map_err(|e| e.to_string())?;
                self.running = Some(Running::Consumer(p));
            }
            "start_recipient" => {
                self.stop();
                let p = recipient::start(
                    text(c, "participant")?,
                    text(c, "operation")?,
                    text(c, "hub")?,
                    &dir()?,
                    options(&self.told),
                )
                .map_err(|e| e.to_string())?;
                self.running = Some(Running::Other(p));
            }
            "add_stream" | "complete" => {
                let Some(Running::Source(s)) = &self.running else {
                    return Err("no source running".into());
                };
                let stream = text(c, "stream")?;
                if cmd == "add_stream" {
                    s.add_stream(stream, PathBuf::from(text(c, "path")?));
                } else if !s.complete(stream) {
                    return Err("unknown stream".into());
                }
            }
            "send_message" => {
                let p = self
                    .running
                    .as_ref()
                    .ok_or("nothing running")?
                    .participant();
                let to: Vec<String> = c["to"]
                    .as_array()
                    .ok_or("missing to")?
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                let id = p
                    .send_message(&to, text(c, "kind")?, &unhex(text(c, "body_hex")?)?)
                    .map_err(|e| e.to_string())?;
                let mut out = Map::new();
                out.insert("id".into(), json!(id));
                return Ok(out);
            }
            "export_media" => {
                let medium = PathBuf::from(text(c, "medium")?);
                let max = c["max_bytes"].as_u64();
                let rep = match self.running.as_ref().ok_or("nothing running")? {
                    Running::Source(s) => s.export_media(&medium, max),
                    Running::Consumer(k) => k.export_media(&medium, max),
                    Running::Other(p) => p.export_media(&medium, max),
                }
                .map_err(|e| e.to_string())?;
                let out = json!({"complete": rep.complete, "stream_bytes": rep.stream_bytes, "messages": rep.messages,
                                 "repair_bytes": rep.repair_bytes, "path": rep.path});
                return Ok(out.as_object().unwrap().clone());
            }
            "import_media" => {
                let medium = PathBuf::from(text(c, "medium")?);
                let reps = match self.running.as_ref().ok_or("nothing running")? {
                    Running::Source(s) => s.import_media(&medium),
                    Running::Consumer(k) => k.import_media(&medium),
                    Running::Other(p) => p.import_media(&medium),
                }
                .map_err(|e| e.to_string())?;
                let problems: Vec<&String> = reps.iter().flat_map(|r| r.problems.iter()).collect();
                let out = json!({"bundles": reps.len(), "problems": problems});
                return Ok(out.as_object().unwrap().clone());
            }
            "status" => return Ok(self.status()),
            "stop" => self.stop(),
            other => return Err(format!("not implemented in rope-rs yet: {}", other)),
        }
        Ok(Map::new())
    }

    fn status(&self) -> Map<String, Value> {
        let mut out = json!({"running": self.running.is_some(), "connected": false, "rejected": null, "checked": {},
                             "told": *self.told.lock().unwrap(), "inbox": [], "streams": []});
        if let Some(r) = &self.running {
            let p = r.participant();
            out["connected"] = json!(p.is_connected());
            if let Some(rej) = p.rejected() {
                out["rejected"] = json!({"code": rej.code, "operation": rej.operation});
            }
            let inbox: Vec<Value> = p
                .inbox()
                .iter()
                .map(|m| json!({"id": m.id, "from": m.from, "kind": m.kind, "body_hex": hex(&p.message_body(&m.id).unwrap_or_default())}))
                .collect();
            out["inbox"] = json!(inbox);
            match r {
                Running::Source(s) => out["checked"] = json!(s.checked()),
                Running::Consumer(c) => {
                    let streams: Vec<Value> = c
                        .streams()
                        .iter()
                        .map(|s| {
                            json!({"stream": s.stream, "gen": s.gen, "committed": s.committed,
                                   "verified": s.verified, "path": s.path})
                        })
                        .collect();
                    out["streams"] = json!(streams);
                }
                Running::Other(_) => {}
            }
        }
        match out {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }
}

fn main() {
    let mut adapter = Adapter {
        running: None,
        hub: None,
        told: Arc::new(Mutex::new(BTreeMap::new())),
    };
    let stdout = io::stdout();
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let answer = match serde_json::from_str::<Value>(line) {
            Ok(c) if c["cmd"] == "quit" => {
                adapter.stop();
                let mut out = stdout.lock();
                let _ = writeln!(out, "{}", json!({"ok": true}));
                let _ = out.flush();
                return;
            }
            Ok(c) => match adapter.handle(&c) {
                Ok(mut m) => {
                    m.insert("ok".into(), json!(true));
                    Value::Object(m)
                }
                Err(e) => json!({"ok": false, "error": e}),
            },
            Err(e) => json!({"ok": false, "error": format!("not JSON: {}", e)}),
        };
        let mut out = stdout.lock();
        let _ = writeln!(out, "{}", answer);
        let _ = out.flush();
    }
}
