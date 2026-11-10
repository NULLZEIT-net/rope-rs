//! Conformance adapter (the command interface of `rope/conformance/README.md`): one JSON command per line on stdin,
//! one JSON answer per line on stdout, logging on stderr. Lets the ROPE conformance suite test this implementation:
//!
//! ```text
//! python conformance/run.py --rope ../rope
//! ```
//!
//! Implemented so far: source and recipient, messages. Other commands answer `"ok": false`.

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
    Other(Participant),
}

impl Running {
    fn participant(&self) -> &Participant {
        match self {
            Running::Source(s) => s,
            Running::Other(p) => p,
        }
    }
    fn stop(&mut self) {
        match self {
            Running::Source(s) => s.stop(),
            Running::Other(p) => p.stop(),
        }
    }
}

struct Adapter {
    running: Option<Running>,
    told: Told,
}

fn text<'a>(c: &'a Value, key: &str) -> Result<&'a str, String> {
    c[key].as_str().ok_or_else(|| format!("missing {}", key))
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
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
    }

    fn handle(&mut self, c: &Value) -> Result<Map<String, Value>, String> {
        let cmd = c["cmd"].as_str().unwrap_or("");
        let dir = || text(c, "dir").map(PathBuf::from);
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
            if let Running::Source(s) = r {
                out["checked"] = json!(s.checked());
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
