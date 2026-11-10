//! Conformance adapter (the command interface of `rope/conformance/README.md`): one JSON command per line on stdin,
//! one JSON answer per line on stdout, logging on stderr. Lets the ROPE conformance suite test this implementation:
//!
//! ```text
//! python ../rope/conformance/harness.py --adapter "target/release/rope-conformance-adapter" -k source
//! ```
//!
//! Implemented so far: the source role. Other commands answer `"ok": false`.

use rope_protocol::source::{Options, Source};
use serde_json::{json, Map, Value};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;

/// The harness uses short timeouts to stay quick (conformance/README.md).
fn options() -> Options {
    Options {
        segment_bytes: 64 * 1024,
        poll: Duration::from_millis(50),
        timeout: Duration::from_secs(1),
        backoff: (Duration::from_millis(50), Duration::from_millis(300)),
        ..Options::default()
    }
}

struct Adapter {
    source: Option<Source>,
}

fn text<'a>(c: &'a Value, key: &str) -> Result<&'a str, String> {
    c[key].as_str().ok_or_else(|| format!("missing {}", key))
}

impl Adapter {
    fn handle(&mut self, c: &Value) -> Result<Map<String, Value>, String> {
        let cmd = c["cmd"].as_str().unwrap_or("");
        match cmd {
            "start_source" => {
                if let Some(mut old) = self.source.take() {
                    old.stop();
                }
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
                    &PathBuf::from(text(c, "dir")?),
                    options(),
                )
                .map_err(|e| e.to_string())?;
                self.source = Some(s);
                Ok(Map::new())
            }
            "add_stream" => {
                let s = self.source.as_ref().ok_or("no source running")?;
                s.add_stream(text(c, "stream")?, PathBuf::from(text(c, "path")?));
                Ok(Map::new())
            }
            "complete" => {
                let s = self.source.as_ref().ok_or("no source running")?;
                if !s.complete(text(c, "stream")?) {
                    return Err("unknown stream".into());
                }
                Ok(Map::new())
            }
            "status" => Ok(self.status()),
            "stop" => {
                if let Some(mut s) = self.source.take() {
                    s.stop();
                }
                Ok(Map::new())
            }
            other => Err(format!("not implemented in rope-rs yet: {}", other)),
        }
    }

    fn status(&self) -> Map<String, Value> {
        let mut out = json!({"running": self.source.is_some(), "connected": false, "rejected": null, "checked": {},
                             "told": {}, "inbox": [], "streams": []});
        if let Some(s) = &self.source {
            out["connected"] = json!(s.is_connected());
            if let Some(r) = s.rejected() {
                out["rejected"] = json!({"code": r.code, "operation": r.operation});
            }
            out["checked"] = json!(s.checked());
        }
        match out {
            Value::Object(m) => m,
            _ => unreachable!(),
        }
    }
}

fn main() {
    let mut adapter = Adapter { source: None };
    let stdout = io::stdout();
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let answer = match serde_json::from_str::<Value>(line) {
            Ok(c) if c["cmd"] == "quit" => {
                if let Some(mut s) = adapter.source.take() {
                    s.stop();
                }
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
