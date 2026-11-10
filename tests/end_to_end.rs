//! Rust only, end to end: a source sends a growing file through the hub to a consumer, and a recipient sends a
//! message to the consumer and to the hub itself – bit for bit, verified, every message exactly once, with the
//! delivery report back at the sender. (The conformance suite tests each role against the Python reference.)

use rope_protocol::consumer::Consumer;
use rope_protocol::hub::{Hub, HubOptions};
use rope_protocol::mailbox::OnMessage;
use rope_protocol::participant::Options;
use rope_protocol::recipient;
use rope_protocol::source::Source;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn temp_dir() -> PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let d = std::env::temp_dir().join(format!("rope-rs-e2e-{}-{}", std::process::id(), n));
    fs::create_dir_all(&d).unwrap();
    d
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(30);
    while !cond() {
        assert!(Instant::now() < end, "timed out: {}", what);
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn fast(on_message: Option<OnMessage>) -> Options {
    Options {
        segment_bytes: 64 * 1024,
        poll: Duration::from_millis(20),
        timeout: Duration::from_secs(1),
        backoff: (Duration::from_millis(20), Duration::from_millis(200)),
        on_message,
        ..Options::default()
    }
}

/// (message id, body) of every message an application was told, in order.
type Told = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

fn counter() -> (Told, OnMessage) {
    let got = Arc::new(Mutex::new(Vec::new()));
    let g = got.clone();
    let cb: OnMessage = Arc::new(move |meta, body| {
        g.lock()
            .unwrap()
            .push((meta["id"].as_str().unwrap_or("").to_string(), body.to_vec()));
    });
    (got, cb)
}

#[test]
fn source_hub_consumer_and_messages_all_in_rust() {
    let dir = temp_dir();
    let data: Vec<u8> = (0..700_000u32).map(|i| (i * 7 % 251) as u8).collect();
    let file = dir.join("rec-0001.dat");
    fs::write(&file, &data[..300_000]).unwrap();

    let participants: BTreeMap<String, String> = [
        ("source-1", "source"),
        ("c-1", "consumer"),
        ("r-1", "recipient"),
    ]
    .iter()
    .map(|(p, r)| (p.to_string(), r.to_string()))
    .collect();
    let (at_hub, hub_cb) = counter();
    let opts = HubOptions {
        port: 0,
        heartbeat: Duration::from_millis(200),
        timeout: Duration::from_secs(1),
        segment_bytes: 64 * 1024,
        on_message: Some(hub_cb),
        ..HubOptions::default()
    };
    let mut hub = Hub::start(&dir.join("hub"), participants, "op-1", opts).unwrap();
    let addr = format!("127.0.0.1:{}", hub.port());

    let mut source = Source::start(
        "source-1",
        "op-1",
        &addr,
        vec![("rec-0001.dat".into(), file.clone())],
        &dir.join("src"),
        fast(None),
    )
    .unwrap();
    let (at_consumer, consumer_cb) = counter();
    let mut consumer = Consumer::start(
        "c-1",
        "op-1",
        &addr,
        &dir.join("c"),
        fast(Some(consumer_cb)),
    )
    .unwrap();
    let mut sender = recipient::start("r-1", "op-1", &addr, &dir.join("r"), fast(None)).unwrap();

    // the file grows while it is sent, then it is complete
    wait_for("first part at the consumer", || {
        consumer.streams().iter().any(|s| s.committed > 0)
    });
    fs::write(&file, &data).unwrap();
    source.complete("rec-0001.dat");
    wait_for("final check at the source", || {
        source.checked().get("rec-0001.dat").map(String::as_str) == Some("ok")
    });
    wait_for("verified at the consumer", || {
        consumer
            .streams()
            .iter()
            .any(|s| s.stream == "source-1/rec-0001.dat" && s.verified.as_deref() == Some("ok"))
    });
    let got = consumer
        .streams()
        .into_iter()
        .find(|s| s.stream == "source-1/rec-0001.dat")
        .unwrap();
    assert_eq!(fs::read(&got.path).unwrap(), data);

    // a message to the consumer and to the hub itself; the report comes back to the sender
    let id = sender
        .send_message(&["c-1".into(), "hub".into()], "note", b"check position 7")
        .unwrap();
    wait_for("message at the consumer", || {
        !at_consumer.lock().unwrap().is_empty()
    });
    wait_for("message at the hub", || !at_hub.lock().unwrap().is_empty());
    wait_for("delivery report", || {
        sender.outbox().iter().any(|m| {
            m.message.id == id
                && m.report
                    .as_ref()
                    .is_some_and(|r| r["pending"].as_array().is_some_and(|p| p.is_empty()))
        })
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        *at_consumer.lock().unwrap(),
        vec![(id.clone(), b"check position 7".to_vec())]
    );
    assert_eq!(
        *at_hub.lock().unwrap(),
        vec![(id.clone(), b"check position 7".to_vec())]
    );
    let report = sender
        .outbox()
        .into_iter()
        .find(|m| m.message.id == id)
        .unwrap()
        .report
        .unwrap();
    let mut delivered: Vec<&str> = report["delivered"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    delivered.sort();
    assert_eq!(delivered, vec!["c-1", "hub"]);

    sender.stop();
    consumer.stop();
    source.stop();
    hub.stop();
    let _ = fs::remove_dir_all(dir);
}
