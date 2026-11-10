//! Consumer: receives the streams the hub relays (spec 7, 8.3, 8.4), named `<source>/<stream>`, and messages.
//! Stores durably before acknowledging, checks every completed stream, asks for a block repair on a mismatch.

use crate::frames::Frame;
use crate::participant::{Options, Participant, Role};
use crate::receiver::{Store, StreamInfo};
use crate::session::{Conn, End};
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A running consumer. Messages, connection state and `stop` come from [`Participant`] (via `Deref`).
pub struct Consumer {
    store: Arc<Mutex<Store>>,
    participant: Participant,
}

impl Deref for Consumer {
    type Target = Participant;
    fn deref(&self) -> &Participant {
        &self.participant
    }
}

impl DerefMut for Consumer {
    fn deref_mut(&mut self) -> &mut Participant {
        &mut self.participant
    }
}

impl Consumer {
    /// Starts a consumer; `state_dir` holds the received streams and the mailbox.
    pub fn start(
        participant: &str,
        operation: &str,
        hub: &str,
        state_dir: &Path,
        opts: Options,
    ) -> io::Result<Consumer> {
        let store = Arc::new(Mutex::new(Store::open(state_dir)?));
        let role = ConsumerRole {
            store: store.clone(),
        };
        let participant =
            Participant::spawn(participant, operation, hub, state_dir, opts, Box::new(role))?;
        Ok(Consumer { store, participant })
    }

    /// Everything received so far, by stream and generation.
    pub fn streams(&self) -> Vec<StreamInfo> {
        self.store.lock().unwrap().streams()
    }
}

struct ConsumerRole {
    store: Arc<Mutex<Store>>,
}

impl Role for ConsumerRole {
    fn role(&self) -> &'static str {
        "consumer"
    }
    fn have(&mut self) -> Vec<Value> {
        self.store.lock().unwrap().have()
    }
    fn start_session(&mut self, _have: HashMap<(String, u64), u64>) -> Result<(), End> {
        Ok(())
    }
    fn handle(&mut self, f: &Frame, c: &mut Conn) -> Result<bool, End> {
        self.store.lock().unwrap().handle(f, c)
    }
    fn work(&mut self, _c: &mut Conn) -> Result<bool, End> {
        Ok(false)
    }
}
