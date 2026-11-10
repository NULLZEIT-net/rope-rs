//! Recipient: a participant for messages only, no streams (spec 7, 8.5) – e.g. a tablet that receives tasks.

use crate::frames::Frame;
use crate::participant::{Options, Participant, Role};
use crate::session::{Conn, End};
use std::collections::HashMap;
use std::io;
use std::path::Path;

struct RecipientRole;

impl Role for RecipientRole {
    fn role(&self) -> &'static str {
        "recipient"
    }
    fn start_session(&mut self, _have: HashMap<(String, u64), u64>) -> Result<(), End> {
        Ok(())
    }
    fn handle(&mut self, _f: &Frame, _c: &mut Conn) -> Result<bool, End> {
        Ok(false)
    }
    fn work(&mut self, _c: &mut Conn) -> Result<bool, End> {
        Ok(false)
    }
}

/// Starts a recipient; `state_dir` holds its outbox and inbox. Messages arrive through `opts.on_message` and in
/// [`Participant::inbox`].
pub fn start(
    participant: &str,
    operation: &str,
    hub: &str,
    state_dir: &Path,
    opts: Options,
) -> io::Result<Participant> {
    Participant::spawn(
        participant,
        operation,
        hub,
        state_dir,
        opts,
        Box::new(RecipientRole),
    )
}
