//! Receiving side of streams (spec 8.3, 8.4, 14.7): store durably, then acknowledge; final check; block sums for the
//! repair. Used by the consumer (from the hub); later also by a hub (from sources).
//!
//! Layout below the store folder: `streams/<key>/data` (the received bytes) and `streams/<key>/state.json`
//! (stream, gen, committed, complete, verified). The key is derived from the stream name and generation – a stream
//! name is never used as a path (spec 14.4).

use crate::frames::{self, kind, Frame, FrameError};
use crate::media::{BundleWriter, ImportReport, Skipped};
use crate::meta;
use crate::session::{Conn, End};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Block size and sums per frame for the repair after a mismatch (spec 14.7, 14.8).
pub const BLOCK_SIZE: u64 = 1024 * 1024;
pub const SUMS_PER_FRAME: usize = 512;

/// A stored stream as the application sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamInfo {
    pub stream: String,
    pub gen: u64,
    pub committed: u64,
    pub complete: bool,
    /// `None` before the final check, then "ok" or "mismatch".
    pub verified: Option<String>,
    /// File holding the received bytes (the first `committed` of them count).
    pub path: PathBuf,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{:02x}", x)).collect()
}

fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

/// One generation of one stream at the receiver.
struct Received {
    folder: PathBuf,
    info: StreamInfo,
    /// SHA-256 of the committed bytes, kept up to date while appending; `None` after a repair (recomputed when needed).
    sha: Option<Sha256>,
}

impl Received {
    fn open(folder: PathBuf, stream: &str, gen: u64) -> io::Result<Received> {
        fs::create_dir_all(&folder)?;
        let state: Value = match fs::read(folder.join("state.json")) {
            Ok(raw) => serde_json::from_slice(&raw)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Value::Null,
            Err(e) => return Err(e),
        };
        let path = folder.join("data");
        let committed = state["committed"].as_u64().unwrap_or(0);
        // write access, not append: Windows refuses to shorten a file opened for appending only
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        if f.metadata()?.len() != committed {
            // cut off anything written but never committed (a crash between writing and the state update)
            f.set_len(committed)?;
            f.sync_all()?;
        }
        let info = StreamInfo {
            stream: stream.into(),
            gen,
            committed,
            complete: state["complete"].as_bool().unwrap_or(false),
            verified: state["verified"].as_str().map(String::from),
            path,
        };
        // a new stream hashes while it grows; a reopened one hashes its file once, at the final check
        let sha = if committed == 0 {
            Some(Sha256::new())
        } else {
            None
        };
        let r = Received { folder, info, sha };
        if state.is_null() {
            r.save()?;
        }
        Ok(r)
    }

    fn save(&self) -> io::Result<()> {
        let i = &self.info;
        let state = json!({"stream": i.stream, "gen": i.gen, "committed": i.committed, "complete": i.complete,
                           "verified": i.verified});
        write_atomic(
            &self.folder.join("state.json"),
            &serde_json::to_vec(&state)?,
        )
    }

    /// Stores `data` at `off` durably and returns the new committed offset, or `Err(Ok(committed))` for a gap.
    /// Bytes already stored are not written again – unless `repair`: then the part below the committed offset
    /// overwrites them (spec 14.7). A verified stream takes nothing any more.
    fn write(
        &mut self,
        off: u64,
        data: &[u8],
        repair: bool,
    ) -> Result<u64, Result<u64, io::Error>> {
        let committed = self.info.committed;
        if off > committed {
            return Err(Ok(committed));
        }
        if self.info.complete {
            return Ok(committed);
        }
        let end = off + data.len() as u64;
        let io = |e: io::Error| Err(e);
        if repair && off < committed {
            let n = (committed - off) as usize;
            let mut f = OpenOptions::new()
                .write(true)
                .open(&self.info.path)
                .map_err(io)?;
            f.seek(SeekFrom::Start(off)).map_err(io)?;
            f.write_all(&data[..n.min(data.len())]).map_err(io)?;
            f.sync_all().map_err(io)?;
            self.sha = None;
        }
        if end <= committed {
            return Ok(committed);
        }
        let tail = &data[(committed - off) as usize..];
        let mut f = OpenOptions::new()
            .write(true)
            .open(&self.info.path)
            .map_err(io)?;
        f.seek(SeekFrom::Start(committed)).map_err(io)?;
        f.write_all(tail).map_err(io)?;
        f.sync_all().map_err(io)?;
        if let Some(h) = self.sha.as_mut() {
            h.update(tail);
        }
        self.info.committed = end;
        self.save().map_err(io)?;
        Ok(end)
    }

    fn digest(&mut self) -> io::Result<String> {
        if self.sha.is_none() {
            let mut h = Sha256::new();
            let mut r = File::open(&self.info.path)?.take(self.info.committed);
            let mut buf = vec![0u8; 1024 * 1024];
            loop {
                let n = r.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                h.update(&buf[..n]);
            }
            self.sha = Some(h);
        }
        Ok(hex(&self.sha.clone().unwrap().finalize()))
    }

    /// Final check (spec 8.4): "ok" or "mismatch", recorded. The bytes are kept either way.
    fn finish(&mut self, size: u64, sha256: &str) -> io::Result<String> {
        let ok = size == self.info.committed && self.digest()? == sha256;
        self.info.complete = ok;
        self.info.verified = Some(if ok { "ok" } else { "mismatch" }.into());
        self.save()?;
        Ok(self.info.verified.clone().unwrap())
    }

    /// SHA-256 of every block of the stored data (the last one may be shorter).
    fn block_sums(&self, block: u64) -> io::Result<Vec<String>> {
        let mut f = File::open(&self.info.path)?;
        let mut sums = Vec::new();
        let mut left = self.info.committed;
        let mut buf = vec![0u8; block as usize];
        while left > 0 {
            let n = left.min(block) as usize;
            f.read_exact(&mut buf[..n])?;
            sums.push(hex(&Sha256::digest(&buf[..n])));
            left -= n as u64;
        }
        Ok(sums)
    }
}

/// All streams received from one sender.
pub struct Store {
    folder: PathBuf,
    open: BTreeMap<(String, u64), Received>,
}

fn key(stream: &str, gen: u64) -> String {
    let h = Sha256::digest(stream.as_bytes());
    format!("{}-g{}", &hex(&h)[..24], gen)
}

impl Store {
    /// Opens the store below `root` and every stream already in it.
    pub fn open(root: &Path) -> io::Result<Store> {
        let folder = root.join("streams");
        fs::create_dir_all(&folder)?;
        let mut open = BTreeMap::new();
        for entry in fs::read_dir(&folder)? {
            let dir = entry?.path();
            let Ok(raw) = fs::read(dir.join("state.json")) else {
                continue;
            };
            let state: Value = serde_json::from_slice(&raw)?;
            let (Some(stream), Some(gen)) = (state["stream"].as_str(), state["gen"].as_u64())
            else {
                continue;
            };
            open.insert((stream.to_string(), gen), Received::open(dir, stream, gen)?);
        }
        Ok(Store { folder, open })
    }

    fn get(&mut self, stream: &str, gen: u64) -> io::Result<&mut Received> {
        let k = (stream.to_string(), gen);
        if !self.open.contains_key(&k) {
            let r = Received::open(self.folder.join(key(stream, gen)), stream, gen)?;
            self.open.insert(k.clone(), r);
        }
        Ok(self.open.get_mut(&k).unwrap())
    }

    /// Everything stored, ordered by stream and generation.
    pub fn streams(&self) -> Vec<StreamInfo> {
        self.open.values().map(|r| r.info.clone()).collect()
    }

    /// `have` for the greeting: what is stored (spec 14.5).
    pub fn have(&self) -> Vec<Value> {
        self.open.values().map(|r| json!({"stream": r.info.stream, "gen": r.info.gen, "committed": r.info.committed})).collect()
    }

    /// SEGMENT or STREAM_END from the sender: store and acknowledge, or check. False for other frames.
    pub fn handle(&mut self, f: &Frame, c: &mut Conn) -> Result<bool, End> {
        match f.ftype {
            kind::SEGMENT => {
                let data = frames::payload(f)?; // damaged: ERROR, connection closed
                let (stream, gen, off) = (f.str("stream").to_string(), f.u64("gen"), f.u64("off"));
                let repair = f.meta.get("repair").and_then(Value::as_bool) == Some(true);
                let r = self.get(&stream, gen).map_err(storage)?;
                match r.write(off, &data, repair) {
                    Ok(committed) => {
                        c.send(&Frame::new(
                            kind::ACK,
                            meta! {"stream" => stream, "gen" => gen, "committed" => committed},
                        ))?;
                    }
                    Err(Ok(committed)) => {
                        // a gap: ask for exactly what is missing (spec 8.3)
                        let len = off + data.len() as u64 - committed;
                        c.send(&Frame::new(
                            kind::RESEND,
                            meta! {"stream" => stream, "gen" => gen, "off" => committed, "len" => len},
                        ))?;
                    }
                    Err(Err(e)) => return Err(storage(e)),
                }
            }
            kind::STREAM_END => {
                let (stream, gen) = (f.str("stream").to_string(), f.u64("gen"));
                let r = self.get(&stream, gen).map_err(storage)?;
                let result = r.finish(f.u64("size"), f.str("sha256")).map_err(storage)?;
                let size = r.info.committed;
                c.send(&Frame::new(
                    kind::STREAM_CHECK,
                    meta! {"stream" => stream, "gen" => gen, "result" => result, "size" => size},
                ))?;
                if result != "ok" {
                    // per-block checksums, so that the sender resends only the differing blocks (spec 14.7)
                    let sums = r.block_sums(BLOCK_SIZE).map_err(storage)?;
                    let mut first = 0;
                    loop {
                        let part = &sums[first..(first + SUMS_PER_FRAME).min(sums.len())];
                        c.send(&Frame::new(
                            kind::BLOCK_SUMS,
                            meta! {"stream" => stream, "gen" => gen, "block_size" => BLOCK_SIZE, "first" => first,
                                   "sums" => part},
                        ))?;
                        first += SUMS_PER_FRAME;
                        if first >= sums.len() {
                            break;
                        }
                    }
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

impl Store {
    // --- offline media (spec 8.6) ------------------------------------------------------------------------------

    /// Receipts for a bundle to the sender: `ACK` with what is held, and for every stream whose final check failed
    /// `STREAM_CHECK` `mismatch` with its `BLOCK_SUMS` (a repair request, as long as the check fails). False when the
    /// medium is full.
    pub(crate) fn media_receipts(&mut self, w: &mut BundleWriter) -> io::Result<bool> {
        for r in self.open.values() {
            let i = &r.info;
            if !w.add(&Frame::new(
                kind::ACK,
                meta! {"stream" => i.stream, "gen" => i.gen, "committed" => i.committed},
            ))? {
                return Ok(false);
            }
        }
        for r in self
            .open
            .values()
            .filter(|r| r.info.verified.as_deref() == Some("mismatch"))
        {
            let i = &r.info;
            let check = meta! {"stream" => i.stream, "gen" => i.gen, "result" => "mismatch", "size" => i.committed};
            if !w.add(&Frame::new(kind::STREAM_CHECK, check))? {
                return Ok(false);
            }
            let sums = r.block_sums(BLOCK_SIZE)?;
            let mut first = 0;
            loop {
                let part = &sums[first..(first + SUMS_PER_FRAME).min(sums.len())];
                let m = meta! {"stream" => i.stream, "gen" => i.gen, "block_size" => BLOCK_SIZE, "first" => first, "sums" => part};
                if !w.add(&Frame::new(kind::BLOCK_SUMS, m))? {
                    return Ok(false);
                }
                first += SUMS_PER_FRAME;
                if first >= sums.len() {
                    break;
                }
            }
            w.report.repair_requests += 1;
        }
        Ok(true)
    }

    /// SEGMENT or STREAM_END from a bundle, stored as from the network (spec 8.6): never beyond the committed offset
    /// (a gap skips the rest of that stream in this bundle), duplicates ignored, a verified stream never changed.
    /// False for other frames.
    pub(crate) fn media_frame(
        &mut self,
        f: &Frame,
        rep: &mut ImportReport,
        skipped: &mut Skipped,
    ) -> io::Result<bool> {
        let key = (f.str("stream").to_string(), f.u64("gen"));
        match f.ftype {
            kind::SEGMENT => {
                if skipped.contains(&key) {
                    return Ok(true);
                }
                let off = f.u64("off");
                let data = match frames::payload(f) {
                    Ok(d) => d,
                    Err(e) => {
                        rep.problems.push(format!(
                            "stream {} generation {} at byte {}: {} - skipped",
                            key.0, key.1, off, e.text
                        ));
                        skipped.insert(key);
                        return Ok(true);
                    }
                };
                let repair = f.meta.get("repair").and_then(Value::as_bool) == Some(true);
                let r = self.get(&key.0, key.1)?;
                let before = r.info.committed;
                match r.write(off, &data, repair) {
                    Ok(committed) => rep.new_bytes += committed - before,
                    Err(Ok(committed)) => {
                        rep.problems.push(format!(
                            "stream {} generation {}: bytes {} to {} are missing here; this bundle starts at byte {} - an \
                             earlier bundle or the network must bring them first",
                            key.0, key.1, committed, off, off
                        ));
                        skipped.insert(key);
                    }
                    Err(Err(e)) => return Err(e),
                }
            }
            kind::STREAM_END => {
                if skipped.contains(&key) {
                    return Ok(true);
                }
                let r = self.get(&key.0, key.1)?;
                if r.info.complete {
                    return Ok(true); // the bundle may be older than a repair over the network
                }
                let size = f.u64("size");
                if r.info.committed < size {
                    rep.problems.push(format!(
                        "stream {} generation {}: final check skipped, {} of {} bytes are here",
                        key.0, key.1, r.info.committed, size
                    ));
                    return Ok(true);
                }
                let result = r.finish(size, f.str("sha256"))?;
                if result != "ok" {
                    rep.problems.push(format!(
                        "stream {} generation {} differs from the sender's file; the next bundle asks the sender for \
                         the differing blocks (or the network repairs it, spec 14.7)",
                        key.0, key.1
                    ));
                }
                rep.checked.push((key.0, key.1, result));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}

/// A disk error: the receiver must not acknowledge (spec 14.10); ERROR `storage`, connection closed.
fn storage(e: io::Error) -> End {
    End::Bad(FrameError::new(
        "storage",
        format!("the receiver cannot store data: {}", e),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "rope-rs-test-{}-{}",
            name,
            crate::mailbox::new_id()
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn sha(data: &[u8]) -> String {
        hex(&Sha256::digest(data))
    }

    #[test]
    fn appends_ignores_duplicates_and_reports_gaps() {
        let dir = temp_dir("append");
        let mut r = Received::open(dir.join("s"), "source-1/a.dat", 1).unwrap();
        assert_eq!(r.write(0, b"hello ", false).unwrap(), 6);
        assert_eq!(r.write(0, b"HELLO ", false).unwrap(), 6); // a duplicate changes nothing
        assert_eq!(r.write(10, b"x", false).unwrap_err().unwrap(), 6); // a gap: the committed offset
        assert_eq!(r.write(3, b"lo world", false).unwrap(), 11); // overlapping: only the new tail
        assert_eq!(fs::read(&r.info.path).unwrap(), b"hello world");
        assert_eq!(r.finish(11, &sha(b"hello world")).unwrap(), "ok");
        assert_eq!(r.write(11, b"more", false).unwrap(), 11); // verified: takes nothing any more
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn repair_overwrites_below_the_committed_offset() {
        let dir = temp_dir("repair");
        let mut r = Received::open(dir.join("s"), "source-1/a.dat", 1).unwrap();
        r.write(0, b"0123456789", false).unwrap();
        assert_eq!(r.finish(10, &sha(b"AB23456789")).unwrap(), "mismatch");
        assert_eq!(
            r.block_sums(4).unwrap(),
            vec![sha(b"0123"), sha(b"4567"), sha(b"89")]
        );
        assert_eq!(r.write(0, b"AB", false).unwrap(), 10); // without repair: a duplicate, not written
        assert_eq!(fs::read(&r.info.path).unwrap(), b"0123456789");
        assert_eq!(r.write(0, b"AB", true).unwrap(), 10);
        assert_eq!(r.finish(10, &sha(b"AB23456789")).unwrap(), "ok");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reopening_cuts_uncommitted_bytes_and_keeps_the_state() {
        let dir = temp_dir("reopen");
        {
            let mut s = Store::open(&dir).unwrap();
            let r = s.get("source-1/a.dat", 2).unwrap();
            r.write(0, b"abc", false).unwrap();
            let mut f = OpenOptions::new().append(true).open(&r.info.path).unwrap();
            f.write_all(b"never committed").unwrap(); // a crash between writing and the state update
        }
        let s = Store::open(&dir).unwrap();
        let got = s.streams();
        assert_eq!(got.len(), 1);
        assert_eq!(
            (got[0].stream.as_str(), got[0].gen, got[0].committed),
            ("source-1/a.dat", 2, 3)
        );
        assert_eq!(fs::read(&got[0].path).unwrap(), b"abc");
        assert_eq!(
            s.have(),
            vec![json!({"stream": "source-1/a.dat", "gen": 2, "committed": 3})]
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
