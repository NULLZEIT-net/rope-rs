//! Offline media as a second path (spec 8.6, media format 1): segments and messages on removable media, e.g. a USB
//! stick.
//!
//! A **bundle** carries the frames the exporter would send over the network, in the wire format of spec 14. Import
//! follows the network rules: store durably, never beyond the committed offset, duplicates ignored, damage reported.
//!
//! Layout on the medium:
//!
//! ```text
//! rope/<operation>/<from>-to-<to>-<created ms>-<8 hex>/
//!     frames-0001.rope ...    frames, at most 1 GiB per file (FAT32 allows 4 GiB)
//!     bundle.json             written last: format, operation, from, to, created, files, frames, complete
//! ```
//!
//! A bundle is written below a name ending in `.partial` and renamed when complete and durable, so a medium pulled
//! out during the export never holds a half bundle under a valid name. Bundles are never deleted here.

use crate::frames::{self, kind, Frame};
use crate::mailbox::new_id;
use crate::now_ms;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Format of the bundle folder (not the wire format, which is the frames' version byte).
pub const FORMAT: u64 = 1;
pub const MEDIA_DIR: &str = "rope";
pub const MANIFEST: &str = "bundle.json";
pub const PARTIAL: &str = ".partial";
/// Raw bytes per segment on a medium: one frame per MiB keeps the frame count low.
pub const SEGMENT_BYTES: u64 = frames::MAX_BLOB as u64;
/// Bytes per frames file, below the 4 GiB file limit of FAT32.
pub const FILE_BYTES: u64 = 1024 * 1024 * 1024;

/// What an export wrote.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExportReport {
    pub path: PathBuf,
    pub bytes: u64,
    pub frames: u64,
    pub segments: u64,
    pub stream_bytes: u64,
    pub messages: u64,
    /// Streams for which this bundle asks the sender for a repair.
    pub repair_requests: u64,
    /// Bytes resent as repair.
    pub repair_bytes: u64,
    /// False if `max_bytes` stopped the export before everything fitted; the rest goes with the next bundle.
    pub complete: bool,
}

/// What an import did with one bundle. `problems` are plain-language sentences for the operator; empty means the
/// bundle was read completely and every item was accepted or was already there.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ImportReport {
    pub path: PathBuf,
    pub sender: Option<String>,
    pub frames: u64,
    /// Stream bytes stored for the first time.
    pub new_bytes: u64,
    /// (stream, gen, result) of the final checks done.
    pub checked: Vec<(String, u64, String)>,
    /// Ids of new messages taken into custody.
    pub messages: Vec<String>,
    /// (stream, gen, committed) the sender holds.
    pub acks: Vec<(String, u64, u64)>,
    pub problems: Vec<String>,
}

fn is_identifier(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
        && s != "."
        && s != ".."
}

fn invalid(text: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, text)
}

/// Writes one bundle: `add` frames (false when `max_bytes` would be exceeded), then `close`. Dropped without `close`,
/// the partial folder is removed.
pub struct BundleWriter {
    operation: String,
    sender: String,
    to: String,
    max_bytes: Option<u64>,
    created: u64,
    path: PathBuf,
    tmp: PathBuf,
    files: Vec<(String, u64, u64)>, // (name, size, frames)
    file: Option<File>,
    pub report: ExportReport,
    closed: bool,
}

impl BundleWriter {
    pub fn new(
        medium: &Path,
        operation: &str,
        sender: &str,
        to: &str,
        max_bytes: Option<u64>,
    ) -> io::Result<Self> {
        for (what, v) in [
            ("operation", operation),
            ("sender", sender),
            ("addressee", to),
        ] {
            if !is_identifier(v) {
                return Err(invalid(format!(
                    "{} {:?} is not a valid identifier (spec 14.4)",
                    what, v
                )));
            }
        }
        let created = now_ms();
        let name = format!("{}-to-{}-{}-{}", sender, to, created, &new_id()[..8]);
        let path = medium.join(MEDIA_DIR).join(operation).join(name);
        let tmp = PathBuf::from(format!("{}{}", path.display(), PARTIAL));
        fs::create_dir_all(&tmp)?;
        Ok(BundleWriter {
            operation: operation.into(),
            sender: sender.into(),
            to: to.into(),
            max_bytes,
            created,
            path,
            tmp,
            files: Vec::new(),
            file: None,
            report: ExportReport {
                complete: true,
                ..Default::default()
            },
            closed: false,
        })
    }

    pub fn add(&mut self, frame: &Frame) -> io::Result<bool> {
        let raw = frames::encode(frame).map_err(|e| invalid(e.to_string()))?;
        let n = raw.len() as u64;
        if self.max_bytes.is_some_and(|m| self.report.bytes + n > m) {
            self.report.complete = false;
            return Ok(false);
        }
        if self.file.is_none() || self.files.last().is_some_and(|f| f.1 + n > FILE_BYTES) {
            self.next_file()?;
        }
        self.file.as_mut().unwrap().write_all(&raw)?;
        let last = self.files.last_mut().unwrap();
        last.1 += n;
        last.2 += 1;
        self.report.bytes += n;
        self.report.frames += 1;
        Ok(true)
    }

    fn next_file(&mut self) -> io::Result<()> {
        self.close_file()?;
        let name = format!("frames-{:04}.rope", self.files.len() + 1);
        self.file = Some(File::create(self.tmp.join(&name))?);
        self.files.push((name, 0, 0));
        Ok(())
    }

    fn close_file(&mut self) -> io::Result<()> {
        if let Some(f) = self.file.take() {
            f.sync_all()?;
        }
        Ok(())
    }

    /// Makes the bundle durable and gives it its final name.
    pub fn close(mut self) -> io::Result<ExportReport> {
        self.close_file()?;
        let files: Vec<Value> = self
            .files
            .iter()
            .map(|(n, s, f)| json!({"name": n, "size": s, "frames": f}))
            .collect();
        let manifest = json!({"format": FORMAT, "operation": self.operation, "from": self.sender, "to": self.to,
                              "created": self.created, "files": files, "frames": self.report.frames,
                              "complete": self.report.complete});
        {
            let mut f = File::create(self.tmp.join(MANIFEST))?;
            f.write_all(&serde_json::to_vec(&manifest)?)?;
            f.sync_all()?;
        }
        fs::rename(&self.tmp, &self.path)?;
        self.closed = true;
        self.report.path = self.path.clone();
        Ok(self.report.clone())
    }
}

impl Drop for BundleWriter {
    fn drop(&mut self) {
        if !self.closed {
            self.file = None;
            let _ = fs::remove_dir_all(&self.tmp);
        }
    }
}

/// A bundle found on a medium: its folder and its manifest (None if it cannot be read).
pub type Found = (PathBuf, Option<Map<String, Value>>);

/// Complete bundles on `medium` for `to` in `operation`, oldest first: (path, manifest). Partial folders and bundles
/// for others are skipped; a bundle whose manifest cannot be read comes with `None`, so that the importer reports it.
pub fn find_bundles(medium: &Path, operation: &str, to: &str) -> io::Result<Vec<Found>> {
    let folder = medium.join(MEDIA_DIR).join(operation);
    if !folder.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&folder)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.ends_with(PARTIAL) || !path.join(MANIFEST).is_file() {
            continue;
        }
        let manifest =
            fs::read(path.join(MANIFEST))
                .ok()
                .and_then(|raw| match serde_json::from_slice(&raw) {
                    Ok(Value::Object(m)) => Some(m),
                    _ => None,
                });
        match manifest {
            None => out.push((path, None)),
            Some(m)
                if m.get("to").and_then(Value::as_str) == Some(to)
                    && m.get("operation").and_then(Value::as_str) == Some(operation) =>
            {
                out.push((path, Some(m)))
            }
            Some(_) => {}
        }
    }
    out.sort_by_key(|(p, m)| {
        (
            m.as_ref()
                .and_then(|m| m.get("created"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
            p.clone(),
        )
    });
    Ok(out)
}

/// Plain-language reason why the bundle cannot be imported, or None.
pub fn check_manifest(
    manifest: Option<&Map<String, Value>>,
    operation: &str,
    to: &str,
) -> Option<String> {
    let Some(m) = manifest else {
        return Some(format!(
            "the bundle's {} cannot be read (damaged medium?) - the bundle was skipped",
            MANIFEST
        ));
    };
    if m.get("format").and_then(Value::as_u64) != Some(FORMAT) {
        return Some(format!(
            "bundle format {} is not supported (this program reads format {})",
            m.get("format").unwrap_or(&Value::Null),
            FORMAT
        ));
    }
    if m.get("operation").and_then(Value::as_str) != Some(operation)
        || m.get("to").and_then(Value::as_str) != Some(to)
    {
        return Some("the bundle is for someone else".into());
    }
    if !m
        .get("from")
        .and_then(Value::as_str)
        .is_some_and(is_identifier)
    {
        return Some("the bundle names no valid sender".into());
    }
    let files_ok = m.get("files").and_then(Value::as_array).is_some_and(|fs| {
        fs.iter().all(|f| {
            f["name"].as_str().is_some_and(|n| {
                n.len() == "frames-0001.rope".len()
                    && n.starts_with("frames-")
                    && n.ends_with(".rope")
                    && n[7..11].bytes().all(|c| c.is_ascii_digit())
            })
        })
    });
    if !files_ok {
        return Some("the bundle has no valid list of frame files".into());
    }
    None
}

/// Reads the validated frames of a bundle in order, one at a time, and hands each to `take`. Damage goes into
/// `report.problems`: a frame with a wrong CRC is skipped (its length is known, so reading goes on); a damaged header
/// ends the reading of its file, because the position of the next frame is lost; a cut-off file is reported.
pub fn read_frames(
    path: &Path,
    manifest: &Map<String, Value>,
    report: &mut ImportReport,
    take: &mut dyn FnMut(Frame, &mut ImportReport) -> io::Result<()>,
) -> io::Result<()> {
    for entry in manifest["files"].as_array().into_iter().flatten() {
        let name = entry["name"].as_str().unwrap_or("");
        let mut f = match File::open(path.join(name)) {
            Ok(f) => io::BufReader::new(f),
            Err(e) => {
                report
                    .problems
                    .push(format!("{} cannot be read: {}", name, e));
                continue;
            }
        };
        let mut count = 0u64;
        let mut whole = true;
        loop {
            let mut head = [0u8; frames::HEADER_LEN];
            let n = read_full(&mut f, &mut head)?;
            if n == 0 {
                break;
            }
            count += 1;
            if n < head.len() {
                report.problems.push(format!(
                    "{} ends in the middle of frame {} (cut off)",
                    name, count
                ));
                whole = false;
                break;
            }
            let m = u32::from_be_bytes(head[4..8].try_into().unwrap()) as usize;
            let b = u32::from_be_bytes(head[8..12].try_into().unwrap()) as usize;
            if &head[..2] != frames::MAGIC || m > frames::MAX_META || b > frames::MAX_BLOB {
                report.problems.push(format!(
                    "{} is damaged at frame {}; the rest of this file was not read",
                    name, count
                ));
                whole = false;
                break;
            }
            let mut raw = head.to_vec();
            raw.resize(frames::HEADER_LEN + m + b + frames::TRAILER_LEN, 0);
            if read_full(&mut f, &mut raw[frames::HEADER_LEN..])? < m + b + frames::TRAILER_LEN {
                report.problems.push(format!(
                    "{} ends in the middle of frame {} (cut off)",
                    name, count
                ));
                whole = false;
                break;
            }
            match frames::decode(&raw).and_then(|fr| frames::validate(&fr).map(|_| fr)) {
                Ok(frame) => {
                    report.frames += 1;
                    take(frame, report)?;
                }
                Err(e) => report.problems.push(format!(
                    "{}, frame {} is damaged ({}) - skipped",
                    name, count, e.text
                )),
            }
        }
        if whole && Some(count) != entry["frames"].as_u64() {
            report.problems.push(format!(
                "{} holds {} frames, the bundle lists {}",
                name, count, entry["frames"]
            ));
        }
    }
    Ok(())
}

/// Reads until `buf` is full or the file ends; returns how many bytes were read.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

/// Collects STREAM_CHECK `mismatch` and BLOCK_SUMS of a bundle (a receiver asks for a repair, spec 8.6). True if `f`
/// was one of them.
pub fn collect_repair_frame(
    f: &Frame,
    requests: &mut BTreeMap<(String, u64), RepairRequest>,
) -> bool {
    let key = (f.str("stream").to_string(), f.u64("gen"));
    match f.ftype {
        kind::STREAM_CHECK => {
            if f.str("result") == "mismatch" {
                requests.insert(
                    key,
                    RepairRequest {
                        size: f.u64("size"),
                        block_size: 0,
                        sums: BTreeMap::new(),
                    },
                );
            }
            true
        }
        kind::BLOCK_SUMS => {
            if let Some(req) = requests.get_mut(&key) {
                req.block_size = f.u64("block_size");
                let first = f.u64("first");
                for (i, d) in f.meta["sums"].as_array().into_iter().flatten().enumerate() {
                    req.sums
                        .insert(first + i as u64, d.as_str().unwrap_or("").to_string());
                }
            }
            true
        }
        _ => false,
    }
}

/// A receiver's request for a block repair: its size and the SHA-256 of every block it holds.
#[derive(Clone, Debug, PartialEq)]
pub struct RepairRequest {
    pub size: u64,
    pub block_size: u64,
    pub sums: BTreeMap<u64, String>,
}

impl RepairRequest {
    /// All block sums, if every one of them arrived (a request cut off by damage is dropped; the receiver asks again).
    pub fn complete_sums(&self) -> Option<Vec<String>> {
        if self.block_size == 0 {
            return None;
        }
        (0..self.size.div_ceil(self.block_size))
            .map(|i| self.sums.get(&i).cloned())
            .collect()
    }
}

/// Keys of streams with a gap in the bundle being imported: their later segments are skipped as well.
pub type Skipped = HashSet<(String, u64)>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rope-rs-test-{}-{}", name, new_id()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn beat(t: u64) -> Frame {
        Frame::new(kind::HEARTBEAT, meta! {"t" => t})
    }

    fn read_all(medium: &Path) -> (ImportReport, Vec<u64>) {
        let (path, manifest) = find_bundles(medium, "op-1", "hub").unwrap().remove(0);
        let mut rep = ImportReport::default();
        let mut got = Vec::new();
        read_frames(&path, manifest.as_ref().unwrap(), &mut rep, &mut |f, _| {
            got.push(f.u64("t"));
            Ok(())
        })
        .unwrap();
        (rep, got)
    }

    #[test]
    fn a_bundle_round_trips_and_respects_max_bytes() {
        let medium = temp_dir("bundle");
        let mut w = BundleWriter::new(&medium, "op-1", "source-1", "hub", Some(50)).unwrap();
        assert!(w.add(&beat(1)).unwrap());
        assert!(w.add(&beat(2)).unwrap());
        assert!(!w.add(&beat(3)).unwrap()); // a heartbeat frame is 23 bytes: the third does not fit in 50
        let rep = w.close().unwrap();
        assert!(!rep.complete && rep.frames == 2);
        let (path, manifest) = &find_bundles(&medium, "op-1", "hub").unwrap()[0];
        assert!(check_manifest(manifest.as_ref(), "op-1", "hub").is_none());
        assert!(check_manifest(manifest.as_ref(), "op-1", "c-1").is_some());
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("source-1-to-hub-"));
        let (rep, got) = read_all(&medium);
        assert_eq!((rep.problems.len(), got), (0, vec![1, 2]));
        assert!(find_bundles(&medium, "op-1", "c-1").unwrap().is_empty());
        fs::remove_dir_all(medium).unwrap();
    }

    #[test]
    fn an_unfinished_bundle_leaves_nothing_behind() {
        let medium = temp_dir("partial");
        {
            let mut w = BundleWriter::new(&medium, "op-1", "source-1", "hub", None).unwrap();
            w.add(&beat(1)).unwrap();
        } // dropped without close: e.g. an error during the export
        assert_eq!(
            fs::read_dir(medium.join(MEDIA_DIR).join("op-1"))
                .unwrap()
                .count(),
            0
        );
        fs::remove_dir_all(medium).unwrap();
    }

    #[test]
    fn a_damaged_frame_is_skipped_and_reading_goes_on() {
        let medium = temp_dir("damage");
        let mut w = BundleWriter::new(&medium, "op-1", "source-1", "hub", None).unwrap();
        for t in 1..=3 {
            w.add(&beat(t)).unwrap();
        }
        let rep = w.close().unwrap();
        let file = rep.path.join("frames-0001.rope");
        let mut raw = fs::read(&file).unwrap();
        let len = raw.len() / 3;
        raw[len + 15] ^= 0xFF; // inside the meta of the second frame: CRC mismatch, length still known
        fs::write(&file, &raw).unwrap();
        let (rep, got) = read_all(&medium);
        assert_eq!(got, vec![1, 3]);
        assert_eq!(rep.problems.len(), 1); // the damaged frame (it is counted, so the manifest count still fits)
        raw.truncate(raw.len() - 5); // and cut off in the last frame
        fs::write(&file, &raw).unwrap();
        let (rep, got) = read_all(&medium);
        assert_eq!(got, vec![1]);
        assert!(rep.problems.iter().any(|p| p.contains("cut off")));
        fs::remove_dir_all(medium).unwrap();
    }
}
