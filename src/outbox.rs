//! The durable outbox: an append-only file written and fsynced before anything is sent.
//!
//! Lines: `E <checksum> <base64(json)>` an entry, `D <opId>` acknowledged. On open the
//! file is scanned: acknowledged entries drop, an entry whose checksum fails (or a torn
//! last line) is parked in `parked.log` and announced once, and the file is rewritten
//! with the pending entries only, so a bad entry is never retried and never shown twice.

use crate::error::{Error, Result};
use crate::link::open_private;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
    /// Client-produced bytes for `/append`.
    Append,
    /// An op envelope for the server-applied `/ops` path (no bytes).
    Ops,
    /// An op whose bytes are not derived yet: it is applied to the reloaded document and
    /// becomes an `Append` entry (after a parked predecessor, whose clocks later bytes depend on).
    Reapply,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub op_id: String,
    pub kind: EntryKind,
    pub op: Value,
    /// Sub-document name to update bytes (empty for `Ops`).
    pub updates: BTreeMap<String, Vec<u8>>,
}

/// A parked entry, announced once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parked {
    pub op_id: Option<String>,
    pub reason: String,
}

fn checksum(payload: &[u8]) -> String {
    Sha256::digest(payload)[..8]
        .iter()
        .fold(String::new(), |mut s, b| {
            s.push_str(&format!("{b:02x}"));
            s
        })
}

impl Entry {
    fn to_json(&self) -> Vec<u8> {
        let mut updates = Map::new();
        for (k, v) in &self.updates {
            updates.insert(k.clone(), Value::String(B64.encode(v)));
        }
        let kind = match self.kind {
            EntryKind::Append => "append",
            EntryKind::Ops => "ops",
            EntryKind::Reapply => "reapply",
        };
        serde_json::to_vec(
            &json!({"opId": self.op_id, "kind": kind, "op": self.op, "updates": updates}),
        )
        .expect("json")
    }

    fn from_json(bytes: &[u8]) -> Option<Self> {
        let v: Value = serde_json::from_slice(bytes).ok()?;
        let kind = match v.get("kind")?.as_str()? {
            "append" => EntryKind::Append,
            "ops" => EntryKind::Ops,
            "reapply" => EntryKind::Reapply,
            _ => return None,
        };
        let mut updates = BTreeMap::new();
        for (k, b) in v.get("updates")?.as_object()? {
            updates.insert(k.clone(), B64.decode(b.as_str()?).ok()?);
        }
        Some(Self {
            op_id: v.get("opId")?.as_str()?.to_owned(),
            kind,
            op: v.get("op")?.clone(),
            updates,
        })
    }
}

pub struct Outbox {
    path: PathBuf,
    parked_path: PathBuf,
    file: File,
    pending: Vec<Entry>,
    notices: Vec<Parked>,
    fail_rewrite: bool,
}

impl Outbox {
    pub fn open(dir: &Path) -> Result<Self> {
        crate::link::create_private_dir(dir)?;
        let path = dir.join("outbox.log");
        let parked_path = dir.join("parked.log");
        crate::link::tighten(&[&path, &parked_path]);
        // Only a missing file is an empty outbox: any other read error must not turn into one.
        let text = match std::fs::read(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let mut entries: Vec<Entry> = Vec::new();
        let mut done: BTreeSet<String> = BTreeSet::new();
        let mut notices = Vec::new();
        let complete = text.ends_with(b"\n") || text.is_empty();
        let lines: Vec<&[u8]> = text.split(|b| *b == b'\n').collect();
        let last = lines.len().saturating_sub(1);
        for (i, line) in lines.iter().enumerate() {
            if line.is_empty() {
                continue;
            }
            let torn = i == last && !complete;
            let s = String::from_utf8_lossy(line);
            let mut parts = s.splitn(3, '\t');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("D"), Some(id), None) if !torn => {
                    done.insert(id.to_owned());
                }
                (Some("E"), Some(sum), Some(b64)) if !torn => {
                    let entry = B64
                        .decode(b64)
                        .ok()
                        .filter(|p| checksum(p) == sum)
                        .and_then(|p| Entry::from_json(&p));
                    if let Some(e) = entry {
                        entries.push(e);
                    } else {
                        notices.push(Parked {
                            op_id: None,
                            reason: "bad checksum".into(),
                        });
                        park_line(&parked_path, "bad checksum", line)?;
                    }
                }
                _ => {
                    notices.push(Parked {
                        op_id: None,
                        reason: "torn or unreadable line".into(),
                    });
                    park_line(&parked_path, "unreadable", line)?;
                }
            }
        }
        entries.retain(|e| !done.contains(&e.op_id));
        let mut seen = BTreeSet::new();
        entries.retain(|e| seen.insert(e.op_id.clone()));
        let file = rewrite(&path, &entries)?;
        Ok(Self {
            path,
            parked_path,
            file,
            pending: entries,
            notices,
            fail_rewrite: false,
        })
    }

    /// Append an entry and fsync before returning: from here on the edit is committed.
    pub fn append(&mut self, entry: &Entry) -> Result<()> {
        let payload = entry.to_json();
        let line = format!("E\t{}\t{}\n", checksum(&payload), B64.encode(&payload));
        let before = self.file.metadata()?.len();
        let written = self
            .file
            .write_all(line.as_bytes())
            .and_then(|()| self.file.sync_all());
        if let Err(e) = written {
            // A partial line must not glue onto the next entry.
            let _ = self.file.set_len(before);
            return Err(e.into());
        }
        self.pending.push(entry.clone());
        Ok(())
    }

    /// Replace every pending entry (same order, possibly changed kind or bytes), atomically.
    pub fn replace_all(&mut self, entries: &[Entry]) -> Result<()> {
        if self.fail_rewrite {
            return Err(Error::Io("injected rewrite failure".into()));
        }
        self.file = rewrite(&self.path, entries)?;
        self.pending = entries.to_vec();
        Ok(())
    }

    /// The entry was acknowledged (2xx or duplicate): drop it.
    pub fn ack(&mut self, op_id: &str) -> Result<()> {
        self.pending.retain(|e| e.op_id != op_id);
        self.file.write_all(format!("D\t{op_id}\n").as_bytes())?;
        self.file.sync_all()?;
        if self.pending.is_empty() {
            self.file = rewrite(&self.path, &[])?;
        }
        Ok(())
    }

    /// The server refused the entry for good: move it to `parked.log`, announce once.
    pub fn park(&mut self, op_id: &str, reason: &str) -> Result<()> {
        if let Some(e) = self.pending.iter().find(|e| e.op_id == op_id) {
            let payload = e.to_json();
            park_line(&self.parked_path, reason, B64.encode(payload).as_bytes())?;
        }
        self.notices.push(Parked {
            op_id: Some(op_id.to_owned()),
            reason: reason.to_owned(),
        });
        self.ack(op_id)
    }

    /// The server refused `op_id` for good: record it in `parked.log`, then drop it from the
    /// outbox and replace the entries that follow it (`rest`, same order) in ONE atomic rewrite.
    /// A crash before the rewrite leaves the entry pending, so it is sent and parked again;
    /// there is no state in which the entry is gone but its dependants still carry their old bytes.
    pub fn park_replace(&mut self, op_id: &str, reason: &str, rest: &[Entry]) -> Result<()> {
        if let Some(e) = self.pending.iter().find(|e| e.op_id == op_id) {
            let payload = e.to_json();
            park_line(&self.parked_path, reason, B64.encode(payload).as_bytes())?;
        }
        self.replace_all(rest)?;
        self.notices.push(Parked {
            op_id: Some(op_id.to_owned()),
            reason: reason.to_owned(),
        });
        Ok(())
    }

    /// Test hook: make the next rewrites fail.
    #[doc(hidden)]
    pub fn inject_rewrite_failure(&mut self, fail: bool) {
        self.fail_rewrite = fail;
    }

    /// Test hook: make every following write fail with ENOSPC (`/dev/full`).
    #[doc(hidden)]
    pub fn inject_write_failure(&mut self) -> Result<()> {
        self.file = std::fs::OpenOptions::new().write(true).open("/dev/full")?;
        Ok(())
    }

    /// Test hook: reopen the real file after [`Outbox::inject_write_failure`].
    #[doc(hidden)]
    pub fn repair(&mut self) -> Result<()> {
        self.file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        Ok(())
    }

    #[must_use]
    pub fn pending(&self) -> &[Entry] {
        &self.pending
    }

    /// Parked entries since the last call: each is shown once.
    pub fn take_notices(&mut self) -> Vec<Parked> {
        std::mem::take(&mut self.notices)
    }
}

fn park_line(path: &Path, reason: &str, line: &[u8]) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    // Windows: no mode bits; the per-user profile ACL already applies.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(path)?;
    f.write_all(format!("{reason}\t{}\n", B64.encode(line)).as_bytes())?;
    f.sync_all()?;
    Ok(())
}

fn rewrite(path: &Path, entries: &[Entry]) -> Result<File> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = open_private(&tmp)?;
        for e in entries {
            let p = e.to_json();
            f.write_all(format!("E\t{}\t{}\n", checksum(&p), B64.encode(&p)).as_bytes())?;
        }
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    crate::link::sync_parent(path)?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let d = std::env::temp_dir().join(format!("cvdb-ob-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn entry(id: &str) -> Entry {
        let mut updates = BTreeMap::new();
        updates.insert("cv-profile".to_owned(), vec![1, 2, 3, id.len() as u8]);
        Entry {
            op_id: id.to_owned(),
            kind: EntryKind::Append,
            op: json!({"kind": "node.update", "opId": id}),
            updates,
        }
    }

    #[test]
    fn survives_reopen_and_ack_drops() {
        let d = tmp();
        let mut o = Outbox::open(&d).unwrap();
        o.append(&entry("aaaaaaaa")).unwrap();
        o.append(&entry("bbbbbbbb")).unwrap();
        drop(o);
        let mut o = Outbox::open(&d).unwrap();
        assert_eq!(o.pending().len(), 2);
        assert_eq!(o.pending()[0], entry("aaaaaaaa"));
        o.ack("aaaaaaaa").unwrap();
        drop(o);
        let o = Outbox::open(&d).unwrap();
        assert_eq!(o.pending(), &[entry("bbbbbbbb")]);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn bad_checksum_is_parked_once_and_never_retried() {
        let d = tmp();
        let mut o = Outbox::open(&d).unwrap();
        o.append(&entry("aaaaaaaa")).unwrap();
        o.append(&entry("bbbbbbbb")).unwrap();
        drop(o);
        let p = d.join("outbox.log");
        let mut text = std::fs::read_to_string(&p).unwrap();
        // flip one character of the first entry's checksum
        let at = text.find("E\t").unwrap() + 2;
        let c = if &text[at..=at] == "0" { "1" } else { "0" };
        text.replace_range(at..=at, c);
        std::fs::write(&p, text).unwrap();
        let mut o = Outbox::open(&d).unwrap();
        assert_eq!(o.pending(), &[entry("bbbbbbbb")]);
        assert_eq!(o.take_notices().len(), 1);
        assert!(o.take_notices().is_empty());
        drop(o);
        let mut o = Outbox::open(&d).unwrap();
        assert!(o.take_notices().is_empty(), "shown once only");
        assert_eq!(o.pending().len(), 1);
        assert!(d.join("parked.log").exists());
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn torn_last_line_is_parked() {
        let d = tmp();
        let mut o = Outbox::open(&d).unwrap();
        o.append(&entry("aaaaaaaa")).unwrap();
        drop(o);
        let p = d.join("outbox.log");
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(b"E\tdeadbeef\tAAAA").unwrap();
        drop(f);
        let mut o = Outbox::open(&d).unwrap();
        assert_eq!(o.pending().len(), 1);
        assert_eq!(o.take_notices().len(), 1);
        std::fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn park_moves_entry_out() {
        let d = tmp();
        let mut o = Outbox::open(&d).unwrap();
        o.append(&entry("aaaaaaaa")).unwrap();
        o.park("aaaaaaaa", "422").unwrap();
        assert!(o.pending().is_empty());
        assert_eq!(
            o.take_notices(),
            vec![Parked {
                op_id: Some("aaaaaaaa".into()),
                reason: "422".into()
            }]
        );
        drop(o);
        assert!(Outbox::open(&d).unwrap().pending().is_empty());
        std::fs::remove_dir_all(d).unwrap();
    }
}
