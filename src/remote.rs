//! The byte transport to a `CareerVector` light server: framed `GET /sub` (head plus tail) and
//! `POST /append` (the client's update bytes plus the typed op envelope, numbered by the server).
//!
//! It moves opaque bytes. It never decodes Yjs, never applies an op and never merges; the op
//! envelope is JSON the caller built, forwarded as it came. Appends go through the durable
//! [`Outbox`] first, so an edit made offline survives a restart and replays in order.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::{Map, Value, json};

use crate::error::{Error, Result};
use crate::frames;
use crate::http::{Counts, Http, Kind, Response};
use crate::outbox::{Entry, EntryKind, Outbox};

/// One sub-document as the server holds it: the head and the tail of updates after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched {
    /// The head state, first frame of the response.
    pub head: Vec<u8>,
    /// The updates after the head, oldest first; the first one has clock `head_clock + 1`.
    pub tail: Vec<Vec<u8>>,
    /// The clock of the last update (`x-cv-clock`).
    pub clock: u64,
    pub etag: String,
}

impl Fetched {
    /// The clock the head state stands at.
    #[must_use]
    pub fn head_clock(&self) -> u64 {
        self.clock - self.tail.len() as u64
    }
}

/// What the server answered for an appended update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Appended {
    pub sub_doc: String,
    pub clock: u64,
    /// False for a duplicate or no-op: the server already had it.
    pub written: bool,
}

/// What an append came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The server numbered the update.
    Done(Vec<Appended>),
    /// The server was not reachable; the entry waits in the outbox.
    Queued,
}

/// The result of replaying the outbox.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Flushed {
    pub acked: usize,
    pub parked: usize,
    pub left: usize,
}

/// A light-server workspace, reached over [`Http`].
pub struct Remote {
    http: Arc<dyn Http>,
    counts: Counts,
    base: String,
    outbox: Mutex<Outbox>,
}

fn api_error(resp: &Response) -> Option<String> {
    let v: Value = serde_json::from_slice(&resp.body).ok()?;
    v.get("error").and_then(Value::as_str).map(str::to_owned)
}

fn text(resp: &Response) -> String {
    String::from_utf8_lossy(&resp.body).into_owned()
}

fn appended(v: &Value) -> Option<Vec<Appended>> {
    let one = |e: &Value| {
        Some(Appended {
            sub_doc: e.get("subDoc")?.as_str()?.to_owned(),
            clock: e.get("clock")?.as_u64()?,
            written: e.get("written").and_then(Value::as_bool).unwrap_or(true),
        })
    };
    match v.get("results").and_then(Value::as_array) {
        Some(list) => list.iter().map(one).collect(),
        None => Some(vec![one(v)?]),
    }
}

impl Remote {
    /// `api_base` is the API root (`https://host/api`), `ws_id` the workspace, `dir` holds the
    /// durable outbox.
    pub fn new(http: Arc<dyn Http>, api_base: &str, ws_id: &str, dir: &Path) -> Result<Self> {
        Ok(Self {
            http,
            counts: Counts::default(),
            base: format!("{}/workspaces/{ws_id}", api_base.trim_end_matches('/')),
            outbox: Mutex::new(Outbox::open(dir)?),
        })
    }

    /// Request counters, for budget checks.
    #[must_use]
    pub fn counts(&self) -> Counts {
        self.counts.clone()
    }

    fn call(
        &self,
        kind: Kind,
        method: &str,
        tail: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        self.counts.bump(kind);
        self.http
            .request(method, &format!("{}{tail}", self.base), headers, body)
    }

    /// Fetch one sub-document. With `etag` of a previous fetch, `None` means unchanged (304).
    pub fn fetch(&self, sub: &str, etag: Option<&str>) -> Result<Option<Fetched>> {
        let mut headers = Vec::new();
        if let Some(e) = etag {
            headers.push(("If-None-Match", e));
        }
        let resp = self.call(Kind::Sub, "GET", &format!("/sub/{sub}"), &headers, None)?;
        match resp.status {
            304 => Ok(None),
            200 => {
                let clock = resp
                    .header("x-cv-clock")
                    .and_then(|c| c.parse::<u64>().ok())
                    .ok_or_else(|| Error::Protocol("missing x-cv-clock".into()))?;
                let mut frames: Vec<Vec<u8>> = match resp.header("x-cv-update-frames") {
                    Some(n) => {
                        let n: usize = n
                            .parse()
                            .map_err(|_| Error::Protocol("frame count".into()))?;
                        frames::split_frames(&resp.body, n)?
                            .into_iter()
                            .map(<[u8]>::to_vec)
                            .collect()
                    }
                    None => vec![resp.body.clone()],
                };
                let tail = frames.split_off(1);
                if tail.len() as u64 > clock {
                    return Err(Error::Protocol("more updates than the clock".into()));
                }
                Ok(Some(Fetched {
                    head: frames.pop().unwrap_or_default(),
                    tail,
                    clock,
                    etag: resp.header("etag").unwrap_or_default().to_owned(),
                }))
            }
            403 => Err(Error::ReadOnlyMirror),
            s => Err(Error::Server(s)),
        }
    }

    /// Append client-produced update bytes with the typed op envelope `op` (JSON the caller built;
    /// `actorClass` is always human). `expected` is the clock the caller read each sub-document at:
    /// when the server is ahead, the append is refused with [`Error::Conflict`] before it is sent;
    /// a writer that races in between is refused by the server the same way. A refused or lost
    /// append is never retried by itself: the bytes depend on the clocks they were made at.
    pub fn append(
        &self,
        op_id: &str,
        op: &Value,
        updates: &BTreeMap<String, Vec<u8>>,
        expected: &BTreeMap<String, u64>,
    ) -> Result<Outcome> {
        let entry = Entry {
            op_id: op_id.into(),
            kind: EntryKind::Append,
            op: op.clone(),
            updates: updates.clone(),
        };
        self.outbox
            .lock()
            .map_err(|_| Error::EngineStopped)?
            .append(&entry)?;
        for (sub, clock) in expected {
            match self.fetch(sub, None) {
                Ok(Some(f)) if f.clock != *clock => {
                    self.park(op_id, "conflict: the server clock moved")?;
                    return Err(Error::Conflict);
                }
                Err(Error::Offline) => return Ok(Outcome::Queued),
                Err(e) => return Err(e),
                Ok(_) => {}
            }
        }
        match self.send(&entry)? {
            Sent::Done(v) => Ok(Outcome::Done(v)),
            Sent::Stay => Ok(Outcome::Queued),
            Sent::Refused(Error::OpRejected(m)) if m.starts_with("409") => Err(Error::Conflict),
            Sent::Refused(e) => Err(e),
        }
    }

    fn park(&self, op_id: &str, reason: &str) -> Result<()> {
        self.outbox
            .lock()
            .map_err(|_| Error::EngineStopped)?
            .park(op_id, reason)
    }

    fn send(&self, entry: &Entry) -> Result<Sent> {
        let mut updates = Map::new();
        for (sub, bytes) in &entry.updates {
            updates.insert(sub.clone(), Value::String(B64.encode(bytes)));
        }
        let body = serde_json::to_vec(&json!({
            "op": entry.op,
            "opId": entry.op_id,
            "actorClass": "human",
            "updates": updates,
        }))
        .map_err(|e| Error::Protocol(e.to_string()))?;
        let resp = match self.call(
            Kind::Append,
            "POST",
            "/append",
            &[("Content-Type", "application/json")],
            Some(&body),
        ) {
            Ok(r) => r,
            Err(Error::Offline) => return Ok(Sent::Stay),
            Err(e) => return Err(e),
        };
        let refuse = |reason: String, err: Error| -> Result<Sent> {
            self.park(&entry.op_id, &reason.chars().take(160).collect::<String>())?;
            Ok(Sent::Refused(err))
        };
        match resp.status {
            200..=299 => {
                let v: Value = serde_json::from_slice(&resp.body)
                    .map_err(|_| Error::Protocol("append answer".into()))?;
                let done = appended(&v).ok_or_else(|| Error::Protocol("append answer".into()))?;
                self.outbox
                    .lock()
                    .map_err(|_| Error::EngineStopped)?
                    .ack(&entry.op_id)?;
                Ok(Sent::Done(done))
            }
            409 if text(&resp).contains("commit_conflict") => refuse(
                "409: commit_conflict".into(),
                Error::OpRejected("409: commit_conflict".into()),
            ),
            409 if text(&resp).contains("not_on_append_path") => {
                refuse("409: not_on_append_path".into(), Error::NotOnAppendPath)
            }
            403 => refuse("403".into(), Error::ReadOnlyMirror),
            400 | 422 if api_error(&resp).is_some() => {
                let reason = format!("{}: {}", resp.status, api_error(&resp).unwrap_or_default());
                refuse(reason.clone(), Error::OpRejected(reason))
            }
            // 426 (outdated client), edge pages and everything else say nothing about the op.
            _ => Ok(Sent::Stay),
        }
    }

    /// Entries still waiting in the outbox.
    pub fn queued(&self) -> usize {
        self.outbox.lock().map_or(0, |o| o.pending().len())
    }

    /// Replay the outbox in order; stops at the first entry the server cannot take yet.
    pub fn flush(&self) -> Result<Flushed> {
        let pending: Vec<Entry> = self
            .outbox
            .lock()
            .map_err(|_| Error::EngineStopped)?
            .pending()
            .to_vec();
        let mut out = Flushed::default();
        for entry in pending {
            match self.send(&entry)? {
                Sent::Done(_) => out.acked += 1,
                Sent::Refused(_) => out.parked += 1,
                Sent::Stay => break,
            }
        }
        out.left = self.queued();
        Ok(out)
    }
}

enum Sent {
    Done(Vec<Appended>),
    Stay,
    Refused(Error),
}
