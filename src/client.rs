//! The sync client: cold load, live updates, edits through the durable outbox, and the
//! two write modes.
//!
//! * **Append mode** (`writePath: append`): the op is applied locally, the bytes it
//!   produced go to `/append` (human class only), one request per user action.
//! * **Server-applied mode** (`writePath: legacy`): the op is never applied locally first;
//!   [`cvws::Workspace::prepare`] builds the envelope, `/ops` sends it, and the server's
//!   canonical bytes come back through `receive` (live frame or a refresh). A local
//!   optimistic apply would be applied a second time when those bytes arrive.
//!
//! Every edit is written to the outbox (fsync) before anything is sent; a replay sends
//! the same opId and the same bytes.

use crate::error::{Error, Result};
use crate::frames;
use crate::http::{Counts, Http, Kind, Response};
use crate::link::Link;
use crate::outbox::{Entry, EntryKind, Outbox, Parked};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use cvws::{ActorClass, Applied, Outgoing, SUB_DOCS, SubDoc, Workspace};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// A resync or a mode probe happens at most this often (the Cloudflare budget).
pub const RESYNC_INTERVAL: Duration = Duration::from_secs(300);
/// The stack `cvws` requires for every call that touches received bytes.
const ENGINE_STACK: usize = 256 << 20;

/// Monotonic time since an arbitrary origin; injectable so budget tests run a 2-hour
/// session in milliseconds.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

pub struct SystemClock(Instant);

impl Default for SystemClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Append,
    /// Server-applied.
    Legacy,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// The API origin, for example `https://api.careervector.corbet.ch` (no trailing slash).
    pub api_base: String,
    /// Relay base URLs in preference order (`wss://...`), used by [`crate::Live`].
    pub relays: Vec<String>,
    /// Where the outbox, state and cache live (created, files 0600).
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditOutcome {
    /// The op changed nothing: nothing was sent.
    NoChange,
    /// The server has it; `clock` is its ledger clock when it said so.
    Saved { clock: Option<u64> },
    /// Written to the outbox, not yet acknowledged (offline, conflict or a server error).
    Queued,
    /// The server refused it for good; the entry was parked and the local document will
    /// be reloaded from the server once the outbox is empty.
    Rejected(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub online: bool,
    pub mode: Option<Mode>,
    pub replayed: usize,
    pub left: usize,
}

type Job = Box<dyn FnOnce(&mut Workspace) + Send>;

/// The `cvws` workspace on its own thread with the stack its README requires.
struct Engine {
    tx: mpsc::Sender<Job>,
}

impl Engine {
    fn spawn(states: Vec<(SubDoc, Vec<u8>)>) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("cvdb-engine".into())
            .stack_size(ENGINE_STACK)
            .spawn(move || {
                let refs: Vec<(SubDoc, &[u8])> =
                    states.iter().map(|(s, b)| (*s, b.as_slice())).collect();
                let mut ws = match Workspace::open(&refs) {
                    Ok(ws) => {
                        let _ = ready_tx.send(Ok(()));
                        ws
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(Error::Engine(e.to_string())));
                        return;
                    }
                };
                while let Ok(job) = rx.recv() {
                    job(&mut ws);
                }
            })?;
        ready_rx
            .recv()
            .map_err(|_| Error::Engine("engine thread".into()))??;
        Ok(Self { tx })
    }

    fn run<R: Send + 'static>(&self, f: impl FnOnce(&mut Workspace) -> R + Send + 'static) -> R {
        let (rtx, rrx) = mpsc::channel();
        self.tx
            .send(Box::new(move |ws| {
                let _ = rtx.send(f(ws));
            }))
            .expect("engine thread alive");
        rrx.recv().expect("engine thread alive")
    }
}

#[derive(Default)]
struct State {
    mode: Option<Mode>,
    etags: BTreeMap<String, String>,
}

impl State {
    fn load(dir: &Path) -> Self {
        let v: Value = std::fs::read(dir.join("state.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        let mode = match v.get("mode").and_then(Value::as_str) {
            Some("append") => Some(Mode::Append),
            Some("legacy") => Some(Mode::Legacy),
            _ => None,
        };
        let etags = v
            .get("etags")
            .and_then(Value::as_object)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        Self { mode, etags }
    }

    fn save(&self, dir: &Path) -> Result<()> {
        let mode = self.mode.map(|m| {
            if m == Mode::Append {
                "append"
            } else {
                "legacy"
            }
        });
        write_atomic(
            &dir.join("state.json"),
            &serde_json::to_vec(&json!({"mode": mode, "etags": self.etags})).expect("json"),
        )
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut f = crate::link::open_private(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub struct Cvdb {
    link: Link,
    cfg: Config,
    http: Arc<dyn Http>,
    counts: Counts,
    clock: Arc<dyn Clock>,
    engine: Engine,
    outbox: Outbox,
    state: State,
    live: bool,
    last_resync: Option<Duration>,
    last_probe: Option<Duration>,
    resync_wanted: bool,
    rebuild_pending: bool,
}

fn new_op_id() -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    (0..21)
        .map(|_| A[fastrand::usize(..A.len())] as char)
        .collect()
}

/// The JSON body of `/append`. Only the human actor class ever reaches it: agent bytes
/// are refused by the server, and a FIXED-node edit is never sent as an agent.
pub fn append_body(entry: &Entry, actor: ActorClass) -> Result<Vec<u8>> {
    if actor != ActorClass::Human {
        return Err(Error::NotHuman);
    }
    let mut updates = Map::new();
    for (sub, bytes) in &entry.updates {
        updates.insert(sub.clone(), Value::String(B64.encode(bytes)));
    }
    let body = json!({
        "opCatalogVersion": cvws::OP_CATALOG_VERSION,
        "op": entry.op,
        "opId": entry.op_id,
        "actorClass": "human",
        "updates": updates,
    });
    Ok(serde_json::to_vec(&body).expect("json"))
}

#[must_use]
pub fn ops_body(entry: &Entry) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "opCatalogVersion": cvws::OP_CATALOG_VERSION,
        "op": entry.op,
        "actorClass": "human",
    }))
    .expect("json")
}

fn text_of(resp: &Response) -> String {
    String::from_utf8_lossy(&resp.body).into_owned()
}

impl Cvdb {
    /// Open the client: state, outbox and cached documents from `cfg.dir`. No network.
    pub fn open(
        link: Link,
        cfg: Config,
        http: Arc<dyn Http>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        std::fs::create_dir_all(cfg.dir.join("cache"))?;
        let outbox = Outbox::open(&cfg.dir)?;
        let state = State::load(&cfg.dir);
        let mut states = Vec::new();
        for sub in SUB_DOCS {
            if let Ok(bytes) =
                std::fs::read(cfg.dir.join("cache").join(format!("{}.bin", sub.as_str())))
            {
                states.push((sub, bytes));
            }
        }
        let engine = Engine::spawn(states)?;
        Ok(Self {
            link,
            cfg,
            http,
            counts: Counts::default(),
            clock,
            engine,
            outbox,
            state,
            live: false,
            last_resync: None,
            last_probe: None,
            resync_wanted: false,
            rebuild_pending: false,
        })
    }

    #[must_use]
    pub fn counts(&self) -> Counts {
        self.counts.clone()
    }

    #[must_use]
    pub fn mode(&self) -> Option<Mode> {
        self.state.mode
    }

    #[must_use]
    pub fn link(&self) -> &Link {
        &self.link
    }

    /// Entries written to the outbox and not yet acknowledged.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.outbox.pending().len()
    }

    /// Parked entries since the last call (each is shown once).
    pub fn take_notices(&mut self) -> Vec<Parked> {
        self.outbox.take_notices()
    }

    /// Tell the client whether a live relay connection delivers server bytes: in
    /// server-applied mode it then skips the refresh after a write.
    pub fn set_live(&mut self, live: bool) {
        self.live = live;
    }

    /// Read one node (never the network).
    pub fn view(&self, sub: SubDoc, branch: &str, node: &str) -> Result<Value> {
        let (b, n) = (branch.to_owned(), node.to_owned());
        self.engine
            .run(move |ws| ws.view(sub, &b, &n))
            .map_err(|e| Error::Engine(e.to_string()))
    }

    fn url(&self, tail: &str) -> String {
        format!(
            "{}/workspaces/{}{}",
            self.cfg.api_base.trim_end_matches('/'),
            self.link.ws_id(),
            tail
        )
    }

    fn call(
        &self,
        kind: Kind,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        self.counts.bump(kind);
        self.http.request(method, url, headers, body)
    }

    // ---- attach, load, resync -------------------------------------------------------

    /// Detect the write path, load both profile sub-documents, replay the outbox.
    /// Offline with a cached mode is not an error: edits queue.
    pub fn attach(&mut self) -> Result<Report> {
        let mut report = Report::default();
        match self.detect_mode() {
            Ok(()) => report.online = true,
            Err(Error::Offline) if self.state.mode.is_some() => {}
            Err(e) => return Err(e),
        }
        if report.online {
            for sub in SUB_DOCS {
                self.load(sub)?;
            }
            let (sent, left) = self.replay()?;
            report.replayed = sent;
            report.left = left;
        } else {
            report.left = self.queued();
        }
        report.mode = self.state.mode;
        Ok(report)
    }

    /// `GET /workspaces/:id`: the `writePath` field says which mode the workspace is in.
    pub fn detect_mode(&mut self) -> Result<()> {
        let resp = self.call(Kind::Workspace, "GET", &self.url(""), &[], None)?;
        self.last_probe = Some(self.clock.now());
        match resp.status {
            200..=299 => {}
            403 => return Err(Error::ReadOnlyMirror),
            s => return Err(Error::Server(s)),
        }
        let v: Value = serde_json::from_slice(&resp.body)
            .map_err(|_| Error::Protocol("workspace json".into()))?;
        let mode = match v.get("writePath").and_then(Value::as_str) {
            Some("append") => Mode::Append,
            // A server that does not name a write path is the legacy one.
            _ => Mode::Legacy,
        };
        self.state.mode = Some(mode);
        self.state.save(&self.cfg.dir)
    }

    fn fetch_sub(&mut self, sub: SubDoc) -> Result<Option<(Vec<Vec<u8>>, String)>> {
        let name = sub.as_str();
        let etag = self.state.etags.get(name).cloned();
        let url = self.url(&format!("/sub/{name}"));
        let mut headers: Vec<(&str, &str)> = Vec::new();
        let cached = self
            .cfg
            .dir
            .join("cache")
            .join(format!("{name}.bin"))
            .exists();
        if let (Some(e), true) = (etag.as_deref(), cached) {
            headers.push(("If-None-Match", e));
        }
        let resp = self.call(Kind::Sub, "GET", &url, &headers, None)?;
        match resp.status {
            304 => Ok(None),
            200 => {
                let frames: Vec<Vec<u8>> = match resp.header("x-cv-update-frames") {
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
                Ok(Some((
                    frames,
                    resp.header("etag").unwrap_or_default().to_owned(),
                )))
            }
            403 => Err(Error::ReadOnlyMirror),
            s => Err(Error::Server(s)),
        }
    }

    /// Cold load or refresh of one sub-document. Frames go to `cvws` in order, head first.
    /// With no cached document the head replaces the empty one through `open` (a whole
    /// state has no frame cap); with one it merges through `receive`.
    pub fn load(&mut self, sub: SubDoc) -> Result<()> {
        let cached = self
            .cfg
            .dir
            .join("cache")
            .join(format!("{}.bin", sub.as_str()))
            .exists();
        self.load_inner(sub, !cached)
    }

    fn load_inner(&mut self, sub: SubDoc, replace: bool) -> Result<()> {
        let Some((frames, etag)) = self.fetch_sub(sub)? else {
            return Ok(());
        };
        let applied = self.engine.run(move |ws| {
            let mut iter = frames.into_iter();
            if replace && let Some(head) = iter.next() {
                let other: Vec<(SubDoc, Vec<u8>)> = SUB_DOCS
                    .iter()
                    .filter(|s| **s != sub)
                    .map(|s| (*s, ws.encode_state(*s)))
                    .collect();
                let mut states: Vec<(SubDoc, &[u8])> =
                    other.iter().map(|(s, b)| (*s, b.as_slice())).collect();
                states.push((sub, head.as_slice()));
                *ws = Workspace::open(&states).map_err(|e| Error::Engine(e.to_string()))?;
            }
            let mut flagged = false;
            for f in iter {
                if let Ok(a) = ws.receive(&f, Some(sub)) {
                    flagged |= a.resync;
                }
            }
            flagged |= ws.take_resync();
            Ok::<(bool, Vec<u8>), Error>((flagged, ws.encode_state(sub)))
        });
        let (flagged, bytes) = applied?;
        self.resync_wanted |= flagged;
        write_atomic(
            &self
                .cfg
                .dir
                .join("cache")
                .join(format!("{}.bin", sub.as_str())),
            &bytes,
        )?;
        self.state.etags.insert(sub.as_str().to_owned(), etag);
        self.state.save(&self.cfg.dir)
    }

    /// Reload every sub-document from the server, discarding the local document (used
    /// after a mode flip and after a rejected edit, when the outbox is empty).
    fn rebuild(&mut self) -> Result<()> {
        for sub in SUB_DOCS {
            self.state.etags.remove(sub.as_str());
            let _ = std::fs::remove_file(
                self.cfg
                    .dir
                    .join("cache")
                    .join(format!("{}.bin", sub.as_str())),
            );
        }
        self.engine = Engine::spawn(Vec::new())?;
        for sub in SUB_DOCS {
            self.load_inner(sub, true)?;
        }
        self.rebuild_pending = false;
        Ok(())
    }

    /// Take update bytes from the live relay. A raised resync flag leads to one reload of
    /// both sub-documents, at most every five minutes however often it is raised.
    pub fn apply_live(&mut self, update: &[u8]) -> Result<Applied> {
        let bytes = update.to_vec();
        let applied = self
            .engine
            .run(move |ws| ws.receive(&bytes, None))
            .map_err(|e| Error::Engine(e.to_string()))?;
        self.resync_wanted |= applied.resync;
        // A failed resync (offline, a bad answer) never fails the receive: the flag stays
        // raised and the next attempt waits for the five-minute gate again.
        let _ = self.maybe_resync();
        Ok(applied)
    }

    /// Run the gated resync (and the gated mode probe in server-applied mode) when due.
    pub fn maybe_resync(&mut self) -> Result<bool> {
        let now = self.clock.now();
        let due =
            |last: Option<Duration>| last.is_none_or(|t| now.saturating_sub(t) >= RESYNC_INTERVAL);
        let mut did = false;
        if self.resync_wanted && due(self.last_resync) {
            self.resync_wanted = false;
            self.last_resync = Some(now);
            for sub in SUB_DOCS {
                self.state.etags.remove(sub.as_str());
                if let Err(e) = self.load_inner(sub, false) {
                    self.resync_wanted = true;
                    return Err(e);
                }
            }
            did = true;
        }
        Ok(did)
    }

    // ---- edits -----------------------------------------------------------------------

    /// One user action: a single op, or a `batch` of ops (one request for all of them).
    /// Call it once per committed field edit (blur or confirm), never per keystroke.
    pub fn edit(&mut self, op: &Value) -> Result<EditOutcome> {
        if self.state.mode == Some(Mode::Legacy) && self.probe_due() {
            // A flag flipped back to append is noticed here, at most every five minutes.
            let _ = self.detect_mode();
        }
        let mode = self.state.mode.ok_or(Error::ModeUnknown)?;
        let mut op = op.clone();
        if let Some(obj) = op.as_object_mut() {
            if obj
                .get("opId")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                obj.insert("opId".into(), Value::String(new_op_id()));
            }
        } else {
            return Err(Error::Engine("op must be an object".into()));
        }
        let op_id = op["opId"].as_str().unwrap_or_default().to_owned();
        let entry = match mode {
            Mode::Append => {
                let o = op.clone();
                let outgoing = self.engine.run(move |ws| {
                    ws.apply(&o, ActorClass::Human)?;
                    ws.take_outgoing()
                        .map_err(|e| cvws::OpError::from_message(e.to_string()))
                });
                let outgoing: Vec<Outgoing> = outgoing.map_err(|e| Error::Engine(e.to_string()))?;
                if outgoing.is_empty() {
                    return Ok(EditOutcome::NoChange);
                }
                let mut updates = BTreeMap::new();
                for o in outgoing {
                    updates.insert(o.sub_doc.as_str().to_owned(), o.update);
                }
                Entry {
                    op_id,
                    kind: EntryKind::Append,
                    op,
                    updates,
                }
            }
            Mode::Legacy => {
                // Never applied locally: the server's canonical bytes come back through receive.
                let o = op.clone();
                let prepared = self
                    .engine
                    .run(move |ws| ws.prepare(&o, ActorClass::Human))
                    .map_err(|e| Error::Engine(e.to_string()))?;
                if prepared.would_change.is_empty() {
                    return Ok(EditOutcome::NoChange);
                }
                Entry {
                    op_id,
                    kind: EntryKind::Ops,
                    op: prepared.envelope,
                    updates: BTreeMap::new(),
                }
            }
        };
        self.outbox.append(&entry)?;
        let outcome = self.send_entry(&entry)?;
        if matches!(outcome, EditOutcome::Saved { .. })
            && entry.kind == EntryKind::Ops
            && !self.live
        {
            self.refresh_after_ops(&entry)?;
        }
        Ok(outcome)
    }

    fn probe_due(&self) -> bool {
        self.last_probe
            .is_none_or(|t| self.clock.now().saturating_sub(t) >= RESYNC_INTERVAL)
    }

    /// Server-applied write without a live connection: fetch the touched sub-documents so
    /// the canonical bytes arrive (one request per touched sub-document).
    fn refresh_after_ops(&mut self, entry: &Entry) -> Result<()> {
        let o = entry.op.clone();
        let routes = self
            .engine
            .run(move |ws| ws.prepare(&o, ActorClass::Human).map(|p| p.routes))
            .unwrap_or_else(|_| SUB_DOCS.to_vec());
        for sub in routes {
            self.state.etags.remove(sub.as_str());
            self.load_inner(sub, false)?;
        }
        Ok(())
    }

    fn send_entry(&mut self, entry: &Entry) -> Result<EditOutcome> {
        let (path, body) = match entry.kind {
            EntryKind::Append => ("/append", append_body(entry, ActorClass::Human)?),
            EntryKind::Ops => ("/ops", ops_body(entry)),
        };
        let kind = if entry.kind == EntryKind::Append {
            Kind::Append
        } else {
            Kind::Ops
        };
        let resp = match self.call(
            kind,
            "POST",
            &self.url(path),
            &[("Content-Type", "application/json")],
            Some(&body),
        ) {
            Ok(r) => r,
            Err(Error::Offline) => return Ok(EditOutcome::Queued),
            Err(e) => return Err(e),
        };
        match resp.status {
            200..=299 => {
                let clock = serde_json::from_slice::<Value>(&resp.body)
                    .ok()
                    .and_then(|v| v.get("clock").and_then(Value::as_u64));
                self.outbox.ack(&entry.op_id)?;
                Ok(EditOutcome::Saved { clock })
            }
            409 if text_of(&resp).contains("not_on_append_path") => {
                self.flip_to_legacy()?;
                Ok(EditOutcome::Saved { clock: None })
            }
            409 => Ok(EditOutcome::Queued),
            s if s >= 500 || s == 429 => Ok(EditOutcome::Queued),
            s => {
                let reason = format!(
                    "{s}: {}",
                    text_of(&resp).chars().take(120).collect::<String>()
                );
                self.outbox.park(&entry.op_id, &reason)?;
                self.rebuild_pending = true;
                self.finish_rebuild()?;
                Ok(EditOutcome::Rejected(reason))
            }
        }
    }

    fn finish_rebuild(&mut self) -> Result<()> {
        if self.rebuild_pending && self.outbox.pending().is_empty() {
            self.rebuild()?;
        }
        Ok(())
    }

    /// The flag went dark under us (409 `not_on_append_path`): send every queued op
    /// through the server-applied path with the same opId, then reload the documents from
    /// the server. The local copies of those edits are discarded by the reload, so the
    /// server's canonical bytes are the only copy (no duplicate text).
    fn flip_to_legacy(&mut self) -> Result<()> {
        self.state.mode = Some(Mode::Legacy);
        self.state.save(&self.cfg.dir)?;
        let pending: Vec<Entry> = self.outbox.pending().to_vec();
        for e in pending {
            let ops = Entry {
                kind: EntryKind::Ops,
                updates: BTreeMap::new(),
                ..e
            };
            let resp = match self.call(
                Kind::Ops,
                "POST",
                &self.url("/ops"),
                &[("Content-Type", "application/json")],
                Some(&ops_body(&ops)),
            ) {
                Ok(r) => r,
                Err(Error::Offline) => return Ok(()),
                Err(e) => return Err(e),
            };
            match resp.status {
                200..=299 => self.outbox.ack(&ops.op_id)?,
                s if s >= 500 || s == 429 || s == 409 => return Ok(()),
                s => {
                    self.outbox
                        .park(&ops.op_id, &format!("{s} after the mode flip"))?;
                    self.rebuild_pending = true;
                }
            }
        }
        if self.outbox.pending().is_empty() {
            self.rebuild()?;
        }
        Ok(())
    }

    /// Send queued entries in producing order, same opId and same bytes; stops at the
    /// first one that cannot be delivered (order is kept). Returns (sent, left).
    pub fn replay(&mut self) -> Result<(usize, usize)> {
        let mut sent = 0;
        let pending: Vec<Entry> = self.outbox.pending().to_vec();
        for e in pending {
            if self.state.mode == Some(Mode::Legacy) && e.kind == EntryKind::Append {
                // The mode flipped while it waited: the flip path converts it.
                self.flip_to_legacy()?;
                break;
            }
            match self.send_entry(&e)? {
                EditOutcome::Saved { .. } | EditOutcome::Rejected(_) => sent += 1,
                _ => break,
            }
        }
        Ok((sent, self.outbox.pending().len()))
    }

    /// Persist the cached documents (call on a clean shutdown).
    pub fn flush_cache(&self) -> Result<()> {
        for sub in SUB_DOCS {
            let bytes = self.engine.run(move |ws| ws.encode_state(sub));
            write_atomic(
                &self
                    .cfg
                    .dir
                    .join("cache")
                    .join(format!("{}.bin", sub.as_str())),
                &bytes,
            )?;
        }
        Ok(())
    }

    /// Start the live relay connection; events arrive on the returned receiver. The host
    /// feeds `Event::Update` to [`Cvdb::apply_live`].
    #[must_use]
    pub fn start_live(&self) -> (crate::live::Live, mpsc::Receiver<crate::live::Event>) {
        let (tx, rx) = mpsc::channel();
        let device = crate::live::device_id(&self.cfg.dir);
        let live = crate::live::Live::spawn(
            self.cfg.relays.clone(),
            self.link.ws_id().to_owned(),
            device,
            self.counts.clone(),
            tx,
        );
        (live, rx)
    }
}
