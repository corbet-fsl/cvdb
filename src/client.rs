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
//! Every edit is written to the outbox (fsync) before anything is sent, entries are sent
//! strictly in order (an edit queues behind older ones), and a replay sends the same opId
//! and the same bytes.

use crate::error::{Error, Result};
use crate::frames;
use crate::http::{Counts, Http, Kind, Response};
use crate::link::{Link, create_private_dir, sync_parent};
use crate::outbox::{Entry, EntryKind, Outbox, Parked};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use cvws::{ActorClass, Applied, ForeignSub, Outgoing, SUB_DOCS, SubDoc, Workspace};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// A resync or a mode probe happens at most this often (the Cloudflare budget).
pub const RESYNC_INTERVAL: Duration = Duration::from_secs(300);
/// The stack `cvws` requires for every call that touches received bytes.
const ENGINE_STACK: usize = 256 << 20;
/// The non-profile sub-documents, loaded once as routing shadows (5 reads) so that foreign
/// relay frames are attributed exactly instead of being held and evicted into resyncs.
const FOREIGN: [ForeignSub; 5] = [
    ForeignSub::Jobs,
    ForeignSub::Settings,
    ForeignSub::Layout,
    ForeignSub::OrderState,
    ForeignSub::Notes,
];

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
    /// Where the outbox, state and cache live (created 0700, files 0600, locked exclusively).
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditOutcome {
    /// The op changed nothing: nothing was sent.
    NoChange,
    /// The server has it; `clock` is its ledger clock when it said so.
    Saved { clock: Option<u64> },
    /// Written to the outbox, not yet acknowledged (offline, behind an older entry, a
    /// conflict, a server error, a catalog mismatch).
    Queued,
    /// The server refused it for good (400 envelope, 422); the entry was parked.
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
        ready_rx.recv().map_err(|_| Error::EngineStopped)??;
        Ok(Self { tx })
    }

    /// A dead engine thread is an error for the caller, never a panic in the host.
    fn run<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Workspace) -> R + Send + 'static,
    ) -> Result<R> {
        let (rtx, rrx) = mpsc::channel();
        self.tx
            .send(Box::new(move |ws| {
                let _ = rtx.send(f(ws));
            }))
            .map_err(|_| Error::EngineStopped)?;
        rrx.recv().map_err(|_| Error::EngineStopped)
    }
}

#[derive(Default)]
struct State {
    mode: Option<Mode>,
    etags: BTreeMap<String, String>,
    /// The local document holds edits the server will not take (a parked entry, a flip to
    /// server-applied mode): reload it once no entry with local bytes is pending.
    rebuild: bool,
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
        Self {
            mode,
            etags,
            rebuild: v.get("rebuild").and_then(Value::as_bool).unwrap_or(false),
        }
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
            &serde_json::to_vec(
                &json!({"mode": mode, "etags": self.etags, "rebuild": self.rebuild}),
            )
            .expect("json"),
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
    sync_parent(path)
}

/// What sending one entry came to.
enum Sent {
    Acked(Option<u64>),
    /// Not delivered, nothing decided: keep it queued and stop (order is kept).
    Stay,
    /// The server refused it for good; the entry is parked.
    Parked(String),
    /// The workspace is not on the append path (409): queued entries were converted.
    Flipped,
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
    /// Held for the life of the client: a second instance on the directory is refused.
    _lock: std::fs::File,
    acked: BTreeMap<String, Option<u64>>,
    rejected: BTreeMap<String, String>,
    touched: BTreeSet<SubDoc>,
    events: Vec<HostEvent>,
    noops: BTreeSet<String>,
    rebuild_failed_at: Option<Duration>,
    rebuilt_this_flush: bool,
}

/// Something the host should tell the user, reported once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostEvent {
    /// The server refused an op because this client's op catalog is out of date (426):
    /// edits queue until the client is updated.
    Outdated,
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

/// The API's own error shape: a JSON object with a string `error`. Anything else (an edge
/// error page, a proxy answer) says nothing about the op.
fn api_error(resp: &Response) -> Option<String> {
    serde_json::from_slice::<Value>(&resp.body)
        .ok()?
        .get("error")?
        .as_str()
        .map(str::to_owned)
}

fn cache_path(dir: &Path, sub: SubDoc) -> PathBuf {
    dir.join("cache").join(format!("{}.bin", sub.as_str()))
}

fn lock_dir(dir: &Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join("lock"))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => Err(Error::Locked),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

impl Cvdb {
    /// Open the client: state, outbox and cached documents from `cfg.dir`. No network.
    /// Queued edits that were never acknowledged are applied to the loaded document again,
    /// so a crash does not leave the view without them (a re-edit would duplicate text).
    pub fn open(
        link: Link,
        cfg: Config,
        http: Arc<dyn Http>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        create_private_dir(&cfg.dir)?;
        create_private_dir(&cfg.dir.join("cache"))?;
        let lock = lock_dir(&cfg.dir)?;
        crate::link::tighten(&[
            &cfg.dir.join("state.json"),
            &cfg.dir.join("device"),
            &cfg.dir.join("lock"),
            &cache_path(&cfg.dir, SubDoc::CvProfile),
            &cache_path(&cfg.dir, SubDoc::ClProfile),
        ]);
        let outbox = Outbox::open(&cfg.dir)?;
        let mut state = State::load(&cfg.dir);
        let mut states = Vec::new();
        for sub in SUB_DOCS {
            if let Ok(bytes) = std::fs::read(cache_path(&cfg.dir, sub)) {
                states.push((sub, bytes));
            }
        }
        let engine = Engine::spawn(states)?;
        for e in outbox.pending() {
            match e.kind {
                EntryKind::Append => {
                    for (name, bytes) in &e.updates {
                        if let Some(sub) = SubDoc::parse(name) {
                            let b = bytes.clone();
                            engine.run(move |ws| ws.receive(&b, Some(sub)))?.ok();
                        }
                    }
                }
                EntryKind::Reapply => state.rebuild = true,
                EntryKind::Ops => {}
            }
        }
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
            _lock: lock,
            acked: BTreeMap::new(),
            rejected: BTreeMap::new(),
            touched: BTreeSet::new(),
            events: Vec::new(),
            noops: BTreeSet::new(),
            rebuild_failed_at: None,
            rebuilt_this_flush: false,
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

    /// Test hook: see [`Outbox::inject_write_failure`].
    #[doc(hidden)]
    pub fn inject_outbox_failure(&mut self, broken: bool) -> Result<()> {
        if broken {
            self.outbox.inject_write_failure()
        } else {
            self.outbox.repair()
        }
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
            .run(move |ws| ws.view(sub, &b, &n))?
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

    /// Detect the write path, load both profile sub-documents and the routing shadows,
    /// resolve what the outbox still owes, replay it. Offline with a cached mode is not an
    /// error: edits queue.
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
            self.load_shadows();
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
    /// A change to server-applied mode converts queued append entries (see `convert_to_ops`).
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
        let was = self.state.mode;
        self.state.mode = Some(mode);
        if mode == Mode::Legacy && was != Some(Mode::Legacy) {
            self.convert_to_ops()?;
        }
        self.state.save(&self.cfg.dir)
    }

    fn fetch_sub(
        &mut self,
        name: &str,
        conditional: bool,
    ) -> Result<Option<(Vec<Vec<u8>>, String)>> {
        let etag = self.state.etags.get(name).cloned();
        let url = self.url(&format!("/sub/{name}"));
        let mut headers: Vec<(&str, &str)> = Vec::new();
        if let (Some(e), true) = (etag.as_deref(), conditional) {
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
    /// With no cached document (or a head over the frame cap) the head replaces the document
    /// through `open` (a whole state has no frame cap); with one it merges through `receive`.
    pub fn load(&mut self, sub: SubDoc) -> Result<()> {
        let cached = cache_path(&self.cfg.dir, sub).exists();
        self.load_inner(sub, !cached)
    }

    fn load_inner(&mut self, sub: SubDoc, replace: bool) -> Result<()> {
        let conditional = cache_path(&self.cfg.dir, sub).exists();
        let Some((frames, etag)) = self.fetch_sub(sub.as_str(), conditional)? else {
            return Ok(());
        };
        let replace = replace || frames.first().is_some_and(|h| h.len() > frames::MAX_FRAME);
        let (flagged, bytes) = apply_frames(&self.engine, sub, frames, replace)?;
        self.resync_wanted |= flagged;
        if replace {
            self.reapply_pending_bytes(Some(sub))?;
        }
        write_atomic(&cache_path(&self.cfg.dir, sub), &bytes)?;
        self.state.etags.insert(sub.as_str().to_owned(), etag);
        self.state.save(&self.cfg.dir)
    }

    /// Apply the bytes of unacknowledged append entries to the document again (idempotent).
    fn reapply_pending_bytes(&self, only: Option<SubDoc>) -> Result<()> {
        for e in self.outbox.pending() {
            if e.kind != EntryKind::Append {
                continue;
            }
            for (name, bytes) in &e.updates {
                let Some(sub) = SubDoc::parse(name) else {
                    continue;
                };
                if only.is_some_and(|o| o != sub) {
                    continue;
                }
                let b = bytes.clone();
                self.engine.run(move |ws| ws.receive(&b, Some(sub)))?.ok();
            }
        }
        Ok(())
    }

    /// Load the non-profile sub-documents as routing shadows, once per attach (5 reads).
    /// Best effort: shadows only sharpen frame routing, so a sub-document that fails is
    /// skipped (and nothing else waits for it); a network failure ends the round.
    pub fn load_shadows(&mut self) {
        for which in FOREIGN {
            match self.fetch_sub(which.as_str(), false) {
                Ok(Some((frames, _))) => {
                    let _ = self.engine.run(move |ws| {
                        for f in frames {
                            ws.load_shadow(which, &f).ok();
                        }
                    });
                }
                Ok(None) | Err(Error::Server(_) | Error::Protocol(_)) => {}
                Err(_) => break,
            }
        }
    }

    /// Reload every profile sub-document from the server into a FRESH engine and swap it in
    /// only when all of them arrived: a failure leaves the current document untouched.
    fn rebuild(&mut self) -> Result<()> {
        let fresh = Engine::spawn(Vec::new())?;
        let mut loaded = Vec::new();
        for sub in SUB_DOCS {
            let (frames, etag) = self
                .fetch_sub(sub.as_str(), false)?
                .ok_or_else(|| Error::Protocol("an unconditional read answered 304".into()))?;
            let (flagged, bytes) = apply_frames(&fresh, sub, frames, true)?;
            loaded.push((sub, etag, bytes, flagged));
        }
        self.engine = fresh;
        for (sub, etag, bytes, flagged) in loaded {
            self.resync_wanted |= flagged;
            write_atomic(&cache_path(&self.cfg.dir, sub), &bytes)?;
            self.state.etags.insert(sub.as_str().to_owned(), etag);
        }
        self.state.save(&self.cfg.dir)?;
        self.load_shadows();
        Ok(())
    }

    /// Take update bytes from the live relay. A raised resync flag leads to one reload of
    /// both sub-documents, at most every five minutes however often it is raised.
    pub fn apply_live(&mut self, update: &[u8]) -> Result<Applied> {
        let bytes = update.to_vec();
        let applied = self
            .engine
            .run(move |ws| ws.receive(&bytes, None))?
            .map_err(|e| Error::Engine(e.to_string()))?;
        self.resync_wanted |= applied.resync;
        // A failed resync (offline, a bad answer) never fails the receive: the flag stays
        // raised and the next attempt waits for the five-minute gate again.
        let _ = self.maybe_resync();
        Ok(applied)
    }

    /// Run the gated resync when due.
    pub fn maybe_resync(&mut self) -> Result<bool> {
        let now = self.clock.now();
        let due =
            |last: Option<Duration>| last.is_none_or(|t| now.saturating_sub(t) >= RESYNC_INTERVAL);
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
            return Ok(true);
        }
        Ok(false)
    }

    // ---- edits -----------------------------------------------------------------------

    /// One user action: a single op, or a `batch` of ops (one request for all of them).
    /// Call it once per committed field edit (blur or confirm), never per keystroke.
    /// An edit queues behind older queued entries and is sent after them, in order.
    ///
    /// The entry stores the checked envelope with the ids the engine generates (`prepare`),
    /// so deriving it again after a reload, or sending it through `/ops`, keeps the same ids.
    pub fn edit(&mut self, op: &Value) -> Result<EditOutcome> {
        if self.state.mode == Some(Mode::Legacy) && self.probe_due() {
            // A flag flipped back to append is noticed here, at most every five minutes.
            let _ = self.detect_mode();
        }
        let mode = self.state.mode.ok_or(Error::ModeUnknown)?;
        let mut op = op.clone();
        let Some(obj) = op.as_object_mut() else {
            return Err(Error::Engine("op must be an object".into()));
        };
        if obj
            .get("opId")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            obj.insert("opId".into(), Value::String(new_op_id()));
        }
        let op_id = op["opId"].as_str().unwrap_or_default().to_owned();
        let derive_later = self.needs_rebuild();
        let nothing_pending = self.outbox.pending().is_empty();
        let prepared = self.prepare(&op);
        let entry = match (mode, prepared) {
            (Mode::Append, prepared) if derive_later => Entry {
                op_id: op_id.clone(),
                kind: EntryKind::Reapply,
                // Stamped when the stale document still allows it; the raw op otherwise.
                op: prepared.map_or(op, |p| p.envelope),
                updates: BTreeMap::new(),
            },
            (Mode::Append, prepared) => {
                let envelope = prepared?.envelope;
                let outgoing = self.apply_local(&envelope)?;
                if outgoing.is_empty() {
                    return Ok(EditOutcome::NoChange);
                }
                let mut updates = BTreeMap::new();
                for o in &outgoing {
                    updates.insert(o.sub_doc.as_str().to_owned(), o.update.clone());
                }
                let entry = Entry {
                    op_id: op_id.clone(),
                    kind: EntryKind::Append,
                    op: envelope,
                    updates,
                };
                if let Err(e) = self.outbox.append(&entry) {
                    // The bytes were drained from the engine: hand them back so the next
                    // edit carries them (peers could not integrate later updates without).
                    self.engine.run(move |ws| ws.restore_outgoing(outgoing))?;
                    return Err(e);
                }
                return self.finish_edit(&op_id);
            }
            (Mode::Legacy, prepared) => {
                // Never applied locally: the server's canonical bytes come back through receive.
                let prepared = prepared?;
                // The local document never holds the queued `/ops` entries, so "no change" is
                // only knowable when nothing is queued (undoing a queued edit is a real edit).
                if prepared.would_change.is_empty() && nothing_pending {
                    return Ok(EditOutcome::NoChange);
                }
                Entry {
                    op_id: op_id.clone(),
                    kind: EntryKind::Ops,
                    op: prepared.envelope,
                    updates: BTreeMap::new(),
                }
            }
        };
        self.outbox.append(&entry)?;
        self.finish_edit(&op_id)
    }

    fn prepare(&self, op: &Value) -> Result<cvws::Prepared> {
        let o = op.clone();
        self.engine
            .run(move |ws| ws.prepare(&o, ActorClass::Human))?
            .map_err(|e| Error::Engine(e.to_string()))
    }

    fn finish_edit(&mut self, op_id: &str) -> Result<EditOutcome> {
        self.flush()?;
        Ok(if let Some(clock) = self.acked.remove(op_id) {
            EditOutcome::Saved { clock }
        } else if let Some(reason) = self.rejected.remove(op_id) {
            EditOutcome::Rejected(reason)
        } else if self.noops.remove(op_id) {
            EditOutcome::NoChange
        } else {
            EditOutcome::Queued
        })
    }

    fn apply_local(&self, op: &Value) -> Result<Vec<Outgoing>> {
        let o = op.clone();
        self.engine
            .run(move |ws| {
                ws.apply(&o, ActorClass::Human)?;
                ws.take_outgoing()
                    .map_err(|e| cvws::OpError::from_message(e.to_string()))
            })?
            .map_err(|e| Error::Engine(e.to_string()))
    }

    fn probe_due(&self) -> bool {
        self.last_probe
            .is_none_or(|t| self.clock.now().saturating_sub(t) >= RESYNC_INTERVAL)
    }

    /// Host-facing events since the last call (each is reported once).
    pub fn take_events(&mut self) -> Vec<HostEvent> {
        std::mem::take(&mut self.events)
    }

    fn send_entry(&mut self, entry: &Entry) -> Result<Sent> {
        let (path, body, kind) = match entry.kind {
            EntryKind::Append => (
                "/append",
                append_body(entry, ActorClass::Human)?,
                Kind::Append,
            ),
            EntryKind::Ops => ("/ops", ops_body(entry), Kind::Ops),
            EntryKind::Reapply => return Ok(Sent::Stay),
        };
        let resp = match self.call(
            kind,
            "POST",
            &self.url(path),
            &[("Content-Type", "application/json")],
            Some(&body),
        ) {
            Ok(r) => r,
            Err(Error::Offline) => return Ok(Sent::Stay),
            Err(e) => return Err(e),
        };
        match resp.status {
            200..=299 => {
                let clock = serde_json::from_slice::<Value>(&resp.body)
                    .ok()
                    .and_then(|v| v.get("clock").and_then(Value::as_u64));
                self.outbox.ack(&entry.op_id)?;
                Ok(Sent::Acked(clock))
            }
            // Only an append entry can find the flag dark; `/ops` has no such answer.
            409 if entry.kind == EntryKind::Append
                && text_of(&resp).contains("not_on_append_path") =>
            {
                self.state.mode = Some(Mode::Legacy);
                self.convert_to_ops()?;
                self.state.save(&self.cfg.dir)?;
                Ok(Sent::Flipped)
            }
            // The server runs its catalog check first: this client is outdated. The entry
            // stays queued; the host is told once.
            426 => {
                if !self.events.contains(&HostEvent::Outdated) {
                    self.events.push(HostEvent::Outdated);
                }
                Ok(Sent::Stay)
            }
            // Only the API's own refusals (its JSON error shape) end an entry for good:
            // a 400 envelope error or a 422 `op_apply_failed`. An edge page or any other
            // answer says nothing about the op: it stays queued.
            400 | 422 if api_error(&resp).is_some() => {
                let reason = format!("{}: {}", resp.status, api_error(&resp).unwrap_or_default());
                let reason: String = reason.chars().take(160).collect();
                if entry.kind == EntryKind::Append {
                    // The later entries carry bytes that depend on this one's clocks. The
                    // reload flag is saved first; then the parked line and the rewrite that
                    // drops this entry and turns the rest into `Reapply` are ONE step.
                    self.state.rebuild = true;
                    self.state.save(&self.cfg.dir)?;
                    let rest: Vec<Entry> = self
                        .outbox
                        .pending()
                        .iter()
                        .filter(|e| e.op_id != entry.op_id)
                        .cloned()
                        .map(|mut e| {
                            if e.kind == EntryKind::Append {
                                e.kind = EntryKind::Reapply;
                                e.updates.clear();
                            }
                            e
                        })
                        .collect();
                    self.outbox.park_replace(&entry.op_id, &reason, &rest)?;
                } else {
                    self.outbox.park(&entry.op_id, &reason)?;
                }
                Ok(Sent::Parked(reason))
            }
            _ => Ok(Sent::Stay),
        }
    }

    /// Send the outbox in producing order until something cannot be delivered. After every
    /// ack a pending reload is attempted (once nothing with local bytes is queued; at most
    /// one attempt per flush, failed ones gated by the five-minute rule).
    fn flush(&mut self) -> Result<(usize, usize)> {
        self.acked.clear();
        self.rejected.clear();
        self.noops.clear();
        self.touched.clear();
        self.rebuilt_this_flush = false;
        let mut sent = 0;
        loop {
            self.maybe_rebuild()?;
            let Some(head) = self.outbox.pending().first().cloned() else {
                break;
            };
            if head.kind == EntryKind::Reapply {
                break; // waiting for the reload
            }
            match self.send_entry(&head)? {
                Sent::Acked(clock) => {
                    sent += 1;
                    self.acked.insert(head.op_id.clone(), clock);
                    if head.kind == EntryKind::Ops {
                        self.note_touched(&head);
                    }
                }
                Sent::Stay => break,
                Sent::Flipped => {}
                Sent::Parked(reason) => {
                    sent += 1;
                    self.rejected.insert(head.op_id.clone(), reason);
                }
            }
        }
        self.refresh_touched()?;
        Ok((sent, self.outbox.pending().len()))
    }

    fn note_touched(&mut self, entry: &Entry) {
        let routes = self
            .prepare(&entry.op)
            .map_or_else(|_| SUB_DOCS.to_vec(), |p| p.routes);
        self.touched.extend(routes);
    }

    /// Server-applied writes without a live connection: fetch the touched sub-documents so
    /// the canonical bytes arrive (one request per touched sub-document, once per flush).
    fn refresh_touched(&mut self) -> Result<()> {
        if self.live || self.touched.is_empty() || self.needs_rebuild() {
            self.touched.clear();
            return Ok(());
        }
        for sub in std::mem::take(&mut self.touched) {
            self.state.etags.remove(sub.as_str());
            self.load_inner(sub, false)?;
        }
        Ok(())
    }

    /// Queued append entries become `/ops` entries (same opId, the stored envelope with its
    /// stamped ids, no bytes); the local document is reloaded once none is left, so the
    /// server's bytes for those ops are the only copy.
    fn convert_to_ops(&mut self) -> Result<()> {
        let mut changed = false;
        let entries: Vec<Entry> = self
            .outbox
            .pending()
            .iter()
            .cloned()
            .map(|mut e| {
                if e.kind != EntryKind::Ops {
                    changed = true;
                    e.kind = EntryKind::Ops;
                    e.updates.clear();
                }
                e
            })
            .collect();
        if changed {
            // The flag first: a crash between the two steps must not lose the reload.
            self.state.rebuild = true;
            self.state.save(&self.cfg.dir)?;
            self.outbox.replace_all(&entries)?;
        }
        Ok(())
    }

    /// The local document holds edits the server will not take, or entries wait to be derived.
    fn needs_rebuild(&self) -> bool {
        self.state.rebuild
            || self
                .outbox
                .pending()
                .iter()
                .any(|e| e.kind == EntryKind::Reapply)
    }

    /// Reload the local document when it needs it and no entry with local bytes is pending;
    /// then derive the `Reapply` entries against it. A failed reload leaves the document as
    /// it was and is not tried again within five minutes.
    fn maybe_rebuild(&mut self) -> Result<()> {
        if !self.needs_rebuild()
            || self.rebuilt_this_flush
            || self
                .outbox
                .pending()
                .iter()
                .any(|e| e.kind == EntryKind::Append)
        {
            return Ok(());
        }
        let now = self.clock.now();
        if self
            .rebuild_failed_at
            .is_some_and(|t| now.saturating_sub(t) < RESYNC_INTERVAL)
        {
            return Ok(());
        }
        self.rebuilt_this_flush = true;
        match self.rebuild() {
            Ok(()) => self.rebuild_failed_at = None,
            Err(e @ (Error::Engine(_) | Error::EngineStopped | Error::Io(_))) => return Err(e),
            Err(_) => {
                self.rebuild_failed_at = Some(now);
                return Ok(());
            }
        }
        self.state.rebuild = false;
        self.state.save(&self.cfg.dir)?;
        self.reapply_entries()
    }

    fn reapply_entries(&mut self) -> Result<()> {
        let mode = self.state.mode;
        let mut entries: Vec<Entry> = self.outbox.pending().to_vec();
        let mut i = 0;
        while i < entries.len() {
            if entries[i].kind != EntryKind::Reapply {
                i += 1;
                continue;
            }
            if mode == Some(Mode::Legacy) {
                entries[i].kind = EntryKind::Ops;
                i += 1;
                continue;
            }
            let op = entries[i].op.clone();
            match self.apply_local(&op) {
                Ok(out) if out.is_empty() => {
                    // Nothing left to do: the server's document already has the effect.
                    self.noops.insert(entries.remove(i).op_id);
                }
                Ok(out) => {
                    entries[i].kind = EntryKind::Append;
                    entries[i].updates = out
                        .into_iter()
                        .map(|o| (o.sub_doc.as_str().to_owned(), o.update))
                        .collect();
                    i += 1;
                }
                Err(Error::Engine(e)) => {
                    // The op no longer applies to the server's document.
                    let reason = format!("does not apply after the reload: {e}");
                    let id = entries[i].op_id.clone();
                    entries.remove(i);
                    self.outbox.park_replace(&id, &reason, &entries)?;
                    self.rejected.insert(id, reason);
                }
                // The engine is gone, not the op: keep every entry and let the host see it.
                Err(e) => {
                    self.outbox.replace_all(&entries)?;
                    return Err(e);
                }
            }
        }
        self.outbox.replace_all(&entries)
    }

    /// Send queued entries in producing order, same opId and same bytes; stops at the
    /// first one that cannot be delivered (order is kept). Returns (sent, left).
    pub fn replay(&mut self) -> Result<(usize, usize)> {
        self.flush()
    }

    /// Test hook: make outbox rewrites fail.
    #[doc(hidden)]
    pub fn inject_outbox_rewrite_failure(&mut self, fail: bool) {
        self.outbox.inject_rewrite_failure(fail);
    }

    /// Persist the cached documents (call on a clean shutdown).
    pub fn flush_cache(&self) -> Result<()> {
        for sub in SUB_DOCS {
            let bytes = self.engine.run(move |ws| ws.encode_state(sub))?;
            write_atomic(&cache_path(&self.cfg.dir, sub), &bytes)?;
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

/// Apply the frames of one sub-document to `engine`: with `replace` the head becomes the
/// document through `open`, otherwise every frame merges through `receive`. Returns whether
/// the resync flag was raised and the merged state.
fn apply_frames(
    engine: &Engine,
    sub: SubDoc,
    frames: Vec<Vec<u8>>,
    replace: bool,
) -> Result<(bool, Vec<u8>)> {
    engine.run(move |ws| {
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
        Ok((flagged, ws.encode_state(sub)))
    })?
}
