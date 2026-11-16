//! End to end against a LOCAL `wrangler dev` relay + api (never a deployed Worker).
//!
//! Run with `CVDB_E2E_CV=<CareerVector checkout> cargo test --test e2e -- --nocapture`;
//! without the variable the test prints a note and passes. One test function runs every
//! scenario in turn so a single stack serves them all.
#![allow(clippy::too_many_lines)]

use cvdb::client::RESYNC_INTERVAL;
use cvdb::http::Response;
use cvdb::{Clock, Config, Cvdb, EditOutcome, Error, Event, Http, Kind, Link, Mode, UreqHttp};
use cvws::SubDoc;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CV: &str = "cv_profile.en";
const CL: &str = "cl_profile.en";

struct Stack {
    child: Child,
    api: String,
    relay: String,
    state: PathBuf,
    cv_root: PathBuf,
}

impl Stack {
    fn start(cv_root: &Path) -> Self {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/e2e/stack.ts");
        let mut child = Command::new("bun")
            .arg(script)
            .env("CV_ROOT", cv_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("bun");
        let out = child.stdout.take().expect("stdout");
        let mut line = String::new();
        BufReader::new(out)
            .read_line(&mut line)
            .expect("READY line");
        let v: Value =
            serde_json::from_str(line.trim().strip_prefix("READY ").expect("READY")).expect("json");
        Self {
            child,
            api: v["api"].as_str().unwrap().to_owned(),
            relay: v["relay"].as_str().unwrap().to_owned(),
            state: PathBuf::from(v["state"].as_str().unwrap()),
            cv_root: cv_root.to_owned(),
        }
    }

    fn create_workspace(&self) -> String {
        let r = UreqHttp::new()
            .request(
                "POST",
                &format!("{}/workspaces", self.api),
                &[("Content-Type", "application/json")],
                Some(br#"{"name":"cvdb-e2e"}"#),
            )
            .expect("create");
        assert_eq!(r.status, 200);
        serde_json::from_slice::<Value>(&r.body).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    fn d1(&self) -> PathBuf {
        let dir = self.state.join("v3/d1/miniflare-D1DatabaseObject");
        let files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.extension().is_some_and(|e| e == "sqlite") && !p.ends_with("metadata.sqlite")
            })
            .collect();
        assert_eq!(files.len(), 1);
        files[0].clone()
    }

    /// Flip the write path as the server does: to `legacy` only after folding every tail
    /// into its head (`foldAndFlipToLegacy`), to `append` directly.
    fn set_write_path(&self, ws: &str, target: &str) {
        let script = "import { DatabaseSync } from 'node:sqlite'; \
            import { setWritePath, foldAndFlipToLegacy } from '@cv/server/workspace-append'; \
            const [file, ws, target] = process.argv.slice(1); \
            const db = new DatabaseSync(file); db.exec('PRAGMA busy_timeout = 5000'); \
            const stmt = (sql, args = []) => ({ bind: (...a) => stmt(sql, a), \
              run: async () => ({ success: true, meta: { changes: Number(db.prepare(sql).run(...args).changes) } }), \
              first: async () => db.prepare(sql).get(...args) ?? null, \
              all: async () => ({ results: db.prepare(sql).all(...args) }) }); \
            const d1 = { prepare: (sql) => stmt(sql), batch: async (xs) => { const r = []; for (const x of xs) r.push(await x.run()); return r; } }; \
            const r = target === 'append' ? await setWritePath(d1, ws, 'append') : await foldAndFlipToLegacy(d1, ws); \
            console.log(JSON.stringify(r)); if (!r.ok) process.exit(3); db.close();";
        let out = Command::new("bun")
            .args(["-e", script])
            .arg(self.d1())
            .arg(ws)
            .arg(target)
            .current_dir(self.cv_root.join("ui"))
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn ledger_rows(&self, ws: &str, op_id: &str) -> u64 {
        let script = "import {DatabaseSync} from 'node:sqlite'; const [f,w,o]=process.argv.slice(1); \
            const db=new DatabaseSync(f); db.exec('PRAGMA busy_timeout=5000'); \
            console.log(db.prepare('SELECT count(*) AS n FROM workspace_sub_doc_ops WHERE workspace_id=? AND op_id=?').get(w,o).n);";
        let out = Command::new("bun")
            .args(["-e", script])
            .arg(self.d1())
            .arg(ws)
            .arg(op_id)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().parse().unwrap()
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        std::thread::sleep(Duration::from_millis(500));
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone, Debug)]
struct Rec {
    method: String,
    path: String,
    status: u16,
    req: Vec<u8>,
    resp: Vec<u8>,
}

struct Spy {
    inner: UreqHttp,
    log: Mutex<Vec<Rec>>,
    offline: AtomicBool,
    lose_response: AtomicBool,
}

impl Spy {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: UreqHttp::new(),
            log: Mutex::default(),
            offline: AtomicBool::new(false),
            lose_response: AtomicBool::new(false),
        })
    }
    fn recs(&self) -> Vec<Rec> {
        self.log.lock().unwrap().clone()
    }
    fn posts(&self, suffix: &str) -> Vec<Rec> {
        self.recs()
            .into_iter()
            .filter(|r| r.method == "POST" && r.path.ends_with(suffix))
            .collect()
    }
}

impl Http for Spy {
    fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, Error> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(Error::Offline);
        }
        let resp = self.inner.request(method, url, headers, body)?;
        let path = url
            .split_once("/workspaces/")
            .map_or(url, |(_, p)| p)
            .split_once('/')
            .map_or(String::new(), |(_, p)| p.to_owned());
        self.log.lock().unwrap().push(Rec {
            method: method.to_owned(),
            path,
            status: resp.status,
            req: body.map(<[u8]>::to_vec).unwrap_or_default(),
            resp: resp.body.clone(),
        });
        if self.lose_response.load(Ordering::SeqCst) {
            return Err(Error::Offline);
        }
        Ok(resp)
    }
}

#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance(&self, d: Duration) {
        self.0.fetch_add(d.as_millis() as u64, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
}

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("cvdb-e2e-{tag}-{}", fastrand::u64(..)));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn client(stack: &Stack, ws: &str, dir: &Path, spy: &Arc<Spy>, clock: &Arc<ManualClock>) -> Cvdb {
    let cfg = Config {
        api_base: stack.api.clone(),
        relays: vec![stack.relay.clone()],
        dir: dir.to_owned(),
    };
    Cvdb::open(Link::parse(ws).unwrap(), cfg, spy.clone(), clock.clone()).unwrap()
}

fn node(id: &str, kind: &str, data: Value, children: Vec<Value>) -> Value {
    let active: Vec<Value> = children.iter().map(|c| c["id"].clone()).collect();
    json!({ "id": id, "kind": kind, "tier": "HYBRID", "data": data, "selection": { "active": active }, "children": children })
}

fn seed(c: &mut Cvdb) {
    let cv = node(
        "root",
        "cv-root",
        json!({}),
        vec![node(
            "item",
            "list-item",
            json!({ "heading": "Engineer" }),
            vec![node(
                "b1",
                "bullet",
                json!({ "text": "Built things" }),
                vec![],
            )],
        )],
    );
    let cl = node(
        "cl-root",
        "cl-root",
        json!({}),
        vec![node(
            "body",
            "body",
            json!({ "archetype": "text", "text": "Dear team" }),
            vec![],
        )],
    );
    for (kind, tree) in [("cv_profile.replace", cv), ("cl_profile.replace", cl)] {
        let out = c
            .edit(&json!({ "kind": kind, "lang": "en", "tree": tree }))
            .unwrap();
        assert!(matches!(out, EditOutcome::Saved { .. }), "{out:?}");
    }
}

fn set_cv(text: &str) -> Value {
    json!({ "kind": "node.update", "branch_path": CV, "node_path": "root.item.b1", "patch": { "data": { "text": text } } })
}

fn set_cl(text: &str) -> Value {
    json!({ "kind": "node.update", "branch_path": CL, "node_path": "cl-root.body", "patch": { "data": { "text": text } } })
}

fn cv_text(c: &Cvdb) -> String {
    c.view(SubDoc::CvProfile, CV, "root.item.b1").unwrap()["data"]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn cl_text(c: &Cvdb) -> String {
    c.view(SubDoc::ClProfile, CL, "cl-root.body").unwrap()["data"]["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn wait_for<T>(mut f: impl FnMut() -> Option<T>) -> T {
    for _ in 0..200 {
        if let Some(v) = f() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out");
}

/// Every body the client ever sent is human class, carries no agentId, and /append never saw anything else.
fn assert_human_only(spy: &Spy) {
    for r in spy.recs().into_iter().filter(|r| r.method == "POST") {
        let v: Value = serde_json::from_slice(&r.req).unwrap();
        assert_eq!(v["actorClass"], "human", "{}", r.path);
        assert!(v.get("agentId").is_none());
    }
}

fn ws_clean(spy: &Spy, ws: &str) {
    for r in spy.recs() {
        assert!(!r.path.contains(ws));
    }
}

#[test]
fn e2e() {
    let Ok(cv_root) = std::env::var("CVDB_E2E_CV") else {
        println!("CVDB_E2E_CV not set: skipping the local-stack scenarios");
        return;
    };
    let stack = Stack::start(Path::new(&cv_root));
    cold_load_and_live(&stack);
    append_kill_restart_replay(&stack);
    legacy_and_mode_switches(&stack);
    rejection_and_corruption(&stack);
    budget(&stack);
    ts_interop(&stack);
}

fn ts_peer(stack: &Stack, args: &[&str]) -> String {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/e2e/ts_peer.ts");
    let out = Command::new("bun")
        .arg(script)
        .arg(args[0])
        .arg(&stack.api)
        .args(&args[1..])
        .env("CV_ROOT", &stack.cv_root)
        .current_dir(stack.cv_root.join("ui"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Real `CareerVector` TS SDK appends (human class) reach a cvdb peer through the relay byte for byte.
fn ts_interop(stack: &Stack) {
    use base64::Engine as _;
    // T1 fixtures: produced by the SDK on a fresh append workspace; set CVDB_REGEN_FIXTURES=1 to refresh the checked-in copy.
    let fresh = stack.create_workspace();
    stack.set_write_path(&fresh, "append");
    let dir = tmp("fixtures");
    ts_peer(stack, &["fixtures", &fresh, dir.to_str().unwrap()]);
    if std::env::var("CVDB_REGEN_FIXTURES").is_ok() {
        let dest = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
        std::fs::create_dir_all(&dest).unwrap();
        for f in [
            "u0.bin",
            "u1.bin",
            "u2.bin",
            "frames.bin",
            "relay.bin",
            "expected.json",
            "source.txt",
        ] {
            std::fs::copy(dir.join(f), dest.join(f)).unwrap();
        }
    }

    // Live interop: a cvdb peer seeds, the TS SDK edits, the relay delivers the SDK's exact bytes.
    let ws = stack.create_workspace();
    stack.set_write_path(&ws, "append");
    let (spy, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let mut a = client(stack, &ws, &tmp("interop"), &spy, &clock);
    a.attach().unwrap();
    seed(&mut a);
    let (_live, rx) = a.start_live();
    a.set_live(true);
    wait_for(|| {
        matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Ok(Event::Connected)
        )
        .then_some(())
    });
    let ops = tmp("ops").join("ops.json");
    std::fs::write(
        &ops,
        serde_json::to_vec(&json!([
            set_cv("from the TS SDK"),
            set_cl("cl from the TS SDK")
        ]))
        .unwrap(),
    )
    .unwrap();
    let out = ts_peer(stack, &["edit", &ws, ops.to_str().unwrap()]);
    let accepted: Vec<(String, Vec<u8>)> = out
        .lines()
        .filter(|l| l.starts_with('{'))
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            (
                v["sub"].as_str().unwrap().to_owned(),
                base64::engine::general_purpose::STANDARD
                    .decode(v["update"].as_str().unwrap())
                    .unwrap(),
            )
        })
        .collect();
    assert_eq!(accepted.len(), 2, "{out}");
    let mut received = Vec::new();
    wait_for(|| {
        if let Ok(Event::Update(u)) = rx.recv_timeout(Duration::from_millis(50)) {
            received.push(u);
        }
        (received.len() >= 2).then_some(())
    });
    for (sub, bytes) in &accepted {
        assert!(
            received.contains(bytes),
            "the relay delivered the SDK's {sub} bytes exactly"
        );
    }
    for u in &received {
        a.apply_live(u).unwrap();
    }
    assert_eq!(cv_text(&a), "from the TS SDK");
    assert_eq!(cl_text(&a), "cl from the TS SDK");
    // A cold peer loads the same result (head plus the SDK's tail), and the SDK read the Rust peer's seed.
    let mut b = client(stack, &ws, &tmp("interop-b"), &Spy::new(), &clock);
    b.attach().unwrap();
    assert_eq!(cv_text(&b), "from the TS SDK");
    assert_human_only(&spy);
    println!("ts_interop ok");
}

fn cold_load_and_live(stack: &Stack) {
    let ws = stack.create_workspace();
    stack.set_write_path(&ws, "append");
    let (spy_a, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let mut a = client(stack, &ws, &tmp("a"), &spy_a, &clock);
    let report = a.attach().unwrap();
    assert!(report.online);
    assert_eq!(
        a.mode(),
        Some(Mode::Append),
        "mode detection through writePath"
    );
    seed(&mut a);
    assert_eq!(cv_text(&a), "Built things");

    // A FIXED node is still edited as a human (never as an agent).
    let fixed = json!({ "kind": "node.update", "branch_path": CV, "node_path": "root.item.b1", "patch": { "tier": "FIXED" } });
    assert!(matches!(a.edit(&fixed).unwrap(), EditOutcome::Saved { .. }));
    assert!(matches!(
        a.edit(&set_cv("fixed but human")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    for i in 0..30 {
        a.edit(&set_cv(&format!("tail {i}"))).unwrap();
    }

    // Cold load of both sub-documents by a second peer: head plus tail frames.
    let (spy_b, clock_b) = (Spy::new(), Arc::new(ManualClock::default()));
    let dir_b = tmp("b");
    let mut b = client(stack, &ws, &dir_b, &spy_b, &clock_b);
    b.attach().unwrap();
    assert_eq!(cv_text(&b), "tail 29");
    assert_eq!(cl_text(&b), "Dear team");
    assert_eq!(
        spy_b
            .recs()
            .iter()
            .filter(|r| r.path.starts_with("sub/"))
            .count(),
        2
    );

    // Second load: the cached state answers 304.
    b.flush_cache().unwrap();
    drop(b);
    let spy_b2 = Spy::new();
    let mut b = client(stack, &ws, &dir_b, &spy_b2, &clock_b);
    b.attach().unwrap();
    let subs: Vec<_> = spy_b2
        .recs()
        .into_iter()
        .filter(|r| r.path.starts_with("sub/"))
        .collect();
    assert_eq!(subs.len(), 2);
    assert!(
        subs.iter().all(|r| r.status == 304),
        "{:?}",
        subs.iter().map(|r| r.status).collect::<Vec<_>>()
    );
    assert_eq!(cv_text(&b), "tail 29");

    // Live: B receives A's next edit through the relay (a second peer stands in for a browser).
    let (_live, rx) = b.start_live();
    b.set_live(true);
    wait_for(|| {
        matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Ok(Event::Connected)
        )
        .then_some(())
    });
    a.edit(&set_cl("hello from A")).unwrap();
    let update = wait_for(|| match rx.recv_timeout(Duration::from_millis(50)) {
        Ok(Event::Update(u)) => Some(u),
        _ => None,
    });
    let applied = b.apply_live(&update).unwrap();
    assert!(applied.of(SubDoc::ClProfile).is_some());
    assert_eq!(cl_text(&b), "hello from A");
    assert_human_only(&spy_a);
    ws_clean(&spy_a, "never-in-a-path");
    println!("cold_load_and_live ok");
}

fn append_kill_restart_replay(stack: &Stack) {
    let ws = stack.create_workspace();
    stack.set_write_path(&ws, "append");
    let (spy, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let dir = tmp("replay");
    let mut a = client(stack, &ws, &dir, &spy, &clock);
    a.attach().unwrap();
    seed(&mut a);

    // Offline: committed locally and in the outbox.
    spy.offline.store(true, Ordering::SeqCst);
    assert_eq!(
        a.edit(&set_cv("offline edit")).unwrap(),
        EditOutcome::Queued
    );
    assert_eq!(a.queued(), 1);
    drop(a); // kill

    // Restart: the server receives the request but the answer is lost.
    let mut a = client(stack, &ws, &dir, &spy, &clock);
    assert_eq!(a.queued(), 1, "the outbox survived the restart");
    spy.offline.store(false, Ordering::SeqCst);
    spy.lose_response.store(true, Ordering::SeqCst);
    assert_eq!(a.replay().unwrap(), (0, 1));
    spy.lose_response.store(false, Ordering::SeqCst);
    drop(a);

    // Restart again: replay sends the same opId and the same bytes; the server answers duplicate.
    let mut a = client(stack, &ws, &dir, &spy, &clock);
    assert_eq!(a.replay().unwrap(), (1, 0));
    let posts = spy.posts("append");
    let last_two = &posts[posts.len() - 2..];
    assert_eq!(last_two[0].req, last_two[1].req, "same opId, same bytes");
    let first: Value = serde_json::from_slice(&last_two[0].resp).unwrap();
    let again: Value = serde_json::from_slice(&last_two[1].resp).unwrap();
    assert_eq!(first["written"], true);
    assert_eq!(again["written"], false, "duplicate opId is idempotent");
    assert_eq!(first["clock"], again["clock"]);
    let op_id = serde_json::from_slice::<Value>(&last_two[0].req).unwrap()["opId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(stack.ledger_rows(&ws, &op_id), 1, "one ledger row");
    assert_eq!(a.queued(), 0);
    assert_human_only(&spy);
    println!("append_kill_restart_replay ok");
}

fn legacy_and_mode_switches(stack: &Stack) {
    // A fresh workspace is on the legacy path: server-applied, never applied locally first.
    let ws = stack.create_workspace();
    let (spy, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let mut a = client(stack, &ws, &tmp("legacy"), &spy, &clock);
    a.attach().unwrap();
    assert_eq!(a.mode(), Some(Mode::Legacy));
    seed(&mut a);
    assert_eq!(cv_text(&a), "Built things", "one copy, no duplicate");
    assert!(matches!(
        a.edit(&set_cv("XX")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    assert_eq!(cv_text(&a), "XX", "the 'XX' hazard: no duplicated edit");
    assert_eq!(spy.posts("append").len(), 0);
    assert!(spy.posts("ops").len() >= 3);

    // Flag flips to append: noticed by the gated probe (at most every five minutes).
    stack.set_write_path(&ws, "append");
    a.edit(&set_cv("still legacy before the probe")).unwrap();
    assert_eq!(spy.posts("append").len(), 0);
    clock.advance(RESYNC_INTERVAL + Duration::from_secs(1));
    assert!(matches!(
        a.edit(&set_cv("now append")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    assert_eq!(a.mode(), Some(Mode::Append));
    assert_eq!(spy.posts("append").len(), 1);
    assert_eq!(cv_text(&a), "now append");

    // Flag goes dark mid-session: the 409 flips the mode, the op goes through /ops, one copy remains.
    stack.set_write_path(&ws, "legacy");
    assert!(matches!(
        a.edit(&set_cv("dark flag")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    assert_eq!(a.mode(), Some(Mode::Legacy));
    assert_eq!(a.queued(), 0);
    assert_eq!(cv_text(&a), "dark flag", "no duplicate after the flip");
    assert_human_only(&spy);
    println!("legacy_and_mode_switches ok");
}

fn rejection_and_corruption(stack: &Stack) {
    // A 422: a stale peer edits a node another peer deleted (legacy path, no local apply).
    let ws = stack.create_workspace();
    let (spy_a, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let mut a = client(stack, &ws, &tmp("rej-a"), &spy_a, &clock);
    a.attach().unwrap();
    seed(&mut a);
    let spy_b = Spy::new();
    let mut b = client(stack, &ws, &tmp("rej-b"), &spy_b, &clock);
    b.attach().unwrap();
    let del = json!({ "kind": "node.delete", "branch_path": CV, "node_path": "root.item.b1" });
    assert!(matches!(a.edit(&del).unwrap(), EditOutcome::Saved { .. }));
    let out = b.edit(&set_cv("edit of a deleted node")).unwrap();
    assert!(
        matches!(out, EditOutcome::Rejected(ref r) if r.starts_with("422")),
        "{out:?}"
    );
    assert_eq!(b.queued(), 0, "parked, never retried forever");
    let notices = b.take_notices();
    assert_eq!(notices.len(), 1);
    assert!(b.take_notices().is_empty(), "shown once");

    // Outbox corruption: a bad checksum is parked and shown once; nothing is sent for it.
    let ws = stack.create_workspace();
    stack.set_write_path(&ws, "append");
    let (spy, dir) = (Spy::new(), tmp("corrupt"));
    let mut c = client(stack, &ws, &dir, &spy, &clock);
    c.attach().unwrap();
    seed(&mut c);
    spy.offline.store(true, Ordering::SeqCst);
    assert_eq!(c.edit(&set_cv("lost?")).unwrap(), EditOutcome::Queued);
    drop(c);
    let p = dir.join("outbox.log");
    let mut text = std::fs::read_to_string(&p).unwrap();
    let at = text.find("E\t").unwrap() + 2;
    let flip = if &text[at..=at] == "0" { "1" } else { "0" };
    text.replace_range(at..=at, flip);
    std::fs::write(&p, text).unwrap();
    spy.offline.store(false, Ordering::SeqCst);
    let before = spy.posts("append").len();
    let mut c = client(stack, &ws, &dir, &spy, &clock);
    assert_eq!(c.queued(), 0);
    assert_eq!(c.take_notices().len(), 1);
    c.attach().unwrap();
    assert_eq!(
        spy.posts("append").len(),
        before,
        "the corrupt entry is never sent"
    );
    assert_eq!(c.take_notices(), [] as [cvdb::outbox::Parked; 0]);
    println!("rejection_and_corruption ok");
}

fn budget(stack: &Stack) {
    let ws = stack.create_workspace();
    stack.set_write_path(&ws, "append");
    let (spy, clock) = (Spy::new(), Arc::new(ManualClock::default()));
    let mut a = client(stack, &ws, &tmp("budget"), &spy, &clock);
    a.attach().unwrap();
    seed(&mut a);
    let counts = a.counts();
    let (api0, append0) = (counts.api_total(), counts.get(Kind::Append));
    let (_live, rx) = a.start_live();
    a.set_live(true);
    wait_for(|| {
        matches!(
            rx.recv_timeout(Duration::from_millis(50)),
            Ok(Event::Connected)
        )
        .then_some(())
    });

    // A scripted two-hour session: 150 committed edits, a fifth of them one batch across both profiles.
    let mut actions = 0;
    for i in 0..150 {
        clock.advance(Duration::from_secs(48));
        let op = if i % 5 == 0 {
            json!({ "kind": "batch", "ops": [
                { "subDoc": "cv-profile", "op": set_cv(&format!("cv {i}")) },
                { "subDoc": "cl-profile", "op": set_cl(&format!("cl {i}")) } ] })
        } else if i % 2 == 0 {
            set_cv(&format!("cv {i}"))
        } else {
            set_cl(&format!("cl {i}"))
        };
        let out = a.edit(&op).unwrap();
        assert!(matches!(out, EditOutcome::Saved { .. }), "{i}: {out:?}");
        a.maybe_resync().unwrap();
        actions += 1;
    }
    let appends = counts.get(Kind::Append) - append0;
    assert_eq!(
        appends, actions,
        "exactly one append per user action, batches included"
    );
    assert_eq!(
        counts.get(Kind::Workspace),
        1,
        "mode detection once per attach, never per edit"
    );
    let session_api = counts.api_total() - api0;
    println!(
        "budget: appends={appends} workspace_get={} sub_get={} upgrades={} api_total={} (session after seeding {session_api})",
        counts.get(Kind::Workspace),
        counts.get(Kind::Sub),
        counts.get(Kind::Upgrade),
        counts.api_total()
    );
    assert!(counts.api_total() <= 160, "{}", counts.api_total());
    assert_eq!(
        counts.get(Kind::Upgrade),
        1,
        "one relay upgrade, no reconnects"
    );

    // The resync flag raised a hundred times inside five minutes costs one resync.
    let sub0 = counts.get(Kind::Sub);
    for _ in 0..100 {
        a.apply_live(&[0xff, 0xff, 0xff, 0xff]).unwrap();
    }
    let first = counts.get(Kind::Sub) - sub0;
    assert!(first <= 2, "{first}");
    clock.advance(RESYNC_INTERVAL + Duration::from_secs(1));
    for _ in 0..100 {
        a.apply_live(&[0xff, 0xff, 0xff, 0xff]).unwrap();
    }
    let second = counts.get(Kind::Sub) - sub0;
    assert!(second <= 4, "{second}");
    println!("budget resync gate: {first} then {second} sub reads for 200 flagged frames");
    assert_human_only(&spy);
}
