//! Regression tests for the Opus review of 139bba6: each one fails without its fix.
use cvdb::client::Config;
use cvdb::http::Response;
use cvdb::{
    Clock, Cvdb, EditOutcome, Error, Event, HostEvent, Http, Kind, Link, Live, Mode, SystemClock,
};
use cvws::{ActorClass, SubDoc, Workspace};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SECRET: &str = "reviewworkspaceid77";

type Handler = Box<dyn Fn(&Req) -> Result<Response, Error> + Send + Sync>;

#[derive(Clone, Debug)]
struct Req {
    method: String,
    path: String,
    body: Vec<u8>,
}

struct Fake {
    handler: Mutex<Handler>,
    log: Mutex<Vec<Req>>,
}

impl Fake {
    fn new(h: impl Fn(&Req) -> Result<Response, Error> + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            handler: Mutex::new(Box::new(h)),
            log: Mutex::default(),
        })
    }
    fn posts(&self) -> Vec<Req> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .cloned()
            .collect()
    }
    fn paths(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect()
    }
}

impl Http for Fake {
    fn request(
        &self,
        method: &str,
        url: &str,
        _h: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, Error> {
        let path = url
            .split_once(SECRET)
            .map_or(url, |(_, p)| p)
            .trim_start_matches('/')
            .to_owned();
        let req = Req {
            method: method.to_owned(),
            path,
            body: body.map(<[u8]>::to_vec).unwrap_or_default(),
        };
        self.log.lock().unwrap().push(req.clone());
        (self.handler.lock().unwrap())(&req)
    }
}

fn resp(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Result<Response, Error> {
    Ok(Response {
        status,
        headers: headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        body: body.to_vec(),
    })
}

fn ok_json(body: &str) -> Result<Response, Error> {
    resp(200, &[], body.as_bytes())
}

fn ws_json(path: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"id": "x", "writePath": path})).unwrap()
}

/// A server that is in `mode`, holds nothing, and answers every write with `write`.
fn server(
    mode: &'static str,
    write: impl Fn(&Req) -> Result<Response, Error> + Send + Sync + 'static,
) -> Arc<Fake> {
    Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json(mode)),
        "GET" => resp(200, &[], &[0, 0]),
        _ => write(r),
    })
}

fn tmp(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cvdb-rev-{tag}-{}", fastrand::u64(..)))
}

fn open_in(dir: &Path, http: &Arc<Fake>) -> Result<Cvdb, Error> {
    open_with(dir, http, Arc::new(SystemClock::default()))
}

fn open_with(dir: &Path, http: &Arc<Fake>, clock: Arc<dyn Clock>) -> Result<Cvdb, Error> {
    let cfg = Config {
        api_base: "http://127.0.0.1:9".into(),
        relays: vec![],
        dir: dir.to_owned(),
    };
    Cvdb::open(Link::parse(SECRET).unwrap(), cfg, http.clone(), clock)
}

#[derive(Default)]
struct ManualClock(Mutex<Duration>);

impl ManualClock {
    fn advance(&self, d: Duration) {
        *self.0.lock().unwrap() += d;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}

fn node(id: &str, kind: &str, data: Value, ch: Vec<Value>) -> Value {
    let active: Vec<Value> = ch.iter().map(|c| c["id"].clone()).collect();
    json!({"id": id, "kind": kind, "tier": "HYBRID", "data": data, "selection": {"active": active}, "children": ch})
}

fn base_op(text: &str) -> Value {
    json!({"kind": "cv_profile.replace", "lang": "en", "tree": node("root", "cv-root", json!({}),
        vec![node("b1", "bullet", json!({"text": text}), vec![])])})
}

fn set_text(text: &str) -> Value {
    json!({"kind": "node.update", "branch_path": "cv_profile.en", "node_path": "root.b1", "patch": {"data": {"text": text}}})
}

fn add_bullet(id: &str) -> Value {
    json!({"kind": "node.create", "branch_path": "cv_profile.en", "parent_path": "root",
        "node": node(id, "bullet", json!({"text": id}), vec![])})
}

fn text(c: &Cvdb, path: &str) -> Option<String> {
    c.view(SubDoc::CvProfile, "cv_profile.en", path)
        .ok()
        .and_then(|v| v["data"]["text"].as_str().map(str::to_owned))
}

fn updates_of(body: &[u8]) -> Vec<Vec<u8>> {
    use base64::Engine as _;
    let v: Value = serde_json::from_slice(body).unwrap();
    v["updates"]
        .as_object()
        .unwrap()
        .values()
        .map(|b| {
            base64::engine::general_purpose::STANDARD
                .decode(b.as_str().unwrap())
                .unwrap()
        })
        .collect()
}

// ---- M1: only the API's own refusals park an entry ------------------------------------

#[test]
fn m1_catalog_mismatch_and_edge_pages_keep_entries_queued() {
    for (status, body) in [
        (426, &b"op_catalog_version_mismatch: please reload"[..]),
        (
            426,
            br#"{"error":"op_catalog_version_mismatch: client=9 server=10"}"#,
        ),
        (401, b"<html>sign in</html>"),
        (403, b"<html>forbidden</html>"),
        (404, b"<html>not found</html>"),
        (400, b"<html>bad gateway page</html>"),
    ] {
        let fake = server("append", move |_| resp(status, &[], body));
        let dir = tmp("m1");
        let mut c = open_in(&dir, &fake).unwrap();
        c.attach().unwrap();
        assert_eq!(
            c.edit(&base_op("one")).unwrap(),
            EditOutcome::Queued,
            "{status}"
        );
        assert_eq!(c.edit(&set_text("two")).unwrap(), EditOutcome::Queued);
        assert_eq!(c.queued(), 2, "{status}: nothing parked");
        assert!(c.take_notices().is_empty());
        assert_eq!(
            text(&c, "root.b1").as_deref(),
            Some("two"),
            "local copy kept"
        );
        // The first entry blocked the second: only one request per flush, in order.
        assert!(fake.posts().len() <= 4);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn m1_parked_entry_rederives_later_entries_after_a_reload() {
    // Seed the server with a base tree, then: A (422) is parked, B queued behind it.
    let base_bytes: Arc<Mutex<Vec<u8>>> = Arc::default();
    let n = Arc::new(AtomicUsize::new(0));
    let (bb, nn) = (base_bytes.clone(), n.clone());
    let fake = Fake::new(move |r| match (r.method.as_str(), r.path.as_str()) {
        ("GET", "") => resp(200, &[], &ws_json("append")),
        ("GET", p) if p.starts_with("sub/cv-profile") => {
            let head = bb.lock().unwrap().clone();
            resp(200, &[], if head.is_empty() { &[0, 0] } else { &head })
        }
        ("GET", _) => resp(200, &[], &[0, 0]),
        _ => match nn.fetch_add(1, Ordering::SeqCst) {
            0 => {
                *bb.lock().unwrap() = updates_of(&r.body).remove(0);
                ok_json(r#"{"ok":true,"clock":1}"#)
            }
            1 => resp(422, &[], br#"{"error":"op_apply_failed: boom"}"#),
            _ => ok_json(r#"{"ok":true,"clock":2}"#),
        },
    });
    let mut c = open_in(&tmp("m1b"), &fake).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    // A: refused by the server for good. B: an additive edit made after A.
    let a = c.edit(&add_bullet("a1")).unwrap();
    assert!(matches!(a, EditOutcome::Rejected(_)), "{a:?}");
    let b = c.edit(&add_bullet("b2")).unwrap();
    assert!(matches!(b, EditOutcome::Saved { .. }), "{b:?}");
    assert_eq!(c.queued(), 0);
    assert_eq!(c.take_notices().len(), 1);
    // The server holds only the base and B: B's bytes must apply to exactly that (they would wait
    // forever for A's clocks if they had been derived before the reload).
    let posts = fake.posts();
    let b_bytes = updates_of(&posts.last().unwrap().body).remove(0);
    let base = base_bytes.lock().unwrap().clone();
    let mut ws = Workspace::open(&[(SubDoc::CvProfile, base.as_slice())]).unwrap();
    ws.receive(&b_bytes, Some(SubDoc::CvProfile)).unwrap();
    assert!(
        ws.view(SubDoc::CvProfile, "cv_profile.en", "root.b2")
            .is_ok(),
        "B applies to the server's document"
    );
    assert!(
        ws.view(SubDoc::CvProfile, "cv_profile.en", "root.a1")
            .is_err()
    );
    // And the local view agrees: B kept, the refused A gone.
    assert!(text(&c, "root.b2").is_some());
    assert!(text(&c, "root.a1").is_none());
}

// ---- M2: an edit queues behind older entries -----------------------------------------

#[test]
fn m2_edits_are_sent_in_order_behind_a_queued_one() {
    let n = Arc::new(AtomicUsize::new(0));
    let nn = n.clone();
    let fake = server("legacy", move |_| {
        if nn.fetch_add(1, Ordering::SeqCst) == 0 {
            resp(503, &[], b"busy")
        } else {
            ok_json(r#"{"ok":true}"#)
        }
    });
    let mut c = open_in(&tmp("m2"), &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("A")).unwrap(), EditOutcome::Queued);
    assert!(matches!(
        c.edit(&base_op("B")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    assert_eq!(c.queued(), 0);
    let texts: Vec<String> = fake
        .posts()
        .iter()
        .map(|r| {
            serde_json::from_slice::<Value>(&r.body).unwrap()["op"]["tree"]["children"][0]["data"]["text"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    // 503 for A, then A again (first), then B: the older edit is never overtaken.
    assert_eq!(texts, ["A", "A", "B"]);
}

// ---- M3: only NotFound is an empty outbox --------------------------------------------

#[test]
fn m3_unreadable_outbox_is_never_replaced_by_an_empty_one() {
    let fake = server("append", |_| Err(Error::Offline));
    let dir = tmp("m3");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("keep me")).unwrap(), EditOutcome::Queued);
    drop(c);
    // A read error that is not NotFound (here EISDIR: open() also repairs bad modes, so a
    // chmod would no longer make a file unreadable).
    let log = dir.join("outbox.log");
    let saved = dir.join("outbox.saved");
    std::fs::rename(&log, &saved).unwrap();
    std::fs::create_dir(&log).unwrap();
    assert!(
        open_in(&dir, &fake).is_err(),
        "open must fail, not start empty"
    );
    assert!(log.is_dir(), "nothing was replaced");
    std::fs::remove_dir(&log).unwrap();
    std::fs::rename(&saved, &log).unwrap();
    assert_eq!(open_in(&dir, &fake).unwrap().queued(), 1);
}

// ---- S1: a crash does not lose the queued edit from the view -------------------------

#[test]
fn s1_queued_edits_are_in_the_view_after_a_restart() {
    let fake = server("append", |_| Err(Error::Offline));
    let dir = tmp("s1");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("queued")).unwrap(), EditOutcome::Queued);
    drop(c); // crash: no flush_cache
    let c = open_in(&dir, &fake).unwrap();
    assert_eq!(text(&c, "root.b1").as_deref(), Some("queued"));
}

// ---- S2: a failed outbox write keeps the bytes ---------------------------------------

#[test]
fn s2_disk_full_hands_the_bytes_back() {
    let sent: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let s2 = sent.clone();
    let fake = server("append", move |r| {
        s2.lock().unwrap().push(r.body.clone());
        ok_json(r#"{"ok":true,"clock":1}"#)
    });
    let mut c = open_in(&tmp("s2"), &fake).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    let base = updates_of(&sent.lock().unwrap()[0]).remove(0);
    c.inject_outbox_failure(true).unwrap();
    assert!(matches!(c.edit(&add_bullet("a1")), Err(Error::Io(_))));
    assert_eq!(c.queued(), 0);
    c.inject_outbox_failure(false).unwrap();
    assert!(matches!(
        c.edit(&add_bullet("b2")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    // The next update carries A's bytes too: a peer holding only the base integrates both.
    let last = updates_of(&sent.lock().unwrap().last().unwrap().clone()).remove(0);
    let mut ws = Workspace::open(&[(SubDoc::CvProfile, base.as_slice())]).unwrap();
    ws.receive(&last, Some(SubDoc::CvProfile)).unwrap();
    for id in ["root.a1", "root.b2"] {
        assert!(
            ws.view(SubDoc::CvProfile, "cv_profile.en", id).is_ok(),
            "{id}"
        );
    }
}

#[test]
fn s2_failed_write_leaves_no_partial_line() {
    use cvdb::outbox::{Entry, EntryKind, Outbox};
    let dir = tmp("s2b");
    let mut o = Outbox::open(&dir).unwrap();
    let e = Entry {
        op_id: "abcdefgh".into(),
        kind: EntryKind::Ops,
        op: json!({"kind": "x"}),
        updates: std::collections::BTreeMap::default(),
    };
    o.inject_write_failure().unwrap();
    assert!(o.append(&e).is_err());
    assert!(o.pending().is_empty());
    o.repair().unwrap();
    o.append(&e).unwrap();
    drop(o);
    let mut o = Outbox::open(&dir).unwrap();
    assert_eq!(o.pending().len(), 1);
    assert!(o.take_notices().is_empty());
}

// ---- S3: a flip while offline converts, queues and reloads later ----------------------

#[test]
fn s3_flip_to_legacy_while_offline_stays_queued_then_reloads() {
    let ops_online = Arc::new(Mutex::new(false));
    let online = ops_online.clone();
    let fake = Fake::new(move |r| match (r.method.as_str(), r.path.as_str()) {
        ("GET", "") => resp(200, &[], &ws_json("append")),
        ("GET", _) => resp(200, &[], &[0, 0]),
        (_, "append") => resp(409, &[], br#"{"error":"not_on_append_path: use /ops"}"#),
        (_, "ops") if *online.lock().unwrap() => ok_json(r#"{"ok":true}"#),
        _ => Err(Error::Offline),
    });
    let dir = tmp("s3");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    // The 409 flips the mode; /ops is unreachable: the edit is NOT reported saved.
    assert_eq!(c.edit(&base_op("flip")).unwrap(), EditOutcome::Queued);
    assert_eq!(c.mode(), Some(Mode::Legacy));
    assert_eq!(c.queued(), 1);
    drop(c);
    // Restart (still offline for /ops): the entry is an /ops entry, the reload is still owed.
    let mut c = open_in(&dir, &fake).unwrap();
    assert_eq!(c.queued(), 1);
    *ops_online.lock().unwrap() = true;
    let before = fake.paths().len();
    c.attach().unwrap();
    assert_eq!(c.queued(), 0);
    let after: Vec<String> = fake.paths()[before..].to_vec();
    let reload = after
        .iter()
        .position(|p| p.starts_with("GET sub/cv-profile"))
        .unwrap();
    let send = after.iter().position(|p| p == "POST ops").unwrap();
    assert!(
        reload < send,
        "the optimistic copy is reloaded before the server's bytes arrive: {after:?}"
    );
}

// ---- S7: one instance per directory ---------------------------------------------------

#[test]
fn s7_second_instance_on_a_directory_is_refused() {
    let fake = server("append", |_| Err(Error::Offline));
    let dir = tmp("s7");
    let first = open_in(&dir, &fake).unwrap();
    assert!(matches!(open_in(&dir, &fake), Err(Error::Locked)));
    drop(first);
    assert!(open_in(&dir, &fake).is_ok());
}

// ---- N1: modes ------------------------------------------------------------------------

#[test]
fn n1_directory_and_parked_log_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let fake = server("append", |_| {
        resp(422, &[], br#"{"error":"op_apply_failed: no"}"#)
    });
    let dir = tmp("n1");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("x")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    let mode = |p: PathBuf| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(dir.clone()), 0o700);
    assert_eq!(mode(dir.join("parked.log")), 0o600);
    assert_eq!(mode(dir.join("outbox.log")), 0o600);
}

// ---- S4: the live relay ---------------------------------------------------------------

#[test]
fn s4_a_4xx_upgrade_stops_the_thread_after_one_attempt() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for s in listener.incoming() {
            let mut s = s.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf);
            let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
        }
    });
    let counts = cvdb::Counts::default();
    let (tx, rx) = std::sync::mpsc::channel();
    let _live = Live::spawn(
        vec![format!("http://127.0.0.1:{port}")],
        "abcdef".into(),
        "dev".into(),
        counts.clone(),
        tx,
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        Event::Gone(404)
    );
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(counts.get(Kind::Upgrade), 1, "a 404 is not retried");
}

#[test]
fn s4_dropping_live_never_blocks_on_a_stalled_handshake() {
    // A server that accepts the TCP connection and never answers the handshake.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let held = std::thread::spawn(move || {
        let _conn = listener.accept().unwrap();
        std::thread::sleep(Duration::from_secs(15));
    });
    let (tx, _rx) = std::sync::mpsc::channel();
    let live = Live::spawn(
        vec![format!("http://127.0.0.1:{port}")],
        "abcdef".into(),
        "dev".into(),
        cvdb::Counts::default(),
        tx,
    );
    std::thread::sleep(Duration::from_millis(300));
    let t = std::time::Instant::now();
    drop(live);
    assert!(
        t.elapsed() < Duration::from_secs(1),
        "drop returned at once"
    );
    drop(held);
}

#[test]
fn only_human_class_is_ever_sent() {
    let fake = server("append", |_| ok_json(r#"{"ok":true,"clock":1}"#));
    let mut c = open_in(&tmp("human"), &fake).unwrap();
    c.attach().unwrap();
    c.edit(&base_op("h")).unwrap();
    for r in fake.posts() {
        let v: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["actorClass"], "human");
        assert!(v.get("agentId").is_none());
    }
    let _ = ActorClass::Human;
}

// ======================= second review ===============================================

/// Virtual time for the reconnect loop: a sleep advances the clock and returns at once.
struct SimPace(Mutex<Duration>);

impl cvdb::live::Pace for SimPace {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
    fn sleep(&self, d: Duration, stop: &std::sync::atomic::AtomicBool) {
        if !stop.load(Ordering::SeqCst) {
            *self.0.lock().unwrap() += d;
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

fn upgrading_then_closing_relay() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            // Complete the WebSocket upgrade, then drop the connection.
            drop(tungstenite::accept(s));
        }
    });
    port
}

#[test]
fn must1_a_relay_that_accepts_then_closes_is_backed_off() {
    let port = upgrading_then_closing_relay();
    let counts = cvdb::Counts::default();
    let pace = Arc::new(SimPace(Mutex::default()));
    let (tx, rx) = std::sync::mpsc::channel();
    let live = Live::spawn_with(
        vec![format!("http://127.0.0.1:{port}")],
        "abcdef".into(),
        "dev".into(),
        counts.clone(),
        tx,
        pace.clone(),
    );
    // Ten simulated minutes (or eight real seconds, which only a hot loop would run into).
    let start = std::time::Instant::now();
    while *pace.0.lock().unwrap() < Duration::from_secs(600)
        && start.elapsed() < Duration::from_secs(8)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    live.stop();
    let simulated = *pace.0.lock().unwrap();
    let upgrades = counts.get(Kind::Upgrade);
    assert!(
        simulated >= Duration::from_secs(600),
        "backoff delays ran: {simulated:?}, {upgrades} upgrades"
    );
    assert!(
        upgrades <= 19,
        "{upgrades} upgrades in ten simulated minutes"
    );
    assert!(rx.try_iter().any(|e| e == Event::Connected));
    println!("relay accepts then closes: {upgrades} upgrades in {simulated:?} simulated");
}

/// A relay that completes the upgrade and stays open; counts accepted connections.
fn steady_relay() -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = n.clone();
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            if let Ok(ws) = tungstenite::accept(s) {
                seen.fetch_add(1, Ordering::SeqCst);
                std::thread::spawn(move || {
                    let mut ws = ws;
                    while ws.read().is_ok() {}
                });
            }
        }
    });
    (port, n)
}

#[test]
fn walker_moves_to_the_second_relay_after_two_bad_closes() {
    let bad = upgrading_then_closing_relay();
    let (good, good_n) = steady_relay();
    let counts = cvdb::Counts::default();
    let pace = Arc::new(SimPace(Mutex::default()));
    let (tx, _rx) = std::sync::mpsc::channel();
    let live = Live::spawn_with(
        vec![
            format!("http://127.0.0.1:{bad}"),
            format!("http://127.0.0.1:{good}"),
        ],
        "abcdef".into(),
        "dev".into(),
        counts.clone(),
        tx,
        pace,
    );
    let start = std::time::Instant::now();
    while good_n.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_secs(8) {
        std::thread::sleep(Duration::from_millis(10));
    }
    live.stop();
    assert_eq!(
        good_n.load(Ordering::SeqCst),
        1,
        "settled on the fallback relay"
    );
    // bad, bad (two closes), then the good one: three upgrades, not an endless CF loop.
    assert_eq!(
        counts.get(Kind::Upgrade),
        3,
        "{}",
        counts.get(Kind::Upgrade)
    );
}

#[test]
fn walker_budget_with_both_relays_closing_stays_bounded() {
    let a = upgrading_then_closing_relay();
    let b = upgrading_then_closing_relay();
    let counts = cvdb::Counts::default();
    let pace = Arc::new(SimPace(Mutex::default()));
    let (tx, _rx) = std::sync::mpsc::channel();
    let live = Live::spawn_with(
        vec![
            format!("http://127.0.0.1:{a}"),
            format!("http://127.0.0.1:{b}"),
        ],
        "abcdef".into(),
        "dev".into(),
        counts.clone(),
        tx,
        pace.clone(),
    );
    let start = std::time::Instant::now();
    while *pace.0.lock().unwrap() < Duration::from_secs(600)
        && start.elapsed() < Duration::from_secs(8)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    live.stop();
    let upgrades = counts.get(Kind::Upgrade);
    assert!(*pace.0.lock().unwrap() >= Duration::from_secs(600));
    // Still one upgrade per backoff step: a second relay adds no attempts when the first accepts.
    assert!(
        upgrades <= 19,
        "{upgrades} upgrades in ten simulated minutes"
    );
}

#[test]
fn s4_two_relays_that_both_answer_404_stop() {
    use std::io::{Read, Write};
    let relay = || {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for s in l.incoming().flatten() {
                let mut s = s;
                let mut b = [0u8; 1024];
                let _ = s.read(&mut b);
                let _ = s.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
            }
        });
        format!("http://127.0.0.1:{port}")
    };
    let counts = cvdb::Counts::default();
    let (tx, rx) = std::sync::mpsc::channel();
    let _live = Live::spawn(
        vec![relay(), relay()],
        "abcdef".into(),
        "dev".into(),
        counts.clone(),
        tx,
    );
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        Event::Gone(404)
    );
    std::thread::sleep(Duration::from_millis(1200));
    assert_eq!(
        counts.get(Kind::Upgrade),
        2,
        "one walk of both relays, then it stopped"
    );
}

#[test]
fn live_stop_never_blocks() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let _c = listener.accept().unwrap();
        std::thread::sleep(Duration::from_secs(15));
    });
    let (tx, _rx) = std::sync::mpsc::channel();
    let live = Live::spawn(
        vec![format!("http://127.0.0.1:{port}")],
        "abcdef".into(),
        "dev".into(),
        cvdb::Counts::default(),
        tx,
    );
    std::thread::sleep(Duration::from_millis(300));
    let t = std::time::Instant::now();
    live.stop();
    assert!(t.elapsed() < Duration::from_secs(1));
}

fn bullet_without_id(text: &str) -> Value {
    json!({"kind": "node.create", "branch_path": "cv_profile.en", "parent_path": "root",
        "node": {"kind": "bullet", "tier": "HYBRID", "data": {"text": text}, "selection": {"active": []}, "children": []}})
}

#[test]
fn s3_a_crash_between_park_and_rewrite_never_resends_stale_bytes() {
    let online = Arc::new(Mutex::new(false));
    let on = online.clone();
    let first = Arc::new(AtomicUsize::new(0));
    let fi = first.clone();
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json("append")),
        "GET" => resp(200, &[], &[0, 0]),
        _ if !*on.lock().unwrap() => Err(Error::Offline),
        _ if fi.fetch_add(1, Ordering::SeqCst) == 0 => {
            resp(422, &[], br#"{"error":"op_apply_failed: no"}"#)
        }
        _ => ok_json(r#"{"ok":true,"clock":1}"#),
    });
    let dir = tmp("s3crash");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    c.edit(&base_op("A")).unwrap(); // queued (offline)
    c.edit(&add_bullet("b2")).unwrap(); // queued behind it, bytes depend on A
    let a_id = serde_json::from_slice::<Value>(&fake.posts()[0].body).unwrap()["opId"]
        .as_str()
        .unwrap()
        .to_owned();
    *online.lock().unwrap() = true;
    c.inject_outbox_rewrite_failure(true);
    assert!(
        c.replay().is_err(),
        "the crash window: parked line written, rewrite failed"
    );
    drop(c);
    let before = fake.posts().len();
    fake.log.lock().unwrap().clear();
    first.store(1, Ordering::SeqCst); // everything the server sees from now on is accepted
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    let ids: Vec<String> = fake
        .posts()
        .iter()
        .map(|r| {
            serde_json::from_slice::<Value>(&r.body).unwrap()["opId"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        ids.first(),
        Some(&a_id),
        "A is sent again before anything that depends on it ({before} earlier posts)"
    );
}

#[test]
fn s5_a_legacy_undo_of_a_queued_edit_is_still_queued() {
    // The server holds "Senior"; offline the user changes it to Lead and back.
    let mut ws = Workspace::open(&[]).unwrap();
    ws.apply(&base_op("Senior"), ActorClass::Human).unwrap();
    let head = ws.take_outgoing().unwrap().remove(0).update;
    let online = Arc::new(Mutex::new(false));
    let on = online.clone();
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json("legacy")),
        "GET" if r.path == "sub/cv-profile" => resp(200, &[], &head),
        "GET" => resp(200, &[], &[0, 0]),
        _ if *on.lock().unwrap() => ok_json(r#"{"ok":true}"#),
        _ => Err(Error::Offline),
    });
    let mut c = open_in(&tmp("s5"), &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&set_text("Lead")).unwrap(), EditOutcome::Queued);
    assert_eq!(
        c.edit(&set_text("Senior")).unwrap(),
        EditOutcome::Queued,
        "the undo is a real edit"
    );
    assert_eq!(c.queued(), 2);
    *online.lock().unwrap() = true;
    c.replay().unwrap();
    let texts: Vec<String> = fake
        .posts()
        .iter()
        .filter(|r| r.path == "ops")
        .filter_map(|r| {
            serde_json::from_slice::<Value>(&r.body).ok()?["op"]["patch"]["data"]["text"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(
        texts.last().map(String::as_str),
        Some("Senior"),
        "the last committed value is the last one sent: {texts:?}"
    );
}

#[test]
fn note_426_is_reported_to_the_host_once() {
    let fake = server("append", |_| resp(426, &[], b"please reload"));
    let mut c = open_in(&tmp("n426"), &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("x")).unwrap(), EditOutcome::Queued);
    assert_eq!(c.edit(&set_text("y")).unwrap(), EditOutcome::Queued);
    assert_eq!(c.take_events(), vec![HostEvent::Outdated]);
    assert!(c.take_events().is_empty());
    // A later 426 in the same session is not reported again.
    assert_eq!(c.edit(&set_text("z")).unwrap(), EditOutcome::Queued);
    assert!(c.take_events().is_empty(), "once per session");
}

#[test]
fn note_a_409_on_an_ops_entry_is_not_a_flip() {
    let fake = server("legacy", |_| {
        resp(409, &[], br#"{"error":"not_on_append_path: x"}"#)
    });
    let mut c = open_in(&tmp("n409"), &fake).unwrap();
    c.attach().unwrap();
    let t = std::time::Instant::now();
    assert_eq!(c.edit(&base_op("x")).unwrap(), EditOutcome::Queued);
    assert!(t.elapsed() < Duration::from_secs(2), "no spin");
    assert_eq!(fake.posts().len(), 1, "sent once, not looped");
    assert_eq!(c.mode(), Some(Mode::Legacy));
}

#[test]
fn note_existing_directory_and_files_are_tightened() {
    use std::os::unix::fs::PermissionsExt;
    let fake = server("append", |_| Err(Error::Offline));
    let dir = tmp("ntight");
    std::fs::create_dir_all(dir.join("cache")).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(dir.join("parked.log"), b"x\n").unwrap();
    std::fs::set_permissions(
        dir.join("parked.log"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    let _c = open_in(&dir, &fake).unwrap();
    let mode = |p: PathBuf| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(dir.clone()), 0o700);
    assert_eq!(mode(dir.join("parked.log")), 0o600);
}

/// A server in append mode that keeps the bytes of the first append as the stored head of
/// `cv-profile` (what a reload reads), with switches for failing reads and a failing shadow.
struct HeadServer {
    fake: Arc<Fake>,
    reads_ok: Arc<Mutex<bool>>,
    reads_500: Arc<Mutex<bool>>,
    online: Arc<Mutex<bool>>,
}

fn head_server(
    shadow_500: bool,
    write: impl Fn(usize) -> Result<Response, Error> + Send + Sync + 'static,
) -> HeadServer {
    let head: Arc<Mutex<Vec<u8>>> = Arc::default();
    let reads_ok = Arc::new(Mutex::new(true));
    let online = Arc::new(Mutex::new(true));
    let reads_500 = Arc::new(Mutex::new(false));
    let n = Arc::new(AtomicUsize::new(0));
    let (h, ro, on, r5) = (
        head.clone(),
        reads_ok.clone(),
        online.clone(),
        reads_500.clone(),
    );
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json("append")),
        "GET" if !*ro.lock().unwrap() && r.path.starts_with("sub/c") => Err(Error::Offline),
        "GET" if *r5.lock().unwrap() && r.path.starts_with("sub/c") => resp(500, &[], b"boom"),
        "GET" if shadow_500 && r.path == "sub/jobs" => resp(500, &[], b"boom"),
        "GET" if r.path == "sub/cv-profile" => {
            let b = h.lock().unwrap().clone();
            resp(200, &[], if b.is_empty() { &[0, 0] } else { &b })
        }
        "GET" => resp(200, &[], &[0, 0]),
        _ if !*on.lock().unwrap() => Err(Error::Offline),
        _ => {
            let i = n.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                *h.lock().unwrap() = updates_of(&r.body).remove(0);
            }
            write(i)
        }
    });
    HeadServer {
        fake,
        reads_ok,
        reads_500,
        online,
    }
}

fn refuse() -> Result<Response, Error> {
    resp(422, &[], br#"{"error":"op_apply_failed: no"}"#)
}

#[test]
fn must2_rederived_entries_keep_the_stamped_ids() {
    // Online: the base is saved. Offline: A (will be refused), B creates a bullet without an
    // id, C edits that bullet by the id the engine minted.
    let srv = head_server(false, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_in(&tmp("must2"), &srv.fake).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    *srv.online.lock().unwrap() = false;
    assert_eq!(c.edit(&set_text("A")).unwrap(), EditOutcome::Queued);
    assert_eq!(
        c.edit(&bullet_without_id("new")).unwrap(),
        EditOutcome::Queued
    );
    let root = c.view(SubDoc::CvProfile, "cv_profile.en", "root").unwrap();
    let id = root["children"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|n| n["id"].as_str())
        .find(|i| *i != "b1")
        .expect("an id was minted")
        .to_owned();
    let edit = json!({"kind": "node.update", "branch_path": "cv_profile.en",
        "node_path": format!("root.{id}"), "patch": {"data": {"text": "typed"}}});
    assert_eq!(c.edit(&edit).unwrap(), EditOutcome::Queued);
    assert_eq!(c.queued(), 3);
    *srv.online.lock().unwrap() = true;
    c.replay().unwrap();
    // A was refused; the create and the edit were derived again with the SAME id.
    assert_eq!(c.queued(), 0);
    let notices = c.take_notices();
    assert_eq!(
        notices.len(),
        1,
        "{notices:?}: only the refused entry is parked"
    );
    assert_eq!(text(&c, &format!("root.{id}")).as_deref(), Some("typed"));
}

#[test]
fn s4_a_failed_rebuild_leaves_the_document_and_is_gated() {
    let clock = Arc::new(ManualClock::default());
    let srv = head_server(false, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_with(&tmp("s4rb"), &srv.fake, clock.clone()).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    *srv.reads_500.lock().unwrap() = true;
    assert!(matches!(
        c.edit(&add_bullet("a1")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    // The reload cannot happen: the document the user sees is untouched, the next edit queues.
    assert_eq!(text(&c, "root.b1").as_deref(), Some("base"), "view kept");
    assert_eq!(c.edit(&add_bullet("b2")).unwrap(), EditOutcome::Queued);
    let cv_reads = || {
        srv.fake
            .paths()
            .iter()
            .filter(|p| *p == "GET sub/cv-profile")
            .count()
    };
    let before = cv_reads();
    for _ in 0..5 {
        c.replay().unwrap();
    }
    assert_eq!(
        cv_reads(),
        before,
        "a failed reload is not retried within five minutes"
    );
    *srv.reads_500.lock().unwrap() = false;
    clock.advance(Duration::from_secs(301));
    c.replay().unwrap();
    assert_eq!(
        c.queued(),
        0,
        "after the gate the reload succeeds and the entry is sent"
    );
    assert!(text(&c, "root.b2").is_some());
}

#[test]
fn s4_a_failing_shadow_blocks_neither_attach_nor_edits() {
    let srv = head_server(true, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_in(&tmp("s4sh"), &srv.fake).unwrap();
    c.attach().expect("attach survives a failing shadow");
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    assert!(matches!(
        c.edit(&add_bullet("a1")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    let b = c.edit(&add_bullet("b2")).unwrap();
    assert!(matches!(b, EditOutcome::Saved { .. }), "{b:?}");
    let reads = srv
        .fake
        .paths()
        .iter()
        .filter(|p| p.starts_with("GET sub/"))
        .count();
    // attach: 2 + 5; one reload: 2 + 5. Nothing multiplies.
    assert!(reads <= 14, "{reads} sub reads");
}

#[test]
fn note_a_reapplied_no_op_reports_no_change() {
    // Offline: A changes the text. Online, A is refused; B (made after A) sets the text back
    // to what the server holds: after the reload it is a no-op.
    let srv = head_server(false, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_in(&tmp("nnoop"), &srv.fake).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("same")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    *srv.online.lock().unwrap() = false;
    assert_eq!(c.edit(&set_text("changed")).unwrap(), EditOutcome::Queued);
    *srv.online.lock().unwrap() = true;
    assert_eq!(c.edit(&set_text("same")).unwrap(), EditOutcome::NoChange);
    assert_eq!(c.queued(), 0);
}

// ======================= third review follow-up =======================================

#[test]
fn offline_does_not_hold_the_reload_off() {
    let clock = Arc::new(ManualClock::default());
    let srv = head_server(false, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_with(&tmp("offhold"), &srv.fake, clock).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    *srv.reads_ok.lock().unwrap() = false;
    assert!(matches!(
        c.edit(&add_bullet("a1")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    assert_eq!(c.edit(&add_bullet("b2")).unwrap(), EditOutcome::Queued);
    // The network comes back: no five-minute wait, the next flush reloads and sends.
    *srv.reads_ok.lock().unwrap() = true;
    c.replay().unwrap();
    assert_eq!(c.queued(), 0);
}

#[test]
fn a_successful_probe_clears_the_reload_hold() {
    let clock = Arc::new(ManualClock::default());
    let srv = head_server(false, |i| {
        if i == 1 {
            refuse()
        } else {
            ok_json(r#"{"ok":true,"clock":1}"#)
        }
    });
    let mut c = open_with(&tmp("probehold"), &srv.fake, clock).unwrap();
    c.attach().unwrap();
    assert!(matches!(
        c.edit(&base_op("base")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    *srv.reads_500.lock().unwrap() = true;
    assert!(matches!(
        c.edit(&add_bullet("a1")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    assert_eq!(c.edit(&add_bullet("b2")).unwrap(), EditOutcome::Queued);
    // The server answered 500: held off. Healthy again, and a probe that succeeds clears the hold.
    *srv.reads_500.lock().unwrap() = false;
    c.replay().unwrap();
    assert_eq!(c.queued(), 1, "still held off");
    c.detect_mode().unwrap();
    c.replay().unwrap();
    assert_eq!(c.queued(), 0);
}

#[test]
fn s5_a_just_saved_write_whose_echo_is_outstanding_is_not_undone_into_nothing() {
    let mut ws = Workspace::open(&[]).unwrap();
    ws.apply(&base_op("Senior"), ActorClass::Human).unwrap();
    let head = ws.take_outgoing().unwrap().remove(0).update;
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json("legacy")),
        "GET" if r.path == "sub/cv-profile" => resp(200, &[], &head),
        "GET" => resp(200, &[], &[0, 0]),
        _ => ok_json(r#"{"ok":true}"#),
    });
    let mut c = open_in(&tmp("s5echo"), &fake).unwrap();
    c.attach().unwrap();
    c.set_live(true); // the relay will deliver the server's bytes; none arrived yet
    assert!(matches!(
        c.edit(&set_text("Lead")).unwrap(),
        EditOutcome::Saved { .. }
    ));
    // The user reverts before the echo arrives: the local document still says Senior.
    let out = c.edit(&set_text("Senior")).unwrap();
    assert!(matches!(out, EditOutcome::Saved { .. }), "{out:?}");
    let sent: Vec<String> = fake
        .posts()
        .iter()
        .filter_map(|r| {
            serde_json::from_slice::<Value>(&r.body).ok()?["op"]["patch"]["data"]["text"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(sent, ["Lead", "Senior"]);
}

#[test]
fn s5_append_mode_with_server_applied_entries_queued_joins_them() {
    let mode = Arc::new(Mutex::new("legacy"));
    let writes = Arc::new(Mutex::new(false));
    let (m, w) = (mode.clone(), writes.clone());
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json(*m.lock().unwrap())),
        "GET" => resp(200, &[], &[0, 0]),
        _ if *w.lock().unwrap() => ok_json(r#"{"ok":true,"clock":1}"#),
        _ => Err(Error::Offline),
    });
    let clock = Arc::new(ManualClock::default());
    let mut c = open_with(&tmp("s5ops"), &fake, clock.clone()).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("one")).unwrap(), EditOutcome::Queued); // /ops entry, offline
    *mode.lock().unwrap() = "append";
    clock.advance(Duration::from_secs(301));
    assert_eq!(c.edit(&base_op("two")).unwrap(), EditOutcome::Queued);
    assert_eq!(
        c.mode(),
        Some(Mode::Append),
        "the probe noticed the flip back"
    );
    *writes.lock().unwrap() = true;
    c.replay().unwrap();
    let paths: Vec<String> = fake.posts().iter().map(|r| r.path.clone()).collect();
    assert!(
        paths.iter().all(|p| p == "ops"),
        "both go the same way, in order: {paths:?}"
    );
    assert_eq!(c.queued(), 0);
}

#[test]
fn reapply_stamps_ids_when_the_reload_finally_allows_it() {
    // The server's document has `root.item`; the client's stale one does not.
    let mut w1 = Workspace::open(&[]).unwrap();
    w1.apply(&base_op("base"), ActorClass::Human).unwrap();
    let head1 = w1.take_outgoing().unwrap().remove(0).update;
    let tree2 = json!({"kind": "cv_profile.replace", "lang": "en", "tree": node("root", "cv-root", json!({}),
        vec![node("item", "list-item", json!({"heading": "H"}), vec![node("b1", "bullet", json!({"text": "base"}), vec![])])])});
    let mut w2 = Workspace::open(&[]).unwrap();
    w2.apply(&tree2, ActorClass::Human).unwrap();
    let head2 = w2.take_outgoing().unwrap().remove(0).update;
    let head: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(head1));
    let reads_ok = Arc::new(Mutex::new(true));
    let n = Arc::new(AtomicUsize::new(0));
    let (h, ro, nn) = (head.clone(), reads_ok.clone(), n.clone());
    let fake = Fake::new(move |r| match r.method.as_str() {
        "GET" if r.path.is_empty() => resp(200, &[], &ws_json("append")),
        "GET" if !*ro.lock().unwrap() && r.path.starts_with("sub/c") => Err(Error::Offline),
        "GET" if r.path == "sub/cv-profile" => resp(200, &[], &h.lock().unwrap().clone()),
        "GET" => resp(200, &[], &[0, 0]),
        _ => {
            if nn.fetch_add(1, Ordering::SeqCst) == 0 {
                refuse()
            } else {
                ok_json(r#"{"ok":true,"clock":1}"#)
            }
        }
    });
    let mut c = open_in(&tmp("restamp"), &fake).unwrap();
    c.attach().unwrap();
    *reads_ok.lock().unwrap() = false;
    // A is refused and the reload cannot happen; B is born a Reapply entry against the stale document.
    assert!(matches!(
        c.edit(&set_text("A")).unwrap(),
        EditOutcome::Rejected(_)
    ));
    let under_item = json!({"kind": "node.create", "branch_path": "cv_profile.en", "parent_path": "root.item",
        "node": {"kind": "bullet", "tier": "HYBRID", "data": {"text": "new"}, "selection": {"active": []}, "children": []}});
    assert_eq!(c.edit(&under_item).unwrap(), EditOutcome::Queued);
    *head.lock().unwrap() = head2;
    *reads_ok.lock().unwrap() = true;
    c.replay().unwrap();
    assert_eq!(c.queued(), 0);
    let sent: Value = serde_json::from_slice(&fake.posts().last().unwrap().body).unwrap();
    let id = sent["op"]["node"]["id"]
        .as_str()
        .expect("the stored envelope carries the stamped id");
    let item = c
        .view(SubDoc::CvProfile, "cv_profile.en", "root.item")
        .unwrap();
    assert!(
        item["children"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["id"] == id)
    );
}
