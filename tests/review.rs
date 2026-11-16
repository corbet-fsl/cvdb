//! Regression tests for the Opus review of 139bba6: each one fails without its fix.
use cvdb::client::Config;
use cvdb::http::Response;
use cvdb::{Clock, Cvdb, EditOutcome, Error, Event, Http, Kind, Link, Live, Mode, SystemClock};
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
    let cfg = Config {
        api_base: "http://127.0.0.1:9".into(),
        relays: vec![],
        dir: dir.to_owned(),
    };
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::default());
    Cvdb::open(Link::parse(SECRET).unwrap(), cfg, http.clone(), clock)
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
    use std::os::unix::fs::PermissionsExt;
    let fake = server("append", |_| Err(Error::Offline));
    let dir = tmp("m3");
    let mut c = open_in(&dir, &fake).unwrap();
    c.attach().unwrap();
    assert_eq!(c.edit(&base_op("keep me")).unwrap(), EditOutcome::Queued);
    drop(c);
    let log = dir.join("outbox.log");
    let before = std::fs::read(&log).unwrap();
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o000)).unwrap();
    assert!(
        open_in(&dir, &fake).is_err(),
        "open must fail, not start empty"
    );
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        std::fs::read(&log).unwrap(),
        before,
        "the outbox is untouched"
    );
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
fn s4_backoff_starts_over_only_after_a_stable_connection() {
    use cvdb::live::{STABLE_AFTER, next_attempt};
    assert_eq!(next_attempt(5, Duration::from_secs(1)), 5);
    assert_eq!(
        next_attempt(
            5,
            STABLE_AFTER.checked_sub(Duration::from_millis(1)).unwrap()
        ),
        5
    );
    assert_eq!(next_attempt(5, STABLE_AFTER), 0);
}

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
