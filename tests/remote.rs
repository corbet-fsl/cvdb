use std::collections::BTreeMap;
use std::sync::Arc;

use cvdb::Error;
use cvdb::fake::FakeServer;
use cvdb::remote::{Appended, Outcome, Remote};
use serde_json::json;

fn remote(server: &FakeServer, dir: &std::path::Path) -> Remote {
    Remote::new(
        Arc::new(server.clone()),
        "https://cv.invalid/api",
        "ws1",
        dir,
    )
    .unwrap()
}

fn one(sub: &str, bytes: &[u8]) -> BTreeMap<String, Vec<u8>> {
    BTreeMap::from([(sub.to_owned(), bytes.to_vec())])
}

fn clocks(sub: &str, clock: u64) -> BTreeMap<String, u64> {
    BTreeMap::from([(sub.to_owned(), clock)])
}

fn op() -> serde_json::Value {
    json!({ "kind": "setField", "path": "a.b" })
}

#[test]
fn append_returns_receipts_and_fetch_returns_head_and_tail() {
    let server = FakeServer::new();
    let dir = tempfile::tempdir().unwrap();
    let r = remote(&server, dir.path());
    let first = r
        .append("op1", &op(), &one("jobs", b"u1"), &clocks("jobs", 0))
        .unwrap();
    assert_eq!(
        first,
        Outcome::Done(vec![Appended {
            sub_doc: "jobs".into(),
            clock: 1,
            written: true
        }])
    );
    r.append("op2", &op(), &one("jobs", b"u2"), &clocks("jobs", 1))
        .unwrap();
    let f = r.fetch("jobs", None).unwrap().unwrap();
    assert_eq!((f.clock, f.head_clock()), (2, 0));
    assert_eq!(f.tail, vec![b"u1".to_vec(), b"u2".to_vec()]);
    // A fold moves the tail into the head; the clock stays.
    server.fold("jobs", b"HEAD");
    let f = r.fetch("jobs", None).unwrap().unwrap();
    assert_eq!(
        (f.head.as_slice(), f.tail.len(), f.clock, f.head_clock()),
        (&b"HEAD"[..], 0, 2, 2)
    );
    // The validator skips an unchanged document.
    assert_eq!(r.fetch("jobs", Some(&f.etag)).unwrap(), None);
    assert_eq!(r.queued(), 0);
}

#[test]
fn cas_conflicts_are_refused() {
    let server = FakeServer::new();
    let dir = tempfile::tempdir().unwrap();
    let r = remote(&server, dir.path());
    r.append("op1", &op(), &one("jobs", b"u1"), &clocks("jobs", 0))
        .unwrap();
    // Another writer got in: this writer's clock is stale. Nothing is sent, the entry is parked.
    server.external_append("jobs", b"theirs");
    let posts = |s: &FakeServer| {
        s.requests()
            .iter()
            .filter(|l| l.starts_with("POST"))
            .count()
    };
    let before = posts(&server);
    let stale = r.append("op2", &op(), &one("jobs", b"u2"), &clocks("jobs", 1));
    assert!(matches!(stale, Err(Error::Conflict)));
    assert_eq!(posts(&server), before);
    assert_eq!(r.queued(), 0);
    // A race that slips past the pre-check is refused by the server the same way.
    server.conflict_next();
    let raced = r.append("op3", &op(), &one("jobs", b"u3"), &clocks("jobs", 2));
    assert!(matches!(raced, Err(Error::Conflict)));
    assert_eq!(
        server.tail("jobs"),
        vec![b"u1".to_vec(), b"theirs".to_vec()]
    );
    assert_eq!(r.queued(), 0);
    // Rebased on the current clock it goes through.
    let ok = r
        .append("op4", &op(), &one("jobs", b"u4"), &clocks("jobs", 2))
        .unwrap();
    assert!(matches!(ok, Outcome::Done(ref v) if v[0].clock == 3));
}

#[test]
fn offline_appends_queue_and_replay_in_order_after_a_restart() {
    let server = FakeServer::new();
    let dir = tempfile::tempdir().unwrap();
    {
        let r = remote(&server, dir.path());
        r.append("op1", &op(), &one("jobs", b"u1"), &BTreeMap::new())
            .unwrap();
        server.set_offline(true);
        assert_eq!(
            r.append("op2", &op(), &one("jobs", b"u2"), &BTreeMap::new())
                .unwrap(),
            Outcome::Queued
        );
        assert_eq!(
            r.append("op3", &op(), &one("settings", b"s1"), &BTreeMap::new())
                .unwrap(),
            Outcome::Queued
        );
        assert_eq!(r.queued(), 2);
        assert!(matches!(r.fetch("jobs", None), Err(Error::Offline)));
    }
    // A new process on the same directory still owes both, and flushes only when online.
    let r = remote(&server, dir.path());
    assert_eq!(r.queued(), 2);
    let flushed = r.flush().unwrap();
    assert_eq!((flushed.acked, flushed.left), (0, 2));
    server.set_offline(false);
    let flushed = r.flush().unwrap();
    assert_eq!((flushed.acked, flushed.parked, flushed.left), (2, 0, 0));
    assert_eq!(server.tail("jobs"), vec![b"u1".to_vec(), b"u2".to_vec()]);
    assert_eq!(server.tail("settings"), vec![b"s1".to_vec()]);
    // Replaying an entry the server already numbered does not write it twice.
    r.append("op3", &op(), &one("settings", b"s1"), &BTreeMap::new())
        .unwrap();
    assert_eq!(server.tail("settings").len(), 1);
}

#[test]
fn refusals_park_and_are_reported() {
    let server = FakeServer::new();
    let dir = tempfile::tempdir().unwrap();
    let r = remote(&server, dir.path());
    server.set_legacy(true);
    let e = r.append("op1", &op(), &one("jobs", b"u"), &BTreeMap::new());
    assert!(matches!(e, Err(Error::NotOnAppendPath)));
    assert_eq!(r.queued(), 0);
    server.set_legacy(false);
    let bad = r.append(
        "op2",
        &json!({"noKind": 1}),
        &one("jobs", b"u"),
        &BTreeMap::new(),
    );
    assert!(matches!(bad, Err(Error::OpRejected(_))));
    assert_eq!(r.queued(), 0);
}
