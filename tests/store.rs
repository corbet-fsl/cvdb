#![cfg(feature = "sqlite")]

use cvdb::store::SqliteStore;
use cvfs::{Commit, Error, Op, Origin, Record, Yjs, content_hash};

fn rec(key: &str, revision: u64, bytes: &[u8]) -> Record {
    Record {
        schema_id: "careervector.node-tree/v1".into(),
        key: key.into(),
        kind: "projected".into(),
        revision,
        content_hash: content_hash(bytes),
        bytes: bytes.to_vec(),
        metadata: r#"{"a":1,"unknown":[true]}"#.into(),
        yjs: None,
        origin: Some(Origin {
            actor: "human".into(),
            device: "d1".into(),
            parent_revision: revision - 1,
        }),
    }
}

fn yrec(key: &str, revision: u64, clock: u64, bytes: &[u8]) -> Record {
    let mut r = rec(key, revision, bytes);
    r.kind = "crdt-snapshot".into();
    r.yjs = Some(Yjs {
        state_vector: vec![0, 1, 2, u8::try_from(clock).unwrap()],
        clock,
    });
    r
}

fn op(clock: u64, bytes: &[u8]) -> Op {
    Op {
        clock,
        actor_class: "human".into(),
        actor_id: "u1".into(),
        at_ms: 1_000 + clock,
        bytes: bytes.to_vec(),
    }
}

fn commit(record: Record, op: Option<Op>) -> Commit {
    Commit {
        record,
        op,
        at_ms: 5_000,
    }
}

#[test]
fn roundtrip_preserves_bytes_revisions_and_receipts() {
    let s = SqliteStore::open_in_memory().unwrap();
    let raw = b"line\r\nnul\0\xff\xfe end";
    let r1 = s
        .commit("ws", &commit(rec("cv.toml", 1, raw), None))
        .unwrap();
    let r2 = s
        .commit("ws", &commit(rec("cv.toml", 2, b"second"), None))
        .unwrap();
    assert_eq!((r1.seq, r2.seq, r2.revision), (1, 2, 2));
    assert_eq!(
        s.get("ws", "cv.toml").unwrap().unwrap(),
        rec("cv.toml", 2, b"second")
    );
    assert_eq!(s.receipts("ws", 1).unwrap(), vec![r2]);
    assert_eq!(
        s.retained("ws", "cv.toml").unwrap(),
        vec![rec("cv.toml", 1, raw)]
    );
    assert_eq!(s.keys("ws").unwrap(), vec!["cv.toml".to_string()]);
    assert_eq!(s.get("ws", "missing").unwrap(), None);
}

#[test]
fn cas_conflicts_are_refused_and_change_nothing() {
    let s = SqliteStore::open_in_memory().unwrap();
    s.commit("ws", &commit(rec("k", 1, b"one"), None)).unwrap();
    assert!(matches!(
        s.commit("ws", &commit(rec("k", 1, b"other"), None)),
        Err(Error::Conflict {
            expected: 0,
            actual: 1,
            ..
        })
    ));
    assert!(matches!(
        s.commit("ws", &commit(rec("k", 3, b"skip"), None)),
        Err(Error::Conflict {
            expected: 2,
            actual: 1,
            ..
        })
    ));
    assert_eq!(s.get("ws", "k").unwrap().unwrap().bytes, b"one");
    assert_eq!(s.receipts("ws", 0).unwrap().len(), 1);
    let mut bad = rec("k", 2, b"two");
    bad.content_hash = content_hash(b"not two");
    assert!(matches!(
        s.commit("ws", &commit(bad, None)),
        Err(Error::Integrity(_))
    ));
}

#[test]
fn yjs_clock_cas_and_op_log() {
    let s = SqliteStore::open_in_memory().unwrap();
    s.commit("ws", &commit(yrec("jobs", 1, 1, b"s1"), Some(op(1, b"u1"))))
        .unwrap();
    s.commit("ws", &commit(yrec("jobs", 2, 2, b"s2"), Some(op(2, b"u2"))))
        .unwrap();
    assert!(matches!(
        s.commit("ws", &commit(yrec("jobs", 3, 2, b"s3"), Some(op(2, b"x")))),
        Err(Error::Conflict {
            expected: 1,
            actual: 2,
            ..
        })
    ));
    assert!(matches!(
        s.commit("ws", &commit(yrec("jobs", 3, 3, b"s"), None)),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        s.commit("ws", &commit(rec("jobs", 3, b"s"), None)),
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        s.ops_since("ws", "jobs", 0).unwrap(),
        vec![op(1, b"u1"), op(2, b"u2")]
    );
    assert_eq!(s.ops_since("ws", "jobs", 1).unwrap(), vec![op(2, b"u2")]);
}

#[test]
fn retention_keeps_the_last_revisions() {
    let s = SqliteStore::open_in_memory().unwrap().with_retention(2);
    for rev in 1..=5u64 {
        s.commit(
            "ws",
            &commit(rec("k", rev, format!("v{rev}").as_bytes()), None),
        )
        .unwrap();
    }
    let kept: Vec<u64> = s
        .retained("ws", "k")
        .unwrap()
        .iter()
        .map(|r| r.revision)
        .collect();
    assert_eq!(kept, vec![3, 4]);
}

#[test]
fn a_file_database_survives_reopening_and_dump_restore_is_lossless() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cv.db");
    {
        let s = SqliteStore::open(&path).unwrap();
        s.commit("ws", &commit(rec("a", 1, b"a1"), None)).unwrap();
        s.commit("ws", &commit(rec("a", 2, b"a2"), None)).unwrap();
        s.commit("ws", &commit(yrec("jobs", 1, 1, b"s1"), Some(op(1, b"u1"))))
            .unwrap();
        s.put_asset("ws", "img/f.bin", b"\x00\x01").unwrap();
    }
    let s = SqliteStore::open(&path).unwrap();
    let dump = s.dump("ws").unwrap();
    assert_eq!(dump.records.len(), 2);
    assert_eq!(dump.receipts.len(), 3);
    assert_eq!(dump.assets.len(), 1);
    let t = SqliteStore::open_in_memory().unwrap();
    t.restore("ws", &dump).unwrap();
    assert_eq!(t.dump("ws").unwrap(), dump);
    assert!(matches!(t.restore("ws", &dump), Err(Error::Invalid(_))));
    assert_eq!(
        t.commit("ws", &commit(rec("a", 3, b"a3"), None))
            .unwrap()
            .seq,
        4
    );
    assert!(s.put_asset("ws", "../x", b"x").is_err());
}
