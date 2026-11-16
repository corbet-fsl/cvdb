//! Behaviour that needs no server: a scripted fake transport.
use cvdb::client::Config;
use cvdb::http::Response;
use cvdb::{Clock, Cvdb, EditOutcome, Error, Http, Link, SystemClock};
use cvws::SubDoc;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const SECRET: &str = "supersecretworkspaceid42";

type Handler = Box<dyn Fn(&str, &str, Option<&[u8]>) -> Result<Response, Error> + Send + Sync>;

struct Fake {
    handler: Handler,
    seen: Mutex<Vec<(String, String)>>,
}

impl Http for Fake {
    fn request(
        &self,
        method: &str,
        url: &str,
        _h: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, Error> {
        self.seen
            .lock()
            .unwrap()
            .push((method.to_owned(), url.to_owned()));
        (self.handler)(method, url, body)
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

fn open(http: Arc<Fake>) -> Cvdb {
    let dir: PathBuf = std::env::temp_dir().join(format!("cvdb-off-{}", fastrand::u64(..)));
    let cfg = Config {
        api_base: "http://127.0.0.1:9".into(),
        relays: vec![],
        dir,
    };
    let clock: Arc<dyn Clock> = Arc::new(SystemClock::default());
    Cvdb::open(Link::parse(SECRET).unwrap(), cfg, http, clock).unwrap()
}

fn workspace_json(path: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "id": "x", "writePath": path })).unwrap()
}

fn tree_op(text: &str) -> Value {
    let node = |id: &str, kind: &str, data: Value, ch: Vec<Value>| {
        let active: Vec<Value> = ch.iter().map(|c| c["id"].clone()).collect();
        json!({ "id": id, "kind": kind, "tier": "HYBRID", "data": data, "selection": { "active": active }, "children": ch })
    };
    json!({ "kind": "cv_profile.replace", "lang": "en", "tree": node("root", "cv-root", json!({}),
        vec![node("b1", "bullet", json!({ "text": text }), vec![])]) })
}

#[test]
fn legacy_mode_never_applies_locally_first() {
    let http = Arc::new(Fake {
        handler: Box::new(|m, u, _| match (m, u.rsplit('/').next().unwrap()) {
            ("GET", _) if u.ends_with(SECRET) => resp(200, &[], &workspace_json("legacy")),
            ("GET", _) => resp(200, &[], &[0, 0]),
            ("POST", "ops") => resp(200, &[], br#"{"ok":true}"#),
            _ => resp(404, &[], b""),
        }),
        seen: Mutex::default(),
    });
    let mut c = open(http.clone());
    c.attach().unwrap();
    let out = c.edit(&tree_op("hello")).unwrap();
    assert!(matches!(out, EditOutcome::Saved { .. }));
    // The op went to the server only: the local document has not seen it (the "XX" hazard).
    assert!(
        c.view(SubDoc::CvProfile, "cv_profile.en", "root.b1")
            .is_err()
    );
    assert!(
        http.seen
            .lock()
            .unwrap()
            .iter()
            .any(|(m, u)| m == "POST" && u.ends_with("/ops"))
    );
    assert!(
        !http
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|(_, u)| u.ends_with("/append"))
    );
}

#[test]
fn append_mode_applies_locally_and_queues_when_offline() {
    let http = Arc::new(Fake {
        handler: Box::new(|m, u, _| match m {
            "GET" if u.ends_with(SECRET) => resp(200, &[], &workspace_json("append")),
            "GET" => resp(200, &[], &[0, 0]),
            _ => Err(Error::Offline),
        }),
        seen: Mutex::default(),
    });
    let mut c = open(http);
    c.attach().unwrap();
    assert_eq!(c.edit(&tree_op("hello")).unwrap(), EditOutcome::Queued);
    assert_eq!(c.queued(), 1);
    assert_eq!(
        c.view(SubDoc::CvProfile, "cv_profile.en", "root.b1")
            .unwrap()["data"]["text"],
        "hello"
    );
}

#[test]
fn errors_never_carry_the_link() {
    let http = Arc::new(Fake {
        handler: Box::new(|_, _, _| Err(Error::Offline)),
        seen: Mutex::default(),
    });
    let mut c = open(http);
    let e = c.attach().unwrap_err();
    let shown = format!("{e} {e:?} {:?} {}", c.link(), c.link());
    assert!(!shown.contains(SECRET), "{shown}");
    let e = c.edit(&tree_op("x")).unwrap_err();
    assert_eq!(e, Error::ModeUnknown);
    assert!(!format!("{e} {e:?}").contains(SECRET));
}

#[test]
fn oversize_frames_are_refused_not_decoded() {
    // Announced as framed, one frame over the 1 MiB cap.
    let mut body = Vec::new();
    cvdb::frames::write_var_bytes(&mut body, &[0, 0]);
    cvdb::frames::write_var_uint(&mut body, (cvdb::frames::MAX_FRAME + 1) as u64);
    body.extend(vec![0u8; 64]);
    let http = Arc::new(Fake {
        handler: Box::new(move |m, u, _| match m {
            "GET" if u.ends_with(SECRET) => resp(200, &[], &workspace_json("append")),
            "GET" => resp(200, &[("x-cv-update-frames", "2")], &body),
            _ => resp(404, &[], b""),
        }),
        seen: Mutex::default(),
    });
    let mut c = open(http);
    assert!(matches!(c.attach(), Err(Error::Protocol(_))));
    // A live frame over the cap is refused by the engine and counted, on its own big-stack thread.
    let applied = c
        .apply_live(&vec![0u8; cvdb::frames::MAX_FRAME + 1])
        .unwrap();
    assert_ne!(applied.refused, [] as [cvws::OpError; 0]);
}

#[test]
fn append_body_is_human_only() {
    let entry = cvdb::outbox::Entry {
        op_id: "abcdefgh".into(),
        kind: cvdb::outbox::EntryKind::Append,
        op: json!({ "kind": "node.update", "opId": "abcdefgh" }),
        updates: std::collections::BTreeMap::from([("cv-profile".to_owned(), vec![1, 2, 3])]),
    };
    let body = cvdb::client::append_body(&entry, cvws::ActorClass::Human).unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["actorClass"], "human");
    assert!(v.get("agentId").is_none());
    assert_eq!(
        cvdb::client::append_body(&entry, cvws::ActorClass::Agent),
        Err(Error::NotHuman)
    );
    assert_eq!(
        cvdb::client::append_body(&entry, cvws::ActorClass::System),
        Err(Error::NotHuman)
    );
}
