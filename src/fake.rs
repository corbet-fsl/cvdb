//! An in-process light server that implements exactly the contract `cvdb::remote` speaks:
//! framed `GET /workspaces/:id/sub/:sub` and `POST /workspaces/:id/append`. For tests; it never
//! touches the network. Enabled by the `fake-server` feature.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::{Value, json};

use crate::error::{Error, Result};
use crate::frames;
use crate::http::{Http, Response};

#[derive(Default)]
struct Sub {
    head: Vec<u8>,
    tail: Vec<Vec<u8>>,
    clock: u64,
}

#[derive(Default)]
struct State {
    subs: BTreeMap<String, Sub>,
    seen: BTreeMap<String, BTreeMap<String, u64>>,
    offline: bool,
    legacy: bool,
    conflict_next: bool,
    requests: Vec<String>,
}

/// The fake server; clones share state.
#[derive(Clone, Default)]
pub struct FakeServer(Arc<Mutex<State>>);

fn json_response(status: u16, v: &Value) -> Response {
    Response {
        status,
        headers: vec![],
        body: serde_json::to_vec(v).expect("json"),
    }
}

fn error(status: u16, message: &str) -> Response {
    json_response(status, &json!({ "error": message }))
}

impl FakeServer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests fail with a transport error while offline.
    pub fn set_offline(&self, offline: bool) {
        self.0.lock().expect("state").offline = offline;
    }

    /// The workspace is not on the append path (`409 not_on_append_path`).
    pub fn set_legacy(&self, legacy: bool) {
        self.0.lock().expect("state").legacy = legacy;
    }

    /// The next append loses a race (`409 commit_conflict`) without being applied.
    pub fn conflict_next(&self) {
        self.0.lock().expect("state").conflict_next = true;
    }

    /// Another writer: the server numbers `bytes` for `sub` as if some other client had appended.
    pub fn external_append(&self, sub: &str, bytes: &[u8]) -> u64 {
        let mut st = self.0.lock().expect("state");
        let s = st.subs.entry(sub.into()).or_default();
        s.clock += 1;
        s.tail.push(bytes.to_vec());
        s.clock
    }

    /// Fold the tail into the head (what the server's fold does).
    pub fn fold(&self, sub: &str, head: &[u8]) {
        let mut st = self.0.lock().expect("state");
        let s = st.subs.entry(sub.into()).or_default();
        s.head = head.to_vec();
        s.tail.clear();
    }

    /// Every request line seen, `METHOD path`.
    #[must_use]
    pub fn requests(&self) -> Vec<String> {
        self.0.lock().expect("state").requests.clone()
    }

    /// The updates the server numbered for `sub`, head excluded.
    #[must_use]
    pub fn tail(&self, sub: &str) -> Vec<Vec<u8>> {
        self.0
            .lock()
            .expect("state")
            .subs
            .get(sub)
            .map(|s| s.tail.clone())
            .unwrap_or_default()
    }
}

impl Http for FakeServer {
    fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let mut st = self.0.lock().expect("state");
        if st.offline {
            return Err(Error::Offline);
        }
        let path = url
            .split("/workspaces/")
            .nth(1)
            .and_then(|p| p.split_once('/'))
            .map_or("", |(_, rest)| rest);
        st.requests.push(format!("{method} /{path}"));
        if let ("GET", Some(sub)) = (method, path.strip_prefix("sub/")) {
            let s = st.subs.entry(sub.into()).or_default();
            let etag = format!("\"{sub}-{}\"", s.clock);
            if headers
                .iter()
                .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && *v == etag)
            {
                return Ok(Response {
                    status: 304,
                    headers: vec![],
                    body: vec![],
                });
            }
            let mut h = vec![
                ("etag".to_owned(), etag),
                ("x-cv-clock".to_owned(), s.clock.to_string()),
            ];
            let body = if s.tail.is_empty() {
                s.head.clone()
            } else {
                h.push(("x-cv-update-frames".into(), (s.tail.len() + 1).to_string()));
                let mut all: Vec<&[u8]> = vec![&s.head];
                all.extend(s.tail.iter().map(Vec::as_slice));
                frames::encode_frames(&all)
            };
            return Ok(Response {
                status: 200,
                headers: h,
                body,
            });
        }
        if method == "POST" && path == "append" {
            let Ok(v) = serde_json::from_slice::<Value>(body.unwrap_or(&[])) else {
                return Ok(error(400, "invalid_json"));
            };
            if v.get("op")
                .and_then(|o| o.get("kind"))
                .and_then(Value::as_str)
                .is_none()
            {
                return Ok(error(400, "missing_op"));
            }
            if v.get("actorClass").and_then(Value::as_str) != Some("human") {
                return Ok(error(403, "append: only human writes"));
            }
            if st.legacy {
                return Ok(error(409, "not_on_append_path: legacy workspace"));
            }
            if st.conflict_next {
                st.conflict_next = false;
                return Ok(error(409, "commit_conflict"));
            }
            let mut updates = Vec::new();
            for (sub, b64) in v
                .get("updates")
                .and_then(Value::as_object)
                .into_iter()
                .flatten()
            {
                let Some(bytes) = b64.as_str().and_then(|t| B64.decode(t).ok()) else {
                    return Ok(error(400, "invalid_update_base64"));
                };
                updates.push((sub.clone(), bytes));
            }
            let op_id = v
                .get("opId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            let mut results = Vec::new();
            if let Some(done) = st.seen.get(&op_id).cloned() {
                for (sub, clock) in done {
                    results.push(json!({ "subDoc": sub, "clock": clock, "written": false, "reason": "duplicate" }));
                }
            } else {
                let mut done = BTreeMap::new();
                for (sub, bytes) in updates {
                    let s = st.subs.entry(sub.clone()).or_default();
                    s.clock += 1;
                    s.tail.push(bytes);
                    done.insert(sub.clone(), s.clock);
                    results.push(json!({ "subDoc": sub, "clock": s.clock, "written": true }));
                }
                st.seen.insert(op_id, done);
            }
            let out = if results.len() == 1 {
                let mut one = results.remove(0);
                one["ok"] = json!(true);
                one
            } else {
                json!({ "ok": true, "results": results, "written": true })
            };
            return Ok(json_response(200, &out));
        }
        Ok(error(404, "not_found"))
    }
}
