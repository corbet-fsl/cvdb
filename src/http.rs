//! The blocking HTTP seam. Every request the client makes goes through [`Http`] and is
//! counted by [`Kind`], which is what the Cloudflare budget tests read.

use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

pub trait Http: Send + Sync {
    /// Any transport failure is `Err(Error::Offline)` with no detail (the URL holds the secret).
    fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response>;
}

/// What a request is for, for the budget counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// `GET /workspaces/:id` (mode detection).
    Workspace,
    /// `GET /workspaces/:id/sub/:sub` (cold load, refresh, resync).
    Sub,
    /// `POST /workspaces/:id/append`.
    Append,
    /// `POST /workspaces/:id/ops` (server-applied mode).
    Ops,
    /// A relay WebSocket upgrade attempt.
    Upgrade,
}

/// Shared request counters.
#[derive(Debug, Clone, Default)]
pub struct Counts(Arc<Mutex<BTreeMap<Kind, u64>>>);

impl Counts {
    pub fn bump(&self, kind: Kind) {
        *self.0.lock().expect("counts").entry(kind).or_default() += 1;
    }

    #[must_use]
    pub fn get(&self, kind: Kind) -> u64 {
        self.0
            .lock()
            .expect("counts")
            .get(&kind)
            .copied()
            .unwrap_or(0)
    }

    /// Worker requests: everything except the relay upgrade (counted separately).
    #[must_use]
    pub fn api_total(&self) -> u64 {
        self.0
            .lock()
            .expect("counts")
            .iter()
            .filter(|(k, _)| **k != Kind::Upgrade)
            .map(|(_, v)| *v)
            .sum()
    }
}

pub struct UreqHttp {
    agent: ureq::Agent,
}

impl Default for UreqHttp {
    fn default() -> Self {
        Self::new()
    }
}

impl UreqHttp {
    #[must_use]
    pub fn new() -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        Self { agent }
    }
}

impl Http for UreqHttp {
    fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response> {
        let result = match (method, body) {
            ("GET", _) => {
                let mut r = self.agent.get(url);
                for (k, v) in headers {
                    r = r.header(*k, *v);
                }
                r.call()
            }
            (_, body) => {
                let mut r = self.agent.post(url);
                for (k, v) in headers {
                    r = r.header(*k, *v);
                }
                r.send(body.unwrap_or(&[]))
            }
        };
        let mut resp = result.map_err(|_| Error::Offline)?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| Some((k.as_str().to_owned(), v.to_str().ok()?.to_owned())))
            .collect();
        let body = if status == 304 || status == 204 {
            Vec::new()
        } else {
            resp.body_mut()
                .with_config()
                .limit(64 << 20)
                .read_to_vec()
                .map_err(|_| Error::Offline)?
        };
        Ok(Response {
            status,
            headers,
            body,
        })
    }
}
