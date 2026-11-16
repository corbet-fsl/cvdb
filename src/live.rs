//! The live relay connection: receive only. It sends nothing after the upgrade (no ping,
//! no awareness), so the Durable Object sees no billable incoming messages; a lost
//! connection is retried with jittered exponential backoff between 1 s and 60 s.

use crate::frames;
use crate::http::{Counts, Kind};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::Duration;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Connected,
    /// Update bytes from another peer (or the server's canonical bytes).
    Update(Vec<u8>),
    Disconnected,
}

/// Seconds to wait before attempt number `attempt` (0 = the first retry): 1, 2, 4 ...
/// capped at 60, scaled by `jitter` in `0.0..=1.0` to between 90% and 100%, never under 1 s.
#[must_use]
pub fn backoff(attempt: u32, jitter: f64) -> Duration {
    let nominal = (1u64 << attempt.min(6)).min(60) as f64;
    Duration::from_secs_f64((nominal * (0.9 + 0.1 * jitter.clamp(0.0, 1.0))).max(1.0))
}

/// A stable random device id stored beside the outbox; the relay uses it to skip our own echo.
#[must_use]
pub fn device_id(dir: &Path) -> String {
    let path = dir.join("device");
    if let Ok(s) = std::fs::read_to_string(&path) {
        let s = s.trim().to_owned();
        if s.len() >= 8 {
            return s;
        }
    }
    let id: String = (0..16)
        .map(|_| char::from(b'a' + fastrand::u8(..26)))
        .collect();
    let _ = std::fs::create_dir_all(dir);
    if let Ok(mut f) = crate::link::open_private(&path) {
        use std::io::Write;
        let _ = f.write_all(id.as_bytes());
    }
    id
}

pub struct Live {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

fn ws_url(base: &str, ws_id: &str, device: &str) -> String {
    let base = base.trim_end_matches('/');
    let base = if let Some(r) = base.strip_prefix("https://") {
        format!("wss://{r}")
    } else if let Some(r) = base.strip_prefix("http://") {
        format!("ws://{r}")
    } else {
        base.to_owned()
    };
    format!("{base}/ws/{ws_id}?userId={device}")
}

fn set_timeout(ws: &mut WebSocket<MaybeTlsStream<std::net::TcpStream>>) {
    let t = Some(Duration::from_millis(250));
    match ws.get_mut() {
        MaybeTlsStream::Plain(s) => {
            let _ = s.set_read_timeout(t);
        }
        MaybeTlsStream::Rustls(s) => {
            let _ = s.get_mut().set_read_timeout(t);
        }
        _ => {}
    }
}

impl Live {
    #[must_use]
    pub fn spawn(
        relays: Vec<String>,
        ws_id: String,
        device: String,
        counts: Counts,
        tx: Sender<Event>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::Builder::new()
            .name("cvdb-live".into())
            .spawn(move || run(&relays, &ws_id, &device, &counts, &tx, &flag))
            .ok();
        Self { stop, handle }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.stop();
    }
}

fn sleep_unless_stopped(d: Duration, stop: &AtomicBool) {
    let end = std::time::Instant::now() + d;
    while std::time::Instant::now() < end && !stop.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run(
    relays: &[String],
    ws_id: &str,
    device: &str,
    counts: &Counts,
    tx: &Sender<Event>,
    stop: &AtomicBool,
) {
    let mut attempt: u32 = 0;
    while !stop.load(Ordering::SeqCst) {
        // The first relay that answers wins; the list is walked in order on every reconnect.
        let mut socket = None;
        for base in relays {
            counts.bump(Kind::Upgrade);
            let Ok(mut req) = ws_url(base, ws_id, device).into_client_request() else {
                continue;
            };
            req.headers_mut()
                .insert("x-cv-actor-class", "human".parse().expect("header"));
            if let Ok((ws, _)) = tungstenite::connect(req) {
                socket = Some(ws);
                break;
            }
        }
        if let Some(mut ws) = socket {
            set_timeout(&mut ws);
            attempt = 0;
            let _ = tx.send(Event::Connected);
            while !stop.load(Ordering::SeqCst) {
                match ws.read() {
                    Ok(Message::Binary(b)) => {
                        // A malformed or oversize frame is dropped, never decoded.
                        if let Ok(Some(update)) = frames::relay_update(&b)
                            && tx.send(Event::Update(update)).is_err()
                        {
                            return;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(tungstenite::Error::Io(e))
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(_) => break,
                }
            }
            let _ = ws.close(None);
            let _ = tx.send(Event::Disconnected);
        }
        sleep_unless_stopped(backoff(attempt, fastrand::f64()), stop);
        attempt = attempt.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_stays_between_one_and_sixty_seconds() {
        for attempt in 0..40 {
            for j in [0.0, 0.5, 1.0] {
                let d = backoff(attempt, j);
                assert!(
                    d >= Duration::from_secs(1) && d <= Duration::from_secs(60),
                    "{attempt} {d:?}"
                );
            }
        }
        assert_eq!(backoff(0, 1.0), Duration::from_secs(1));
        assert_eq!(backoff(10, 1.0), Duration::from_secs(60));
    }

    #[test]
    fn upgrade_attempts_in_ten_minutes_are_bounded() {
        // Worst case: the lowest jitter on every wait, the relay down for 10 minutes.
        let mut t = Duration::ZERO;
        let mut attempts = 1;
        let mut a = 0;
        while t < Duration::from_secs(600) {
            t += backoff(a, 0.0);
            a += 1;
            attempts += 1;
        }
        assert!(attempts <= 18, "{attempts}");
    }

    #[test]
    fn url_scheme() {
        assert_eq!(
            ws_url("http://127.0.0.1:1/", "abc", "d"),
            "ws://127.0.0.1:1/ws/abc?userId=d"
        );
        assert_eq!(
            ws_url("https://r.x", "abc", "d"),
            "wss://r.x/ws/abc?userId=d"
        );
    }
}
