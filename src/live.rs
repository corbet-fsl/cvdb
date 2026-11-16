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
use tungstenite::protocol::WebSocketConfig;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Connected,
    /// Update bytes from another peer (or the server's canonical bytes).
    Update(Vec<u8>),
    Disconnected,
    /// The relay answered the upgrade with a 4xx (for example 404, a deleted workspace):
    /// retrying cannot help, the thread stopped.
    Gone(u16),
}

/// How long a connection must have lasted before the backoff starts over: a relay that
/// accepts and then closes must not be retried every second (each attempt is a billed request).
pub const STABLE_AFTER: Duration = Duration::from_secs(60);
/// Bad closes in a row on one relay before the walker moves to the next (the browser rule).
pub const BAD_CLOSES_BEFORE_ADVANCE: u32 = 2;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Time and sleeping for the reconnect loop; injectable so tests can run ten minutes of
/// backoff in milliseconds.
pub trait Pace: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, d: Duration, stop: &AtomicBool);
}

struct RealPace(std::time::Instant);

impl Pace for RealPace {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn sleep(&self, d: Duration, stop: &AtomicBool) {
        sleep_unless_stopped(d, stop);
    }
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
        Self::spawn_with(
            relays,
            ws_id,
            device,
            counts,
            tx,
            Arc::new(RealPace(std::time::Instant::now())),
        )
    }

    /// [`Live::spawn`] with an injected clock and sleeper.
    pub fn spawn_with(
        relays: Vec<String>,
        ws_id: String,
        device: String,
        counts: Counts,
        tx: Sender<Event>,
        pace: Arc<dyn Pace>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::Builder::new()
            .name("cvdb-live".into())
            .spawn(move || run(&relays, &ws_id, &device, &counts, &tx, &flag, pace.as_ref()))
            .ok();
        Self { stop, handle }
    }

    /// Ask the thread to end. Never blocks: it ends within a quarter second, or after its
    /// connect timeout. [`Live::join`] waits for it.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Stop and wait for the thread.
    pub fn join(mut self) {
        self.stop();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Live {
    /// Never blocks: the thread sees the flag within a quarter second (or after its connect
    /// timeout) and ends on its own.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// Why a connection attempt failed.
enum Connect {
    /// Retry with backoff.
    Retry,
    /// A 4xx answer to the upgrade (not 408/429): stop.
    Gone(u16),
}

type Socket = WebSocket<MaybeTlsStream<std::net::TcpStream>>;

/// TCP connect with a timeout and keepalive (kernel probes, no billed message), then the
/// WebSocket handshake under a read/write timeout, with frame limits just above the cap that
/// `relay_update` enforces, so a link holder cannot make every client buffer 64 MiB.
fn connect(url: &str, stop: &AtomicBool) -> std::result::Result<Socket, Connect> {
    use std::net::ToSocketAddrs;
    let mut req = url.into_client_request().map_err(|_| Connect::Retry)?;
    req.headers_mut()
        .insert("x-cv-actor-class", "human".parse().expect("header"));
    let uri = req.uri().clone();
    let host = uri.host().ok_or(Connect::Retry)?.to_owned();
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let deadline = std::time::Instant::now() + CONNECT_TIMEOUT;
    // Every resolved address is tried (a broken IPv6 route must not hide a working IPv4 one).
    let stream = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|_| Connect::Retry)?
        .find_map(|a| {
            // One overall deadline for all addresses, and the stop flag between them: stop()
            // and Drop never wait for N connect timeouts.
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if stop.load(Ordering::SeqCst) || left.is_zero() {
                return None;
            }
            std::net::TcpStream::connect_timeout(&a, left).ok()
        })
        .ok_or(Connect::Retry)?;
    let _ = stream.set_nodelay(true);
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(15))
        .with_retries(3);
    let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&ka);
    let _ = stream.set_read_timeout(Some(CONNECT_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CONNECT_TIMEOUT));
    let cap = frames::MAX_FRAME + 64;
    let config = WebSocketConfig::default()
        .max_message_size(Some(cap))
        .max_frame_size(Some(cap));
    match tungstenite::client_tls_with_config(req, stream, Some(config), None) {
        Ok((mut ws, _)) => {
            set_timeout(&mut ws);
            Ok(ws)
        }
        Err(tungstenite::HandshakeError::Failure(tungstenite::Error::Http(r)))
            if r.status().is_client_error() && !matches!(r.status().as_u16(), 408 | 429) =>
        {
            Err(Connect::Gone(r.status().as_u16()))
        }
        Err(_) => Err(Connect::Retry),
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
    pace: &dyn Pace,
) {
    let mut attempt: u32 = 0;
    // The relay walker, the same rule as the browser's: stay on the current relay; move to the
    // next (wrapping) when it cannot be opened, or after two bad closes in a row (a connection
    // that ended before `STABLE_AFTER`). A connection that lasted resets to the first relay.
    let mut start: usize = 0;
    let mut active: usize = 0;
    let mut bad_closes: u32 = 0;
    while !stop.load(Ordering::SeqCst) {
        let mut socket = None;
        let mut gone = 0;
        let mut last_gone = 404;
        for k in 0..relays.len() {
            let i = (start + k) % relays.len();
            let base = &relays[i];
            counts.bump(Kind::Upgrade);
            match connect(&ws_url(base, ws_id, device), stop) {
                Ok(ws) => {
                    socket = Some(ws);
                    active = i;
                    start = i;
                    break;
                }
                Err(Connect::Gone(s)) => {
                    gone += 1;
                    last_gone = s;
                }
                Err(Connect::Retry) => {}
            }
        }
        // Every relay answered 4xx: retrying cannot help.
        if socket.is_none() && !relays.is_empty() && gone == relays.len() {
            let _ = tx.send(Event::Gone(last_gone));
            return;
        }
        if let Some(mut ws) = socket {
            let since = pace.now();
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
            // Only a connection that lasted starts the backoff over (and may reconnect at
            // once). A short one falls through to the sleep: a relay that accepts and then
            // closes is never retried back to back.
            if pace.now().saturating_sub(since) >= STABLE_AFTER {
                attempt = 0;
                bad_closes = 0;
                start = 0;
                continue;
            }
            bad_closes += 1;
            if bad_closes >= BAD_CLOSES_BEFORE_ADVANCE && relays.len() > 1 {
                start = (active + 1) % relays.len();
                bad_closes = 0;
            }
        }
        pace.sleep(backoff(attempt, fastrand::f64()), stop);
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
