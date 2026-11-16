//! Errors. None of them carries a URL, a header or the workspace id: the private
//! link is a bearer secret and must not leak through `Display` or `Debug`.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The network failed (no answer). The edit stays queued.
    Offline,
    /// The workspace is still on the legacy write path (409 `not_on_append_path`).
    NotOnAppendPath,
    /// The link opens a read-only mirror (403).
    ReadOnlyMirror,
    /// 403 for a non-human actor class: never sent by this client.
    NotHuman,
    /// The server refused the op (422 `op_apply_failed`, 400 envelope, 426 catalog).
    OpRejected(String),
    /// 409 `commit_conflict`: retried later.
    Conflict,
    /// Any other status.
    Server(u16),
    /// A malformed answer or frame (never decoded past the cap).
    Protocol(String),
    /// The link or id is not valid.
    BadLink,
    /// The write path of the workspace is not known yet and nothing is cached.
    ModeUnknown,
    /// The engine refused the op locally (`cvws:` error).
    Engine(String),
    /// Local storage failed.
    Io(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Offline => f.write_str("offline"),
            Self::NotOnAppendPath => f.write_str("workspace is not on the append path"),
            Self::ReadOnlyMirror => f.write_str("read-only mirror"),
            Self::NotHuman => f.write_str("only human writes go to /append"),
            Self::OpRejected(m) => write!(f, "op rejected: {m}"),
            Self::Conflict => f.write_str("commit conflict"),
            Self::Server(s) => write!(f, "server answered {s}"),
            Self::Protocol(m) => write!(f, "protocol error: {m}"),
            Self::BadLink => f.write_str("not a valid workspace link"),
            Self::ModeUnknown => f.write_str("write path unknown (attach while online first)"),
            Self::Engine(m) => write!(f, "engine: {m}"),
            Self::Io(m) => write!(f, "storage: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.kind().to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
