//! `cvdb`: the database backend of the `CareerVector` workspace: the SQLite store and the byte
//! transport pieces. It moves opaque bytes and never applies or merges documents.

#![forbid(unsafe_code)]

pub mod error;
#[cfg(feature = "fake-server")]
pub mod fake;
pub mod frames;
pub mod http;
pub mod link;
pub mod live;
pub mod outbox;
pub mod remote;
#[cfg(feature = "sqlite")]
pub mod store;

pub use error::Error;
pub use http::{Counts, Http, Kind, UreqHttp};
pub use link::Link;
pub use live::{Event, Live};
