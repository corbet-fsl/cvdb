//! `cvdb`: the workspace sync client for `CareerVector`.
//!
//! It moves bytes and typed envelopes between a [`cvws::Workspace`] and a `CareerVector`
//! workspace server; it never interprets what the documents say. See the README.

#![forbid(unsafe_code)]

pub mod client;
pub mod error;
pub mod frames;
pub mod http;
pub mod link;
pub mod live;
pub mod outbox;
#[cfg(feature = "sqlite")]
pub mod store;

pub use client::{Clock, Config, Cvdb, EditOutcome, HostEvent, Mode, SystemClock};
pub use error::Error;
pub use http::{Counts, Http, Kind, UreqHttp};
pub use link::Link;
pub use live::{Event, Live};
