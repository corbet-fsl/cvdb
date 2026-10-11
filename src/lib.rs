//! Read JSON Lines records with bounded, per-record buffering.
#![forbid(unsafe_code)]

use std::io::{self, BufRead};

/// Stream JSON values, skipping blank lines and reporting malformed records.
///
/// Each item is one nonblank input line. A failed item does not prevent the
/// caller from reading later records; consumers may stop on the first error.
pub fn values(reader: impl BufRead) -> impl Iterator<Item = io::Result<serde_json::Value>> {
    reader.lines().filter_map(|line| match line {
        Ok(line) if line.trim().is_empty() => None,
        Ok(line) => Some(serde_json::from_str(&line).map_err(io::Error::from)),
        Err(error) => Some(Err(error)),
    })
}
