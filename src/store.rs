//! The `SQLite` storage backend: the same logical rows `cvfs` keeps as files (documents, op log,
//! receipts, retained snapshots, assets), in one database file or in memory.
//!
//! It speaks the neutral vocabulary of [`cvfs`] (`Record`, `Op`, `Receipt`, `Commit`, `Dump`), so a
//! workspace moves between the two backends through `dump` and `restore` without loss. It never
//! parses document bytes or metadata. The logical schema is also what the D1 adapter of the web
//! mirrors.

use std::path::Path;
use std::sync::Mutex;

use cvfs::{Commit, Dump, Error, Op, Origin, Receipt, Record, Result, Yjs, content_hash};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS documents (
  ws TEXT NOT NULL, key TEXT NOT NULL, schema_id TEXT NOT NULL, kind TEXT NOT NULL,
  revision INTEGER NOT NULL, content_hash TEXT NOT NULL, bytes BLOB NOT NULL, metadata TEXT NOT NULL,
  yjs_state_vector BLOB, yjs_clock INTEGER,
  origin_actor TEXT, origin_device TEXT, origin_parent INTEGER,
  PRIMARY KEY (ws, key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS snapshots (
  ws TEXT NOT NULL, key TEXT NOT NULL, schema_id TEXT NOT NULL, kind TEXT NOT NULL,
  revision INTEGER NOT NULL, content_hash TEXT NOT NULL, bytes BLOB NOT NULL, metadata TEXT NOT NULL,
  yjs_state_vector BLOB, yjs_clock INTEGER,
  origin_actor TEXT, origin_device TEXT, origin_parent INTEGER,
  PRIMARY KEY (ws, key, revision)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS workspace_sub_doc_ops (
  ws TEXT NOT NULL, key TEXT NOT NULL, clock INTEGER NOT NULL, actor_class TEXT NOT NULL,
  actor_id TEXT NOT NULL, at_ms INTEGER NOT NULL, hash TEXT NOT NULL, bytes BLOB NOT NULL,
  PRIMARY KEY (ws, key, clock)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS receipts (
  ws TEXT NOT NULL, seq INTEGER NOT NULL, key TEXT NOT NULL, revision INTEGER NOT NULL,
  clock INTEGER, content_hash TEXT NOT NULL, at_ms INTEGER NOT NULL,
  PRIMARY KEY (ws, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS files (
  ws TEXT NOT NULL, path TEXT NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY (ws, path)) WITHOUT ROWID;
";

const COLUMNS: &str = "schema_id, key, kind, revision, content_hash, bytes, metadata, \
    yjs_state_vector, yjs_clock, origin_actor, origin_device, origin_parent";

const KEEP: usize = 8;

fn db(e: rusqlite::Error) -> Error {
    Error::Io(std::io::Error::other(e))
}

fn int(v: u64) -> Result<i64> {
    i64::try_from(v)
        .map_err(|_| Error::Invalid("number beyond the database's integer range".into()))
}

fn uint(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| Error::Integrity("negative number in the database".into()))
}

fn read_record(row: &Row<'_>) -> rusqlite::Result<(Record, i64, Option<i64>, Option<i64>)> {
    let sv: Option<Vec<u8>> = row.get(7)?;
    let clock: Option<i64> = row.get(8)?;
    let actor: Option<String> = row.get(9)?;
    let device: Option<String> = row.get(10)?;
    let parent: Option<i64> = row.get(11)?;
    let rev: i64 = row.get(3)?;
    let rec = Record {
        schema_id: row.get(0)?,
        key: row.get(1)?,
        kind: row.get(2)?,
        revision: 0,
        content_hash: row.get(4)?,
        bytes: row.get(5)?,
        metadata: row.get(6)?,
        yjs: sv.map(|state_vector| Yjs {
            state_vector,
            clock: 0,
        }),
        origin: actor.zip(device).map(|(actor, device)| Origin {
            actor,
            device,
            parent_revision: 0,
        }),
    };
    Ok((rec, rev, clock, parent))
}

/// Finish a record read: integers back to `u64`, bytes checked against the hash.
fn finish(parts: (Record, i64, Option<i64>, Option<i64>)) -> Result<Record> {
    let (mut rec, rev, clock, parent) = parts;
    rec.revision = uint(rev)?;
    if let (Some(y), Some(c)) = (rec.yjs.as_mut(), clock) {
        y.clock = uint(c)?;
    }
    if let (Some(o), Some(p)) = (rec.origin.as_mut(), parent) {
        o.parent_revision = uint(p)?;
    }
    if content_hash(&rec.bytes) != rec.content_hash {
        return Err(Error::Integrity(format!(
            "{}: bytes do not match the recorded hash",
            rec.key
        )));
    }
    Ok(rec)
}

fn select_docs(tx: &Connection, table: &str, ws: &str, key: Option<&str>) -> Result<Vec<Record>> {
    let sql = format!(
        "SELECT {COLUMNS} FROM {table} WHERE ws = ?1 AND (?2 IS NULL OR key = ?2) ORDER BY key, revision"
    );
    let mut stmt = tx.prepare(&sql).map_err(db)?;
    let rows = stmt
        .query_map(params![ws, key], read_record)
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(db)?;
    rows.into_iter().map(finish).collect()
}

fn insert_doc(tx: &Connection, table: &str, ws: &str, r: &Record) -> Result<()> {
    let sql = format!(
        "INSERT OR REPLACE INTO {table} (ws, {COLUMNS}) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)"
    );
    tx.execute(
        &sql,
        params![
            ws,
            r.schema_id,
            r.key,
            r.kind,
            int(r.revision)?,
            r.content_hash,
            r.bytes,
            r.metadata,
            r.yjs.as_ref().map(|y| y.state_vector.clone()),
            r.yjs.as_ref().map(|y| int(y.clock)).transpose()?,
            r.origin.as_ref().map(|o| o.actor.clone()),
            r.origin.as_ref().map(|o| o.device.clone()),
            r.origin
                .as_ref()
                .map(|o| int(o.parent_revision))
                .transpose()?,
        ],
    )
    .map_err(db)?;
    Ok(())
}

fn insert_op(tx: &Connection, ws: &str, key: &str, op: &Op) -> Result<()> {
    tx.execute(
        "INSERT OR REPLACE INTO workspace_sub_doc_ops (ws, key, clock, actor_class, actor_id, at_ms, hash, bytes) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![ws, key, int(op.clock)?, op.actor_class, op.actor_id, int(op.at_ms)?, content_hash(&op.bytes), op.bytes],
    )
    .map_err(db)?;
    Ok(())
}

fn insert_receipt(tx: &Connection, ws: &str, r: &Receipt) -> Result<()> {
    tx.execute(
        "INSERT OR REPLACE INTO receipts (ws, seq, key, revision, clock, content_hash, at_ms) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![
            ws,
            int(r.seq)?,
            r.key,
            int(r.revision)?,
            r.clock.map(int).transpose()?,
            r.content_hash,
            int(r.at_ms)?
        ],
    )
    .map_err(db)?;
    Ok(())
}

fn select_ops(
    tx: &Connection,
    ws: &str,
    key: Option<&str>,
    after: u64,
) -> Result<Vec<(String, Op)>> {
    let mut stmt = tx
        .prepare(
            "SELECT key, clock, actor_class, actor_id, at_ms, hash, bytes FROM workspace_sub_doc_ops \
             WHERE ws = ?1 AND (?2 IS NULL OR key = ?2) AND clock > ?3 ORDER BY key, clock",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![ws, key, int(after)?], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Vec<u8>>(6)?,
            ))
        })
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(db)?;
    rows.into_iter()
        .map(|(key, clock, actor_class, actor_id, at_ms, hash, bytes)| {
            if content_hash(&bytes) != hash {
                return Err(Error::Integrity(format!(
                    "{key}: op {clock} does not match its hash"
                )));
            }
            Ok((
                key,
                Op {
                    clock: uint(clock)?,
                    actor_class,
                    actor_id,
                    at_ms: uint(at_ms)?,
                    bytes,
                },
            ))
        })
        .collect()
}

fn select_receipts(tx: &Connection, ws: &str, after: u64) -> Result<Vec<Receipt>> {
    let mut stmt = tx
        .prepare("SELECT seq, key, revision, clock, content_hash, at_ms FROM receipts WHERE ws = ?1 AND seq > ?2 ORDER BY seq")
        .map_err(db)?;
    let rows = stmt
        .query_map(params![ws, int(after)?], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(db)?;
    rows.into_iter()
        .map(|(seq, key, revision, clock, content_hash, at_ms)| {
            Ok(Receipt {
                seq: uint(seq)?,
                key,
                revision: uint(revision)?,
                clock: clock.map(uint).transpose()?,
                content_hash,
                at_ms: uint(at_ms)?,
            })
        })
        .collect()
}

fn select_assets(tx: &Connection, ws: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let mut stmt = tx
        .prepare("SELECT path, bytes FROM files WHERE ws = ?1 ORDER BY path")
        .map_err(db)?;
    stmt.query_map(params![ws], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(db)
}

fn check_record(r: &Record) -> Result<()> {
    if r.revision == 0 {
        return Err(Error::Invalid("revision 0 is not a stored revision".into()));
    }
    if content_hash(&r.bytes) != r.content_hash {
        return Err(Error::Integrity(format!(
            "{}: hash does not match the bytes",
            r.key
        )));
    }
    if r.yjs.as_ref().is_some_and(|y| y.clock == 0) {
        return Err(Error::Invalid("clock 0 is not a stored clock".into()));
    }
    if r.key.is_empty() {
        return Err(Error::Invalid("empty key".into()));
    }
    Ok(())
}

/// A workspace store in a `SQLite` database.
pub struct SqliteStore {
    conn: Mutex<Connection>,
    keep: usize,
}

impl SqliteStore {
    /// Open (and create) a database file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::init(Connection::open(path).map_err(db)?)
    }

    /// A store in memory, gone with the value.
    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory().map_err(db)?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(10))
            .map_err(db)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(db)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(db)?;
        conn.execute_batch(SCHEMA).map_err(db)?;
        Ok(Self {
            conn: Mutex::new(conn),
            keep: KEEP,
        })
    }

    /// Keep this many earlier revisions of every document (default 8).
    #[must_use]
    pub fn with_retention(mut self, keep: usize) -> Self {
        self.keep = keep;
        self
    }

    fn tx<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| Error::Integrity("database lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;
        let out = f(&tx)?;
        tx.commit().map_err(db)?;
        Ok(out)
    }

    /// Apply a conditional write. Returns its receipt, or `Error::Conflict` when the stored
    /// revision (or clock) is not the one the commit builds on; nothing is written then.
    pub fn commit(&self, ws: &str, c: &Commit) -> Result<Receipt> {
        let rec = &c.record;
        check_record(rec)?;
        match (&rec.yjs, &c.op) {
            (Some(y), Some(op)) if op.clock == y.clock && !op.bytes.is_empty() => {}
            (None, None) => {}
            _ => {
                return Err(Error::Invalid(
                    "a Yjs record needs an op of its clock, and only it".into(),
                ));
            }
        }
        if ws.is_empty() {
            return Err(Error::Invalid("empty name".into()));
        }
        self.tx(|tx| {
            let cur = select_docs(tx, "documents", ws, Some(&rec.key))?.pop();
            let rev = cur.as_ref().map_or(0, |r| r.revision);
            if rec.revision != rev + 1 {
                return Err(Error::Conflict { key: rec.key.clone(), expected: rec.revision - 1, actual: rev });
            }
            let clock = cur.as_ref().and_then(|r| r.yjs.as_ref()).map(|y| y.clock);
            match (&rec.yjs, clock) {
                (Some(y), cur) => {
                    let cur = cur.unwrap_or(0);
                    if y.clock != cur + 1 {
                        return Err(Error::Conflict { key: rec.key.clone(), expected: y.clock - 1, actual: cur });
                    }
                }
                (None, Some(_)) => return Err(Error::Invalid("a Yjs document cannot lose its clock".into())),
                (None, None) => {}
            }
            tx.execute("INSERT OR IGNORE INTO workspaces (id) VALUES (?1)", params![ws]).map_err(db)?;
            if let Some(old) = &cur {
                insert_doc(tx, "snapshots", ws, old)?;
                let keep = int(self.keep as u64)?;
                tx.execute(
                    "DELETE FROM snapshots WHERE ws = ?1 AND key = ?2 AND revision NOT IN \
                     (SELECT revision FROM snapshots WHERE ws = ?1 AND key = ?2 ORDER BY revision DESC LIMIT ?3)",
                    params![ws, rec.key, keep],
                )
                .map_err(db)?;
            }
            if let Some(op) = &c.op {
                insert_op(tx, ws, &rec.key, op)?;
            }
            insert_doc(tx, "documents", ws, rec)?;
            let last: Option<i64> = tx
                .query_row("SELECT MAX(seq) FROM receipts WHERE ws = ?1", params![ws], |r| r.get(0))
                .optional()
                .map_err(db)?
                .flatten();
            let receipt = Receipt {
                seq: last.map_or(Ok(1), |s| uint(s).map(|s| s + 1))?,
                key: rec.key.clone(),
                revision: rec.revision,
                clock: rec.yjs.as_ref().map(|y| y.clock),
                content_hash: rec.content_hash.clone(),
                at_ms: c.at_ms,
            };
            insert_receipt(tx, ws, &receipt)?;
            Ok(receipt)
        })
    }

    /// The current record of a document.
    pub fn get(&self, ws: &str, key: &str) -> Result<Option<Record>> {
        self.tx(|tx| Ok(select_docs(tx, "documents", ws, Some(key))?.pop()))
    }

    /// Every document key of a workspace, sorted.
    pub fn keys(&self, ws: &str) -> Result<Vec<String>> {
        self.tx(|tx| {
            Ok(select_docs(tx, "documents", ws, None)?
                .into_iter()
                .map(|r| r.key)
                .collect())
        })
    }

    /// The ops of a Yjs document with a clock above `after`, in clock order.
    pub fn ops_since(&self, ws: &str, key: &str, after: u64) -> Result<Vec<Op>> {
        self.tx(|tx| {
            Ok(select_ops(tx, ws, Some(key), after)?
                .into_iter()
                .map(|(_, o)| o)
                .collect())
        })
    }

    /// Receipts with a sequence number above `after`, in order.
    pub fn receipts(&self, ws: &str, after: u64) -> Result<Vec<Receipt>> {
        self.tx(|tx| select_receipts(tx, ws, after))
    }

    /// Earlier revisions kept of a document, oldest first.
    pub fn retained(&self, ws: &str, key: &str) -> Result<Vec<Record>> {
        self.tx(|tx| select_docs(tx, "snapshots", ws, Some(key)))
    }

    /// Store an asset under a `/`-separated relative path.
    pub fn put_asset(&self, ws: &str, path: &str, bytes: &[u8]) -> Result<()> {
        let bad = path
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
            || path.contains('\\');
        if bad || ws.is_empty() {
            return Err(Error::Invalid(format!("bad asset path {path:?}")));
        }
        self.tx(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO workspaces (id) VALUES (?1)",
                params![ws],
            )
            .map_err(db)?;
            tx.execute(
                "INSERT OR REPLACE INTO files (ws, path, bytes) VALUES (?1,?2,?3)",
                params![ws, path, bytes],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    /// The bytes of an asset.
    pub fn get_asset(&self, ws: &str, path: &str) -> Result<Option<Vec<u8>>> {
        self.tx(|tx| {
            tx.query_row(
                "SELECT bytes FROM files WHERE ws = ?1 AND path = ?2",
                params![ws, path],
                |r| r.get(0),
            )
            .optional()
            .map_err(db)
        })
    }

    /// Everything of a workspace in a deterministic order.
    pub fn dump(&self, ws: &str) -> Result<Dump> {
        self.tx(|tx| {
            Ok(Dump {
                records: select_docs(tx, "documents", ws, None)?,
                ops: select_ops(tx, ws, None, 0)?,
                receipts: select_receipts(tx, ws, 0)?,
                retained: select_docs(tx, "snapshots", ws, None)?,
                assets: select_assets(tx, ws)?,
            })
        })
    }

    /// Load a dump into an empty workspace, keeping receipts, clocks and retained revisions exactly.
    pub fn restore(&self, ws: &str, d: &Dump) -> Result<()> {
        for r in d.records.iter().chain(&d.retained) {
            check_record(r)?;
        }
        self.tx(|tx| {
            let held: i64 = tx
                .query_row(
                    "SELECT (SELECT COUNT(*) FROM documents WHERE ws = ?1) + (SELECT COUNT(*) FROM receipts WHERE ws = ?1) \
                     + (SELECT COUNT(*) FROM files WHERE ws = ?1)",
                    params![ws],
                    |r| r.get(0),
                )
                .map_err(db)?;
            if held != 0 || ws.is_empty() {
                return Err(Error::Invalid(format!("workspace {ws} is not empty")));
            }
            tx.execute("INSERT OR IGNORE INTO workspaces (id) VALUES (?1)", params![ws]).map_err(db)?;
            for r in &d.records {
                insert_doc(tx, "documents", ws, r)?;
            }
            for r in &d.retained {
                insert_doc(tx, "snapshots", ws, r)?;
            }
            for (key, op) in &d.ops {
                insert_op(tx, ws, key, op)?;
            }
            for r in &d.receipts {
                insert_receipt(tx, ws, r)?;
            }
            for (path, bytes) in &d.assets {
                tx.execute("INSERT INTO files (ws, path, bytes) VALUES (?1,?2,?3)", params![ws, path, bytes]).map_err(db)?;
            }
            Ok(())
        })
    }
}
