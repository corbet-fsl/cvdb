# cvdb

<!-- ccid:security:begin (rendered by ccid render; do not edit by hand) -->
**Security and dependencies:** watched by Renovate (with OSV vulnerability alerts); an update is merged only when its tests pass. See the [security policy](SECURITY.md).
<!-- ccid:security:end -->

The database backend of the CareerVector workspace (`cvws`). It moves opaque bytes and never applies
or merges documents. It holds the SQLite store (`cvdb::store`) and the byte transport pieces
(`http`, `link`, `live` relay, durable `outbox`).

The session client that applied ops to a local Yrs document (`Cvdb`) was removed from this crate so
that `ccvl -> cvws -> cvdb` has no cycle; it is preserved at tag `pre-session-split`.

## SQLite storage backend (`cvdb::store`, feature `sqlite`, on by default)

`cvdb::store::SqliteStore` is the database backend of the CareerVector workspace (`cvws`): the same
logical rows `cvfs` keeps as files, in one SQLite file or in memory. Tables: `workspaces`,
`documents`, `snapshots` (retained revisions), `workspace_sub_doc_ops` (Yjs op log), `receipts`,
`files` (assets). Conditional writes (CAS on revision and Yjs clock), hash integrity and receipts
behave exactly as in `cvfs`; both speak the `cvfs` record vocabulary, so `dump` and `restore` move a
workspace between the two without loss. It never parses document bytes or metadata.
