# cvdb

<!-- ccid:security:begin (rendered by ccid render; do not edit by hand) -->
**Security and dependencies:** watched by Renovate (with OSV vulnerability alerts); an update is merged only when its tests pass. See the [security policy](SECURITY.md).
<!-- ccid:security:end -->

The workspace sync client for [CareerVector](https://careervector.corbet.ch), over the
[`cvws`](https://git.corbet.ch/corbet-fsl/cvws) workspace API. It moves bytes and typed
envelopes between a `cvws::Workspace` and a workspace server; it never reads what the
documents say. Blocking I/O (`ureq`, `tungstenite`, rustls), no async runtime.

Version 0.2.0 replaces the 0.1.0 JSON Lines utility.

| piece | what it does |
|---|---|
| `Link` | a pasted private link or a bare workspace id; the bearer secret is never printed, logged or put in an error; stored 0600 |
| `Cvdb::attach` | `GET /workspaces/:id` for `writePath`, then a conditional (`If-None-Match`) load of `cv-profile` and `cl-profile`, then an outbox replay |
| `Cvdb::edit` | one user action (one op or one `batch`): one request |
| outbox | append-only file with a per-entry checksum, fsynced before anything is sent; a replay sends the same opId and the same bytes; a bad entry is parked, shown once, never retried |
| `Cvdb::start_live` | relay WebSocket, receive only (no ping, no awareness); reconnect with jittered backoff of 1 to 60 s |
| `Cvdb::apply_live` | frames go to `cvws::receive` on a thread with the 256 MiB stack its README requires; frames over its cap are refused, never decoded; a raised resync flag reloads at most once every 5 minutes |

## Write modes

* **Append** (`writePath: append`): the op is applied locally, the bytes it produced go to
  `POST /append`, human class only.
* **Server-applied** (`writePath: legacy`): the op is never applied locally first. `cvws::prepare`
  builds the envelope, `POST /ops` sends it, and the server's canonical bytes come back through
  `receive` (relay frame, or a refresh of the touched sub-documents when no relay is connected).
  A local apply would be applied twice when those bytes arrive.

A flag that goes dark under an append write (409 `not_on_append_path`) flips the client to
server-applied mode: queued ops go through `/ops` with the same opId and the documents are reloaded
from the server, so the server's bytes are the only copy. A flag flipped back to `append` is noticed by
the five-minute probe.

## Rules for hosts

* **Logging (hard rule).** The private link is a bearer secret and lives in request URLs. `tungstenite`
  logs `Trying to contact <uri>` at debug level and `ureq` logs URLs at trace level, so a host that
  installs a logger must keep the `tungstenite` and `ureq` log targets at `info` or below. cvdb itself
  logs nothing and its errors never carry a URL.
* **One instance per directory.** `Cvdb::open` takes an exclusive lock on `cfg.dir/lock` and refuses a
  second instance (`Error::Locked`). The directory is 0700, every file 0600.
* **Queue behaviour.** Entries are sent strictly in order and an edit queues behind older ones. Only the
  API's own refusals (JSON `{"error": ...}` with status 400 or 422) park an entry; a 426 (catalog
  mismatch), an edge page or any other answer keeps it queued. A parked append entry makes the later
  ones derive their bytes again against the reloaded document.
* **Frames.** Relay and tail frames are capped at 1 MiB, the stored head at 32 MiB (it is a whole state).
  WebSocket messages are limited to the same cap plus 64 bytes.
* **Shadows.** `attach` loads the five non-profile sub-documents once as routing shadows (5 reads), so a
  busy browser peer's frames never pile up and evict into resyncs.

## Cloudflare budget

No request per keystroke. One `/append` per committed edit (blur or confirm); a `batch` is one request;
mode detection once per attach; resync and mode probes at most once every 5 minutes; reconnects back
off. `Counts` counts every request by kind; `tests/e2e.rs` asserts the budget for a scripted two-hour
session.

## Tests

```sh
cargo test --locked                       # unit tests and tests/offline.rs (fake transport)
CVDB_E2E_CV=<CareerVector checkout> cargo test --test e2e -- --nocapture
```

The end-to-end test starts a local `wrangler dev` relay and api (`tests/e2e/stack.ts`, built on the
CareerVector `ui/e2e/stack.ts` service commands; needs `bun install` in that checkout) and never touches
a deployed Worker.

## Licence

FSL-1.1-ALv2, see `LICENSE.md`.
