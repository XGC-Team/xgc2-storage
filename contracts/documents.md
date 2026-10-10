# Document store: bounded snapshots and atomic compare-and-set plans

Owner: xgc2-storage. Go wire types: `api/types.go`; remote client: `client`; in-process
client: `host.Host.Client`. The XRPC service is `xgc2.storage.v1.Storage`, api_version `1`.
Each physical database has its own persistent `database_id`. Product authorization and
domain validation happen in the product; the daemon additionally authenticates an owner
grant on every request. Browsers use their product's same-origin gateway. No ordinary call
accepts SQL, paths, DDL, schema registrations, transaction handles, callbacks or unlimited
collections.

The document store has two doors onto one implementation. A process that embeds the owner
calls `Snapshot`, `Batch` and `Receipt` as Go functions (no wire codec, no token, no
loopback). A process that does not (the Lichtblick launcher, the camera calibration tools)
calls the same functions through the daemon's XRPC exposure. Both reach the same
transactions, quotas and the single writer.

## Wire operations

All routes are POST and take/return UTF-8 JSON. Required XRPC headers are supplied by its
SDK: finite budget, request identity and server instance binding.

| HTTP route | Request | Successful response |
| --- | --- | --- |
| `/v1/snapshot` | SnapshotRequest | SnapshotResponse |
| `/v1/batch` | BatchRequest | Receipt |
| `/v1/receipt` | ReceiptRequest | Receipt |

The gRPC service has the same three methods. The typed Core operations (configuration,
Runs, Sessions, recordings) are Go functions of `modules/coredata` and are not exposed on the
wire.

`scope={namespace,user,workspace}` is checked against the authenticated grant, including
exact user/workspace values or explicitly reviewed wildcards. Collections must be declared
in the manifest.

## The manifest is configuration

A manifest declares namespaces and collections with their limits and indexes. It is read at
every open and is not a stored identity:

- a new collection, a larger limit or a changed `schema` string take effect when the owner
  opens (tokens carry the namespace `schema`, so a client holding an old token gets a
  conflict and re-reads);
- an index that changed is rebuilt from the records in one transaction; a unique index that
  the stored data violates fails the open with the database untouched;
- a collection that is no longer declared is ignored and its stored index signature dropped;
- a namespace names the compiled data modules it uses (`"modules": ["coredata"]`). The
  daemon compiles none: it serves documents only.

## Snapshot

Snapshot reads all queries in one read transaction and returns fully materialized results
before releasing it. A query selects exact `keys`, one declared equality index (`index` and
scalar `equal` tuple), or a key-ordered page (`after`, `limit`). No joins, predicate
programs, arbitrary fields, aggregates, offsets, or SQL exist on the wire. Missing requested
keys have version `"0"`, missing=true; tombstones have deleted=true and a nonzero version.
Full scans omit tombstones unless include_deleted=true. Limits: 64 queries, 2048 total
returned rows, 4 MiB response (measured on the encoded response by the wire handlers).
Pages are key-ordered and return next_after only when more rows exist. Supplying `at` pins
subsequent pages to the same schema/database/revision: conflict means restart the snapshot,
never silently mix revisions. No server snapshot is kept open across requests.

In process, `WithReadSnapshot` lends a context whose Snapshot calls are all fenced to the
revision of the first one. The view holds no reader connection between calls, so the consumer
may compute freely; a write in between makes the next read fail with a conflict and the
consumer restarts the view. Mutations through a view are refused.

## Batch

A batch contains at most 256 mutations and 4 MiB total request bytes. Every mutation
specifies an exact expected_version (including create=`"0"`), and every batch specifies the
snapshot token's exact expected revision. Whole-scope revision checking protects missing-key,
range, unique-index and admission predicates from phantoms. It is deliberately conservative:
unrelated writes in that scope can conflict. A plan read set cannot cross databases or
namespace scopes. Products compose their read-your-writes overlay locally, then submit
**one** complete batch. A conflict requires a fresh read and a new plan, with a finite
product retry budget. No retry policy is hidden in the storage client.

All mutations, unique indexes, scope revision and receipt commit in one SQLite transaction.
Deletes retain tombstones; recreation compares the tombstone version, avoiding ABA. A batch
cannot mutate the same collection/key twice. New versions are the committed scope revision,
represented as canonical decimal strings (all languages retain integer precision).

### Receipts

A receipt exists only for a request that carries a request id. Over XRPC every batch does
(HTTP header and body must match); an in-process caller that needs no replay protection
leaves the id empty and pays no receipt. An identical normalized request digest returns its
original receipt, including after restart. Reusing an identity with different content is a
conflict. A reply lost after COMMIT is outcome_unknown: query `/v1/receipt` with the same
identity. not_found does not prove a previous request cannot still commit; callers must fence
the earlier call before deciding to retry.

The per-scope receipt count is a counter maintained with the revision, never a COUNT(*).
Receipts have a manifest TTL and quota. Cleanup is scheduled for the next expiry time, in
bounded batches; after expiry no exactly-once claim is made.

### Durability

A batch over XRPC is always durable: the receipt says `durability: "sqlite-full"`, meaning
SQLite's synchronous=FULL contract for that commit; it is not a guarantee against broken
storage hardware. An in-process batch may set `Relaxed`: it commits with synchronous=NORMAL,
survives the death of the process, may lose the last relaxed commits on power loss, and
becomes durable at the next checkpoint or durable commit (within the checkpoint delay,
default 1 s). Its receipt says `sqlite-normal`. The writer connection switches the level per
transaction.

## Quotas

Per-collection `max_records` and `max_bytes` count live records and bytes: a delete frees
them at once. The tombstone a delete leaves keeps its CAS version and is not charged, but at
most `max_records` tombstones are retained per collection, oldest dropped first (a dropped
tombstone leaves its key plainly absent, so recreating it expects version `"0"`). Per-record
bytes, scopes per namespace, receipts per scope, database pages, WAL and backup outputs each
have separate finite limits.

## Admission, maintenance, errors

One writer and a fixed reader count. Admission, queue wait and SQL all use the caller's
deadline or, without one, the owner's call budget. A full writer queue makes the caller wait
until its deadline (deadline_exceeded), never fail only because others are ahead. SQL errors
distinguish disk_full/io_error/corrupt/busy/cancel/deadline.

Maintenance is event driven: a commit arms one checkpoint after the checkpoint delay, a WAL
above its threshold is checkpointed at once, and a stored receipt arms its own expiry. Nothing
runs while the database is idle. `Stats` reports commits by class, durability barriers,
queue wait, SQL time, checkpoints, receipts pruned and physical DB/WAL/SHM bytes.

## Schema versions and migrations

The engine and every data module store an integer schema version. Opening a database with
the same versions succeeds whatever the code. An older schema is migrated forward in one
transaction after a consistent private backup (`<db>.before-<module>-v<from>.<time>.db`
next to the database); a newer schema is refused with an error that names the component.
`storage-admin` never migrates. SQLite must be 3.51.3 or newer.

## Assets, capacity, administration

Documents carry JSON values. Large extensions/images/bags/logs remain in the existing
filesystem owner and use `{owner,asset_id,sha256,bytes}` references; storage does not
resolve arbitrary asset paths or pretend file publication and DB commit are atomic.
History has an explicit retention owner; automatic deletion of saved user data is not
enabled merely to recover quota. DB max_page_count bounds logical pages; before-write disk
watermarks and the WAL admission limit are conservative soft checks. Filesystem/project
quota is required for a hard combined DB+WAL+temporary+backup disk cap.

Administrative check, stats, checkpoint and backup are not data routes. Backup uses a
consistent VACUUM INTO, fsync, an integrity and identity check, and no-overwrite
publication; never copy only a live .db file. The owner lock and a restrictive managed
directory are verified. Restore uses a verified backup as the new managed file while no
owner exists; no live WAL/SHM are unlinked.
