# Storage v1: bounded snapshots and atomic compare-and-set plans

Owner: xgc2-storage. Go wire types: `api/types.go`; transport client: `client`.
ServiceRef service=`xgc2.storage.v1.Storage`, api_version=`1`. Each physical
database has its own persistent database_id; Core and each Agent have independent
local owners. Product authorization and domain validation happen in the product;
storage additionally authenticates an owner grant on every request. Browsers use
their product's same-origin gateway. No ordinary RPC accepts SQL, paths, DDL,
schema registrations, transaction handles, callbacks, or unlimited collections.

## Wire operations

All routes are POST and take/return UTF-8 JSON. Required XRPC headers are supplied
by its SDK: finite budget, request identity, and server instance binding.

| HTTP route | Request | Successful response |
| --- | --- | --- |
| `/v1/snapshot` | SnapshotRequest | SnapshotResponse |
| `/v1/batch` | BatchRequest | Receipt |
| `/v1/receipt` | ReceiptRequest | Receipt |

`scope={namespace,user,workspace}` is checked against the authenticated deployment
grant, including exact user/workspace values or explicitly reviewed wildcards.
Collections must be registered in the deployment manifest. Schema/owner/limits/
index/retention/recovery changes require an explicit reviewed rebuild of the new
schema. Ordinary startup refuses a manifest mismatch or a non-storage database.
The user authorized a destructive zero-compatibility rebuild on 2026-10-09:
no legacy schema import, automatic upgrade, parity conversion, dual reads/writes,
or old database fallback belongs to the normal product. Rebuild activation is an
explicit deployment operation, never a silent empty-database discovery.

Snapshot reads all queries in one read transaction and returns fully materialized
results before releasing it. A query selects exact `keys`, one declared equality
index (`index` and scalar `equal` tuple), or a key-ordered page (`after`, `limit`).
No joins, predicate programs, arbitrary fields, aggregates, offsets, or SQL exist
on the wire. Missing requested keys have version `"0"`, missing=true; tombstones
have deleted=true and a nonzero version. Full scans omit tombstones unless
include_deleted=true. Limits: 64 queries, 2048 total returned rows, 4 MiB response.
Pages are key-ordered and return next_after only when more rows exist. Supplying
`at` pins subsequent pages to the same schema/database/revision: conflict means
restart the snapshot, never silently mix revisions. No server snapshot is kept
open across requests.

Batch contains at most 256 mutations and 4 MiB total request bytes. Every mutation
specifies an exact expected_version (including create=`"0"`), and every batch
specifies the snapshot token's exact expected revision. Whole-scope revision
checking protects missing-key, range, unique-index and admission predicates from
phantoms. It is deliberately conservative: unrelated writes in that scope can
conflict. A plan read set cannot cross databases or namespace scopes. Products
compose their read-your-writes overlay locally, then submit **one** complete
batch. A conflict requires a fresh read and new request identity/plan, with a
finite product retry budget. No retry policy is hidden in the storage client.

All mutations, unique indexes, scope revision and request receipt commit in one
SQLite transaction. Deletes retain tombstones; recreation compares the tombstone
version, avoiding ABA. A batch cannot mutate the same collection/key twice. New
versions are the committed scope revision, represented as canonical decimal
strings (all languages retain integer precision). State/event/receipt and layout/
active-pointer plans must appear in that single batch. Products allocate event
keys/sequences using the guarded snapshot; server validation enforces declared
unique indexes. Notification hubs may publish only after a durable Receipt.

The batch request identity is durable and scoped; HTTP header and body must
match. An identical normalized request digest returns its original receipt,
including after restart. Reusing identity with different content is conflict.
Replies lost after COMMIT are outcome_unknown: query `/v1/receipt` using the same
identity and refreshed instance reference. not_found does not prove a previous
request cannot still commit; callers must fence/finish that call before deciding
to retry. Receipts have a manifest TTL and finite count; cleanup removes only
expired receipts in bounded offline/maintenance batches. After expiry, no
exactly-once claim is made. Keep receipt identities until uncertainty is resolved.

SQLite uses WAL and synchronous=FULL. Receipt durability=`sqlite-full` means
SQLite's FULL commit/fsync contract; it is not a guarantee against broken storage
hardware. Engine version is pinned and queried; versions affected by the WAL reset
bug are refused. Read transactions never await a network peer. Admission, queue
wait and SQL all use the original caller budget. One writer, fixed reader count,
bounded admitted writers; overload is resource_exhausted, with no endless BUSY
retry. SQL errors distinguish disk_full/io_error/corrupt/busy/cancel/deadline.

## Migrate the actual invariants

| Existing boundary | Plan composition retained in product |
| --- | --- |
| execution.Store.TransactDB nested callbacks/savepoints | local plan overlay; child rollback discards its changes; one outer batch/commit, then notifications |
| state + execution events + command receipt | one batch containing state, sequenced events, product receipt; service receipt commits alongside |
| workflow run admission + run + reconcile task + attached owners | one snapshot + revision guard + combined batch; slot predicate cannot race |
| config CAS branch/resource + nodes/commit + typed snapshot + change/receipt | one guarded snapshot + all collection mutations in one batch |
| runtime preparation group/effect/binding checkpoints | one batch before product contacts provider |
| cross-store projections | one SnapshotRequest with all required collection queries |
| layout + activity pointer/preferences | same scope, one batch with exact versions |
| ResearchOS read-modify-write/plan saves | guarded snapshot + one plan per local data owner; no atomicity claimed across its two DBs |
| AppStore rename + installed state | product durable filesystem reconciliation stays outside DB; publish state/recovery facts via atomic batch |

The previous 108-table Core union and 57/62-table Agent catalog are audit evidence
for business capability and atomicity, not a requirement to retain that schema.
The new relational data modules must retain real workflow/configuration ability,
constraints, atomic commits and restart recovery. Document primitives are not the
sole Core API. Named group preparation/namespace clone have separate relational
contracts and bounds; they must not be split into partial commits at the ordinary
256-mutation limit. Activation belongs to the deployment owner; no compatibility
shim or unknown-history inference is part of normal startup.

## Assets, capacity, maintenance

Documents carry JSON values. Large extensions/images/bags/logs remain in the
existing filesystem owner and use `{owner,asset_id,sha256,bytes}` references;
storage does not resolve arbitrary asset paths or pretend file publication and DB
commit are atomic. Product owners declare import/publish/reconcile/recovery.
Registered per-record/per-collection byte/count limits include tombstones.
Receipts, scopes, DB pages, WAL and backup outputs each have separate finite
limits. High-frequency telemetry/stdout/media are not business records.
History has explicit retention owner; automatic deletion of saved user data is
not enabled merely to recover quota. Exhaustion requires declared cleanup or an
explicit limit change. DB max_page_count bounds logical pages; before-write disk
watermarks/WAL admission limits are conservative soft checks. Filesystem/project
quota is required for a hard combined DB+WAL+temporary+backup disk cap.

Administrative backup/checkpoint/rebuild/restore are not public data routes.
Backup uses a consistent VACUUM INTO or native online backup, fsync and
no-overwrite publication; never copies only a live .db file. Source owner lock
and restrictive managed directory are verified. Restore verifies integrity,
manifest and logical contents on a private candidate before activation; no live
WAL/SHM are unlinked. Metrics report queue/SQL time, physical DB/WAL/SHM bytes,
free bytes, busy/errors/conflicts and checkpoint pressure.

This first contract preserves bounded composition; installed services, full
consumer migration and live deployment acceptance are separate evidence gates.
