# xgc2-storage

Storage owns persistence for XGC2: SQLite connections, schema, migrations, transactions,
durability, maintenance, backup and recovery. It is a **Go library first**. Core embeds it and
calls typed Go functions; there is no daemon in front of Core and nothing to start before it.
For processes that cannot embed it, the same owner can be exposed over XRPC (HTTP and gRPC on
private Unix sockets): the `xgc2-storage` daemon does that for the Lichtblick launcher. The
Agent uses no Storage.

Consumers keep the domain decisions and decide when to read and write. Storage guards the data
and the atomic boundaries; it contains no workflow logic.

| Package | Role |
| --- | --- |
| `host` | opens one owner (`Open`), hands out the in-process document client and the typed Core API, optional XRPC exposure (`Serve`) |
| `modules/coredata` | typed Core data: configuration, Runs, Sessions, recordings (`modules/coredata/model` holds the types) |
| `engine` | the SQLite owner: connections, durability classes, document store, schema versions, maintenance |
| `client`, `server`, `protocol` | XRPC client and handlers for the document store, protobuf for gRPC |
| `api` | wire types and manifest |
| `cmd/xgc2-storage`, `cmd/storage-admin` | the daemon (documents over XRPC) and the offline administration tool |

Contracts: [document store](contracts/documents.md), [Core data](contracts/core-data.md).
Measurements: [docs/benchmarks.md](docs/benchmarks.md).

## Embedding it (Core)

```go
h, err := host.Open(ctx, host.Config{
    Path:     "/var/lib/xgc2/core-storage.db", // the directory is mode 0700 and owned by the process
    Create:   firstStart,                      // explicit; an existing database is never replaced
    Manifest: manifest,                        // namespaces and document collections
    ConfigurationDomains: domains,             // the accepted configuration catalog
    MaxDBBytes: 8 << 30,
})
defer h.Close(shutdownCtx)

core, _ := h.Core(api.Scope{Namespace: "core", User: user, Workspace: workspace}) // typed API
docs, _ := h.Client(api.Scope{Namespace: "core", User: user, Workspace: workspace}) // documents
```

`host.Open` starts no listener and writes no token, bootstrap, reference or identity file. One
owner holds the database through an exclusive file lock; a second process fails fast with
`conflict`. Nothing runs while the database is idle.

### Configuration (`host.Config`)

| Field | Default | Meaning |
| --- | --- | --- |
| `Readers` | 4 (max 16) | concurrent read connections |
| `WriterQueue` | 64 | writers admitted at once; later callers **wait until their own deadline** rather than fail |
| `CallBudget` | 30 s | cap on every call; a shorter caller deadline wins and a caller without one gets the budget |
| `MaxDBBytes` | 1 GiB | finite database capacity (page limit) |
| `Diagnostics` | none | receives background maintenance failures; the failed step is retried after 30 s |

### Durability classes

Every write transaction has a class; the single writer connection switches
`synchronous` before each one.

| Class | SQLite | Survives | Used for |
| --- | --- | --- | --- |
| `Durable` | `synchronous=FULL` | process crash and power loss when the call returns | Run accepted/finished, Session open/stop/close, recordings, configuration |
| `Relaxed` | `synchronous=NORMAL` | process crash; power loss may lose the last relaxed commits until the next checkpoint or durable commit (within a second) | Run status transitions, pruning, audit documents |

`Host.Stats()` reports commits by class, durability barriers, queue wait, SQL time, checkpoints
and file sizes for benchmarks and diagnostics. The owner logs nothing per call.

### Typed Core API (`h.Core(scope)`)

All methods take a `context.Context`; each is one transaction (see [Core data](contracts/core-data.md)).

| Group | Functions |
| --- | --- |
| Configuration writes (Durable) | `CreateResource`, `CommitResource`, `CreateBranch`, `ArchiveBranch`, `SetResourceState`, `UpdateResourceMetadata`, `CreateNamespace`, `UpdateNamespace`, `SetNamespaceState`, `CloneNamespace` |
| Configuration reads | `ReadResource`, `Receipt`, `CloneReceipt`, `IncomingReferences`, `NamespaceTree`, `Namespaces`, `Resources`, `Branches`, `Commits`, `Changes` |
| Runs | `CreateRun` (Durable, idempotent by key), `UpdateRunStatus` (Relaxed, batchable), `FinishRun` (Durable), `GetRun`, `ListRuns` (filters, cursor), `FindRunByIdempotencyKey`, `InterruptOpenRuns` (boot), `PruneRuns` (Relaxed) |
| Sessions | `OpenSession` (Durable, one live per target), `RequestStop` (Durable, immutable intent), `CloseSession` (Durable), `GetSession`, `ListSessions`, `CloseOpenSessions` (boot) |
| Recordings | `AddRecording` (Durable), `GetRecording`, `ListRecordings` |

Core's boot sequence is `InterruptOpenRuns`, `CloseOpenSessions`; no half-run graph resumes.

### Documents (`h.Client(scope)`)

Credentials, settings, audit rows and the Agent runtime's conversations are JSON documents in
declared collections: `Snapshot`, `Batch` (atomic compare-and-set, optional `Relaxed`) and
`Receipt`, plus `WithReadSnapshot`, a view that fences reads to one revision without holding a
reader while the consumer computes. A batch has a receipt only when it carries a request id; an
in-process caller that needs no replay protection leaves it empty.

### Quotas

Quotas count **live** rows and bytes: deleting frees them at once. Document collections keep a
bounded number of tombstones for compare-and-set, oldest dropped first. Receipt counts are
counters maintained with the revision, expired by a timer set for the next expiry.

## Schema versions and migrations

The engine and each data module store an integer schema version and ship forward migrations.
Opening a database with the same version succeeds whatever the code. An older schema is
migrated in one transaction after a consistent private backup next to the database
(`<db>.before-<module>-v<from>.<time>.db`); a newer schema is refused with an error naming the
component. The manifest is configuration, not an identity: new collections, new limits and
changed indexes (rebuilt from the records) take effect at the next open.

The Core data module migrates the database written by its first version: configuration data is
kept byte for byte, the workflow engine's collections, command ledger, event log and panel
state are dropped, and the Run, Session and recording tables are created empty.

## Optional XRPC exposure and the daemon

`h.Serve(ctx, host.ServeConfig{TargetID, HTTPSocket, GRPCSocket, Grants, Limits})` serves the
document store's `Snapshot`, `Batch` and `Receipt` for the granted scopes. Limits are a plain
struct (connections, in-flight calls, stream and message sizes); nothing is read from the
environment. The typed Core API is not exposed remotely.

The daemon is that exposure with a database, a manifest and grants:

```sh
xgc2-storage --db "$DB" --manifest "$MANIFEST" --grants "$GRANTS" \
  --http-socket "$SOCKET" --target-id "$TARGET" --ref-out "$REFS" [--create] \
  [--grpc-socket ...] [--identity-out ...] [--readers N] [--writer-queue N] [--call-timeout 30s] [--max-db-bytes N]
```

The deployment owner supplies a mode 0700 database directory, a manifest, a mode 0600 grants
file (`[{"token":"<32+ bytes>","namespace":"lichtblick","user":"operator","workspace":"station"}]`,
wildcards only when explicit) and a private runtime directory. `--ref-out` receives the bound
ServiceRef array; a fresh instance id is generated for each start. `--create` fails if the
database exists. The Lichtblick launcher uses `{documents, extensions}` in the `lichtblick`
namespace; the camera calibration tools use the same daemon through the C++ SDK. Manifest
example: [schemas/documents-example.json](schemas/documents-example.json).

## Administration

`storage-admin check | stats | checkpoint | backup` (installed as `xgc2-storage-admin`) run offline
against a stopped owner (they take the same file lock and never migrate). Backup is a consistent `VACUUM INTO`, fsynced,
integrity- and identity-checked, and never overwrites. Restore means starting the owner on a
verified backup file; there is no tool that edits a live WAL.

## Build and test

```sh
go vet ./... && go test -p 3 ./...               # unit, migration, quota, concurrency, crash and daemon tests
go test -p 3 -race ./engine ./modules/... ./host # race detector (needs cgo)
go test -run '^$' -bench . ./engine ./modules/coredata   # STORAGE_BENCH_DIR=<dir> picks the filesystem
CGO_ENABLED=0 go build -o build/xgc2-storage ./cmd/xgc2-storage
CGO_ENABLED=0 go build -o build/storage-admin ./cmd/storage-admin
```

Go 1.25 or newer on Linux. `modernc.org/sqlite` is pure Go (`CGO_ENABLED=0` builds); the
owner refuses a SQLite older than 3.51.3 because earlier versions can corrupt a WAL database
when the WAL resets. `tests/faults` builds the daemon and the administration tool from this
tree and runs them: a killed daemon, competing compare-and-set, admission limits, backup and
restore, WAL pressure, a child process killed mid-stream (durable and relaxed commits,
Run facts), and the requests of the Lichtblick launcher.

`scripts/build-package.py` builds the Debian packages (amd64/arm64 for focal/jammy/noble); it
requires committed source unless `--preview` is given.

The service holds the file owner lock until both connection pools have closed. Same-UID
untrusted processes are outside the filesystem isolation claim; enforce separate mounts or UIDs
for consumers at integration.
