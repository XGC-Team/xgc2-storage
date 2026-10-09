# xgc2-storage

Independent SQLite owner. The product exposes bounded document snapshots, atomic
CAS batches and durable mutation receipts through the shared XRPC HTTP and native
gRPC SDKs. Core relational data modules own separate named data operations;
document primitives alone do not claim complete workflow migration.

Contracts: [storage-v1](contracts/storage-v1.md), [Core relational operations](contracts/core-data-v1.md), types: [api](api/types.go),
client: [client](client/client.go), native protocol: [protobuf](protocol/storage.proto).

## Build and isolated checks

```sh
GOPRIVATE=github.com/XGC-Team/* go test ./engine ./modules/coredata ./server ./registry ./cmd/...
GOPRIVATE=github.com/XGC-Team/* go test -race ./engine ./modules/coredata ./server
python3 scripts/fault-validate.py
CGO_ENABLED=0 go build -o build/xgc2-storage ./cmd/xgc2-storage
CGO_ENABLED=0 go build -o build/storage-admin ./cmd/storage-admin
```

Go >=1.25, Linux. modernc.org/sqlite v1.46.2 pins SQLite 3.51.3 and matching
modernc.org/libc v1.70.0; runtime verifies the actual engine. Shared XRPC uses the
immutable private remote module in go.mod/go.sum; builds need repository credentials
and never copy a mutable SDK checkout. Transport owns
socket lifetime, instance binding, deadlines and finite connection admission.

The independent fault runner builds private binaries, supplies the required test
environment and records source hashes and results outside this repository. Running
`go test ./tests/faults` alone omits those binaries and intentionally fails.

An isolated Debian build supports amd64/arm64 for focal/jammy/noble. It installs
the two static executables, contracts, schema manifests and build receipt. It
requires committed source by default; `--preview` labels a dirty snapshot.

```sh
python3 scripts/build-package.py --architecture amd64 --distribution noble \
  --output "$XGC_STORAGE_PACKAGE_ARTIFACT"
```

## Explicit deployment

For Core configuration, the owner supplies `--configuration-domains` with the
compiled catalog declaration array. Storage applies those facts in its own
startup transaction before publishing a ServiceRef. Existing schema and
capability changes are rejected; normal data requests never adopt declarations.

The deployment owner supplies a mode0700 managed database directory, a reviewed
manifest, an absent new DB for an explicit rebuild, a mode0600 owner-grant file,
and a private runtime directory. No HOME/CWD/tmp default or old database fallback.
The user authorized zero-compatible rebuild; normal startup never imports legacy
tables or upgrades old schema. `--create` fails if the DB already exists.

```sh
build/xgc2-storage --db "$XGC_STORAGE_DATABASE_GRANT" \
  --manifest "$XGC_STORAGE_MANIFEST" --grants "$XGC_STORAGE_GRANTS" \
  --http-socket "$XGC_STORAGE_HTTP_SOCKET" \
  --grpc-socket "$XGC_STORAGE_GRPC_SOCKET" \
  --target-id "$XGC_STORAGE_TARGET_ID" --ref-out "$XGC_STORAGE_REFERENCE_FILE"
```

`--print-modules` emits the compiled module declarations without opening a database
or listener. Deployment composition can combine these with the client product's
compiled namespaces and domain declarations.

`--identity-out` optionally writes the actual database identity to a private
runtime file before references become ready. It uses the same atomic mode0600
publication as `--ref-out`; consumers do not infer database identity from a data
read.

`--ref-out` contains a JSON array of fully bound ServiceRef values. A fresh process
instance ID is generated for each start; database_id remains in snapshot tokens.
Core and every Agent have separate locally recoverable storage instances. Client
bootstrap consumes one ServiceRef, the exact Scope, and an owner grant. Grants:

```json
[{"token":"<private random token of at least 32 characters>",
  "namespace":"lichtblick","user":"operator","workspace":"station"}]
```

Wildcards must be explicit in the deployment grant. Browser code never receives
this token. Product gateways enforce user/domain authorization and schemas before
calling storage. `httpx.Config` needs both MaxRequestBytes and MaxResponseBytes
set to 4 MiB for this data profile; the SDK default is 1 MiB. Native gRPC has
byte-preserving JSON fields instead of protobuf Struct doubles. The registered
Core module requires 16 MiB transport budgets for bounded group preparation and
namespace clone operations; document operations retain their own 4 MiB bounds.
Use the generated
stubs or `client.Client` with `grpcx.Profile` and the registered descriptors.

`/v1/named` executes only compiled, manifest-matched operations with no SQL or
transaction handle in the request. `/v1/named-result` retrieves the original
committed result. Core group preparation supports 1000 members and 8 MiB of
parameters; namespace clone verifies the complete bounded closure in one
transaction. Point and predicate guards remain module-owned. The daemon resolves
the shared `XGC2_XRPC_` policy with manifest byte ceilings and bounded diagnostics.

The service holds the file owner lock until both DB pools have closed. Its
restricted directory is a deployment grant; enforce separate mounts/UID access
for consumers at integration. Same-UID untrusted processes are outside the
filesystem isolation claim. FULL/WAL commits, finite record/index/tombstone and
receipt quotas, max DB pages and pre-write WAL/free-space watermarks are applied.
A hard combined disk cap additionally requires filesystem/project quota.
One maintenance worker runs a finite PASSIVE checkpoint and removes at most 256
expired receipts every second; declared receipt expiry bounds replay history.
Already committed replay is checked before admitting a new disk write.
Each transport admits at most four connections; gRPC admits one stream per
connection before protobuf decoding, with an eight-call host ceiling. Environment
values cannot exceed these deployment ceilings. This bounds encoded message
concurrency; protobuf, JSON and SQLite allocations add to those encoded bytes.
For continuous two-write-per-second autosave, a six-hour replay TTL retains about
43,200 receipts per active scope, leaving margin in a 65,536-receipt quota. A
one-day TTL needs more than 172,800 receipts plus cleanup margin. Products declare
their required replay window and quota; expiry removes receipts and named replay
results only, preserving saved documents and relational business facts.

## Administrative operations

`storage-admin` acquires the same exclusive file owner; it rejects a live daemon.
Stop through the owning deployment window before offline check/backup/cleanup.
Backup uses consistent VACUUM INTO, integrity/identity checks, fsync, and
no-overwrite publication; never copy only an active main database file. Backup
directory: mode0700, <=128 outputs, <=2 * configured MaxDBBytes. Restore is an
explicit deployment operation: use a verified private backup as the new managed
file while no runtime owner exists. No live WAL/SHM deletion or overwrite tool.

```sh
build/storage-admin check --db "$XGC_STORAGE_DATABASE_GRANT" --manifest "$XGC_STORAGE_MANIFEST"
build/storage-admin backup --db "$XGC_STORAGE_DATABASE_GRANT" --manifest "$XGC_STORAGE_MANIFEST" --destination "$XGC_STORAGE_BACKUP_GRANT"
build/storage-admin checkpoint --db "$XGC_STORAGE_DATABASE_GRANT" --manifest "$XGC_STORAGE_MANIFEST"
build/storage-admin prune-receipts --db "$XGC_STORAGE_DATABASE_GRANT" --manifest "$XGC_STORAGE_MANIFEST" --limit 256
```

No user data, live station, old database import, remote publishing or deployment
is part of these isolated checks. Product acceptance must still prove its real
capabilities through the new consumers and the managed service installation.
