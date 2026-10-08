# xgc2-storage

Independent SQLite owner. The product exposes bounded document snapshots, atomic
CAS batches and durable mutation receipts through the shared XRPC HTTP and native
gRPC SDKs. Core relational data modules own separate named data operations;
document primitives alone do not claim complete workflow migration.

Contracts: [storage-v1](contracts/storage-v1.md), types: [api](api/types.go),
client: [client](client/client.go), native protocol: [protobuf](protocol/storage.proto).

## Build and isolated checks

```sh
go test ./...
go test -race ./engine ./server
CGO_ENABLED=0 go build -o build/xgc2-storage ./cmd/xgc2-storage
CGO_ENABLED=0 go build -o build/storage-admin ./cmd/storage-admin
```

Go >=1.25, Linux. modernc.org/sqlite v1.46.2 pins SQLite 3.51.3 and matching
modernc.org/libc v1.70.0; runtime verifies the actual engine. The current local
XRPC replace is an integration dependency, not a released SDK pin. Transport owns
socket lifetime, instance binding, deadlines and finite connection admission.

## Explicit deployment

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
byte-preserving JSON fields instead of protobuf Struct doubles. Use the generated
stubs or `client.Client` with `grpcx.Profile` and the registered descriptors.

The service holds the file owner lock until both DB pools have closed. Its
restricted directory is a deployment grant; enforce separate mounts/UID access
for consumers at integration. Same-UID untrusted processes are outside the
filesystem isolation claim. FULL/WAL commits, finite record/index/tombstone and
receipt quotas, max DB pages and pre-write WAL/free-space watermarks are applied.
A hard combined disk cap additionally requires filesystem/project quota.

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
