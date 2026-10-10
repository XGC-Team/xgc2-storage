# Core data: typed relational facts

Owner: xgc2-storage, `modules/coredata`. Types and limits: `modules/coredata/model` (no SQL,
driver, engine or XRPC imports). Core calls the functions below directly through
`host.Host.Core(scope)`; they are Go functions with typed arguments, not requests, and they
are not exposed over XRPC unless a remote consumer appears. Core decides every domain matter
(authorization, schemas, visibility, policy, IDs, pins, workflow behavior); Storage checks
the data guards and constraints inside one owner transaction and decides nothing about
workflows.

Every method is one transaction: it commits all of its effects or none. A method runs on the
single writer (writes) or a reader connection (reads), under the caller's deadline or the
owner's call budget. No savepoint, receipt or JSON envelope is involved. Revisions, versions
and counts that can exceed 2^53 are canonical decimal **strings**; Run and Session revisions
are int64.

## Configuration

Resources with branches, immutable commits and CAS heads, kept exactly as they were:
namespaces (folders), resources, branches, commits with their frozen payload, manifest and
references, change records, the accepted domain catalog, and the permanent product mutation
receipts that make every configuration write idempotent by its key. All are Durable.

| Function | Kind | Result |
| --- | --- | --- |
| `CreateResource`, `CommitResource` | write | `ConfigurationMutationResult` (created, committed, noop, identity-only; replayed on a repeated key) |
| `CreateBranch`, `ArchiveBranch`, `SetResourceState`, `UpdateResourceMetadata` | write | `ConfigurationMutationResult` |
| `CreateNamespace`, `UpdateNamespace`, `SetNamespaceState`, `CloneNamespace` | write | `ConfigurationNamespaceResult` |
| `ReadResource` | read | immutable snapshot by resource+branch or resource+commit, with the current main visibility pin |
| `Receipt`, `CloneReceipt` | read | the stored result of a product mutation key |
| `IncomingReferences` | read | live references that point at a resource |
| `NamespaceTree` | read | complete live subtree with every current main, verified against its content digest |
| `Namespaces`, `Resources`, `Branches`, `Commits`, `Changes` | read | catalog metadata, bounded |
| `DeclareConfigurationDomains` (package function) | write | applies the owner's domain catalog; capabilities of a declared domain cannot change |

## Runs

`runs` holds one row per Run: lineage (`root_run_id`, `parent_run_id`, `call_node_id`,
`depth`), `session_id`, a unique `idempotency_key`, the pinned workflow (`workflow_resource_id`,
`workflow_commit_id`, `definition_digest`, `action_id`), the frozen inputs and trigger, the
status, and, once finished, the termination, error, result, node records and cleanup errors.
Statuses: `queued`, `running`, `stopping`, `succeeded`, `failed`, `stopped`, `canceled`,
`interrupted`.

| Function | Class | Behavior |
| --- | --- | --- |
| `CreateRun` | Durable | accepts a Run as `queued` or `running`; lineage is checked (a root is its own root at depth 0); with a key, a repeated call returns the stored Run and `created=false` |
| `UpdateRunStatus(updates...)` | Relaxed | moves open Runs to `running` or `stopping`; one transaction for the whole slice; repeating is a no-op, going back is `failed_precondition`; one bad entry rolls the batch back |
| `FinishRun` | Durable | records a terminal status and the outcome once; recorded even when the quota is full |
| `GetRun`, `FindRunByIdempotencyKey` | read | |
| `ListRuns` | read | newest first by (created_at, id); filters: target, root, session, workflow, statuses, time window; opaque cursor; `OmitPayloads` leaves out the large JSON columns |
| `InterruptOpenRuns` | Durable | Core's boot step: every open Run becomes `interrupted`; no half-run graph resumes |
| `PruneRuns` | Relaxed | deletes finished Runs by age and/or by count (newest kept), at most `Limit` per call; never a Run whose root is still open |

## Sessions

One Session per target is live (`open` or `stopping`); the database enforces it.

| Function | Class | Behavior |
| --- | --- | --- |
| `OpenSession` | Durable | a second live Session on the target is a `conflict`; opening the same Session again returns it with `created=false` |
| `RequestStop` | Durable | `open` to `stopping` with the stop intent (a JSON object, immutable: later calls return the Session unchanged) |
| `CloseSession` | Durable | `open`/`stopping` to `closed`; closing again is a no-op |
| `GetSession`, `ListSessions` | read | newest first; filters: target, experiment resource, statuses |
| `CloseOpenSessions` | Durable | Core's boot step: live Sessions become `interrupted` |

## Recordings

`AddRecording` (Durable, idempotent by id; different facts under one id are a `conflict`),
`GetRecording` and `ListRecordings` (oldest first; filters: session, run, kind) keep the index
of recordings that Runs registered.

## Quota and retention

Rows and bytes of Core data are charged to a per-scope live total (1,000,000 rows, 512 MiB);
deleting a Run frees its share. `CreateRun`, `OpenSession` and `AddRecording` are refused at
the limit with `resource_exhausted`, while `FinishRun` always records the outcome of a Run
that was admitted. Core prunes with `PruneRuns` on its own schedule.

## Schema version and migration

The module's schema version is an integer, currently 2. Opening a database with the same
version succeeds whatever the code; an older one is migrated in one transaction after a
backup; a newer one is refused. Version 1, which also held the workflow engine's data, is
migrated by keeping every configuration table byte for byte and removing the engine's
document collections, command ledger, event log, sealed groups and panel-state namespace.
The migration recognizes version 1 by its complete table set and refuses any other layout.
