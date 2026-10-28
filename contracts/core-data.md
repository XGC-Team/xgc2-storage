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
| `CreateRun` | Durable | accepts a Run as `queued` or `running`; lineage is checked (a root is its own root at depth 0); with a key, a repeated call returns the stored Run and `created=false`. The key lookup does not compare inputs |
| `UpdateRunStatus(updates...)` | Relaxed | moves open Runs to `running` or `stopping`; one transaction for the whole slice; repeating the same status is a no-op; a `running` or `stopping` update of an already terminal Run is a no-op and leaves the revision unchanged; an open reverse transition is `failed_precondition`; one bad entry rolls the batch back |
| `FinishRun` | Durable | records a terminal status and the outcome, even when the quota is full. The same outcome returns the stored Run and does not change the row, its revision, or quota: status and termination match, and error, result, nodes, and cleanup errors match as normalized uncompressed JSON bytes; a zero `At` matches, and a non-zero `At` must equal `FinishedAt`. That replay still enters one Durable write transaction, so the owner's commit and fsync accounting can advance. A different outcome is `failed_precondition` |
| `ApplyRunOps(ctx, creates []NewRun, updates []RunStatusUpdate, finishes []RunFinish) ([]CreatedRun, error)` | Durable when `creates` or `finishes` is non-empty; Relaxed when only `updates` is non-empty; no transaction when all three are empty | one transaction, ordered creates, then updates, then finishes, using each Run's literal id. The result has one `CreatedRun{Run, Created}` per create; a repeated idempotency key returns the stored Run with `Created=false` and does not compare inputs. `len(creates)+len(updates)+len(finishes) > 1000` (`MaxPageSize`) is `invalid_argument` and opens no transaction. Any error rolls the whole batch back: no partial result and no quota residue. Same-outcome finishes and terminal status updates inside the batch follow `FinishRun` and `UpdateRunStatus`: those rows, revisions, and quota do not change, while a batch that contains a finish still uses one Durable transaction |
| `GetRun`, `FindRunByIdempotencyKey` | read | |
| `ListRuns` | read | newest first by (created_at, id); filters: exact id, target, root, session, workflow, accepted Experiment/instance, statuses, time window; opaque cursor; fixed `complete`, `identity` and `observation` projections; observation selects write-derived bounded source/error metadata, exact cleanup cardinality and result presence without selecting or decompressing any full execution JSON. Pages obey the response-byte budget as well as the row limit; an oversized first row is refused explicitly. Exact-id observation uses the primary key for bounded source navigation |
| `InterruptOpenRuns` | Durable | Core's boot step: every open Run becomes `interrupted`; no half-run graph resumes |
| `PruneRuns` | Relaxed | deletes finished Runs by age and/or by count (newest kept), at most `Limit` per call; never a Run whose root is still open. Returns `RunPruneResult{Deleted, OldestEligibleFinishedAt}` from the same indexed eligible set after deletion; zero time means no eligible terminal Run remains |

Listings are answered from an index in page order, so a page costs the same however long the
history is: creation order, target, root, session, workflow, a time window, and the open
statuses (`queued`, `running`, `stopping`; "what is running" reads only the open Runs). A
filter by a finished status alone walks Runs in creation order until the page is full, which
`PruneRuns` retention bounds. `TestRecordQueriesUseTheirIndexes` runs the statements the code
builds through `EXPLAIN QUERY PLAN`.

## Sessions

One Session per target is live (`open` or `stopping`); the database enforces it.
The record keeps the pinned experiment resource and commit, its name (possibly empty,
at most 256 UTF-8 bytes without control characters), and the required run mode (a text
key of at most 64 bytes). A close records an outcome of `stopped` or `failed` and an
optional error JSON object, bounded like the stop intent at 64 KiB. Live and interrupted
Sessions have no outcome; Core maps Storage's `interrupted` status to `closed` with
outcome `interrupted`.

| Function | Class | Behavior |
| --- | --- | --- |
| `OpenSession` | Durable | a second live Session on the target is a `conflict`; opening the same Session again returns it with `created=false`; a different target, experiment commit or run mode under that ID is a `conflict` |
| `RequestStop` | Durable | `open` to `stopping` with the stop intent (a JSON object, immutable: later calls return the Session unchanged) |
| `CloseSession(ctx, id, model.SessionClose)` | Durable | `open`/`stopping` to `closed` with outcome and error; closing a closed or interrupted Session is a no-op that returns the stored record |
| `GetSession`, `ListSessions` | read | newest first; filters: target, experiment resource, statuses |
| `CloseOpenSessions` | Durable | Core's boot step: live Sessions become `interrupted` |

## Recordings

`AddRecording` (Durable, idempotent by id; different facts under one id are a `conflict`),
`GetRecording` and `ListRecordings` (oldest first; filters: session, run, kind) keep the index
of recordings that Runs registered.

## Quota and retention

Rows and bytes of Core data are charged to a per-scope live total (1,000,000 rows, 512 MiB);
deleting a Run frees its share. `CreateRun`, `OpenSession` and `AddRecording` are refused at
the limit with `resource_exhausted`, while the first `FinishRun` of an admitted Run
records its outcome even when the quota is already full. A same-outcome `FinishRun`
does not charge quota again. Core prunes with `PruneRuns` on its own schedule.

## Unresolved remote control ownership

`ReserveRemoteRun` durably places a bounded relation on its accepted owner's existing
Run row before a remote Start leaves. The relation records owner/root/target/remote Run,
call node and optional boot. Boot may bind once; conflicting or downgraded resolution
cannot join a different process. `ResolveRemoteRun` requires exact identity and is called
only after definitive unsent/rejection or authoritative terminal join, never Stop acceptance.
These are Durable owner transactions, with idempotent bytes and exact quota release.

Age/count/all-closed pruning excludes each unresolved owner and its local root in the same
transaction. Other finished development history remains eligible. Close/reopen preserves
those control facts without reading full Run payloads. No extra execution ledger is created.

`ListPendingRemoteRuns` returns at most128 links and inspects at most16 indexed pending
owner rows per call (8MiB of admitted relation JSON), never unrelated history. Its opaque
cursor binds the target and stable owner/remote identities, not mutable array offsets.
An empty page with a nonempty advancing cursor is legitimate. Each owner holds at most
1024 links/512KiB; reader cancellation, row identity, boot and corruption failures are explicit.

## Current schema admission

The Core data schema is version5 and is the only admitted layout. No Core legacy probe,
historical decoder or migration ships. Older/newer stores fail closed without changing
accepted data or creating a migration backup. The embedding product host disables even
the engine's optional generic migration path. Development storage is initialized explicitly;
normal startup never clears history or unresolved remote relations.

Source/error observations are derived at the existing write boundary and kept on the same
row. Storage accepts a bounded source projection from the execution owner and never
reinterprets opaque Trigger JSON. Its generated indexed Experiment/instance coordinates
come from that projection. Source observation is limited to16KiB; an Error preview to4KiB.
Short errors remain byte-exact across every JSON shape; a large full error requires its
explicit bounded preview. Cleanup must be an array or null; its exact cardinality is written
once. Complete reads keep original trigger/error/cleanup/result bytes. Current-format tests
cover rejection without mutation, close/reopen ownership, quota, selected index plans,
corrupt unused detail, byte paging and stable same-time cursors.
