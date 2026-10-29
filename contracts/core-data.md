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

An error from an admitted write carries `engine.WriteError`: `WriteNotCommitted` or
`WriteUnknown`, and whether that attempt's physical transaction had settled at return.
`MayHaveCommitted` means the COMMIT was handed to the physical owner; its call may enter
after the caller returns. `WaitSettlement(ctx)` joins that same physical attempt and its
attached fault-only revalidation, if any; it does not change the original disposition or
prove COMMIT. After a failed physical close, the original completion remains pending
until an explicit `Close` successfully joins that same retained driver. A rollback failure
or lost COMMIT reply is not a committed receipt. Until physical join and a usable replacement, the owner refuses
later calls and retains its exclusive database lock. A later read is a current-state
observation, not proof of the original COMMIT. A possible COMMIT also retains the
existing checkpoint opportunity after settlement; a confirmed rollback creates none.
`CallbackPending` means the callback may still own captured results and validation facts;
the caller must wait for settlement before using those facts. Typed operations detach their
mutable inputs within their admission when needed, so a canceled caller cannot change the
retained attempt. Configuration receipt replays do not copy an unused proposed snapshot.

A deadline or failed rollback during an in-process read can return `engine.ReadError`.
Its `WaitSettlement(ctx)` joins that same read callback and transaction; it never reports a
COMMIT outcome. Typed reads return no partial result on error and retain their request data
until the callback has stopped using it. A raw `Read` callback's captured state stays owned
until settlement. Close joins active readers, and a failed physical reader close retains
the database owner until a later explicit Close joins that connection.

## AgentLink enrollment

`ListAgentLinks` reads the static enrollment projection and its database, complete scope,
and admitted module identity in one transaction. `WriteAgentLink` checks that identity,
the exact Agent ID and its acknowledged row version, and nonempty fingerprint ownership
in one durable transaction. `"0"` expects an absent Agent; a stale version conflicts even
when the proposed facts match. A nullable fingerprint permits multiple unpinned Known
records while the scoped unique index retains pinned and revoked ownership. CreatedAt is
stable. A matching no-op returns the existing guarded row without a durable commit. Live
presence, peer boots and epochs are not stored here. The independent enrollment quota is
2048 rows, 64 MiB including record keys, and 256 KiB per canonical record.

## Configuration

A matching stored configuration mutation receipt is returned after the existing writer
transaction rolls back without changes. That replay does not commit or schedule a checkpoint;
a failed or unjoined rollback is not reported as a successful replay.

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
| `FinishRun(ctx, finish, policy) (RunFinishResult, error)` | Durable | records a terminal status and outcome even at quota; returns the stored `Run` and same-cut `Retention` effect. The same outcome returns the stored Run and does not change the row, revision, quota or effect. Status, termination and normalized uncompressed error, result, nodes and cleanup errors must match; zero `At` matches, a nonzero `At` must equal `FinishedAt`. A replay still enters one Durable transaction. A different outcome is `failed_precondition` |
| `ApplyRunOps(ctx, creates, updates, finishes, policy) (RunOpsResult, error)` | Durable when `creates` or `finishes` is non-empty; Relaxed for updates alone; no transaction when all are empty | one transaction ordered creates, updates, finishes. `Created` contains one `CreatedRun{Run, Created}` per create; a repeated idempotency key returns the stored Run with `Created=false` without comparing inputs. More than `MaxPageSize` combined operations is `invalid_argument`. A confirmed rollback leaves the whole batch unchanged; a physical COMMIT or rollback failure can leave the outcome unknown. `Retention` describes the final committed cut; a changed batch finish does not fetch or decompress a discarded Run detail |
| `GetRun`, `FindRunByIdempotencyKey` | read | |
| `ListRuns` | read | newest first by (created_at, id); filters: exact id, target, root, session, workflow, accepted Experiment/instance, statuses, time window; opaque cursor; fixed `complete`, `identity` and `observation` projections; observation selects write-derived bounded source/error metadata, exact cleanup cardinality and result presence without selecting or decompressing any full execution JSON. Pages obey the response-byte budget as well as the row limit; an oversized first row is refused explicitly. Exact-id observation uses the primary key for bounded source navigation |
| `InterruptOpenRuns(ctx, at, policy) (RunInterruptResult, error)` | Durable | Core's boot step interrupts all open Runs in one transaction; returns `Count` and same-cut `Retention` |
| `PruneRuns` | Relaxed | deletes at most `Limit` eligible finished Runs by strict age and/or count. The age/count union is selected oldest first by `(finished_at,id)`; count keeps the newest `KeepNewest` eligible Runs when positive. Returns `Deleted`, `OldestEligibleFinishedAt` and exact post-delete `HasVictim`; zero time means the eligible set is empty. Continue only when `HasVictim` is true |

`RunRetentionPolicy{OlderThan, KeepNewest}` is supplied to the mutating methods that can
finish a Run or release eligibility; zero fields disable their selector. The committed
`RunRetentionEffect` reports `ChangedFinish`, `EligibilityReleased`, and `HasVictim`.
`HasVictim` is evaluated only for an actual changed finish or eligibility release, against
the same scoped eligible count and oldest row. Replays and no-change releases return a zero
effect. A later prune rechecks all real protections in its own transaction.

For an unknown `CreateRun`, `FinishRun`, or `ApplyRunOps` containing creates or finishes,
`RunWriteFaultError.WaitRevalidation(ctx)` returns a `RunFaultFact` after the exact
physical attempt joins. `Creates` preserves each evaluated `CreatedRun`, including a
`Created=false` replay; after a possible COMMIT its `CurrentKnown/Present/Current`
fields report an exact identity-only row at the retained owner cut. After a confirmed
pre-COMMIT rollback, `NonCommitted` is true and the retained before-state distinguishes
an already-existing row from a replay of an insertion in the same rolled-back batch,
without a current-row read. For
changed finishes, the original policy is evaluated under the same owner using only the
affected identities. No-op and replayed finishes produce no policy event. These facts
do not change the original unknown disposition.

An eligible Run has a positive finish time and no pending owner/root relation, live successor
Run, live root or queued successor Session. An absent root is not live. The indexed derived
flags and scoped eligible count are changed by the existing Run, relation and Session writers
in the same transaction; they are not a second authority. A stale selected flag fails closed
when its real guards are rechecked before deletion.

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
| `QueueRestartSession` | Durable | reserves one queued successor of the exact open predecessor; replay returns the stored successor |
| `ActivateRestartSession(ctx, id, policy) (SessionMutationResult, error)` | Durable | promotes a queued successor only after the predecessor closed cleanly and its open Run, pending relation and failed-cleanup guards clear; returns `Session` and same-cut `Retention` |
| `CloseSession(ctx, id, close, policy) (SessionMutationResult, error)` | Durable | closes a queued, open or stopping Session with outcome and error; a closed/interrupted replay returns it unchanged; returns `Session` and any same-cut eligibility release |
| `GetSession`, `ListSessions` | read | newest first; filters: target, experiment resource, statuses |
| `CloseOpenSessions(ctx, at, policy) (SessionInterruptResult, error)` | Durable | Core's boot step interrupts queued/open/stopping Sessions in one transaction, returning `Count` and `Retention` |

When an actual Session-associated Run finishes with cleanup errors, the same transaction sets
a monotonic failed-cleanup disposition on that Session. Clearing the display Run cannot erase
the predecessor's failure or make restart safe. The Session disposition is not reset merely
because history was removed. When a Session is created after a still-retained failed Run has
already named its ID, its existing writer claims that failure through the exact Session index.
Activation reads the failed-cleanup disposition with the predecessor Session it already
needs; open Runs, counted pending owners and actual pending links are checked through exact
scoped Session indexes without scanning clean or unrelated history.

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
`ResolveRemoteRun(ctx, link, policy)` returns `RemoteRunResolveResult`: the validated `RootRunID`, whether the exact
relation changed, and whether that deletion released the last pending relation of its root.
The last-release result also includes root presence, the root's exact finish time, and the
owner's exact finish time. A missing root differs from a live root; a zero finish time means
live. A duplicate returns no changed or last-release edge. The result is committed with the
deletion; a later reservation may protect the root again, so pruning rechecks protection.
Its independent `Retention` effect can report a real nonfinal owner release; `LastForRoot`
remains the separate final-root fence fact. The existing relation stores the immutable owner's
Session selector for indexed activation checks; its physical bytes are charged and released
with that relation, while the encoded-link capacity accounting remains unchanged.

An unknown `ResolveRemoteRun` error also carries `RemoteRunResolveError`.
Its `ValidatedAttempt` and `AttemptedLink` mean that transaction matched the stored complete
link identity and reached its DELETE attempt; they do not assert deletion, COMMIT, or a release edge.
An absent duplicate, identity refusal or earlier failure has no validated attempt.
`WaitRevalidation(ctx)` returns the `RemoteRunFaultFact` for that exact relation and
captured policy after physical join, before another writer can alter them. `Released`
requires both the transaction's changed validated DELETE and current absence at that
cut; `LastForOwner` and `LastForRoot` remain separate. A no-op or confirmed rollback
has no release or retention event. Neither the attempt nor a later absent row is a
success receipt.

An unknown `ReserveRemoteRun` may carry `RemoteRunReserveError`. Its `Requested` link
is caller input, not validation. `WaitRevalidation(ctx)` returns whether the original
transaction validated the link, its `insert`, `bind`, or `existing` action and candidate
effect, and the exact prior and same-cut current relation including its actual boot ID.
An already-bound replay and a new bind remain distinct even if their current rows match.
Neither a candidate effect nor a same-cut row is a COMMIT receipt.

Age/count/all-closed pruning excludes each unresolved owner and its local root in the same
transaction. Other finished development history remains eligible. Close/reopen preserves
those control facts without reading full Run payloads. No extra execution ledger is created.

`ListPendingRemoteRuns` selects at most 128 target links plus one indexed lookahead, never
unrelated target history. Its opaque cursor binds the target and stable owner/remote
identities. Each owner holds at most
1024 links/512KiB; reader cancellation, row identity, boot and corruption failures are explicit.

## Current schema admission

The Core data schema is version 11 and is the only admitted layout. No Core legacy probe,
historical decoder or migration ships. Older/newer stores fail closed without changing
accepted data or creating a migration backup. The engine also admits current schemas only.
Development storage is initialized explicitly;
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
