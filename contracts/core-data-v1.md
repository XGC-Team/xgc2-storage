# Core relational data v1

Owner: xgc2-storage, `modules/coredata`. Client wire types and `SnapshotDigest`
are in `modules/coredata/model`, with no SQL, driver, engine or XRPC imports.
Consumers import that pure data package, not the service execution package.
`model.Schema`, `model.Module`, operation constants and wire limits are the
shared authority. `api.NamedRequest`/`api.NamedResponse` are the sole envelopes;
Core does not redeclare their types, the DTO fields or operation literals.
This is a new data model, not a mapping
of the retired Core tables. It has no old-schema import, alias, dual read/write,
fallback or workflow runtime. Core decides domain authorization, schemas,
visibility, policy, IDs, pins and typed rewrites. Storage checks the corresponding
data guards and constraints inside its owner's one local transaction.

## Composition and transport

`coredata.Spec()` is the authoritative compiled module registration. Its digest
includes the production Go source and embedded DDL. `Initialize(ctx, tx)` runs
only inside explicit creation of a new managed database. `Execute(ctx, tx, scope,
operation, payload)` uses the engine's authenticated canonical scope identity.
Ordinary RPC never supplies SQL, schema, a path, a callback or a transaction handle.

Use `api.NamedRequest` through `/v1/named` or native gRPC `Storage.Named`:
`module="coredata"`, exact database identity and namespace schema, finite original
deadline, request identity and XRPC instance binding. The engine owns admission,
writer acquisition, disk checks, scope commit position, FULL/WAL COMMIT and the
durable receipt plus named result. There is no second pool, socket, queue, retry
loop, goroutine per member or cross-request transaction in this module.

The module returns a provisional result. Only the engine's committed response
permits publication of a notification or further product action. A write error
requires rollback of the owner transaction. The module also uses a local
savepoint so a failed member/clone does not leave a partially applied operation.
Read operations materialize one bounded transaction, release it before returning,
and produce no receipt. Unrelated writes do not invalidate a read already in
progress, or impose a whole-scope CAS on a later named write.

| Operation | Kind | Result |
| --- | --- | --- |
| `group.prepare` | write | immutable sealed identity/count/membership digest |
| `group.snapshot` | read | sealed metadata and every member, ordered by ordinal |
| `group.member.snapshot` | read | one sealed member selected by exact child or event ID |
| `namespace.clone` | write | new root identity and namespace/resource counts |
| `namespace.get` | read | exact live namespace metadata and revision, without descendants |
| `namespace.snapshot` | read | complete live subtree with each current main and references |
| `execution.commit` | write | point-CAS state, sequenced events and business command receipt |
| `execution.command.get` | read | exact command ID or idempotency-key receipt |
| `execution.events.cursor` | read | persistent stream identity and durable latest offset |
| `execution.events.read` | read | bounded indexed event page through one fixed high-watermark |

Request and response limits are 16 MiB each, including the engine envelope.
The module also checks its payload/result limit. HTTP and gRPC hosts/clients must
consume the registered bounds, rather than the ordinary 4 MiB document profile.
Numbers used as revisions are canonical positive decimal **strings**, preserving
values above 2^53. Namespace root parent revision is explicit `"0"`; a Run root
has an empty parent ID and its own explicit root ID. Counts/ordinals are small
bounded JSON numbers. IDs/names are UTF-8, 1..255 bytes, without surrounding
whitespace, NUL, CR or LF. Core supplies the canonical name key.

Parameter and typed payload/manifest fields are byte arrays in the Go API and
base64 strings in JSON. Their contents remain JSON objects, validated after
decoding. This preserves the actual encoded input bytes without HTML escaping
turning each legal `<`, `>` or `&` byte into six bytes. Large payloads have a
bounded 4/3 base64 wire expansion; storage keeps their decoded bytes once. Small
metadata bodies remain JSON objects. Protobuf's JSON bridge adds a separate byte
encoding overhead, which the XRPC gRPC profile must explicitly account for.

## Group preparation

`GroupPrepare` supplies one ID, parent/invocation identity, occurrence group key,
complete group body, exact live `parent_guard`, `invocation_guard` and `pin_guard`,
mandatory `condition`, and at most **1000** ordered members. The registered
document collections are `runs`, `invocations` and `definitions`;
parent/invocation keys must match the explicit identity. Core validates policy,
input schema, immutable pin and descriptor against those exact versions. Storage
checks the exact versions and the typed preparation condition under the same
WAL writer before inserting any new group facts. Changing a guarded record
conflicts; an unrelated run commit does not.

`model.GroupPrepareCondition` carries exact `RunPrepareState` and
`ProducerPrepareState` predicates, and the complete parent-to-root `Ancestors`
point guards (including `ParentGuard` first), with at most 32 ancestors. Storage
reads their authoritative metadata, requires an occurrence-model parent in
`running` or `waiting`, and requires the exact producer to be `running`, tied to
that Run and node, and explicitly call-capable. The stored metadata must match
the predicates, including target, root, parent edge, model, node and kind. The
producer capability is private in Core's public NodeInvocation JSON; Core must
encode `childRunProducer` explicitly in authoritative storage metadata instead
of losing it through `json:"-"`.

Every ancestor must have the same target/root/model, an exact guarded version,
a non-repeating parent edge and an explicit final root. Storage independently
checks each ancestor's command target `orchestration-run:<id>` for
`workflowruntime.stop-set` receipts in `accepted` or `succeeded` status. This
indexed check shares the writer transaction with group insertion, so a stop
receipt inserted without changing the Run version still fences new preparation.
Missing ancestors, cycles, stale edges and a caller weakening the predicate are
rejected. Failed/rejected stops and unrelated target/action/scope do not fence.
These are fixed data conditions; no descriptor registry, workflow callback or
arbitrary predicate language is installed in storage.
`RecordGuard.Version` is the document store's `api.Record.Version`, distinct from
the product lifecycle revision inside its metadata body. A Core consumer must
retain this point-version alongside its loaded product fact, not substitute a
Run/Invocation/Definition body revision or a scope commit token.

Every member has an item key, child ID, event ID, parameter object and complete
event/link/member bodies. The bodies preserve provider-independent domain fields,
including complete immutable target pins, owner/relation/cancel/result policies,
target-root extensions, ingress metadata and timestamps. Core constructs these
values; storage does not derive policy or install/freeze a workflow definition.
Relational columns are the authoritative lookup identities. Bodies carry the
additional product fields; the consumer must not create conflicting identities
inside them or omit fields it later needs.

Member ordering is the input ordinal. Item keys are unique within a group; child
and event IDs are unique within an authorization scope. The parent/invocation/
group-key tuple is unique. Total parameter bytes may be **8 MiB**; this is a
separate bound from the 16 MiB complete wire plan. Parameters have one physical
copy in the member row, shared by the event and link projections. This avoids
replicating an 8 MiB parameter plan across three blobs, while preserving the
logical facts. `core_group_events` and `core_child_links` expose only sealed groups.

Group plus all members and final seal are one commit, with the engine receipt.
A fault at the last member leaves no group, event, link, member, seal, quota debit
or receipt. An empty group seals with zero members. Ordinal reads use the group
primary index; occurrence/parent lookups and item/child/event uniqueness use
declared indexes. `group.snapshot` returns all members in one immutable read,
checks count/contiguous ordinal and membership digest, and never requires a
restart-on-unrelated-write page protocol. `group.member.snapshot` takes exactly
one `ChildID` or `EventID` in `model.GroupMemberRead` and returns a
`model.GroupMemberSnapshot`: sealed group identity/count/digest, parent,
invocation, group key/body, ordinal, and that same complete member/event/link/
parameter fact. The dispatcher and child reader consume this shared authority;
a child read does not materialize all 1000 members or use a separate event/link
store. The point read verifies a persisted digest of the entire member, sealed group
coordinates and body; changing event/link metadata, parameters or the group
header is data loss rather than a different consumable fact. Both reads hide
provisional groups and issue no write receipt. Neither
read is a claim, permission to launch, nor a ChildRunGroup lifecycle projection.
Final claim or child insertion must recheck its own Core-authorized lifecycle
and stop-fence conditions in that future write transaction.

The immutable preparation digest permanently fences reuse of a group ID with
changed intent: ID, parent/invocation identity, group key, complete group body,
and all ordered member/item/child/event/parameter/event-body/link-body/member-body
facts. `ParentGuard`, `InvocationGuard`, `PinGuard` and `Condition` are submission
checks, excluded from that intent digest. Reloading point versions, ancestor
guards or current state predicates must not change already sealed preparation.
The public membership digest and stored facts remain unchanged by this replay.

With a new transport RequestID, exact immutable replay returns the original
sealed result even if the parent/producer is now terminal or an ancestor now has
a stop receipt. It does not check new-admission eligibility, charge another
preparation, insert an event/link/member, authorize launch or dispatch a child.
A NEW group still needs all live point/predicate guards and the independently
indexed stop-set receipt check in its same writer transaction. The transport
request content digest remains precise over the complete envelope and payload,
including guards: reusing the SAME RequestID with a refreshed guard conflicts
and does not replace its original durable result/receipt.

Transport uncertainty is resolved through the engine's retained request receipt;
there is no automatic write retry or claim of exactly-once external actions.

## Namespace cloning

`NamespaceClone` supplies the domain/root identity and exact source revision;
the target parent and its revision (or empty parent with `"0"`); new root ID,
display name/name key; complete source-to-target namespace mappings; complete
resource mappings; and one new change identity/body.

Storage reads the live recursive subtree with its parent index, and all live
resources with their current `main` branch and immutable snapshot, inside the
write transaction. The plan must contain exactly that current namespace and
resource set. It checks every namespace/resource revision, main branch revision,
main commit and immutable content digest. A new descendant/resource, deleted or
archived source, changed main, omitted mapping, duplicate target or missing main
relation fails the entire operation. A damaged source cycle is rejected. Changes
to an unrelated subtree do not require a clone restart.

Core first obtains `namespace.snapshot`, enforces domain visibility, constructs
new IDs and rewrites the typed payload and manifest in memory. `ResourceCopy`
carries those rewritten payload/manifest/reference values and complete new
resource, branch and commit bodies. The storage call contains no domain callback,
old table name or callback closure. Source guards validate the preparation read
set again at commit. The content digest is over compact JSON payload/manifest and
references sorted by slot; `SnapshotDigest` is the shared helper. Snapshot reads
also verify this digest before publishing data.

The target parent guard can be obtained through `namespace.get`, an exact indexed
metadata read. A large destination's descendants and typed payloads need not be
loaded to clone a smaller source into it. The write checks that parent revision
again alongside the source read set.

The target tree preserves the source topology and non-root canonical names. Each
target resource has revision 1, one `main` branch at revision 1, one version-1
snapshot, source resource/commit provenance, and its supplied references. Source
history and non-main branches are not copied. The target root name is the requested
name. All live sibling names remain unique. Reference targets are explicit data;
Core decides whether a reference remains external or is remapped to a clone ID.
One change fact is stored alongside the complete copied tree. Detailed domain
change summaries remain in its supplied body, not a storage workflow interpreter.

Limits are 1024 live namespaces, 4096 resources, 16384 references and the complete
16 MiB wire plan/result. A subtree beyond those declared bounds is explicitly
rejected; it is never split into partially visible commits. Snapshots execute a
bounded recursive read plus two indexed dependent reads in one transaction,
rather than one network request per namespace/resource. Clone materializes only
the source metadata needed for comparison, not old typed payload/history blobs.

## Persistence, quota and recovery

Authoritative DDL is `modules/coredata/schema.sql`, embedded in the module. The
new tables are groups/members; namespaces/resources/branches/immutable snapshots/
references/change facts; command receipts, event offsets/entity sequences,
execution stream identity; and relational usage. Old ORM table names are absent.
All keys, constraints and indexes include the authenticated scope. A Core user/
workspace has one authorization scope for these atomically related facts; a
per-run conflict guard is distinct from that authorization scope. Core and Agent
database owners remain independent; no distributed transaction is promised.

Each scope permits at most 1,000,000 relational rows and 512 MiB of charged plans.
Charges conservatively include each encoded plan plus 1024 bytes per relational
row for identities/index overhead. They are stored and guarded in the same
transaction, survive restart, and roll back on failure. These are finite logical
limits, not a claim about combined DB/WAL/backup physical disk size. Engine page,
WAL/free-space admission and deployment filesystem quota remain mandatory.

Current/recovery groups, saved configurations and provenance are retained until
the owning product requests an explicit reviewed cleanup. No automatic deletion
or log/telemetry ingestion is implemented here. New-data recovery uses the storage
owner's consistent backup/restore; no old-schema import or repair fallback exists.

## First-batch evidence and remaining consumer work

`GOMAXPROCS=2 go test -race ./modules/coredata/... -count=1` exercises SQLite, not an
in-memory rule model. Cases cover 0/86/1000 members, 8 MiB parameters and overflow,
last-member rollback, 8 MiB HTML parameters/byte-exact recovery, exact guard
conflict versus unrelated writes, immutable group replay with refreshed versions/
predicates, precise transport identity conflicts and complete immutable-field
change rejection, same-version wrong lifecycle or
producer capability, complete ancestor/stop-receipt phantom fences, exact indexed
child/event reads of sealed preparation, corrupt member/event/link/group
metadata rejection and point-read scope isolation,
recursive/current-main clone, last-reference rollback, omission/phantom/stale
source/branch/digest rejection, name conflict, scope isolation, >2^53 revision
precision, bounded recursive expansion, large destination metadata reads and
EXPLAIN-confirmed indexed access. The engine integration test also
checks named result/receipt atomicity, receipt absence on failure, restart replay
and a complete recovered 1000-member read.

This batch is the preparation/clone data boundary. The general configuration
authoring write plan, execution lifecycle/read catalog and actual Core consumers
must be agreed with their owners and wired through named operations on this new
model. The tests seed source authoring facts only in isolated fixtures; they do
not imply a deployed authoring API, full product migration, a live experimental
closure, process-kill evidence or an independent acceptance decision.

## Execution consumer handoff and atomic data operation

The consumer imports `modules/coredata/model` for data and `api` for the named
envelope, and invokes `client.(*Client).Named(ctx, api.NamedRequest{Scope,
DatabaseID, Schema: "core-relational-v1", Module: "coredata", Operation:
"group.prepare", RequestID, Payload})`. Its payload is the encoded
`model.GroupPrepare`; its result decodes into `model.GroupPrepared`. A write must
have a matching non-nil committed receipt before Core wakes durable readers.
`group.snapshot` takes `model.GroupRead` and returns `model.GroupSnapshot`.
`group.member.snapshot` takes `model.GroupMemberRead` and returns
`model.GroupMemberSnapshot`. Use `model.Schema`, `model.Module` and the respective
`model.GroupPrepareOperation`, `model.GroupSnapshotOperation` or
`model.GroupMemberSnapshotOperation` in that envelope. The wire DTO is the shared
storage contract; Core's typed domain Snapshot/Run/Job objects remain local and
are lowered explicitly. Core must not declare a second anonymous wire DTO as its
own authority. No transaction closure, SQL or remote transaction object is sent.

The complete group request is `model.GroupPrepare{ID, ParentID, InvocationID,
GroupKey, ParentGuard, InvocationGuard, PinGuard, Condition, Body, Members}`. Its ordered
`model.GroupMember` entries have `ItemKey, ChildID, EventID, Parameters []byte,
Event, Link, Body`. The three complete metadata bodies are JSON objects; the
parameter object is encoded once as bytes. Body fields preserve the exact
product preparation facts; `GroupPrepared{ID, MemberCount, MembershipDigest}`
does not manufacture a ChildRunGroup lifecycle state, revision or dispatch.

The actual fan-out callers are Core executioncatalog's
`automation_call_groups_prepare.go` and `automation_bound_call_groups.go`, owned
by sol3. The execution/workflow data consumer owned by sol14 supplies an explicit
`PrepareChildRunGroupBatchCommand` port. Those callers must prepare the complete
finite plan locally and make one named commit, replacing their outer transaction
closure and per-member store calls. A sealed aggregate is preparation data;
dispatch and execution decisions remain Core. Preparation must preserve complete
target pins, event/link/member fields and the policies used by later consumers.

`model.ExecutionCommit{State, Events, Command, Completion}` is registered as
`model.ExecutionCommitOperation` (`execution.commit`). It accepts up to 4096
explicit registered-collection point mutations, up to 1000 events, optional
command acceptance and optional terminal completion, within the complete 16 MiB
request/response. An empty action is rejected. Collection names, record limits
and indexes are deployment-owned finite Manifest entries; a caller cannot
supply a schema or index definition. Core lowers its explicit business action to
this finite data plan, retaining each document store point version. Claim/CAS
uses that exact version, not a scope commit token or metadata lifecycle revision.

The service calls `engine.ApplyRecords` synchronously in the owner's existing
writer transaction. It reuses the single Batch record algorithm for registered
validation, exact JSON numbers, atomic unique-index swaps, tombstones, quotas and
point CAS. All changed records receive the owner's next commit version. A write
to an unrelated record does not invalidate this action. There is no ordinary
256-mutation/4 MiB RPC hop, whole-scope CAS, inventory planner, copied mutation
algorithm, new connection or remotely held transaction. State changes, command
acceptance/completion, event offsets/entity sequences, logical quota and the
engine's durable receipt/result commit together. Any failure rolls them all back.
The returned state entries are the storage's exact keys/versions/deletion flags;
Core must not invent versions from its public domain JSON.

Accepting an existing identical business command sets `ExecutionCommitted.Replayed`
and returns its original receipt without applying State or Events again. Exact
terminal replay does likewise; a changed terminal result conflicts. With both
Command and Completion, the same explicit command ID is required. Completing an
existing accepted command applies its new terminal action once; replaying an
already terminal command skips the action. `CommandCreated` refers only to new
acceptance, so Core also lowers `Replayed` explicitly when interpreting whether
this action applied. A transport request replay is different: the engine returns
the original cached result and commit receipt unchanged. Core must not infer
fresh execution or external exactly-once effects from either kind of replay.

The implemented kernel compares command intent/terminal JSON with `UseNumber`,
preserving adjacent integers above 2^53 and distinct number encodings such as
`1`/`1.0`. Key order and insignificant whitespace do not change the intent.
Payload absence remains distinct from an explicit JSON null. Every command has
one idempotency key, audit metadata and optional caller IDs (missing IDs are
generated once). A terminal result transitions only from accepted and can be
replayed only with the identical terminal status, result reference, JSON result
and error fields. The stored timestamps and original bytes are returned on
replay. Command payload, terminal result and event payload use byte fields on
the wire, requiring explicit Core conversion rather than an `any` JSON hop.

Events allocate an entity sequence and an authorization-scope global offset in
the owner's writer transaction, using primary-key counters instead of history
MAX scans. Decimal strings preserve wire precision. The scope's stream identity
derives from a new-database random identity stored once at creation and the
canonical scope; reopening retains it, creating a different database changes it.
`execution.command.get` takes `model.CommandRead` with exactly one ID or
idempotency key and returns `model.CommandFound`. `execution.events.cursor`
takes `{}` and returns `model.EventCursor`. `execution.events.read` takes
`model.EventRead` and returns `model.EventPage`: exact optional filters, an
after-offset and optional fixed high-watermark, with 1..1000 rows and the 16 MiB
response budget. It reads one
transaction and uses declared offset/entity/sequence/type indexes. Event, command
and counter facts debit the same relational quota and roll back together.

Core owns Subscribe's local wake hub. A successful inner savepoint merely joins
the outer pending notifications. It must publish only after a matching storage
commit receipt; a notification prompts re-reading the durable event stream.
An uncertain transport result is resolved with `client.NamedResult(ctx,
lookupRequestID, api.ReceiptRequest{Scope, RequestID: originalRequestID})`, without
executing the write again. Receipt recovery may be the first confirmation of a
commit and can wake readers. It does not prove an external side effect ran once.

Kernel SQLite tests include exact command/terminal replay, final-event failure
after an accepted and completed command, restoration of an already accepted
receipt after completion failure, counter overflow, scope isolation, indexed
fixed-high-watermark reads and stream identity across close/reopen. These do not
replace process-kill, full product closure or consumer notification tests.

The actual registered module's engine integration tests also cover a 300-state
plan exceeding the ordinary 4 MiB profile plus 1000 events in one commit,
matching state versions, indexed reads retaining >2^53 integers, restart result
recovery without repeated events, business/terminal replay skipping State/Events,
and changed-intent rejection. Last-event failure leaves no state, indexes,
command, counters, either quota ledger or either receipt. Completion failure
restores the original accepted receipt/state/event stream. Two concurrent task
claims at the same point version yield one winner and one conflict with no
losing command or event; unique-index swaps, tombstone recreate fences and class
quota rejection reuse the engine implementation. A stop receipt created through
`execution.commit` fences later `group.prepare` even at the unchanged Run version.
The group replay integration test additionally changes all guarded source
versions and parent/producer lifecycle data, confirms identical immutable facts
with a new RequestID, rejects changed guards at the original RequestID without
replacing its receipt, confirms a previously sealed group after a durable stop,
and rejects a new group at exact fresh point versions because of that stop.
These are real SQLite/engine data tests; Core's exact runtime action plans and
notification wiring remain its consumer owner's work.

## Session workflow log snapshot

`session.workflow_logs.snapshot` is registered `ReadOnly=true`. Its request is
`model.SessionWorkflowLogRead{TargetID, SessionID}` in the owner's exact
scope/database/schema/module envelope; it returns no write receipt. The binding
is deployment supplied. `model.SessionGraphCollections()` is the sole new-data
class/index catalog shared by deployment and execution writers:

| Class | Record key | Declared dependency index |
| --- | --- | --- |
| sessions | Session ID | exact point, verify target |
| session_members | member ID | by_session(targetId, sessionId) |
| runs | Run ID | exact point, verify target/root/ownership |
| invocations | invocation ID | by_run(runId) |
| attempts | attempt ID | by_run(runId) |
| run_relations | model.RelationRecordKey(kind, ID) | by_run(runId) |
| workflow_jobs | Job ID | unique by_origin(runTargetId, runId, invocationId), by_run(runTargetId, runId) |
| definitions | immutable pin ID | existing exact point |

Session, member, Run, invocation and attempt records are the writer's actual
metadata objects, not cached snapshots. `model.RunRelationRecord` is one
canonical relation fact `{id,runId,kind,fact}`; kind names one of the eight
ExecutionRelations slices. Private fields may be present in the durable fact;
the reader forwards only explicitly allowlisted public scalar fields. A sealed
`group.prepare` group, its member and its child link are read directly through
`core_groups_parent` from the module's existing authority, including membership
digest validation. Copying those facts into run_relations is rejected as a
duplicate authority. Lifecycle mutations of sealed group facts require an
explicit future operation against that authority; the reader never creates or
repairs rows.

`model.WorkflowJobRecord` is an authoritative Job metadata fact written only by
the trusted workflow Job admission path, atomically with its execution action.
It has flat scalar index coordinates plus explicit `origin` and `job` objects.
Ordinary Job admission supplies no origin. The reader checks the flat coordinates,
selected Run/invocation, complete immutable origin pins and bounded attempt lineage.
No retired private DedupeKey decoding, public-id inference, alias or fallback is
used. An indexed ordinary, mismatched or ambiguous origin is `data_loss`.

The response is `model.SessionWorkflowLogSnapshot`: the Session View as
`session`, unique Run aggregates as `runs` (Run, Invocations, Attempts,
ExecutionRelations) and exact-origin Job projections as `jobs`. Every Job carries
explicit `{origin,job}` because Core's Job projection has `Origin json:"-"`.
Numbers retain their original JSON bytes, including integers above 2^53.
Parameters, results, checkpoints, private worker ownership, lease tokens and
private deduplication/admission credentials are excluded. Public target, owner,
binding and origin coordinates needed by the tree projection remain available.

One storage-owned read transaction selects the exact target/Session, seeds only
its workflow_run/workflow_command member owner IDs, follows bound local child
ownership edges and reads only selected Runs' exact indexed dependency sets.
Shared Runs are materialized once. Prepared links and remote TargetRoot links
remain relation facts; they never manufacture local Run metadata or lifecycle.
Bound local children must agree with the link's parent/root/target and immutable
pins. Cycles, missing dependencies, duplicate ownership, orphan attempts/groups,
wrong target, wrong producer and incompatible origins fail the complete read.
No per-Run RPC, global inventory, authored-output lineage, transaction handle or
changing-revision page restart exists. `engine.ReadRecords` drains all index pages
inside this same transaction using the owner's shared index codec and budgets.

Limits are 4096 unique Runs, 1024 Session members and 16384 materialized dependency
facts (including members, Runs, occurrences, attempt history, relations, Jobs and
Job attempts). This also bounds log sources; limits fail instead of truncating.
The owner additionally bounds all generic materialized rows/bytes; private sealed
group materialization has its own 16 MiB cumulative cap. The complete Named
response envelope is at most 16 MiB. The finite generic dependent-query budget
must cover the legal 4096-Run graph: one cached Run point and four indexed sets
per Run plus Session/member reads and internal index pages.

The real engine tests exercise shared Run materialization, unrelated Session
exclusion, explicit Origin, exact numeric bytes, private-field filtering, prepared
and remote child visibility, missing/corrupt/cyclic ownership, exact member/Run/
dependency limits and overflow, reading group.prepare's sealed authority, and
an atomic writer commit while a reader retains the preceding snapshot. Core's
pure projector and production writers consume this catalog separately; these
storage tests do not claim full product migration or runtime closure.
