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
| `session.workflow_logs.snapshot` | read | bounded exact Session ownership graph in one snapshot |

Request and response limits are 16 MiB each, including the engine envelope.
The module also checks its payload/result limit. HTTP and gRPC hosts/clients must
consume the registered bounds, rather than the ordinary 4 MiB document profile.
Numbers used as revisions are canonical positive decimal **strings**, preserving
values above 2^53. Namespace root parent revision is explicit `"0"`; a Run root
has an empty parent ID and its own explicit root ID. Counts/ordinals are small
bounded JSON numbers. IDs are UTF-8, 1..255 bytes, without surrounding
whitespace, NUL, CR or LF. Names use their distinct rune and UTF-8 byte bounds
below; namespace.clone accepts up to 128 runes/512 bytes for its root name/key.
Core supplies normalization, domain syntax and the canonical name key.

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

The authenticated Invocation snapshot additionally carries only an explicitly
persisted `activeAttemptId` ownership pointer. For a nonempty pointer, the reader
checks the same transaction's already indexed attempt set: exact attempt ID,
same runId and same invocationId. A dangling, cross-invocation, cross-run or
non-string pointer fails the complete snapshot with data_loss and no partial
result/receipt. Missing/empty means no pointer; the reader never infers it from
status, attempt number or ordering. This adds no query or second authority.
Core's public NodeInvocation/Response JSON remains unchanged; lease ownership,
checkpoint and parameter bytes are still excluded from the authenticated wire.

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
The focused active-pointer tests persist the field through execution.commit and
read the actual engine.Named provider, including an atomic pointer/attempt switch
while a read transaction retains the preceding state. They establish this data
seam, not production writer activation or native transport/product closure.

## Configuration first atomic group: shared contract v5

The exact DTO definitions are now authoritative in
`modules/coredata/model/configuration.go`, copied from the frozen consumer v5
Go declarations (source SHA-256
`ddcc13a174aadb9bfbbe3a5e488fd72c716f27be37fca4064d93ccf3e5a89d57`).
The agreed semantics/budgets source SHA-256 is
`8974fa79b37b283031c7cac83c6cbd8808c1cd69b3bb52a0f7bef00bf84e8d2a`.
This v5 supersedes the shared v4 contract and incorporates C1/C2 and the
receipt/transport/resource claim boundaries. Its four operation constants and
twenty DTO field/type/JSON-tag definitions remain identical to v4. Frozen v3/v4
sources and handoff evidence remain unchanged, with no retrospective v5 claim.
The pure `ConfigurationFirstGroupOperations()` declaration defines:

| Operation | Kind | Request | Response |
| --- | --- | --- | --- |
| resource.create | write | ConfigurationResourceCreate | ConfigurationMutationResult |
| resource.commit | write | ConfigurationResourceCommit | ConfigurationMutationResult |
| resource.snapshot | read-only | ConfigurationResourceRead | ConfigurationResourceSnapshot |
| configuration.receipt | read-only | ConfigurationReceipt | ConfigurationMutationResult |

These four operations are now implemented by Execute and registered in
coredata.Spec and schemas/core-relational-v1.json, making fifteen executable
operations. The implementation uses the existing core_resources/branches/
snapshots/references/changes authority, plus a bounded explicit deployment domain
registry and durable product receipt table. Production Core Named wiring and
deployment declaration synchronization remain separate owner work. No retired
schema import, alias, fallback or request-time adoption exists.

ConfigurationDomainGuard declares key/schema identity/version/registry digest
against an explicit deployment registry. SchemaIdentity and RegistryDigest
include the active compiled panel/codec catalog; a catalog change is not an old
business-data schema and must not hide all stored current-schema Experiments.
An authorized explicit startup/deployment declaration synchronization may update
the accepted active catalog identity while the fixed domain schema version and
capabilities remain valid. Request guards fence that accepted active catalog.
Immutable snapshot eligibility uses its own schema version and domain codec;
the accepted catalog is not substituted for an immutable snapshot's version.
No request-time adoption, import, alias, payload rewrite or fallback is allowed.
The structural root is SQL NULL and
`ConfigurationNamespaceGuard{ID:"",ExpectedRevision:"0"}`; every real namespace
has a positive exact revision. Resource/branch/commit guards remain local points,
head and content digests. Revisions, versions and next_version are canonical
signed-64-bit decimal strings, including values above 2^53. There is no whole-scope
CAS. ConfigurationResourceRead selects exactly resource+explicit branch,
resource+commit, or namespace+name key; Core explicitly chooses main when intended.

Every resource.snapshot returns the selected complete immutable facts plus
CurrentMain{Branch,SchemaVersion,Identity}, from the same read transaction.
History and named-branch selection cannot bypass current-main visibility. The
authoritative Payload is one generic envelope `{"identity":{...},"body":{...}}`.
Storage only projects the generic identity member, at most 16 KiB, without a
domain interpreter or Core callback; no separately authored/stored identity row
is introduced. When selected==current main, both outputs derive from the one
selected immutable blob and that blob is fetched only once. Core consumes the
selected identity and current-main projection before decoding opaque body bytes;
inactive identity rejects first, and DecodeStored checks active envelope/body
consistency using that snapshot's codec/version. No separate visibility query or
second immutable decode is permitted by this contract.

Durable product mutation identity is separate from the expiring engine transport
receipt. Core freezes IntentDigest before provider lookup, mutable pin resolution
or random allocation IDs, and reads configuration.receipt first. Equal
key/domain/allowed operation/intent returns the original compact result without
source guards, provider reads or replacing the original plan. New unused
allocation IDs on a later same-intent submission cannot create a second resource
or commit. Changed intent conflicts. Same transport RequestID still has the
engine's exact content-digest fence. Product receipts survive engine receipt TTL;
quota exhaustion rejects the complete write rather than evicting them.

Every admitted writer invocation rechecks the durable product receipt inside the
owner transaction before source/Main guards, allocation-ID reservations or version
allocation. A public preflight miss cannot skip this check. A matching receipt
returns the original accepted plan/head without consuming the loser's IDs or
publishing its freshly prepared bytes. Core may return its local prepared typed
value only for a non-replayed first created/committed result, after matching the
returned accepted CommitID and full PlanDigest; a stored created/committed
disposition on replay never permits returning loser preparation. A
different winner requires the original immutable commit once, under the same
current typed-visibility policy. Metadata replay reports a historical persisted
result, not current typed availability or permission for a new notification.

Each newly prepared complete plan uses a distinct transport RequestID, independent
of the stable product mutation Key/Intent. Concurrent preflight misses with equal
product intent and different allocation IDs must reach the product receipt check
using separate transport identities. Reusing a transport identity is valid only
for identical frozen wire intent; deriving it solely from the product key cannot
satisfy this contract because the engine rightly rejects changed full-plan digests
before Execute. The exact engine transport identity/content fence remains intact.

configuration.receipt is registered ReadOnly in the executable catalog. Receipt-first
does not bypass authorization or bounded owner admission. Writer/disk/engine
receipt-quota admission may reject a concurrent same-intent caller before Execute
can find a winner's product receipt. Return that actual outer admission or
OutcomeUnknown/NotSent error directly, with no hidden read, write or retry. A later
explicit public retry begins with the one bounded read-only product receipt lookup
and may recover the durable original result. This is an admission-qualified replay
guarantee, not unconditional success under write pressure.

A create plan freezes one domain payload, canonical ID-ordered generic manifest,
complete tracking/pinned references and ordered node diff. Storage derives source
reference identity and validates exact target branch/commit/version/root digest/
component against its prospective transaction post-state, including a new
resource's own planned main and manifest when referenced. These are staged finite
facts, without a Core callback or another RPC. It atomically creates resource/main/version
1 snapshot, exact bytes, references, complete aggregate audit and durable product
receipt; initial revisions are 1 and next_version is 2. Returned metadata comes
from persisted facts. No notification or applied claim precedes the owner COMMIT.

Commit checks the exact branch ID/revision/head/content digest and explicit
resource/namespace guards for global identity changes. For domains with current-main
visibility, ConfigurationResourceCommit.Main carries the exact same-read current
main branch/revision/head/content-digest pin, including nonmain publication.
Storage checks this local point in the writer transaction; a concurrent main
change conflicts with no pointer, version, reference, audit or receipt write.
Ordinary domains do not require this visibility guard. Only main can change global
name/location/main pointer. Actual snapshots allocate a unique resource-local
version and update resource counter/revision, branch head/revision, immutable
snapshot/references/audit and receipt in the same writer transaction. Current live
source-head predicates fence removal of tracking-referenced components. Noop and
move-only retain the original persistent payload/manifest bytes, immutable commit,
version and branch revision; move-only increments the resource identity revision
and writes its audit/receipt. Final audit/quota/receipt failure rolls everything
back. Storage never calls a provider or runs typed workflow/domain compilation.

Two outcome rules from the fixed current business reference apply before treating
this shape as an implementation. CONFIG-V3-C1: after the applicable protected,
nonmain and exact local identity guards, an unchanged guarded RootDigest with
AppendUnchangedSnapshot=false and a changed canonical NameKey rejects with
ErrInvalid-equivalent `invalid_argument` before noop or identity-only, without a
receipt, counter or pointer write. A metadata rename cannot be acknowledged as a
discarded noop. Identity-only is a location change with unchanged canonical name;
it retains the original payload/manifest/commit/version and branch revision.

CONFIG-V3-C2: when checking removal of a tracking-referenced component, exclude
only the old outgoing set owned by the exact storage-derived resource source and
branch being replaced. Other live sources remain blockers, including other
branches of that same resource; historical/archived sources and immutable pinned
references do not become live tracking blockers. Validate the complete new
outgoing set against the transaction's post-state, including the new selected
head/manifest and exact immutable pinned target facts. Deleting component X and
that branch's tracking self-reference to X together can succeed. Retaining that
tracking self-reference while deleting X must reject against the new state; an
old pre-state head containing X cannot make it valid. This is bounded relational
validation in the same transaction, with no extra SDK read or domain callback.

These corrections and receipt/transport obligations are bound to astra3's static
proposal review `/tmp/astra3-xrpc-review-20261009/sol15-config-v3-review/review-index.json`
(SHA-256 `4c2751f55723a2c22da45cb446ba7f980bbae37dd54954b85a0658bd3a15135c`),
using the fixed `commit.go` SHA-256
`ac3ab5c91bfd0e14da7c8b5c816846807455a032fd4a2dfd25a5c49d81b3e2c9`.
The consumer incorporates them in frozen v5; the review itself remains a fixed-v3
proposal review, without retroactive v4/v5 or production review credit.
The new SQL implementation has separate focused configuration tests; its evidence
is not attributed to that earlier proposal review.

Payload/Manifest are `[]byte` on the shared Go wire and base64 in outer JSON.
CurrentMain.Identity is also byte[]/base64; the read projection preserves its raw
JSON data and does not decode the opaque domain body. When selected is main,
its raw identity member comes from the already read blob; otherwise SQL projects
only current-main identity JSON from that authoritative blob, without materializing
a second full opaque payload or storing a duplicate identity. Core's explicit frozen
codec retains raw robot extension JSON as byte[], rather
than re-encoding RawMessage. Manifest retains the existing generic NodeDraft
`parentId/documentKind/payloadDigest/sortOrder` field names. The pure shared
ConfigurationManifestNode/ConfigurationManifest representation and
ValidateConfigurationManifest/DecodeConfigurationManifest/
DiffConfigurationManifests helpers extract that data algorithm for actual reuse.
NormalizeConfigurationName provides the common NFC display/folded NFC key rule,
with separate name and identifier bounds. The module calls these helpers to check
root digest, canonical ID ordering, parent graph, sibling names and the complete
ordered audit diff; no domain interpreter is copied.

All independent limits in model/configuration_limits.go apply:

| Dimension | Limit |
| --- | --- |
| one decoded payload / manifest | 10 MiB / 1 MiB |
| manifest nodes / outgoing refs / ordered node changes | 4096 / 4096 / 8192 |
| complete decoded plan or snapshot | 12 MiB |
| actual complete Named request or response wire | 16 MiB |
| compact durable receipt result | 256 KiB |
| current-main identity projection | 16 KiB |
| clone namespaces / resources / references | 1024 / 4096 / 16384 |
| clone manifest nodes / node changes plus summary | 65536 / 65537 |
| namespace depth | 128 |
| namespace name or key | 128 runes / 512 UTF-8 bytes |
| resource name or key | 160 runes / 640 UTF-8 bytes |
| branch name or key | 80 runes / 320 UTF-8 bytes |
| reference slot | 64 runes / 256 UTF-8 bytes |
| identifier | 255 UTF-8 bytes, separate from name |

Whole budgets include metadata, complete refs, audit, receipt and the current-main
identity projection even when derived from selected main; actual wire
includes envelope, base64 and JSON escaping. Individual maxima cannot all coexist.
Owner-computed ConfigurationBudget/ConfigurationCloneBudget helpers check these
independent counters; they are not trusted RPC fields and do not estimate or
validate a caller's schema on the owner's behalf. Consumers may run the same
checks for early rejection. An oversized atomic plan is rejected before commit,
never truncated or split. Current namespace.clone has also stopped passing the
root name/key through textKey255; multibyte name/ID overflow and no-partial-write
negatives exercise the separate bounds.

Accepted object sizes do not establish peak RSS or bounded Prepare/Freeze/Compile
and provider work. Core's existing owner budgets must bound authored input before
preparation, construction/encoding expansion and simultaneous prepared plans;
counts/bytes are checked while building and actual envelope bytes before submission.
These preparation obligations do not add a scheduler or weaken atomic-plan limits.

DeclareConfigurationDomains(ctx, *sql.Tx, declarations) is an explicit owner-held
creation/deployment seam, not a named data operation. It opens no connection and
is never invoked by a request to adopt an undeclared domain. At most 256 domain
rows and 64 unique capabilities per declaration are accepted. Existing fixed
schema version/capabilities/visibility/system policy cannot change; explicit owner
synchronization may update only active schema/catalog identity and registry digest.
A current accepted deployment catalog guard is required before any product
receipt lookup, including a historical hit. A new accepted catalog can recover
the original result/plan; a stale catalog fails before receipt lookup. After that
admission fence, a matching receipt precedes source/Main guards and allocations.
This historical
metadata replay conveys no current typed/domain visibility approval.

ConfigurationCreatePlanDigest/ConfigurationCommitPlanDigest hash the operation and
complete typed plan with exact byte[] payload/manifest, guards, allocated IDs and
ordered changes/references, excluding external transport RequestID. The plan uses
Encoder.SetEscapeHTML(false) and no trailing newline. ConfigurationContentDigest
uses the same SnapshotDigest algorithm as namespace operations: SHA-256 of
`coredata.snapshot.bytes.v1\0`, then uint64 big-endian length plus exact payload,
exact manifest, and canonical Slot-sorted references JSON. Control reference bodies
use UseNumber and stable key ordering; frozen data bytes are never compacted. This
is a new-schema algorithm, with no previous digest compatibility path.

Tracking references have canonical explicit target branch names and component.
Their commit/version/root fields must all be empty; any nonempty field is
invalid_argument, including noop/location-only plans. Pinned references require
the exact immutable triple with a positive canonical int64 decimal version.
Resolved tracking heads never replace the supplied logical refs in stored rows
or shared digests. Storage derives sources from the immutable commit and
live branch heads. An incoming reference on a shared commit remains active for
every live branch pointing to that commit; excluding the replaced branch does not
exclude another branch sharing its old head. Incoming materialization is bounded
at 16384 rows/12MiB, target manifest materialization at 12MiB, and new outgoing refs
at 4096. These are private in-transaction SQL points, not per-SQL RPC calls.

Focused tests cover raw/module and actual engine.Named transactions: exact byte
retention, product replay before loser guards/bytes, C1, C2/new self-reference and
other live/shared-head branch blockers, complete audit validation, late product
receipt and outer named_results rollback, canonical large versions/overflow,
current-main visibility fences, unrelated outer scope writes, concurrent same
branch CAS and same-product prepared attempts, TTL pruning/restart, and read-only
receipt/snapshot classifications. Native authenticated transport, production Core
Named consumer wiring and deployment owner registration are not established by
these in-process tests. Execution-source reference admission remains a later
explicit authority; no existing execution admission closure is claimed.
Later branch/archive/history/reference admission and clone declaration/receipt
extensions are not implied by this first group. Consumer hooks
PrepareSnapshot/EncodePrepared/DecodeStored/RewriteFrozen receive bytes/data;
no transaction, SQL callback, provider fallback or retired ORM belongs in them.
