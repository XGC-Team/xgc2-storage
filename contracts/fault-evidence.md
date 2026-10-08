# Independent storage fault evidence

The independent suite is `tests/faults`; its runner is
`scripts/fault-validate.py`. It opens only newly allocated private test databases,
uses finite private subprocesses, and consumes the real engine, server, client and
XRPC SDK. It does not patch engine code, alter existing implementation tests,
read user databases, operate an existing station, or import legacy schemas.

## Reproduce

```bash
python3 scripts/fault-validate.py --container-image ubuntu:24.04 --race
```

The image must already exist locally; the runner records its immutable image ID
and never pulls it. Omitting `--container-image` explicitly skips the actual
readonly-mount and physical-filesystem-full cases, so that run does not satisfy
the complete matrix. Containers have no network or capabilities, bounded CPU,
memory and PIDs, and only test-owned mounts. Before starting each container the
suite records its exact unique name. The runner removes only recorded names on
normal exit, failure, timeout and interruption; it also kills its own subprocess
group. A separate running-container cleanup control succeeded.

The runner copies storage, checks that copying did not race source changes, and
rejects `go.mod` replacements. It seeds a private module cache from immutable,
already-cached archives whose Go content checksums match the fixed `go.sum`;
there are no network downloads or mutable sibling SDK inputs. Builds use
`-mod=readonly`, bounded parallelism and `GOMAXPROCS=2`. SQL and embedded Go
module sources are included. Every actually used module's archive and extracted
source are additionally verified using Go's `dirhash.Hash1` algorithm.
It writes file hashes, compiler/kernel versions, command exits, test events,
per-case measurements and `source.tar.gz`. Extract the archive into an empty
temporary directory and run the extracted `storage/scripts/fault-validate.py` to
retest the same implementation; pass `--expected-implementation-sha256` with
the recorded hash to reject source drift. Other exact dependencies from
`go.mod/go.sum` must already be cached. The archive includes the immutable XRPC
module payload and storage source. Database files, private module caches and
child binaries are removed. SIGTERM is converted into an exception that unwinds
private process-group and source cleanup; a recorded private-child control
returned nonzero and confirmed both were removed. Both production and administrative
CLIs are built in this private copy. Ordinary Go runs explicitly skip CLI/native
cases without these runner-provided binaries; those skips are not conformance.

## Current module recovery and native result

The complete race run under
`tests/faults/evidence/2026-10-09/module-43b27752/` returned **1**:
**twenty-five of twenty-six** top-level conformance groups passed. The sole
failing leaf is the legal large-metadata HTTP request described below. No cases
were skipped, no race diagnostic occurred, both owned containers were released,
and runner and parent source copies were removed. `TestFaultChild` is excluded
from the conformance group count. This run uses the new immutable SDK pin
`v0.0.0-20261008201732-a26df3039fa7`.

```text
storage implementation  43b27752def2a8edd1bcb39f4c4007fd3c250ae120864a3c080d5012634b38e9
independent suite       bd41a5a7762c8a8610ce61577105df1bb4b91cdae98fdd82a39c2e11c011db0a
XRPC Go source          270cc7092d2a4ab2f2de00c12b2a448af052ac5f2ed89980bbc7e4724df32bdc
production binary       de09d6157165de16d215439ea193ef5f54aae8c4bcea6c3499a85974c0531e64
administrative binary   2287002b645cbde465970f557c3c8e029349132e8e633c668a8ffd991edde126
source archive          e3eb8e1e779198e1d8cff7609426cc78993cee3f03c4da1536c0fdf58e1bf9c7
```

The implementation was restored from the first hash-checked archive, overlaying
only this owner's tests/runner. The full source mapping, SDK ZIP/source payload,
all fourteen used modules, and executed owner code are independently verified
in `integrity.json`. The changing live implementation is outside this signature.
Extract the final archive into an empty temporary directory and run:

```bash
python3 storage/scripts/fault-validate.py --race \
  --container-image sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 \
  --expected-implementation-sha256 43b27752def2a8edd1bcb39f4c4007fd3c250ae120864a3c080d5012634b38e9
```

| Current fixed-input case | Observation |
| --- | --- |
| Existing crash, disk and recovery matrix | All pass: acknowledged and lost-wire-reply kills, uncommitted real group SQL rollback, logical SQLite FULL and private physical tmpfs FULL, readonly controls, competing CAS, complete document/group backup restore, receipt expiry, admin preflight, socket/reader release and WAL recovery. |
| Disk watermark replay | Both existing Batch and new Named replay pass under an impossible free-space watermark. Retained receipt/result are exact; a new Named write is rejected without receipt, business or revision change. |
| Backup 128 boundary | Existing exact count admission and nonempty-directory controls pass again. Two same-Store callers at 127 outputs produce one publication and one precise count-limit error, leave 128 outputs, preserve all existing hashes and restore complete new business data. |
| Namespace COMMIT-before-kill | Real production HTTP and gRPC commit `namespace.clone`, then survive SIGKILL/restart and a consistent backup opened by a fresh daemon. Receipt, NamedResult and the original request replay match the original complete native response within each profile. |
| Namespace business and persistence | Full topology, names, revisions, every body, payload/manifest bytes, internal remap, external reference preservation and real SnapshotDigest are verified. Closed readonly SQL verifies resource provenance, snapshot source/version, full change fact, receipt/result and usage. Seed has ten relation rows; clone charges eight, giving eighteen. All original source/history/dev rows remain byte/field identical; target contains only one current-main branch and version-one snapshot. Source and candidate compare every field in ten fixed tables. |
| Namespace SQL-before-COMMIT kill | A test-only wrapper pauses after the real module SQL succeeds, before engine COMMIT. SIGKILL/reap leaves no target, change, receipt or result. Every field of the ten persisted tables equals the closed baseline; source snapshot and token are unchanged, and integrity succeeds. |
| Legal 8 MiB group | Both native profiles pass exact 1000-member/8 MiB parameter commit, snapshot and retained-result checks. Small requests, module parameter overflow, client envelope overflow and native host rejection controls also pass. |
| New SDK bridge positive | The gRPC legal 8 MiB-parameter + 2 MiB-body request now passes end to end: application **13,474,159**, binary protobuf **13,474,048**, protobuf JSON **17,965,471** bytes. Complete materialized business and retained result are checked. |
| Ordinary document bounds | Exact legal **3 MiB** records pass in both profiles with and without a named-enabled host. The separate **4 MiB** ordinary Batch operation bound still rejects two individually legal large records without business/receipt/revision side effects, even when the native host supports 16 MiB named traffic. No limit was increased by the tests. |
| HTTP large metadata | **FAIL**: the same legal 8 MiB-parameter + 2 MiB-body fixture reaches the unchanged **5-second** response-header deadline, `deadline_exceeded` / `outcome_unknown`. Subsequent native checks observe unchanged revision and absent group/receipt/result before private teardown. Exact source and negative evidence were delivered to sol4. Shared-host results do not establish an exclusive-window performance conclusion. |
| Namespace cumulative response budget | Rejection and subsequent read/write recovery invariants pass again. An observed RSS maximum is recorded; it does not establish an exclusive peak or a combined preallocation bound. The earlier module allocation concern remains outside the passing recovery claim. |

`controls/initial-namespace-cross-profile/` retains the first selected run and
its sole fixture comparison failure: the direct engine's persisted empty
Versions slice was compared with gRPC's empty repeated-field nil representation,
although replay returned no error. The corrected fixture requires both collections
empty, compares result bytes and every receipt field across the boundary, and
compares all three watermark routes exactly against a normal direct-engine
baseline. Every native same-profile exact comparison remains in place. An
independent read-only review checked the fixture and this correction. No engine
code or implementer assertion changed.

The legal gRPC bridge and ordinary 3 MiB positives close the earlier SDK preparse
failures for this exact pin. The HTTP deadline failure remains open. Any owner
repair requires a new implementation hash and rerun. The archive's contract text
records prior results; this file and the manifest describe the final run.

## Earlier complete owner-update result

The later complete race run is archived under
`tests/faults/evidence/2026-10-09/owner-updates-200be855/`. It returned **1**:
**twenty-two of twenty-four** top-level conformance groups passed, with three
failing leaves in two groups. No cases were skipped, no race diagnostic occurred,
both owned containers were released, and private process/database/module source
copies were removed. `TestFaultChild` remains excluded from the group count.
This input includes newer engine, Backup and storage HTTPCaller changes. Its
XRPC module remains the immutable `35c23b558399` pin below; the pending SDK
preparse repair has not been signed by this run.

```text
storage implementation  200be8550a6d3b48c458ef99b33180d2c15122d6c58f8cb7e67e642748dccf5d
independent suite       f7d3a06132fc89659285aad8d8e34b875f091740ab094a48d0f027eb4bc95d5b
XRPC Go source          929afa3b1cd1885b381ebef64ecae30b1d22b84473f251f05809c949defa2731
production binary       e774a9cf1b590cf7d8a22accbbfadef16234636d756b4a08bbc261b97b7a4bb5
administrative binary   2b92ba0b492d80f6cba7610ed965d195ea5754ac8a43f54ed65135053c1a91ff
source archive          af664ca60a63c7346e040c5892934d61f2fb330cf4bf3b1c7626298f32911a1c
```

The archive's per-file mapping and immutable SDK payload are independently
verified. Source was restored from the first hash-checked archive, overlaying
only this owner's tests/runner; the changing live worktree is outside this
signature. Extract the final archive into an empty private directory and run:

```bash
python3 storage/scripts/fault-validate.py --race \
  --container-image sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 \
  --expected-implementation-sha256 200be8550a6d3b48c458ef99b33180d2c15122d6c58f8cb7e67e642748dccf5d
```

| Current owner-update case | Observation |
| --- | --- |
| Original seventeen fault groups | All pass again, including actual SQLite FULL, private physical tmpfs FULL, complete new document/group backup recovery, crash/receipt atomicity, CAS, expiry and WAL reader-release recovery. |
| FULL fixture admission | Page-exhaustion config is the minimum legal **1 MiB** DB cap. State quota is **32 MiB / 2048 records**, while 160 × 8 KiB and 240 × 12 KiB batches remain below the 4 MiB operation limit and every record below 3 MiB. Operation index values are unique. Both cases require actual `disk_full` / SQLite 13, not a logical quota or pre-admission watermark rejection. |
| Nonempty backup positive | A second distinct target in the directory succeeds, leaves the first output's bytes unchanged, and restores all six business collections and the retained receipt from the complete closed backup in a separate private restore directory. Publication directory contains exactly two outputs. |
| 127-to-128 backup boundary | 127 preexisting nonzero regular files precede two calls sharing the same Store. Exactly one succeeds and one rejects with the precise `backup directory count limit reached` message; a further call receives the same count error. All preexisting file hashes remain unchanged, final count is 128, no publication temporary files remain, and admission counters return to zero. This verifies the same Store's serialized writer-gate boundary, not cross-Store directory-lock competition. |
| Bound HTTP consumer Reference | A real storage HTTPCaller rejects syntactically valid but mismatched target and instance references with `invalid_argument` / **`not_sent`**. Actual daemon data/revision/receipt remain unchanged; the original bound client then commits and reads complete business data. This exercises storage HTTPCaller plus its pooled SDK transport, not the SDK's separate `CallWithHeaders` entry point. |
| Exact 8 MiB named ingress | Both native profiles still pass full 1000-member request/snapshot/retained-result byte checks, together with module/client/host overflow layers and the independent ordinary Batch 4 MiB operation cap. |
| Ordinary legal 3 MiB document | **HTTP PASS / gRPC FAIL** without a named module: exact record **3,145,728** bytes, application JSON **3,146,033**, protobuf binary **3,145,873**, all within the unchanged 4 MiB application/native bounds. Protobuf JSON becomes **4,194,612** bytes, exceeding 4,194,304; the pinned SDK rejects locally with its preparse budget error and **`not_sent`**, without any record, receipt or revision change. No message ceiling was increased. |
| Legal named bridge expansion | The prior gRPC **`not_sent`** error remains: application **13,474,159** / protobuf binary **13,474,048** bytes are legal under 16 MiB; protobuf JSON **17,965,471** is rejected at SDK preparse. The prior positive expectation is retained. |
| HTTP large-metadata deadline | **FAIL**, reproduced in three runs of this same implementation: the legal 8 MiB-parameter + 2 MiB-body named fixture times out awaiting response headers at the existing 5-second bound, disposition **`outcome_unknown`**. Subsequent native checks observe unchanged revision and absent group/receipt/result before private teardown. This case passed at `dbd77d54` below. The default timeout and message limits were kept; the repeated observation was sent directly to sol4. These shared-host runs do not establish an exclusive-window performance result. |
| Namespace aggregate response | Rejection/read/write recovery invariants still pass; the previously reported late aggregate encoded-size admission remains open with the module owner. |

`controls/prior-publication-restore-fixture/` retains the initial private test
mistake of restoring inside the publication directory: the engine deliberately
retains a stable `owner.lock`. Restoration now occurs in another private
directory, and no engine lock file is deleted to satisfy count assertions.
`controls/http-repeat/` retains another same-hash complete run, including the
repeated HTTP timeout. The archive manifest and runtime evidence govern each
run; the contract text copied into an archive describes earlier results.

The request for sol2's new SDK pin failed because native Codex queue already
contained more than 100 submissions; it was not delivered or repeatedly retried.
The current HTTP finding was delivered to sol4 through native queue. Immutable
SDK repair and owner implementation fixes require a new hash-guarded run;
neither a larger `MaxMessage` nor a relaxed positive assertion is used here.

## Previous fixed-input result

The complete race run is archived under
`tests/faults/evidence/2026-10-09/ingress-dbd77d54/`. It returned **1**:
**twenty of twenty-one** top-level conformance groups passed. The only failing
leaf is `NativeNamedCapacityLayers/grpc.v1/legal-envelope-bridge-expansion`.
No case was skipped and no race diagnostic occurred. Both private containers
were released and the runner's private binaries, databases and module source
copy were removed. `TestFaultChild` is a subprocess entry point, excluded from
the group count. This is not a complete conformance pass.

```text
storage implementation  dbd77d54c03ac9fb131117d2f9b608b8b7be1b2b220fd9e65de76f84211a1508
independent suite       4a95a5b9274c3935339563bc7f888628659c5f65b702752bb0b41b0c2bd5c5ca
XRPC Go source          929afa3b1cd1885b381ebef64ecae30b1d22b84473f251f05809c949defa2731
production binary       441cdef2aba0368c5c660c25c62a7eb01cb77500549612f0969288510ba5e9bd
administrative binary   965e416ff5a1320a3d6e3b0dd3201489f4539ae13cd295f38a2b86749755b368
source archive          a6fec2ee808df429fe39c057fafe060d39ea5873cd1ff9e84535adca4867e08d
```

The module version, checksum and immutable container image remain the pins
listed below. This content-addressed input includes the owner's dynamic named
transport bounds and WAL reclamation changes; it does not sign later changes
in the live worktree. The source archive was restored and every source file
verified before overlaying only this independent owner's scope for the final
run. To reproduce from the extracted archive:

```bash
python3 storage/scripts/fault-validate.py --race \
  --container-image sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 \
  --expected-implementation-sha256 dbd77d54c03ac9fb131117d2f9b608b8b7be1b2b220fd9e65de76f84211a1508
```

| Current case | Observation |
| --- | --- |
| Original seventeen fault groups | All pass at this hash, including acknowledged/lost commit receipts, SIGKILL rollback, readonly startup, physical filesystem full, CAS/resource cleanup, administrative preflight, receipt expiry and complete new document/group backup recovery. |
| WAL reclamation after owner fix | Actual WAL pressure of 2,171,272 bytes is reclaimed to **0**; identical replay is preserved and a new small current-token write succeeds. The original failure assertion was retained. |
| Reader-pinned WAL negative/recovery control | A real readonly transaction pins an older end mark. With WAL at **2,216,592** bytes, checkpoint returns `Busy=1`, 538 log pages and 14 checkpointed pages in **205,172 ns**, within its 500 ms context. Pressure remains, new-write admission rejects without a receipt, and exact replay still resolves. Closing that reader permits checkpoint to reclaim WAL to **0**, the same rejected write succeeds, seed/large/small bodies are checked byte-for-byte, and final writer/reader admission counts are zero. This verifies Store behavior; it does not claim production Busy alerting. |
| Small and exact 8 MiB named controls | Both HTTP and gRPC complete the real daemon/client/host/module request, snapshot and retained-result paths. All **8,388,608** literal-HTML parameter bytes across 1000 members are checked byte-for-byte. Current application JSON is **11,374,050** bytes, protobuf binary **11,373,939**, and protobuf JSON **15,165,326**. |
| Three named rejection layers | Both profiles reject 8 MiB + 1 at module parameter admission, envelopes above 16 MiB before the storage client's caller, and oversized native messages before data dispatch. HTTP host rejection is **413**, gRPC host rejection is native `ResourceExhausted`. Revision/group/receipt/result remain unchanged or absent. |
| Ordinary Batch under a named-enabled 16 MiB host | Both profiles commit and return **3,145,524** exact document bytes. Two individually legal records form a **4,196,815**-byte application request: the ordinary client rejects before its caller, raw HTTP rejects with **429** and the operation-limit message, generated gRPC rejects with the engine's batch raw-payload-limit message. Records/token/receipt remain absent or unchanged; a subsequent small commit succeeds. gRPC's operation check is after native decoding, not a 4 MiB predecode host cap. |
| Remaining legal gRPC bridge failure | A valid current group with 8 MiB parameters and a 2 MiB metadata body is **13,474,159** application JSON bytes / **13,474,048** protobuf binary bytes, both below 16 MiB. Protobuf JSON is **17,965,471** bytes. The pinned SDK rejects locally with `protobuf JSON input exceeds byte budget`, disposition **`not_sent`**. HTTP completes the same-capacity case. No group/receipt/result or changed revision is published. The positive expectation remains intact; storage/XRPC owners must separate bridge allocation and binary wire bounds. |
| Namespace aggregate response | Rejection and subsequent small read/write recovery still pass. The independent fixture still exposes late encoded-size admission after separately accumulated stage bodies; this is not proof of an aggregate preallocation budget. The module-owner finding remains open. |

The current `GroupPrepare` contract requires authoritative parent/producer
states and a complete ancestor predicate. Private fixtures now seed those
current SQL-free model values and supply exact point guards and the complete
root chain. Existing kill/backup/byte-integrity assertions were preserved.
`controls/prior-fixture-contract/` retains the same implementation's earlier
rejection of fixtures missing that mandatory predicate, explicitly classified
as a fixture contract mismatch rather than an engine regression.

`controls/astra3-static-prior/` contains the supplied five-file snapshot and
static pointer, with all recorded source hashes independently verified. No
runtime failure was executed or claimed for that old pointer. The two current
exact-8-MiB native successes and separate ordinary-operation controls verify
the owner's actual ingress repair at the current hash.

## Historical recorded matrix

The earlier full fault matrix was checked under
`tests/faults/evidence/2026-10-09/fixed-f773c0e3/`. The full race run returned
**1**: sixteen of seventeen top-level fault groups passed; WAL checkpoint write
recovery failed. No case was skipped and no race detector diagnostic occurred.
`TestFaultChild` is a subprocess entry point, not another conformance case.
This records the archived implementation and the bounded matrix below; it is
**not a complete conformance pass**. The precise WAL defect was sent directly
to sol4 through native Codex queue; no engine changes or assertion relaxation
were made. Later session/module additions were rejected by the fixed-hash guard
and are outside this run.

The previous local-SDK input `verified-a8b05fdf/` passed all fourteen then-existing
groups. That historical result does not extend to the pinned remote SDK or the
new maintenance behavior. The `f773c0e3` run restored the independently hashed
`f773c0e3` source archive and overlays only this owner's suite/runner, so ongoing
engine/module work cannot silently enter the signature.

The first two reported engine defects were corrected by their owner without
changing independent expectations. The fixed-input run under
`engine-fixes-08d72fc4/` passed all thirteen then-existing groups. The original
two-defect source archive and its exact-hash reproduction remain in the parent
evidence directory as failure controls. `recheck-3a4675d0/` retains the later
administrative preflight failure. Its owner corrected CLI argument ordering;
the final verification kept the same no-mutation expectations. The current
model-based consumer does not retain aliases for the former coredata DTO location.

| Case | Observation |
| --- | --- |
| Acknowledged commit + SIGKILL | State, event, product receipt, layout, preference, configuration and durable service receipt survive restart; identical replay preserves the original receipt; changed replay and stale instance fail. A second real daemon cannot acquire the same private database. |
| Lost response at two boundaries | Before entering the data handler, SIGKILL leaves no revision/data/receipt. After the real handler has committed, a test-only wire buffer withholds its reply and SIGKILL produces `outcome_unknown`; the restarted service resolves the retained receipt and complete business batch. |
| Uncommitted relational transaction | A test-only registered module wrapper pauses after real `coredata` SQL and before engine COMMIT. SIGKILL rolls back group, members, usage and receipt/result; the preceding guarded records and revision survive. |
| SQLite page exhaustion | A 1 MiB database page cap causes SQLite error 13; the failed 160-record batch leaves no partial values, changed revision or receipt. Integrity and restart followed by a new write succeed. |
| Physical filesystem full | A private 2 MiB tmpfs, with larger DB/WAL caps and sufficient pre-admission free bytes, fails during a 240-record transaction with `disk_full: database or disk is full (13)`. Business state and receipt atomicity survive; restart permits a new write. |
| Readonly startup | Mode0400, Landlock denial of new write opens, and a real readonly bind mount reject startup, publish no service reference and leave committed DB bytes unchanged. Restoring the grant recovers the original revision. |
| CAS competition + resources | Sixteen simultaneous plans share a four-connection client and one token: one commit, fifteen conflicts, no loser receipts. Eight bounded read rounds sample the private PID. After those reads warm the finite SQL reader pool, client close reduces total FDs and leaves exactly the original HTTP listener socket inode. |
| Document business backup | A bare live main-file copy is a failing control. Consistent backup restores complete values/precision/versions, unique lookup, tombstone/ABA state and service receipt; it excludes later commits. Existing output, wrong manifest and corrupt candidate are rejected. |
| Relational business backup | `group.prepare` restores 1000 members and 8,218,890 parameter bytes, the parent/invocation/group identity, header/digest, parameters, event/link/member bodies and durable receipt/result. Closed candidates are additionally audited with fixed readonly SQL. |
| Native named operation | Production daemon registry, client and HTTP/UDS accept an 11,163,830-byte group request using the SQL-free model and base64-encoded JSON parameter bytes. SIGKILL/restart and a backed-up candidate served by a new production daemon preserve all 1000 member fields and the materialized group snapshot; read-only calls leave the revision unchanged. |
| Retained replay under watermark | **PASS after owner fix**: retained receipt and identical `Batch` both return the original commit, despite the free-space watermark. |
| Backup count boundary | **PASS after owner fix**: 128 existing outputs reject a new backup; count stays 128. |
| Administrative CLI recovery | The real `check/backup` commands reject a live owner. Offline `check/stats/backup`, no-overwrite publication, candidate integrity, new production-process business recovery and original service receipt all pass. |
| Invalid administrative operation | **PASS after owner fix**: unknown operation and `backup` without destination reject before opening the backup candidate. Its `.db` hash remains unchanged. The earlier WAL-switching failure is retained as a source-pinned control. Legitimate `check` is allowed to configure WAL. |
| Receipt maintenance | Direct bounded cleanup removes 256 of 260 expired receipts, then four, without changing business values or revision. The real daemon independently expires Batch and named replay rows during an idle window, preserves document/group values and byte-exact encoded JSON parameters across SIGKILL, and releases the two-receipt quota for a new commit. |
| Native HTTP admission cancellation | Four slow header connections are accepted; the fifth waits outside accept. Closing all test connections releases accepted sockets within two seconds, leaves business revision unchanged, and permits a new real business commit. This does not test the gRPC or host in-flight ceiling. |
| WAL checkpoint write recovery | **Historical FAIL, closed at `dbd77d54` above**: with a 1 MiB WAL watermark, a valid 2 MiB value commits. PASSIVE checkpoint reports `busy=0, log_pages=527, checkpointed_pages=527`, but WAL stays 2,171,272 bytes. Identical replay resolves its receipt; a new small write with the current token remains rejected by the physical-length watermark. A successful checkpoint alone therefore does not prove write recovery. |

## Historical fixed input identities

Storage had no Git HEAD at initial takeover; the current baseline HEAD is
`c55f79eae1676b4811fda20b981e5615255dfeca` with subsequent uncommitted owner
changes. SHA-256 identities refer to the exact archived source,
not the concurrently changing live tree or its current HEAD. `manifest.json` contains the
complete per-file mapping. Implementation identity excludes this report, the
independent suite/runner and generated evidence.

```text
storage implementation  f773c0e353bd293de56fc5647218fc047417bbda845fbff7ce9fc998c910e10a
independent suite       8ab58cbc4fc934b96bd41ccc795473b4386e96b300203b8d613b313c00eb7764
XRPC Go source          929afa3b1cd1885b381ebef64ecae30b1d22b84473f251f05809c949defa2731
production binary       5bdadc72d7196c45310e32c77e2958bbe788581295c273504d8ba07f86b6443c
administrative binary   063b97e356699da1f448fc7794271df62a526cb7081237ca78742a0fcd5c78f4
source archive          244772871e944040528f9a4af49943f8dad0ae22b80a888075b1ab15db76067c
container image         sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3
```

XRPC module is `github.com/XGC-Team/xgc2-xrpc/go`
`v0.0.0-20261008183413-35c23b558399`, with source sum
`h1:zww4QE18mlJIhNKVn3CWJyqwGBAa9o5CLA1F0LTT7vw=` and archive SHA-256
`2d5fe8c93e988e17f6dc9ddebf5d858b731d4acdf3b77fc4b12e36af54fb9934`.

The suite/index handoff is an independent diff, not an engine fix or an
implementation-owner assertion change. The preceding three defects were fixed
by their owner and independently closed; this input's WAL defect was later
closed by the current full run above.
Later implementation or dependency
identities need a new recorded run before extending this signature.

## Historical native capacity follow-up

The later **capacity-only** race run is archived under
`tests/faults/evidence/2026-10-09/capacity-7ae6f122/`. It does not extend the
seventeen-group fault result above to later engine/module changes. Its inputs
were restored from a hash-checked archive; only the independent tests and runner
were overlaid. No cases were skipped, no race diagnostic occurred, and its
private children, databases and module source copy were removed.

```text
storage implementation  7ae6f12285d7001fc1d9a7f897396ae0801281662eeb73f3f090950d02ebfd32
independent suite       618de5b6150ff3abf2d51a1e3c874b581b6b2f690c84ab009e42846c4ac970ff
XRPC Go source          929afa3b1cd1885b381ebef64ecae30b1d22b84473f251f05809c949defa2731
production binary       f42743abef3c2e7f3cf87ccab8bd52e6025619b482475a86f21faadce2bf3b20
administrative binary   21777ff8964a9510e92f82b4e6cb9e553f47a07c5dfc1cae8e3848f37c98c05d
source archive          194c4574a969db29185774c7e36876d1bfaa529521f3c4554931fd4931d5a697
```

After extracting that archive into an empty private directory, reproduce with:

```bash
python3 storage/scripts/fault-validate.py --race \
  --expected-implementation-sha256 7ae6f12285d7001fc1d9a7f897396ae0801281662eeb73f3f090950d02ebfd32 \
  --run '^TestFault(NativeNamedCapacityLayers|NativeNamespaceAggregateResponseCapacity)$'
```

The named-capacity matrix has twelve profile/case leaves: eleven passed and
one failed. The independent namespace aggregate-response case passed its
rejection/recovery invariants while exposing a remaining allocation-budget gap.
The combined run therefore returned **1**, not a complete capacity pass.

| Layer / case | Result |
| --- | --- |
| Small control, both profiles | Real production daemon, storage client and HTTP/gRPC host commit the group, materialize its snapshot and resolve its durable named result. |
| Exact 8 MiB, both profiles | All **8,388,608** parameter bytes across 1000 members survive complete native request, response and retained-result paths. Parameters contain literal HTML characters and are checked byte-for-byte. Application JSON is 11,373,708 bytes; protobuf JSON is 15,164,870 bytes; protobuf binary is 11,373,597 bytes. This closes the prior near-8-MiB HTTP-only coverage gap. |
| Module overflow, both profiles | A legal transport envelope containing 8,388,609 parameter bytes reaches module admission and is rejected with the specific 8 MiB parameter error; revision, group, receipt and named result remain absent/unchanged. |
| Storage-client envelope overflow, both profiles | A request above 16 MiB fails before the wrapped XRPC caller is invoked. No business commit is present. |
| Native-host overflow, both profiles | Tests bypass storage-client admission through bounded SDK/native typed calls. HTTP returns **413** with the host's body-limit message. gRPC returns `ResourceExhausted` with the native receive-message-size error. Neither publishes a group, receipt, result or changed revision. |
| Legal application envelope + bridge expansion | **HTTP PASS, gRPC FAIL**: 8 MiB parameters plus a legal 2 MiB group metadata body produce application JSON **13,473,817** bytes and protobuf binary **13,473,706** bytes, both below 16 MiB. Protobuf JSON becomes **17,965,015** bytes. Passing the manifest's 16 MiB bound directly to `grpcx.NewProfile` makes `Profile.prepare` reject this legal request locally with `resource_exhausted`, disposition **`not_sent`**. The business group/receipt/result are absent and revision is unchanged. This bridge/native-budget seam was sent directly to sol4 and the shared SDK owner sol2. The independent same-capacity expectation remains intact. |
| Namespace aggregate response | A newly initialized private database is populated while its engine/daemon is closed: namespace body 8 MiB, valid current snapshot payload 8 MiB, reference body 1 MiB, real content digest and complete main-branch relationships. Each stage is below 16 MiB, but aggregate JSON exceeds 19⅔ MiB after base64. The real daemon returns `core data response byte limit exceeded`; readonly revision/receipt invariants, a subsequent small read and a new commit all pass. |

The old hardcoded 4 MiB named host/client limit is absent in this fixed input:
the production daemon consumes `registry.TransportBounds(manifest)`, and the
two exact-8-MiB native successes verify the actual chain. This does not justify
using one byte budget for both protobuf JSON preprocessing and binary messages.

For `namespace.snapshot`, `readNamespaces`, `readResources` and references each
start a separate decoded-body counter at zero. They do not share an aggregate
encoded-size budget; metadata identities are also omitted from parts of these
counts. `Execute` encodes the complete result before its final 16 MiB check.
The native fixture therefore confirms late rejection, not a preallocation bound.
Its shared-host private-PID samples observed 147,180 KiB RSS versus a 15,432 KiB
baseline; these are observations, not an exclusive peak/performance guarantee.
The cumulative-budget finding and exact fixture were sent directly to module
owner sol17. No module/engine implementation or implementer assertion was changed.

## Evidence limits

Readonly evidence covers startup, not remounting a running SQLite owner or
revoking existing file descriptors. Backup recovery verifies private candidates
and actual new business data; it does not verify a deployment restore-activation
CLI, old schema import, cross-database transactions or real station deployment.
The original kill/restart/backup named test uses HTTP/UDS. The capacity follow-up
adds gRPC request/response and size-rejection coverage; it does not claim
equivalent gRPC kill/backup or cross-language failure coverage.

Resource samples are bounded smoke observations on a shared host, not an
exclusive-window performance benchmark or proof of a transient global peak.
The CAS workload and closure samples observed at most 23 FDs, 8 threads and
20,220 KiB RSS; after client close it observed 19 FDs versus the warmed pool's
23, and only the original listener socket inode remained. Safety caps are 128 FDs,
32 threads and 192 MiB RSS for that small-request workload. The large named
materialization observed 82,568 KiB RSS after work. No durability claim covers
broken hardware or storage that violates SQLite FULL/fsync semantics.
