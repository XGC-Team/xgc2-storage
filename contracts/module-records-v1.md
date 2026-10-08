# Compiled module record helpers v1

Root scope decision: `coordination/storage-boundary-decision.json` (2026-10-09).
This contract freezes callable owner-local helpers. It does not declare a release,
consumer migration, independent fault acceptance or deployment acceptance.

```go
engine.ApplyRecords(ctx context.Context, tx *sql.Tx, scope string,
    mutations []api.Mutation) ([]api.Record, error)
engine.ReadRecords(ctx context.Context, tx *sql.Tx, scope string,
    query api.Query) (api.QueryResult, error)
```

`DataModule.Execute(context.Context, *sql.Tx, scope, operation string,
json.RawMessage)` is unchanged. Use the context, exact transaction and canonical
authenticated scope received by that synchronous compiled invocation. Propagate
every helper error. Calls create no transaction or receipt and never commit.
The capability is sealed when Execute returns; retaining or parallelizing calls
is forbidden. The owner's original finite context governs SQL even when a module
passes `context.WithoutCancel`. A missing binding, different transaction/scope or
sealed invocation fails. Bound helper errors poison the whole invocation even
when a module erroneously ignores the error. A replaced context loses the local
binding and must never be used to bypass error propagation.

ApplyRecords reuses the ordinary Batch algorithm: registered collections,
canonical JSON, exact record-version CAS, tombstones, unique-index maintenance,
record/byte usage and quota checks. It uses the outer next scope revision for all
record versions. Ordinary Batch's whole-scope token is not a Named requirement;
the compiled operation must declare and check its own complete data guards.
Pass a unique-value swap as one complete mutation set. The same collection/key
cannot be mutated twice across helper calls in one invocation. Read-only named
operations cannot apply records. Relational facts and records share the same
outer receipt and COMMIT; every error rolls them all back.

ReadRecords accepts exact keys (including missing/tombstone facts) or a declared
scalar-equality index. An initial After is rejected. Limit selects only the local
page size, at most 2048; index pages are drained inside the same transaction until
the complete set is materialized. Success always has empty NextAfter. Exceeding
a budget returns an explicit error, never a first page or partial closure. There
is no full collection scan, arbitrary predicate, SQL, callback, remote cursor or
transaction handle. A compiled operation chooses its finite typed dependencies;
storage does not interpret a product traversal or scheduling program.

Budgets are cumulative for one Execute invocation:

| Resource | Bound |
| --- | --- |
| State mutations | 4096, with any stricter compiled operation bound |
| State preparation bytes | operation MaxRequestBytes, charging the larger of raw preparation cost and canonical mutation JSON |
| Dependent query pages/helper query calls | 24576 |
| Dependent query input bytes | operation MaxRequestBytes, charging raw input and encoded input, including internal page cursors |
| Dependent scanned/materialized rows | 65536, including missing keys and index lookahead rows |
| Dependent materialized bytes | operation MaxResponseBytes, charging encoded Record JSON including lookahead |
| Named request / response | reviewed operation bounds, at most 16 MiB each, complete envelope/result plus receipt |

The query budget covers the compiled session snapshot's 4096 Runs with five
dependent reads each and two additional member/session reads (20482 total).
The row, byte and exact-key page bounds remain independent of this query budget.

Index lookahead is charged again if read on the next page. These finite work
budgets include intermediate preparation; the final response has its independent
complete-envelope check. Modules must account for their own relational reads,
JSON projections and allocations under reviewed operation bounds. These helpers
do not prove that a legitimate larger consumer plan fits those bounds.

The outer transport RequestID and normalized Named intent recover the original
receipt/result while retained. Business mutation idempotency uses its own durable
identity and lifetime; a replayed business command must return before applying
state or appending events, including after transport receipt expiry. The compiled
module owns that exact data predicate. No exactly-once promise survives deletion
of both business identity and transport evidence.

Large immutable assets remain with their declared file owner. Storage can pin
verified immutable identity/metadata atomically with other data facts; it cannot
promise a cross-filesystem/database atomic commit. Native protobuf/JSON bridge
capacity remains open pending a separate fixed SDK budget change.
