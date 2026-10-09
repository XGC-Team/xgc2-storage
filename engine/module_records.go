package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"sync"

	"github.com/XGC-Team/xgc2-storage/api"
)

type moduleRecordsKey struct{}

// This capability exists only during one compiled module invocation. It is
// never serialized, returned to a consumer, or retained across RPC requests.
type moduleRecords struct {
	mu             sync.Mutex
	active         bool
	owner          *Store
	ctx            context.Context
	tx             *sql.Tx
	scope          string
	namespace      api.Namespace
	operation      api.NamedOperation
	next           int64
	failure        error
	mutations      int
	writeBytes     int
	readRows       int
	readBytes      int
	readQueries    int
	readInputBytes int
	seen           map[string]bool
}

func (s *Store) executeModule(ctx context.Context, tx *sql.Tx, scope string, n api.Namespace, module DataModule, operation api.NamedOperation, next int64, payload json.RawMessage) (json.RawMessage, error) {
	b := &moduleRecords{active: true, owner: s, ctx: ctx, tx: tx, scope: scope, namespace: n, operation: operation, next: next, seen: map[string]bool{}}
	result, err := module.Execute(context.WithValue(ctx, moduleRecordsKey{}, b), tx, scope, operation.ID, payload)
	// Seal against in-flight helper calls before the owner considers COMMIT.
	// Compiled modules must still call helpers synchronously and return errors.
	b.mu.Lock()
	b.active = false
	if err == nil {
		err = b.failure
	}
	b.mu.Unlock()
	return result, err
}

func recordBinding(ctx context.Context) (*moduleRecords, error) {
	b, ok := ctx.Value(moduleRecordsKey{}).(*moduleRecords)
	if !ok {
		return nil, fail("failed_precondition", "records require an active storage-owned module transaction")
	}
	return b, nil
}

func (b *moduleRecords) validate(ctx context.Context, tx *sql.Tx, scope string) error {
	if !b.active || b.tx != tx || b.scope != scope {
		return fail("failed_precondition", "record transaction or authenticated scope mismatch")
	}
	if b.failure != nil {
		return b.failure
	}
	if err := b.ctx.Err(); err != nil {
		return err
	}
	return ctx.Err()
}

// ApplyRecords is the storage-owner record mutator for trusted compiled modules.
// It shares Batch's canonicalization, registered indexes, point CAS, tombstones,
// versions and quotas. It creates no transaction or receipt and never commits.
// Submit the complete state set together so unique-value swaps remain atomic.
// The module must propagate every error and must not retain or parallelize calls.
func ApplyRecords(ctx context.Context, tx *sql.Tx, scope string, mutations []api.Mutation) (out []api.Record, err error) {
	b, err := recordBinding(ctx)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if err != nil && b.active && b.failure == nil {
			b.failure = err
		}
	}()
	if err = b.validate(ctx, tx, scope); err != nil {
		return nil, err
	}
	if b.operation.ReadOnly {
		return nil, fail("failed_precondition", "read-only named operation cannot mutate records")
	}
	if len(mutations) > api.MaxNamedStateMutations-b.mutations {
		return nil, fail("resource_exhausted", "named state mutation budget exhausted")
	}
	rawBytes := 0
	for _, mutation := range mutations {
		rawBytes += len(mutation.Collection) + len(mutation.Key) + len(mutation.ExpectedVersion) + len(mutation.Data) + 128
		if rawBytes > b.operation.MaxRequestBytes-b.writeBytes {
			return nil, fail("resource_exhausted", "named state raw byte budget exhausted")
		}
	}
	prepared, err := prepareMutations(b.ctx, b.namespace, mutations, api.MaxNamedStateMutations-b.mutations, b.operation.MaxRequestBytes-b.writeBytes)
	if err != nil {
		return nil, err
	}
	for _, m := range prepared.mutations {
		if b.seen[m.Collection+"\x00"+m.Key] {
			return nil, fail("invalid_argument", "record appears twice in one named state action")
		}
	}
	wire, err := json.Marshal(prepared.mutations)
	if err != nil {
		return nil, err
	}
	chargedBytes := max(rawBytes, len(wire))
	if chargedBytes+b.writeBytes > b.operation.MaxRequestBytes {
		return nil, fail("resource_exhausted", "named state byte budget exhausted")
	}
	// Always use the owner's original finite context, even if a module passed
	// context.WithoutCancel(ctx), which retains Value but removes cancellation.
	out, err = b.owner.applyMutations(b.ctx, tx, scope, b.next, prepared)
	if err != nil {
		return nil, err
	}
	b.mutations += len(prepared.mutations)
	b.writeBytes += chargedBytes
	for _, m := range prepared.mutations {
		b.seen[m.Collection+"\x00"+m.Key] = true
	}
	return out, nil
}

// ReadRecords materializes a complete exact-key set or declared scalar-equality
// index set in the module's existing transaction. Index pages are drained here,
// under cumulative budgets, so a first page cannot masquerade as a closure.
// After is forbidden; Limit only selects the internal page size (up to 2048).
// The returned NextAfter is always empty. This helper creates no transaction;
// no transaction or reader handle crosses RPC.
// A module owns its finite typed dependency selection; this helper accepts no
// full inventory scan, SQL, traversal callback or whole-table/graph plan.
func ReadRecords(ctx context.Context, tx *sql.Tx, scope string, query api.Query) (out api.QueryResult, err error) {
	b, err := recordBinding(ctx)
	if err != nil {
		return out, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if err != nil && b.active && b.failure == nil {
			b.failure = err
		}
	}()
	if err = b.validate(ctx, tx, scope); err != nil {
		return out, err
	}
	if len(query.Keys) == 0 && query.Index == "" {
		return out, fail("invalid_argument", "named record reads require exact keys or a declared equality index")
	}
	if query.After != "" {
		return out, fail("invalid_argument", "named dependent reads require the complete set, without an initial page cursor")
	}
	// Check raw input before encoding/decoding arbitrary equality values.
	inputBytes := len(query.Collection) + len(query.Index) + 128
	for _, key := range query.Keys {
		inputBytes += len(key) + 8
	}
	for _, equal := range query.Equal {
		inputBytes += len(equal) + 8
	}
	if inputBytes > b.operation.MaxRequestBytes-b.readInputBytes || len(query.Keys) > api.MaxRows {
		return out, fail("resource_exhausted", "named dependent query input budget exhausted")
	}
	queryWire, e := json.Marshal(query)
	if e != nil {
		return out, e
	}
	inputBytes = max(inputBytes, len(queryWire))
	observeRecord := func(record api.Record) error {
		b.readRows++
		// Charge materialized bytes, including JSON escaping and lookahead rows.
		wire, e := json.Marshal(record)
		if e != nil {
			return e
		}
		b.readBytes += len(wire) + 1
		if b.readRows > api.MaxNamedReadRows || b.readBytes > b.operation.MaxResponseBytes {
			return fail("resource_exhausted", "named dependent record read budget exhausted")
		}
		return nil
	}
	appendRecord := func(result *api.QueryResult, record api.Record) error {
		result.Records = append(result.Records, record)
		return nil
	}
	out = api.QueryResult{Collection: query.Collection, Records: []api.Record{}}
	for {
		b.readQueries++
		// The internal cursor is a stored key; JSON escaping can use six bytes
		// per byte. Charge that upper bound before the next page executes.
		b.readInputBytes += inputBytes + 6*len(query.After)
		if b.readQueries > api.MaxNamedReadQueries || b.readInputBytes > b.operation.MaxRequestBytes {
			return api.QueryResult{}, fail("resource_exhausted", "named dependent query work budget exhausted")
		}
		page, e := queryRecords(b.ctx, tx, scope, b.namespace, query, api.MaxRows, observeRecord, appendRecord)
		if e != nil {
			return api.QueryResult{}, e
		}
		out.Records = append(out.Records, page.Records...)
		if page.NextAfter == "" {
			return out, nil
		}
		if page.NextAfter <= query.After {
			return api.QueryResult{}, fail("internal", "dependent index page did not advance")
		}
		query.After = page.NextAfter
	}
}

// ReadRecordVersion revalidates an exact prepared decision source in the
// storage-owned transaction, without loading its already-read body. Missing
// records have version 0; tombstones retain their stored version.
func ReadRecordVersion(ctx context.Context, tx *sql.Tx, scope, collectionID, recordKey string) (version string, err error) {
	b, err := recordBinding(ctx)
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if err != nil && b.active && b.failure == nil {
			b.failure = err
		}
	}()
	if err = b.validate(ctx, tx, scope); err != nil {
		return "", err
	}
	if _, err = collection(b.namespace, collectionID); err != nil {
		return "", err
	}
	if !key(recordKey) {
		return "", fail("invalid_argument", "invalid record key")
	}
	wire, err := json.Marshal(api.Query{Collection: collectionID, Keys: []string{recordKey}})
	if err != nil {
		return "", err
	}
	b.readQueries++
	b.readInputBytes += max(len(collectionID)+len(recordKey)+136, len(wire))
	if b.readQueries > api.MaxNamedReadQueries || b.readInputBytes > b.operation.MaxRequestBytes {
		return "", fail("resource_exhausted", "named dependent query work budget exhausted")
	}
	var v int64
	err = tx.QueryRowContext(b.ctx, "SELECT version FROM records WHERE scope=? AND collection=? AND key=?", scope, collectionID, recordKey).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		v, err = 0, nil
	}
	if err != nil {
		return "", err
	}
	version = strconv.FormatInt(v, 10)
	wire, err = json.Marshal(api.Record{Key: recordKey, Version: version, Missing: v == 0})
	if err != nil {
		return "", err
	}
	b.readRows++
	b.readBytes += len(wire) + 1
	if b.readRows > api.MaxNamedReadRows || b.readBytes > b.operation.MaxResponseBytes {
		return "", fail("resource_exhausted", "named dependent record read budget exhausted")
	}
	return version, nil
}
