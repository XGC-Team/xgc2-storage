package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func mutationWorkStore(t *testing.T) *Store {
	t.Helper()
	declarations := manifest()
	for i := range declarations.Namespaces[0].Collections {
		declarations.Namespaces[0].Collections[i].MaxRecordBytes = 128 << 10
		declarations.Namespaces[0].Collections[i].MaxBytes = 8 << 20
	}
	path := filepath.Join(t.TempDir(), "mutation-work.db")
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(budget(t), Config{Path: path, Create: true, Manifest: declarations})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestNoIndexMutationsReadMetadataWithoutObsoletePayload(t *testing.T) {
	store := mutationWorkStore(t)
	ctx := budget(t)
	read, err := store.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Limit: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]string{"payload": strings.Repeat("界", 20<<10)})
	if err != nil {
		t.Fatal(err)
	}
	const rows = 32
	create := api.BatchRequest{Scope: testScope, Expected: read.Token}
	for i := range rows {
		create.Mutations = append(create.Mutations, api.Mutation{Collection: "events", Key: fmt.Sprintf("row-%02d", i), ExpectedVersion: "0", Data: body})
	}
	created, err := store.Batch(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: created.Token, Mutations: func() []api.Mutation {
		mutations := make([]api.Mutation, rows)
		for i := range mutations {
			mutations[i] = api.Mutation{Collection: "events", Key: fmt.Sprintf("row-%02d", i), ExpectedVersion: created.Token.Revision, Delete: true}
		}
		return mutations
	}()})
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.MutationMetadataReads-before.MutationMetadataReads != rows ||
		after.MutationPayloadReads != before.MutationPayloadReads ||
		after.MutationPayloadBytes != before.MutationPayloadBytes ||
		after.MutationOldDecodes != before.MutationOldDecodes {
		t.Fatalf("unindexed deletion fetched or decoded obsolete SQL payload: before=%+v after=%+v", before, after)
	}
	records, bytes, tombstones := usageOf(t, store, "events")
	if records != 0 || bytes != 0 || tombstones != rows || len(deleted.Versions) != rows {
		t.Fatalf("metadata-only delete lost exact quota/tombstone accounting: records=%d bytes=%d tombstones=%d versions=%d", records, bytes, tombstones, len(deleted.Versions))
	}
	for _, record := range deleted.Versions {
		if !record.Deleted || record.Version != deleted.Token.Revision {
			t.Fatalf("delete lost its exact CAS receipt: %+v", record)
		}
	}
	if _, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: deleted.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "row-00", ExpectedVersion: created.Token.Revision, Data: json.RawMessage(`{"value":"stale"}`)}}}); code(err) != "conflict" {
		t.Fatalf("metadata-only lookup accepted an obsolete version: %v", err)
	}
	// Recreating and then shrinking a Unicode payload verifies that SQL BLOB
	// byte length, rather than text character length, drives the same quota.
	recreated, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: deleted.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "row-00", ExpectedVersion: deleted.Token.Revision, Data: body}}})
	if err != nil {
		t.Fatal(err)
	}
	_, bytes, _ = usageOf(t, store, "events")
	if bytes != int64(len(body)+len("row-00")) {
		t.Fatalf("Unicode BLOB quota used character length: %d", bytes)
	}
	if _, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: recreated.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "row-00", ExpectedVersion: recreated.Token.Revision, Data: json.RawMessage(`{"value":"small"}`)}}}); err != nil {
		t.Fatal(err)
	}
	_, bytes, _ = usageOf(t, store, "events")
	if bytes != int64(len(`{"value":"small"}`)+len("row-00")) {
		t.Fatalf("metadata-only replacement did not free the exact obsolete bytes: %d", bytes)
	}
}

func TestIndexedReplacementKeepsOldPayloadTupleDecode(t *testing.T) {
	store := mutationWorkStore(t)
	ctx := budget(t)
	read := snapshot(t, store, ctx, "indexed")
	body, err := json.Marshal(map[string]string{"name": "selected", "payload": strings.Repeat("x", 64<<10)})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token,
		Mutations: []api.Mutation{{Collection: "state", Key: "indexed", ExpectedVersion: "0", Data: body}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: created.Token,
		Mutations: []api.Mutation{{Collection: "state", Key: "indexed", ExpectedVersion: created.Token.Revision, Data: json.RawMessage(`{"name":"replacement"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.MutationMetadataReads != before.MutationMetadataReads || after.MutationPayloadReads-before.MutationPayloadReads != 1 ||
		after.MutationPayloadBytes-before.MutationPayloadBytes != uint64(len(body)) || after.MutationOldDecodes-before.MutationOldDecodes != 1 {
		t.Fatal("indexed replacement lost its exact old-payload read/decode accounting")
	}
	before = after
	deleted, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: replaced.Token,
		Mutations: []api.Mutation{{Collection: "state", Key: "indexed", ExpectedVersion: replaced.Token.Revision, Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	after, err = store.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.MutationMetadataReads-before.MutationMetadataReads != 1 || after.MutationPayloadReads != before.MutationPayloadReads ||
		after.MutationPayloadBytes != before.MutationPayloadBytes || after.MutationOldDecodes != before.MutationOldDecodes {
		t.Fatal("indexed deletion fetched or decoded an obsolete body")
	}
	if rows := countRows(t, store, "SELECT count(*) FROM lookups WHERE scope=? AND collection='state'", ScopeID(testScope)); rows != 0 {
		t.Fatalf("indexed deletion left %d stale tuples", rows)
	}
	if _, err := store.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: deleted.Token,
		Mutations: []api.Mutation{{Collection: "state", Key: "replacement", ExpectedVersion: "0", Data: body}}}); err != nil {
		t.Fatalf("deleted unique tuple could not be reused: %v", err)
	}
}
