package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

import "github.com/XGC-Team/xgc2-storage/api"

func headerStore(t *testing.T) (*Store, Config, api.Receipt, []byte) {
	t.Helper()
	declarations := manifest()
	declarations.Namespaces[0].Collections[1].Indexes = []api.Index{{ID: "retention", Fields: []string{"sequence", "acceptedAt"}}}
	declarations.Namespaces[0].Collections[1].MaxRecordBytes = 256 << 10
	declarations.Namespaces[0].Collections[1].MaxBytes = 8 << 20
	config := Config{Path: filepath.Join(t.TempDir(), "headers.db"), Create: true, Manifest: declarations}
	if err := os.Chmod(filepath.Dir(config.Path), 0700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(budget(t), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	read, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Keys: []string{"live", "deleted"}}}})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"sequence": json.Number("18446744073709551614"), "acceptedAt": "2026-10-28T00:00:00Z", "payload": strings.Repeat("界", 60<<10)})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Batch(budget(t), api.BatchRequest{Scope: testScope, Expected: read.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "live", ExpectedVersion: "0", Data: body}, {Collection: "events", Key: "deleted", ExpectedVersion: "0", Data: body}}})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.Batch(budget(t), api.BatchRequest{Scope: testScope, Expected: created.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "deleted", ExpectedVersion: created.Token.Revision, Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	return store, config, deleted, body
}

func TestRecordHeadersPreserveExactFactsWithoutPayloadReads(t *testing.T) {
	store, _, committed, body := headerStore(t)
	before, _ := store.Stats()
	query := api.Query{Collection: "events", HeaderIndex: "retention", Keys: []string{"live", "deleted", "absent"}}
	result, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, At: &committed.Token, Queries: []api.Query{query}})
	if err != nil || len(result.Results) != 1 || len(result.Results[0].Records) != 3 {
		t.Fatalf("exact headers unavailable: %+v %v", result, err)
	}
	after, _ := store.Stats()
	if after.SnapshotPayloadReads != before.SnapshotPayloadReads || after.SnapshotPayloadBytes != before.SnapshotPayloadBytes ||
		after.SnapshotHeaderReads-before.SnapshotHeaderReads != 2 || after.SnapshotHeaderBytes-before.SnapshotHeaderBytes > 128 {
		t.Fatal("header selection materialized old bodies or inflated its tuple work")
	}
	live, deleted, absent := result.Results[0].Records[0], result.Results[0].Records[1], result.Results[0].Records[2]
	for _, record := range []api.Record{live, deleted, absent} {
		if record.Header == nil || record.Header.Index != "retention" || len(record.Data) != 0 {
			t.Fatal("header was confused with a full document")
		}
	}
	if live.Version != "1" || live.Header.Bytes != int64(len(body)) || string(live.Header.Tuple) != `[{"number":"18446744073709551614"},"2026-10-28T00:00:00Z"]` || live.Deleted || live.Missing {
		t.Fatalf("live header lost exact number/byte facts: %+v", live)
	}
	if !deleted.Deleted || deleted.Missing || deleted.Version != committed.Token.Revision || deleted.Header.Bytes != 2 || len(deleted.Header.Tuple) != 0 {
		t.Fatalf("tombstone header lost its version: %+v", deleted)
	}
	if !absent.Missing || absent.Deleted || absent.Version != "0" || absent.Header.Bytes != 0 || len(absent.Header.Tuple) != 0 {
		t.Fatalf("missing header fabricated a document: %+v", absent)
	}
	for _, invalid := range []api.Query{
		{Collection: "events", HeaderIndex: "retention"},
		{Collection: "events", HeaderIndex: "unknown", Keys: []string{"live"}},
		{Collection: "events", HeaderIndex: "retention", Keys: []string{"live"}, Limit: 1},
		{Collection: "events", HeaderIndex: "retention", Keys: []string{"live"}, Index: "retention"},
	} {
		if _, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{invalid}}); code(err) != "invalid_argument" {
			t.Fatalf("ambiguous header selection was accepted: %+v %v", invalid, err)
		}
	}
	other := testScope
	other.User = "other"
	foreign, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: other, Queries: []api.Query{query}})
	if err != nil || !foreign.Results[0].Records[0].Missing {
		t.Fatalf("header crossed its exact scope: %v", err)
	}
}

func TestRecordHeaderUsesKeyIndexAndRejectsBrokenTupleWithoutFallback(t *testing.T) {
	store, _, _, body := headerStore(t)
	err := store.Read(budget(t), func(ctx context.Context, tx *sql.Tx) error {
		for _, statement := range []string{recordHeadersQuery(1), "DELETE FROM lookups INDEXED BY lookups_by_key WHERE scope=? AND collection=? AND key=? RETURNING index_name"} {
			args := []any{ScopeID(testScope), "events", "live"}
			if statement == recordHeadersQuery(1) {
				args = []any{"live", ScopeID(testScope), "events", "retention"}
			}
			rows, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+statement, args...)
			if err != nil {
				return err
			}
			indexed := false
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					rows.Close()
					return err
				}
				if strings.Contains(detail, "lookups_by_key") && strings.Contains(detail, "scope=? AND collection=? AND key=?") {
					indexed = true
				}
				if strings.Contains(detail, "SCAN l") || strings.Contains(detail, "SCAN lookups") {
					rows.Close()
					return fmt.Errorf("header/delete scanned index history: %s", detail)
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if !indexed {
				return fmt.Errorf("actual header/delete plan did not use the record-key index")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The projection must not parse an unrelated corrupt Data payload. Its
	// owner-maintained tuple and length remain independently readable.
	if err := store.Write(budget(t), Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE records SET data=? WHERE scope=? AND collection='events' AND key='live'", []byte("bad-json"), ScopeID(testScope))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := api.Query{Collection: "events", HeaderIndex: "retention", Keys: []string{"live"}}
	if result, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{query}}); err != nil || result.Results[0].Records[0].Header.Bytes != 8 {
		t.Fatalf("header parsed unused Data: %v", err)
	}
	if err := store.Write(budget(t), Durable, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "UPDATE records SET data=? WHERE scope=? AND collection='events' AND key='live'", body, ScopeID(testScope)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "DELETE FROM lookups WHERE scope=? AND collection='events' AND key='live'", ScopeID(testScope))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Stats()
	result, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{query}})
	after, _ := store.Stats()
	if code(err) != "data_loss" || len(result.Results) != 0 || after.SnapshotPayloadReads != before.SnapshotPayloadReads {
		t.Fatalf("broken tuple produced a partial result or payload fallback: %+v %v", result, err)
	}
}

func TestEngineEpochTwoIsRefusedWithoutIndexBackfill(t *testing.T) {
	store, config, _, body := headerStore(t)
	if err := store.Write(budget(t), Durable, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "DROP INDEX lookups_by_key"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "UPDATE schema_versions SET version=2 WHERE module='engine'")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config.Create = false
	if opened, err := Open(budget(t), config); code(err) != "failed_precondition" || opened != nil {
		t.Fatalf("older engine was implicitly converted: %v", err)
	}
	raw, err := sql.Open("sqlite", "file:"+config.Path+"?_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var retained []byte
	var indexes, version int
	if err := raw.QueryRow("SELECT data FROM records WHERE collection='events' AND key='live'").Scan(&retained); err != nil || string(retained) != string(body) {
		t.Fatalf("epoch refusal changed accepted Data: %v", err)
	}
	if err := raw.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='lookups_by_key'").Scan(&indexes); err != nil || indexes != 0 {
		t.Fatal("epoch refusal silently built a new index")
	}
	if err := raw.QueryRow("SELECT version FROM schema_versions WHERE module='engine'").Scan(&version); err != nil || version != 2 {
		t.Fatal("epoch refusal changed the stored version")
	}
	if backups, _ := filepath.Glob(config.Path + ".before-*"); len(backups) != 0 {
		t.Fatal("epoch refusal started automatic migration work")
	}
}

func TestRecordHeadersRejectAggregateOverflowWithoutPrefix(t *testing.T) {
	store, _, committed, _ := headerStore(t)
	first := make([]string, api.MaxRows/2)
	second := make([]string, api.MaxRows/2+1)
	for i := range first {
		first[i] = "live"
	}
	for i := range second {
		second[i] = "live"
	}
	queries := []api.Query{{Collection: "events", HeaderIndex: "retention", Keys: first}, {Collection: "events", HeaderIndex: "retention", Keys: second}}
	if result, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: queries}); code(err) != "resource_exhausted" || len(result.Results) != 0 {
		t.Fatalf("aggregate header row limit returned a successful prefix: rows=%d error=%v", len(result.Results), err)
	}
	// Large scalar tuples remain subject to the combined response-byte cap,
	// independently of the unused source documents' sizes.
	token := committed.Token
	keys := make([]string, 1024)
	for start := 0; start < len(keys); start += 128 {
		mutations := make([]api.Mutation, 128)
		for i := range mutations {
			keys[start+i] = fmt.Sprintf("tuple-%04d", start+i)
			body, err := json.Marshal(map[string]any{"sequence": start + i, "acceptedAt": strings.Repeat("t", 4000)})
			if err != nil {
				t.Fatal(err)
			}
			mutations[i] = api.Mutation{Collection: "events", Key: keys[start+i], ExpectedVersion: "0", Data: body}
		}
		result, err := store.Batch(budget(t), api.BatchRequest{Scope: testScope, Expected: token, Mutations: mutations})
		if err != nil {
			t.Fatal(err)
		}
		token = result.Token
	}
	before, _ := store.Stats()
	result, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", HeaderIndex: "retention", Keys: keys}}})
	after, _ := store.Stats()
	if code(err) != "resource_exhausted" || len(result.Results) != 0 || after.SnapshotPayloadReads != before.SnapshotPayloadReads {
		t.Fatalf("aggregate header byte limit returned a prefix or read Data: %v", err)
	}
}

func TestIndexedDeleteRollsBackMalformedEntrySetWithoutBodyFallback(t *testing.T) {
	store, _, committed, body := headerStore(t)
	if err := store.Write(budget(t), Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO lookups VALUES(?,?,?,?,?,NULL)", ScopeID(testScope), "events", "retention", `[null,null]`, "live")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Stats()
	if _, err := store.Batch(budget(t), api.BatchRequest{Scope: testScope, Expected: committed.Token,
		Mutations: []api.Mutation{{Collection: "events", Key: "live", ExpectedVersion: "1", Delete: true}}}); code(err) != "data_loss" {
		t.Fatalf("malformed index entries were silently accepted: %v", err)
	}
	after, _ := store.Stats()
	if after.MutationPayloadReads != before.MutationPayloadReads || after.MutationOldDecodes != before.MutationOldDecodes || after.CommitsDurable != before.CommitsDurable {
		t.Fatal("failed indexed deletion decoded Data or committed a partial cleanup")
	}
	if countRows(t, store, "SELECT count(*) FROM lookups WHERE scope=? AND collection='events' AND key='live'", ScopeID(testScope)) != 2 {
		t.Fatal("failed indexed deletion did not roll back its lookup cleanup")
	}
	read, err := store.Snapshot(budget(t), api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Keys: []string{"live"}}}})
	if err != nil || read.Results[0].Records[0].Deleted || string(read.Results[0].Records[0].Data) != string(body) {
		t.Fatalf("failed indexed deletion changed its accepted body: %v", err)
	}
}
