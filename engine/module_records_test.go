package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func recordModuleFixture(t *testing.T, maximumRequest, maximumResponse int, execute func(context.Context, *sql.Tx, string, string, json.RawMessage) (json.RawMessage, error)) (*Store, context.Context, api.NamedRequest) {
	t.Helper()
	ctx := budget(t)
	spec := api.Module{ID: "records.fixture", Schema: "records.fixture.v1", Digest: hash("records.fixture.v1"), Operations: []api.NamedOperation{
		{ID: "write", MaxRequestBytes: maximumRequest, MaxResponseBytes: maximumResponse},
		{ID: "read", ReadOnly: true, MaxRequestBytes: maximumRequest, MaxResponseBytes: maximumResponse},
	}}
	m := manifest()
	m.Namespaces[0].Modules = []api.Module{spec}
	m.Namespaces[0].Collections[0].Indexes = append(m.Namespaces[0].Collections[0].Indexes, api.Index{ID: "owner", Fields: []string{"owner"}})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, Config{Path: filepath.Join(dir, "records.db"), Create: true, Manifest: m, Modules: []DataModule{{Spec: spec, Execute: execute, Initialize: func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE module_marker(scope TEXT PRIMARY KEY, actions INTEGER NOT NULL)")
		return err
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	binding := snapshot(t, s, ctx, "binding").Token
	return s, ctx, api.NamedRequest{Scope: testScope, DatabaseID: binding.DatabaseID, Schema: binding.Schema, Module: spec.ID, Operation: "write", RequestID: "records-first", Payload: json.RawMessage(`{}`)}
}

func TestModuleRecordsAtomicCASIndexQuotaAndReceipt(t *testing.T) {
	s, ctx, request := recordModuleFixture(t, api.MaxNamedRequestBytes, api.MaxNamedResponseBytes, func(ctx context.Context, tx *sql.Tx, scope, op string, raw json.RawMessage) (json.RawMessage, error) {
		var input struct{ Mutations []api.Mutation }
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO module_marker VALUES(?,1) ON CONFLICT(scope) DO UPDATE SET actions=actions+1", scope); err != nil {
			return nil, err
		}
		versions, err := ApplyRecords(ctx, tx, scope, input.Mutations)
		if err != nil {
			return nil, err
		}
		return json.Marshal(versions)
	})
	set := make([]api.Mutation, 1000)
	for i := range set {
		set[i] = mutation("state", fmt.Sprintf("k%04d", i), "0", fmt.Sprintf(`{"name":"n%04d","owner":"run"}`, i))
	}
	encode := func() { request.Payload, _ = json.Marshal(struct{ Mutations []api.Mutation }{set}) }
	encode()
	first, err := s.Named(ctx, request)
	if err != nil || first.Receipt == nil || first.Receipt.Token.Revision != "1" {
		t.Fatalf("1000 state action: %+v %v", first.Receipt, err)
	}
	if replay, err := s.Named(ctx, request); err != nil || string(replay.Result) != string(first.Result) {
		t.Fatalf("replay changed committed result: %v", err)
	}
	// A valid swap proves the module uses Batch's shared unique-index algorithm.
	request.RequestID = "records-swap"
	set = []api.Mutation{mutation("state", "k0000", "1", `{"name":"n0001","owner":"run"}`), mutation("state", "k0001", "1", `{"name":"n0000","owner":"run"}`)}
	encode()
	if _, err := s.Named(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		id, expected string
		mutations    []api.Mutation
	}{
		{"stale", "conflict", []api.Mutation{mutation("state", "new", "0", `{}`), mutation("state", "k0000", "1", `{}`)}},
		{"unique", "conflict", []api.Mutation{mutation("state", "new", "0", `{"name":"n0000"}`)}},
		{"quota", "resource_exhausted", func() []api.Mutation {
			out := make([]api.Mutation, 1001)
			for i := range out {
				out[i] = mutation("state", fmt.Sprintf("extra%04d", i), "0", `{}`)
			}
			return out
		}()},
	} {
		request.RequestID, set = test.id, test.mutations
		encode()
		if _, err := s.Named(ctx, request); code(err) != test.expected {
			t.Fatalf("%s: %v", test.id, err)
		}
		if _, err := s.NamedResult(ctx, api.ReceiptRequest{Scope: testScope, RequestID: test.id}); code(err) != "not_found" {
			t.Fatalf("failed action has result: %v", err)
		}
	}
	var actions int
	if err := s.reader.QueryRowContext(ctx, "SELECT actions FROM module_marker WHERE scope=?", scopeID(testScope)).Scan(&actions); err != nil || actions != 2 {
		t.Fatalf("relational side effect escaped rollback/replay: %d %v", actions, err)
	}
	read := snapshot(t, s, ctx, "new", "extra1000", "k0000")
	if read.Token.Revision != "2" || !read.Results[0].Records[0].Missing || !read.Results[0].Records[1].Missing || read.Results[0].Records[2].Version != "2" {
		t.Fatalf("failed state action escaped rollback: %+v", read)
	}
}

func TestModuleRecordsCompleteIndexSetAndSealedCapability(t *testing.T) {
	var captured context.Context
	var capturedTx *sql.Tx
	var capturedScope string
	s, ctx, request := recordModuleFixture(t, api.MaxNamedRequestBytes, api.MaxNamedResponseBytes, func(ctx context.Context, tx *sql.Tx, scope, op string, raw json.RawMessage) (json.RawMessage, error) {
		captured, capturedTx, capturedScope = ctx, tx, scope
		if op == "write" {
			_, err := ApplyRecords(ctx, tx, scope, []api.Mutation{mutation("state", "a", "0", `{"owner":"run"}`), mutation("state", "b", "0", `{"owner":"run"}`), mutation("state", "z", "0", `{"owner":"other"}`)})
			return json.RawMessage(`{}`), err
		}
		result, err := ReadRecords(ctx, tx, scope, api.Query{Collection: "state", Index: "owner", Equal: []json.RawMessage{json.RawMessage(`"run"`)}, Limit: 1})
		if err != nil {
			return nil, err
		}
		if len(result.Records) != 2 || result.NextAfter != "" || result.Records[0].Key != "a" || result.Records[1].Key != "b" {
			return nil, fmt.Errorf("incomplete dependent set: %+v", result)
		}
		return json.Marshal(result)
	})
	if _, err := s.Named(ctx, request); err != nil {
		t.Fatal(err)
	}
	request.Operation, request.RequestID = "read", "complete-set"
	if _, err := s.Named(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRecords(captured, capturedTx, capturedScope, api.Query{Collection: "state", Keys: []string{"a"}}); code(err) != "failed_precondition" {
		t.Fatalf("sealed capability accepted: %v", err)
	}
}

func TestModuleRecordsBoundFailuresPoisonOuterAction(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context, *sql.Tx, string)
	}{
		{"scope", func(ctx context.Context, tx *sql.Tx, scope string) {
			_, _ = ApplyRecords(ctx, tx, scope+"other", []api.Mutation{mutation("state", "x", "0", `{}`)})
		}},
		{"cursor", func(ctx context.Context, tx *sql.Tx, scope string) {
			_, _ = ReadRecords(ctx, tx, scope, api.Query{Collection: "state", Index: "owner", Equal: []json.RawMessage{json.RawMessage(`"run"`)}, After: "a"})
		}},
		{"empty-query-budget", func(ctx context.Context, tx *sql.Tx, scope string) {
			for i := 0; i <= api.MaxNamedReadQueries; i++ {
				_, _ = ReadRecords(ctx, tx, scope, api.Query{Collection: "state", Index: "owner", Equal: []json.RawMessage{json.RawMessage(`"absent"`)}})
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, ctx, request := recordModuleFixture(t, api.MaxNamedRequestBytes, api.MaxNamedResponseBytes, func(ctx context.Context, tx *sql.Tx, scope, op string, raw json.RawMessage) (json.RawMessage, error) {
				if _, err := tx.ExecContext(ctx, "INSERT INTO module_marker VALUES(?,1)", scope); err != nil {
					return nil, err
				}
				test.run(ctx, tx, scope)
				return json.RawMessage(`{}`), nil // Deliberately ignore helper failure.
			})
			if _, err := s.Named(ctx, request); err == nil {
				t.Fatal("ignored helper failure committed")
			}
			var count int
			if err := s.reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM module_marker").Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial relational commit: %d %v", count, err)
			}
		})
	}
}
