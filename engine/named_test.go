package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestRegisteredNamedTransactionReceiptAndReadonly(t *testing.T) {
	spec := api.Module{ID: "fixture", Schema: "fixture.v1", Digest: hash("fixture-v1-schema"), Operations: []api.NamedOperation{{ID: "insert", MaxRequestBytes: 1024, MaxResponseBytes: 2048}, {ID: "read", ReadOnly: true, MaxRequestBytes: 1024, MaxResponseBytes: 2048}, {ID: "fail", MaxRequestBytes: 1024, MaxResponseBytes: 2048}}}
	module := DataModule{Spec: spec, Initialize: func(ctx context.Context, tx *sql.Tx) error {
		_, e := tx.ExecContext(ctx, "CREATE TABLE named_fixture(scope TEXT,key TEXT,value TEXT,PRIMARY KEY(scope,key))")
		return e
	}, Execute: func(ctx context.Context, tx *sql.Tx, scope, op string, raw json.RawMessage) (json.RawMessage, error) {
		var r struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if e := json.Unmarshal(raw, &r); e != nil {
			return nil, e
		}
		if op == "read" {
			var value string
			if e := tx.QueryRowContext(ctx, "SELECT value FROM named_fixture WHERE scope=? AND key=?", scope, r.Key).Scan(&value); e != nil {
				return nil, e
			}
			out, _ := json.Marshal(map[string]string{"value": value})
			return out, nil
		}
		if _, e := tx.ExecContext(ctx, "INSERT INTO named_fixture VALUES(?,?,?)", scope, r.Key, r.Value); e != nil {
			return nil, e
		}
		if op == "fail" {
			return nil, fail("conflict", "injected relational constraint failure")
		}
		return json.RawMessage(`{"inserted":true}`), nil
	}}
	ctx := budget(t)
	m := manifest()
	m.Namespaces[0].Modules = []api.Module{spec}
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	config := Config{Path: filepath.Join(dir, "named.db"), Create: true, Manifest: m, Modules: []DataModule{module}}
	s, e := Open(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	read := snapshot(t, s, ctx, "binding")
	request := api.NamedRequest{Scope: testScope, DatabaseID: read.Token.DatabaseID, Schema: read.Token.Schema, Module: "fixture", Operation: "insert", RequestID: "named-insert", Payload: json.RawMessage(`{"key":"a","value":"durable"}`)}
	result, e := s.Named(ctx, request)
	if e != nil || result.Receipt == nil {
		t.Fatalf("named insert %v %v", result, e)
	}
	again, e := s.Named(ctx, request)
	if e != nil || again.Receipt.Digest != result.Receipt.Digest {
		t.Fatalf("named replay %v %v", again, e)
	}
	failed := request
	failed.Operation = "fail"
	failed.RequestID = "named-fail"
	failed.Payload = json.RawMessage(`{"key":"b","value":"rollback"}`)
	if _, e = s.Named(ctx, failed); code(e) != "conflict" {
		t.Fatalf("named failed %v", e)
	}
	query := request
	query.Operation = "read"
	query.RequestID = "named-read"
	query.Payload = json.RawMessage(`{"key":"a"}`)
	loaded, e := s.Named(ctx, query)
	if e != nil || loaded.Receipt != nil || string(loaded.Result) != `{"value":"durable"}` {
		t.Fatalf("readonly result %v %v", loaded, e)
	}
	query.Payload = json.RawMessage(`{"key":"b"}`)
	if _, e = s.Named(ctx, query); e == nil {
		t.Fatal("failed named transaction leaked row")
	}
	s.Close()
	config.Create = false
	s, e = Open(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	recovered, e := s.NamedResult(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID})
	if e != nil || recovered.Receipt.Digest != result.Receipt.Digest || string(recovered.Result) != string(result.Result) {
		t.Fatalf("named recovery %v %v", recovered, e)
	}
	config.Modules = nil
	s.Close()
	if _, e = Open(ctx, config); code(e) != "failed_precondition" {
		t.Fatalf("missing compiled registration %v", e)
	}
}
