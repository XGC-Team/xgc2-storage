package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestUpdateDeploymentPreservesDataAndRequiresExactContracts(t *testing.T) {
	ctx := budget(t)
	oldSpec := api.Module{ID: "fixture", Schema: "fixture.v1", Digest: hash("old implementation"), Operations: []api.NamedOperation{{ID: "put", MaxRequestBytes: 1024, MaxResponseBytes: 1024}}}
	oldModule := DataModule{Spec: oldSpec, Initialize: func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE module_data(value TEXT NOT NULL)")
		return err
	}, Execute: func(ctx context.Context, tx *sql.Tx, _, _ string, _ json.RawMessage) (json.RawMessage, error) {
		_, err := tx.ExecContext(ctx, "INSERT INTO module_data VALUES('durable')")
		return json.RawMessage(`{"saved":true}`), err
	}}
	old := manifest()
	old.Namespaces[0].Modules = []api.Module{oldSpec}
	clone := func(m api.Manifest) api.Manifest {
		raw, _ := json.Marshal(m)
		var out api.Manifest
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	next := clone(old)
	next.Namespaces[0].Modules[0].Digest = hash("new implementation")
	added := next.Namespaces[0].Collections[0]
	added.ID = "new_owner_facts"
	next.Namespaces[0].Collections = append(next.Namespaces[0].Collections, added)
	newModule := oldModule
	newModule.Spec = next.Namespaces[0].Modules[0]
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Path: filepath.Join(dir, "fixture.db"), Create: true, Manifest: old, Modules: []DataModule{oldModule}}
	s, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	read := snapshot(t, s, ctx, "saved")
	batch, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "batch-receipt", Mutations: []api.Mutation{mutation("state", "saved", "0", `{"name":"saved","value":42}`)}})
	if err != nil {
		t.Fatal(err)
	}
	named, err := s.Named(ctx, api.NamedRequest{Scope: testScope, DatabaseID: read.Token.DatabaseID, Schema: read.Token.Schema, Module: oldSpec.ID, Operation: "put", RequestID: "named-receipt", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s, ctx, "saved")
	if _, err = UpdateDeployment(ctx, cfg.Path, old, next, []DataModule{newModule}); code(err) != "conflict" {
		t.Fatalf("active owner not protected: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create, cfg.Manifest, cfg.Modules = false, next, []DataModule{newModule}
	if opened, e := Open(ctx, cfg); code(e) != "failed_precondition" {
		if opened != nil {
			opened.Close()
		}
		t.Fatalf("ordinary startup accepted a new digest: %v", e)
	}
	for _, change := range []func(*api.Manifest){
		func(m *api.Manifest) { m.Namespaces[0].Modules[0].Schema = "fixture.v2" },
		func(m *api.Manifest) { m.Namespaces[0].Modules[0].Operations[0].ReadOnly = true },
		func(m *api.Manifest) { m.Namespaces[0].Collections[0].MaxRecords++ },
		func(m *api.Manifest) { m.Namespaces[0].Collections = m.Namespaces[0].Collections[1:] },
	} {
		bad := clone(next)
		change(&bad)
		if _, e := UpdateDeployment(ctx, cfg.Path, old, bad, []DataModule{newModule}); code(e) != "failed_precondition" {
			t.Fatalf("changed contract accepted: %v", e)
		}
	}
	bad := clone(next)
	bad.Namespaces[0].Modules[0].Digest = hash("not compiled")
	if _, e := UpdateDeployment(ctx, cfg.Path, old, bad, []DataModule{newModule}); code(e) != "failed_precondition" {
		t.Fatalf("uncompiled digest accepted: %v", e)
	}
	wrongOld := clone(old)
	wrongOld.Namespaces[0].Modules[0].Digest = hash("not deployed")
	if _, e := UpdateDeployment(ctx, cfg.Path, wrongOld, next, []DataModule{newModule}); code(e) != "conflict" {
		t.Fatalf("wrong old identity accepted: %v", e)
	}
	updateModule := newModule
	updateModule.Initialize = func(context.Context, *sql.Tx) error { t.Fatal("update initialized module"); return nil }
	updateModule.Deploy = func(context.Context, *sql.Tx) error { t.Fatal("update deployed module"); return nil }
	updated, err := UpdateDeployment(ctx, cfg.Path, old, next, []DataModule{updateModule})
	if err != nil || updated.DatabaseID != before.Token.DatabaseID || updated.OldManifestHash != hash(old) || updated.ManifestHash != hash(next) {
		t.Fatalf("update identity: %+v %v", updated, err)
	}
	if !reflect.DeepEqual(old.Namespaces[0].Modules[0], oldSpec) {
		t.Fatal("old input was mutated")
	}
	s, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if after := snapshot(t, s, ctx, "saved"); !reflect.DeepEqual(after, before) {
		t.Fatalf("records, token or database identity changed: %+v", after)
	}
	savedBatch, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: batch.RequestID})
	if err != nil || !reflect.DeepEqual(savedBatch, batch) {
		t.Fatalf("batch receipt changed: %+v %v", savedBatch, err)
	}
	savedNamed, err := s.NamedResult(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "named-receipt"})
	if err != nil || !reflect.DeepEqual(savedNamed, named) {
		t.Fatalf("named result or receipt changed: %+v %v", savedNamed, err)
	}
	var value string
	if err = s.reader.QueryRowContext(ctx, "SELECT value FROM module_data").Scan(&value); err != nil || value != "durable" {
		t.Fatalf("module data changed: %q %v", value, err)
	}
	fresh, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "new_owner_facts", Keys: []string{"first"}}}})
	if err != nil || len(fresh.Results) != 1 || len(fresh.Results[0].Records) != 1 || !fresh.Results[0].Records[0].Missing {
		t.Fatalf("new collection read: %+v %v", fresh, err)
	}
	if _, err = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: fresh.Token, RequestID: "new-owner-first", Mutations: []api.Mutation{mutation("new_owner_facts", "first", "0", `{"name":"fresh"}`)}}); err != nil {
		t.Fatal(err)
	}

}
