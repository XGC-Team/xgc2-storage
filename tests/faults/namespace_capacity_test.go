//go:build linux

package faults_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func seedNamespaceCapacity(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "fixture.db")
	config := coreConfig(path, true)
	config.MaxDBBytes = 128 << 20
	s, err := engine.Open(deadline(t), config)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Fixture setup uses only the newly initialized, private, closed database.
	// No second writer is opened while its native daemon owns it.
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=rw&_pragma=foreign_keys(1)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(deadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	scopeRaw, _ := json.Marshal(scope)
	namespaceBody, payload, referenceBody := sizedObject(8<<20, "x"), sizedObject(8<<20, "x"), sizedObject(1<<20, "x")
	empty := []byte(`{}`)
	reference := model.Reference{Slot: "slot", TargetDomain: "capacity", TargetResourceID: "resource", TargetCommitID: "commit", Body: referenceBody}
	pin, err := model.SnapshotDigest(payload, empty, []model.Reference{reference})
	if err != nil {
		t.Fatal(err)
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{"INSERT INTO scopes(scope,namespace,revision) VALUES(?,?,1)", []any{string(scopeRaw), scope.Namespace}},
		{"INSERT INTO core_namespaces(scope,domain,id,parent_id,name,name_key,revision,archived,body) VALUES(?,'capacity','root',NULL,'Root','root',1,0,?)", []any{string(scopeRaw), namespaceBody}},
		{"INSERT INTO core_namespaces(scope,domain,id,parent_id,name,name_key,revision,archived,body) VALUES(?,'capacity','tiny',NULL,'Tiny','tiny',1,0,?)", []any{string(scopeRaw), empty}},
		{"INSERT INTO core_resources(scope,domain,id,namespace_id,name,name_key,revision,main_commit_id,archived,origin_resource_id,origin_commit_id,body) VALUES(?,'capacity','resource','root','Resource','resource',1,'commit',0,'','',?)", []any{string(scopeRaw), empty}},
		{"INSERT INTO core_branches(scope,domain,id,resource_id,name_key,head_commit_id,revision,body) VALUES(?,'capacity','branch','resource','main','commit',1,?)", []any{string(scopeRaw), empty}},
		{"INSERT INTO core_snapshots(scope,domain,id,resource_id,branch_id,version,source_commit_id,content_digest,payload,manifest,body) VALUES(?,'capacity','commit','resource','branch',1,'',?,?,?,?)", []any{string(scopeRaw), pin, payload, empty, empty}},
		{"INSERT INTO core_references(scope,domain,commit_id,slot,target_domain,target_resource_id,target_commit_id,body) VALUES(?,'capacity','commit','slot','capacity','resource','commit',?)", []any{string(scopeRaw), referenceBody}},
		{"INSERT INTO core_data_usage(scope,rows,bytes) VALUES(?,6,?)", []any{string(scopeRaw), (17 << 20) + 6*1024}},
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(deadline(t), statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestFaultNativeNamespaceAggregateResponseCapacity(t *testing.T) {
	dir := privateDir(t)
	seedNamespaceCapacity(t, dir)
	d := startWithManifest(t, dir, false, "", coreConfig("", false).Manifest)
	initial := nativeRead(t, d)
	readPayload, _ := json.Marshal(model.NamespaceRead{Domain: "capacity", ID: "tiny"})
	req := api.NamedRequest{Scope: scope, DatabaseID: initial.Token.DatabaseID, Schema: initial.Token.Schema,
		Module: model.Module, Operation: model.NamespaceSnapshotOperation, RequestID: "namespace-tiny", Payload: readPayload}
	if _, err := d.client.Named(deadline(t), req); err != nil {
		t.Fatalf("tiny namespace control failed: %v", err)
	}
	readPayload, _ = json.Marshal(model.NamespaceRead{Domain: "capacity", ID: "root"})
	req.RequestID, req.Payload = "namespace-aggregate-overflow", readPayload
	base := processResources(t, d.cmd.Process.Pid)
	peak := base
	result := make(chan error, 1)
	ctx := deadline(t)
	go func() { _, err := d.client.Named(ctx, req); result <- err }()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	var failure error
	for {
		select {
		case failure = <-result:
			goto done
		case <-tick.C:
			current := processResources(t, d.cmd.Process.Pid)
			peak.FDs, peak.Threads, peak.RSSKiB = max(peak.FDs, current.FDs), max(peak.Threads, current.Threads), max(peak.RSSKiB, current.RSSKiB)
		}
	}
done:
	if code(failure) != "resource_exhausted" {
		t.Fatalf("aggregate response overflow failed at unexpected layer: %v", failure)
	}
	if nativeRead(t, d).Token != initial.Token {
		t.Fatal("rejected readonly namespace response changed revision")
	}
	for _, id := range []string{"namespace-tiny", req.RequestID} {
		if _, err := d.client.Receipt(deadline(t), "namespace-receipt", api.ReceiptRequest{Scope: scope, RequestID: id}); code(err) != "not_found" {
			t.Fatalf("readonly capacity call created receipt: %v", err)
		}
		if _, err := d.client.NamedResult(deadline(t), "namespace-result", api.ReceiptRequest{Scope: scope, RequestID: id}); code(err) != "not_found" {
			t.Fatalf("readonly capacity call retained result: %v", err)
		}
	}
	req.Operation, req.RequestID, req.Payload = model.NamespaceGetOperation, "namespace-after-capacity", json.RawMessage(`{"domain":"capacity","id":"tiny"}`)
	if _, err := d.client.Named(deadline(t), req); err != nil {
		t.Fatalf("small read did not recover after overflow: %v", err)
	}
	commit, err := d.client.Batch(deadline(t), request(initial.Token, "namespace-after-overflow-write"))
	if err != nil || commit.Token.Revision != "2" {
		t.Fatalf("writer did not recover after oversized read: %v", err)
	}
	evidence(t, map[string]any{"profile": d.ref.Profile, "fixture": "new schema, offline private database setup",
		"namespace_body_bytes": 8 << 20, "payload_bytes": 8 << 20, "reference_body_bytes": 1 << 20,
		"each_stage_below_16_mib": true, "aggregate_encoded_result_above_16_mib": true,
		"failure": fmt.Sprint(failure), "readonly_revision_and_receipt_invariants": true,
		"small_read_and_new_write_recovered": true, "resource_baseline": base, "resource_observed_max": peak,
		"exclusive_peak_or_preallocation_bound_proven": false})
}
