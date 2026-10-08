//go:build linux

package faults_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

const namespaceSeedBytes = 128 << 10

// There is no public namespace/resource creation operation. Bootstrap only a
// newly initialized, closed, private current-schema database, never a user DB.
func seedNamespaceBusiness(t *testing.T, dir string) model.NamespaceClone {
	t.Helper()
	path := filepath.Join(dir, "fixture.db")
	s := openCore(t, path, true)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	payload := sizedObject(64<<10, "x")
	manifestBody := []byte(`{"resource":"resource","counter":9007199254740993}`)
	refs := []model.Reference{
		{Slot: "external", TargetDomain: "external", TargetResourceID: "other", TargetCommitID: "other-commit", Body: json.RawMessage(`{"policy":"fixed"}`)},
		{Slot: "self", TargetDomain: "robot", TargetResourceID: "resource", TargetCommitID: "commit", Body: json.RawMessage(`{"counter":9223372036854775806}`)},
	}
	pin, err := model.SnapshotDigest(payload, manifestBody, refs)
	if err != nil {
		t.Fatal(err)
	}
	historyPin, err := model.SnapshotDigest([]byte(`{}`), []byte(`{}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	db := namespaceSQL(t, path, false)
	defer db.Close()
	tx, err := db.BeginTx(deadline(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	scopeRaw, _ := json.Marshal(scope)
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO scopes(scope,namespace,revision) VALUES(?,?,1)", []any{string(scopeRaw), scope.Namespace}},
		{"INSERT INTO core_namespaces VALUES(?,'robot','source',NULL,'Source','source',9007199254740993,0,?)", []any{string(scopeRaw), []byte(`{"id":"source","counter":9007199254740993}`)}},
		{"INSERT INTO core_namespaces VALUES(?,'robot','child','source','Child','child',3,0,?)", []any{string(scopeRaw), []byte(`{"id":"child","parent":"source"}`)}},
		{"INSERT INTO core_resources VALUES(?,'robot','resource','child','Resource','resource',5,'commit',0,'','',?)", []any{string(scopeRaw), []byte(`{"namespace":"child","counter":9007199254740993}`)}},
		{"INSERT INTO core_branches VALUES(?,'robot','branch','resource','main','commit',4,?)", []any{string(scopeRaw), []byte(`{"name":"main","current":"commit"}`)}},
		{"INSERT INTO core_branches VALUES(?,'robot','dev-branch','resource','dev','dev-commit',2,?)", []any{string(scopeRaw), []byte(`{"name":"dev"}`)}},
		{"INSERT INTO core_snapshots VALUES(?,'robot','commit','resource','branch',7,'',?,?,?,?)", []any{string(scopeRaw), pin, payload, manifestBody, []byte(`{"version":7,"counter":9223372036854775806}`)}},
		{"INSERT INTO core_snapshots VALUES(?,'robot','old-commit','resource','branch',6,'',?,'{}','{}','{}')", []any{string(scopeRaw), historyPin}},
		{"INSERT INTO core_snapshots VALUES(?,'robot','dev-commit','resource','dev-branch',8,'',?,'{}','{}','{}')", []any{string(scopeRaw), historyPin}},
		{"INSERT INTO core_data_usage VALUES(?,10,?)", []any{string(scopeRaw), namespaceSeedBytes}},
	}
	for _, statement := range statements {
		if _, err = tx.ExecContext(deadline(t), statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	for _, ref := range refs {
		if _, err = tx.ExecContext(deadline(t), "INSERT INTO core_references VALUES(?,'robot','commit',?,?,?,?,?)", string(scopeRaw), ref.Slot, ref.TargetDomain, ref.TargetResourceID, ref.TargetCommitID, []byte(ref.Body)); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	refs[1].TargetResourceID, refs[1].TargetCommitID = "copy-resource", "copy-commit"
	return model.NamespaceClone{Domain: "robot", SourceID: "source", ExpectedRevision: "9007199254740993",
		TargetID: "copy", ExpectedTargetParentRevision: "0", Name: "Copy", NameKey: "copy", ChangeID: "copy-change",
		Change: json.RawMessage(`{"type":"namespace-cloned","source":"source","target":"copy","counter":9223372036854775806}`),
		Namespaces: []model.NamespaceCopy{
			{SourceID: "source", TargetID: "copy", ExpectedRevision: "9007199254740993", Body: json.RawMessage(`{"id":"copy","counter":9007199254740993}`)},
			{SourceID: "child", TargetID: "copy-child", ExpectedRevision: "3", Body: json.RawMessage(`{"id":"copy-child","parent":"copy"}`)},
		},
		Resources: []model.ResourceCopy{{SourceID: "resource", ExpectedRevision: "5", ExpectedBranchRevision: "4", SourceCommitID: "commit", SourceContentDigest: pin,
			TargetID: "copy-resource", TargetBranchID: "copy-branch", TargetCommitID: "copy-commit",
			Body: json.RawMessage(`{"namespace":"copy-child","counter":9007199254740993}`), BranchBody: json.RawMessage(`{"name":"main","current":"copy-commit"}`),
			CommitBody: json.RawMessage(`{"version":1,"source":"commit","counter":9223372036854775806}`), Payload: payload,
			Manifest: []byte(`{"resource":"copy-resource","counter":9007199254740993}`), References: refs}},
	}
}

func namespaceSQL(t *testing.T, path string, readonly bool) *sql.DB {
	t.Helper()
	query := "mode=rw&_pragma=foreign_keys(1)"
	if readonly {
		query = "mode=ro&_pragma=query_only(1)&_pragma=foreign_keys(1)"
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: query}).String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func namespaceRequest(token api.Token, plan model.NamespaceClone) api.NamedRequest {
	payload, _ := json.Marshal(plan)
	return api.NamedRequest{Scope: scope, DatabaseID: token.DatabaseID, Schema: token.Schema,
		Module: model.Module, Operation: model.NamespaceCloneOperation, RequestID: "clone-request", Payload: payload}
}

// Fixed table allowlist; all candidates are closed and privately owned. Compare
// every persisted field, including source history not exposed by the live DTO.
func namespaceRows(t *testing.T, path string) map[string][][]any {
	t.Helper()
	db := namespaceSQL(t, path, true)
	defer db.Close()
	out := map[string][][]any{}
	for _, table := range []string{"scopes", "core_namespaces", "core_resources", "core_branches", "core_snapshots", "core_references", "core_changes", "core_data_usage", "receipts", "named_results"} {
		order := "1,2,3"
		if table == "core_references" {
			order += ",4" // The reference slot completes the unique key.
		}
		rows, err := db.QueryContext(deadline(t), "SELECT * FROM "+table+" ORDER BY "+order)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = bytes.Clone(raw)
				}
			}
			out[table] = append(out[table], values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func namespaceSourceRows(all map[string][][]any) map[string][][]any {
	ids := map[string]bool{"source": true, "child": true, "resource": true, "branch": true,
		"dev-branch": true, "commit": true, "old-commit": true, "dev-commit": true}
	out := map[string][][]any{}
	for _, table := range []string{"core_namespaces", "core_resources", "core_branches", "core_snapshots", "core_references"} {
		for _, row := range all[table] {
			// Each listed table has scope/domain/source entity or commit ID first.
			if id, ok := row[2].(string); ok && ids[id] {
				out[table] = append(out[table], row)
			}
		}
	}
	return out
}

func namespaceTree(t *testing.T, d *daemon, token api.Token, id string) model.NamespaceTree {
	t.Helper()
	payload, _ := json.Marshal(model.NamespaceRead{Domain: "robot", ID: id})
	out, err := d.client.Named(deadline(t), api.NamedRequest{Scope: scope, DatabaseID: token.DatabaseID, Schema: token.Schema,
		Module: model.Module, Operation: model.NamespaceSnapshotOperation, RequestID: "read-" + id, Payload: payload})
	if err != nil || out.Receipt != nil {
		t.Fatalf("namespace snapshot/readonly receipt: %v %+v", err, out.Receipt)
	}
	var tree model.NamespaceTree
	if err = json.Unmarshal(out.Result, &tree); err != nil {
		t.Fatal(err)
	}
	return tree
}

func verifyNamespaceTree(t *testing.T, got model.NamespaceTree, plan model.NamespaceClone) {
	t.Helper()
	resource := plan.Resources[0]
	pin, err := model.SnapshotDigest(resource.Payload, resource.Manifest, resource.References)
	if err != nil {
		t.Fatal(err)
	}
	want := model.NamespaceTree{Namespaces: []model.NamespaceRow{
		{ID: "copy", ParentID: "", Name: "Copy", NameKey: "copy", Revision: "1", Body: plan.Namespaces[0].Body},
		{ID: "copy-child", ParentID: "copy", Name: "Child", NameKey: "child", Revision: "1", Body: plan.Namespaces[1].Body},
	}, Resources: []model.ResourceSnapshot{{ID: "copy-resource", NamespaceID: "copy-child", Name: "Resource", NameKey: "resource", Revision: "1",
		CommitID: "copy-commit", BranchID: "copy-branch", BranchRevision: "1", ContentDigest: pin, Body: resource.Body,
		BranchBody: resource.BranchBody, CommitBody: resource.CommitBody, Payload: resource.Payload, Manifest: resource.Manifest, References: resource.References}}}
	gotRaw, _ := json.Marshal(got)
	wantRaw, _ := json.Marshal(want)
	if !equalJSON(t, gotRaw, wantRaw) {
		t.Fatal("cloned namespace lost topology, complete bodies, bytes, references or digest")
	}
}

func namespaceAudit(t *testing.T, path string, req api.NamedRequest, committed bool) {
	t.Helper()
	db := namespaceSQL(t, path, true)
	defer db.Close()
	var plan model.NamespaceClone
	if err := json.Unmarshal(req.Payload, &plan); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(plan); err != nil {
		t.Fatal(err)
	}
	checks := map[string]int64{"SELECT COUNT(*) FROM core_namespaces": 2, "SELECT COUNT(*) FROM core_resources": 1,
		"SELECT COUNT(*) FROM core_branches": 2, "SELECT COUNT(*) FROM core_snapshots": 3, "SELECT COUNT(*) FROM core_references": 2,
		"SELECT COUNT(*) FROM core_changes": 0, "SELECT COUNT(*) FROM named_results": 0, "SELECT COUNT(*) FROM receipts": 0,
		"SELECT rows FROM core_data_usage": 10, "SELECT bytes FROM core_data_usage": namespaceSeedBytes, "SELECT revision FROM scopes": 1}
	if committed {
		checks["SELECT COUNT(*) FROM core_namespaces"] = 4
		checks["SELECT COUNT(*) FROM core_resources"] = 2
		checks["SELECT COUNT(*) FROM core_branches"] = 3
		checks["SELECT COUNT(*) FROM core_snapshots"] = 4
		checks["SELECT COUNT(*) FROM core_references"] = 4
		checks["SELECT COUNT(*) FROM core_changes"] = 1
		checks["SELECT COUNT(*) FROM named_results"] = 1
		checks["SELECT COUNT(*) FROM receipts"] = 1
		checks["SELECT rows FROM core_data_usage"] = 18
		checks["SELECT bytes FROM core_data_usage"] += int64(encoded.Len() - 1 + 8*1024)
		checks["SELECT revision FROM scopes"] = 2
	}
	for query, want := range checks {
		var got int64
		if err := db.QueryRowContext(deadline(t), query).Scan(&got); err != nil || got != want {
			t.Fatalf("namespace closed-candidate audit: %s got=%d want=%d error=%v", query, got, want, err)
		}
	}
	if !committed {
		return
	}
	var originResource, originCommit, sourceCommit, domain, entity string
	var version int
	var change []byte
	if err := db.QueryRowContext(deadline(t), "SELECT origin_resource_id,origin_commit_id FROM core_resources WHERE id='copy-resource'").Scan(&originResource, &originCommit); err != nil || originResource != "resource" || originCommit != "commit" {
		t.Fatalf("resource provenance differs: %q %q %v", originResource, originCommit, err)
	}
	if err := db.QueryRowContext(deadline(t), "SELECT version,source_commit_id FROM core_snapshots WHERE id='copy-commit'").Scan(&version, &sourceCommit); err != nil || version != 1 || sourceCommit != "commit" {
		t.Fatalf("snapshot provenance/version differs: %d %q %v", version, sourceCommit, err)
	}
	if err := db.QueryRowContext(deadline(t), "SELECT domain,entity_id,body FROM core_changes WHERE id='copy-change'").Scan(&domain, &entity, &change); err != nil || domain != plan.Domain || entity != plan.TargetID || !equalJSON(t, change, plan.Change) {
		t.Fatalf("clone change fact differs: %v", err)
	}
	for _, query := range []string{"SELECT COUNT(*) FROM core_branches WHERE resource_id='copy-resource'", "SELECT COUNT(*) FROM core_snapshots WHERE resource_id='copy-resource'"} {
		var count int
		if err := db.QueryRowContext(deadline(t), query).Scan(&count); err != nil || count != 1 {
			t.Fatalf("clone copied source history/nonmain branch: %s %d %v", query, count, err)
		}
	}
}

func TestFaultNativeNamespaceCloneKillReceiptAndBackupRestore(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			dir := privateDir(t)
			plan := seedNamespaceBusiness(t, dir)
			originalRows := namespaceSourceRows(namespaceRows(t, filepath.Join(dir, "fixture.db")))
			m := coreConfig("", false).Manifest
			d := startProfileManifest(t, dir, false, "", m, profile)
			initial := nativeRead(t, d).Token
			source := namespaceTree(t, d, initial, "source")
			req := namespaceRequest(initial, plan)
			committed, err := d.client.Named(deadline(t), req)
			if err != nil || committed.Receipt == nil || committed.Receipt.Token.Revision != "2" || committed.Receipt.Token.DatabaseID != initial.DatabaseID || committed.Receipt.Token.Schema != initial.Schema || committed.Receipt.Durability != "sqlite-full" || committed.Receipt.RequestID != req.RequestID {
				t.Fatalf("namespace clone commit: %+v %v", committed, err)
			}
			var meta model.NamespaceCloned
			if err = json.Unmarshal(committed.Result, &meta); err != nil || meta != (model.NamespaceCloned{ID: "copy", NamespaceCount: 2, ResourceCount: 1}) {
				t.Fatalf("clone result differs: %+v %v", meta, err)
			}
			verify := func(d *daemon) {
				retained, err := d.client.Receipt(deadline(t), "clone-receipt", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
				if err != nil || !reflect.DeepEqual(retained, *committed.Receipt) {
					t.Fatalf("clone receipt recovery differs: %v", err)
				}
				result, err := d.client.NamedResult(deadline(t), "clone-result", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
				if err != nil || !reflect.DeepEqual(result, committed) {
					t.Fatalf("clone named result recovery differs: %v", err)
				}
				again, err := d.client.Named(deadline(t), req)
				if err != nil || !reflect.DeepEqual(again, committed) {
					t.Fatalf("clone exact replay differs: %v", err)
				}
				verifyNamespaceTree(t, namespaceTree(t, d, initial, "copy"), plan)
				before, _ := json.Marshal(source)
				after, _ := json.Marshal(namespaceTree(t, d, initial, "source"))
				if !equalJSON(t, before, after) || nativeRead(t, d).Token != committed.Receipt.Token {
					t.Fatal("clone/recovery changed source business tree or readonly scope revision")
				}
			}
			verify(d)
			d.kill(t)
			d = startProfileManifest(t, dir, false, "", m, profile)
			verify(d)
			d.kill(t)
			path := filepath.Join(dir, "fixture.db")
			namespaceAudit(t, path, req, true)
			closedRows := namespaceRows(t, path)
			if !reflect.DeepEqual(namespaceSourceRows(closedRows), originalRows) {
				t.Fatal("clone/replay/restart changed complete source fields, history, version or provenance")
			}
			s := openCore(t, path, false)
			// Preserve a direct-engine baseline for the direct watermark calls.
			// Protobuf represents an empty repeated Versions as nil; the engine's
			// JSON persistence retains []. Verify all fields and require both empty.
			directBaseline, err := s.NamedResult(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
			if err != nil || directBaseline.Receipt == nil || len(directBaseline.Receipt.Versions) != 0 || len(committed.Receipt.Versions) != 0 || !bytes.Equal(directBaseline.Result, committed.Result) {
				t.Fatalf("direct/native clone result baseline differs: %v", err)
			}
			nativeReceipt := *committed.Receipt
			nativeReceipt.Versions = directBaseline.Receipt.Versions
			if !reflect.DeepEqual(nativeReceipt, *directBaseline.Receipt) {
				t.Fatal("direct/native clone receipt fields differ")
			}
			candidateDir := privateDir(t)
			candidate := filepath.Join(candidateDir, "fixture.db")
			if _, err = s.Backup(deadline(t), candidate); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			// Audit the closed consistent candidate, including fields absent in DTOs.
			namespaceAudit(t, candidate, req, true)
			if !reflect.DeepEqual(namespaceRows(t, candidate), closedRows) {
				t.Fatal("consistent backup changed complete namespace relations/history/usage/receipts")
			}
			restored := openCore(t, candidate, false)
			if err = restored.Integrity(deadline(t)); err != nil {
				t.Fatal(err)
			}
			if err = restored.Close(); err != nil {
				t.Fatal(err)
			}
			d = startProfileManifest(t, candidateDir, false, "", m, profile)
			verify(d)
			d.kill(t)
			// Named replay must resolve retained results before new disk admission.
			config := coreConfig(path, false)
			config.MinFreeBytes = 1<<63 - 1
			watermarked, err := engine.Open(deadline(t), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = watermarked.Close() })
			cached, err := watermarked.Named(deadline(t), req)
			if err != nil || !reflect.DeepEqual(cached, directBaseline) {
				t.Fatalf("committed named replay blocked by watermark: %v", err)
			}
			for _, resolve := range []string{"receipt", "result"} {
				lookup := api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}
				if resolve == "receipt" {
					got, err := watermarked.Receipt(deadline(t), lookup)
					if err != nil || !reflect.DeepEqual(got, *directBaseline.Receipt) {
						t.Fatalf("watermarked retained receipt: %v", err)
					}
				} else {
					got, err := watermarked.NamedResult(deadline(t), lookup)
					if err != nil || !reflect.DeepEqual(got, directBaseline) {
						t.Fatalf("watermarked retained result: %v", err)
					}
				}
			}
			newReq := req
			newReq.RequestID = "new-clone-under-watermark"
			if _, err = watermarked.Named(deadline(t), newReq); code(err) != "resource_exhausted" {
				t.Fatalf("new named write bypassed disk watermark: %v", err)
			}
			if _, err = watermarked.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: newReq.RequestID}); code(err) != "not_found" {
				t.Fatalf("rejected watermarked clone published receipt: %v", err)
			}
			if err = watermarked.Close(); err != nil {
				t.Fatal(err)
			}
			namespaceAudit(t, path, req, true)
			evidence(t, map[string]any{"profile": profile, "operation": "namespace.clone", "private_current_schema_bootstrap": true,
				"commit_sigkill_restart_and_consistent_backup": true, "complete_namespace_resource_snapshot_and_reference_fields": true,
				"provenance_change_and_usage_verified_by_closed_readonly_sql": true, "history_and_nonmain_not_cloned": true,
				"named_receipt_result_replay_under_watermark": true, "new_write_rejected_without_receipt": true, "clone_added_rows": 8})
		})
	}
}

func TestFaultKillUncommittedNamespaceCloneAtomicity(t *testing.T) {
	dir := privateDir(t)
	plan := seedNamespaceBusiness(t, dir)
	path := filepath.Join(dir, "fixture.db")
	s := openCore(t, path, false)
	token := read(t, s).Token
	req := namespaceRequest(token, plan)
	sourceReq := req
	sourceReq.Operation, sourceReq.Payload = model.NamespaceSnapshotOperation, json.RawMessage(`{"domain":"robot","id":"source"}`)
	source, err := s.Named(deadline(t), sourceReq)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	namespaceAudit(t, path, req, false)
	beforeRows := namespaceRows(t, path)
	raw, _ := json.Marshal(req)
	if err = os.WriteFile(filepath.Join(dir, "clone-request.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFaultChild$", "-test.v")
	cmd.Env = append(os.Environ(), "FAULT_CHILD=namespace-uncommitted", "FAULT_CHILD_DIR="+dir, "GOMAXPROCS=2")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	reached := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "UNCOMMITTED_NAMESPACE" {
				reached <- true
				return
			}
		}
		reached <- false
	}()
	select {
	case ok := <-reached:
		if !ok {
			_ = cmd.Wait()
			waited = true
			t.Fatalf("namespace child did not reach real SQL transaction: %s", stderr.String())
		}
	case <-time.After(8 * time.Second):
		t.Fatal("namespace child did not reach held uncommitted transaction")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	s = openCore(t, path, false)
	after, err := s.Named(deadline(t), sourceReq)
	if err != nil || !equalJSON(t, source.Result, after.Result) || read(t, s).Token != token {
		t.Fatalf("uncommitted clone changed source business or revision: %v", err)
	}
	missing := sourceReq
	missing.Payload = json.RawMessage(`{"domain":"robot","id":"copy"}`)
	if _, err = s.Named(deadline(t), missing); code(err) != "not_found" {
		t.Fatalf("crashed clone left visible target: %v", err)
	}
	if _, err = s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
		t.Fatalf("uncommitted clone published receipt: %v", err)
	}
	if _, err = s.NamedResult(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
		t.Fatalf("uncommitted clone published result: %v", err)
	}
	if err = s.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	namespaceAudit(t, path, req, false)
	if !reflect.DeepEqual(namespaceRows(t, path), beforeRows) {
		t.Fatal("pre-COMMIT SIGKILL changed complete source relations/history/usage/receipts")
	}
	evidence(t, map[string]any{"boundary": "real namespace.clone SQL completed before engine COMMIT", "signal": "SIGKILL",
		"target_relations_change_receipt_result_and_usage_rolled_back": true, "source_tree_and_token_preserved": true,
		"test_only_module_wrapper": true})
}

func uncommittedNamespaceChild(t *testing.T, dir string) {
	config := coreConfig(filepath.Join(dir, "fixture.db"), false)
	realExecute := config.Modules[0].Execute
	config.Modules[0].Execute = func(ctx context.Context, tx *sql.Tx, scopeID, operation string, payload json.RawMessage) (json.RawMessage, error) {
		result, err := realExecute(ctx, tx, scopeID, operation, payload)
		if err != nil {
			return result, err
		}
		fmt.Println("UNCOMMITTED_NAMESPACE")
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s, err := engine.Open(deadline(t), config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, err := os.ReadFile(filepath.Join(dir, "clone-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	var req api.NamedRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	_, err = s.Named(deadline(t), req)
	t.Fatalf("private held namespace operation should be killed by parent: %v", err)
}
