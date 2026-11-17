package coredata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunCleanupSkipsAllUnrelatedPayloadsAndKeepsBounds(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	request := model.NewRun{ID: "cleanup", TargetID: "local", WorkflowResourceID: "wf", WorkflowCommitID: "c1", DefinitionDigest: "digest", ActionID: "run", Inputs: json.RawMessage(`{}`), Trigger: json.RawMessage(`{}`)}
	if _, _, err := createRun(ctx, tx, testScope, request); err != nil {
		t.Fatal(err)
	}
	want := json.RawMessage(`[{"nodeId":"owner","message":"exact teardown detail 保留"}]`)
	if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: request.ID, Status: model.RunFailed, CleanupErrors: want}); err != nil {
		t.Fatal(err)
	}
	// A broad Get must fail these discarded fields. The selected cleanup read
	// must still return the complete original diagnostic from the same row.
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET inputs_json=x'01',trigger_json=x'02',nodes_json=x'03',result_json=x'04',error_json=x'05' WHERE id=?`, request.ID); err != nil {
		t.Fatal(err)
	}
	out, err := getRunCleanup(ctx, tx, testScope, request.ID)
	if err != nil || out.ID != request.ID || out.CleanupFailed != 1 || !bytes.Equal(out.CleanupErrors, want) || len(out.Inputs)+len(out.Trigger)+len(out.Nodes)+len(out.Result)+len(out.Error)+len(out.SourceObservation)+len(out.ErrorSummary) != 0 {
		t.Fatalf("cleanup projection: %+v %v", out, err)
	}
	if _, err := getRun(ctx, tx, testScope, request.ID); err == nil {
		t.Fatal("broad Get concealed corrupt discarded payloads")
	}
	for _, scope := range []string{testScope, "other-scope"} {
		id := "missing"
		if scope != testScope {
			id = request.ID
		}
		if _, err := getRunCleanup(ctx, tx, scope, id); errorCode(err) != "not_found" {
			t.Fatalf("missing/foreign selected another row: %v", err)
		}
	}
	if _, err := getRunCleanup(ctx, tx, testScope, ""); errorCode(err) != "invalid_argument" {
		t.Fatalf("empty identity: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET cleanup_errors_json=zeroblob(?) WHERE id=?`, 2*model.MaxRunCleanupErrorsBytes+1, request.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunCleanup(ctx, tx, testScope, request.ID); errorCode(err) != "data_loss" {
		t.Fatalf("encoded cleanup bound lost: %v", err)
	}
	encoded, err := encodeRunPayload(bytes.Repeat([]byte("x"), model.MaxRunCleanupErrorsBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET cleanup_errors_json=? WHERE id=?`, encoded, request.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunCleanup(ctx, tx, testScope, request.ID); errorCode(err) != "data_loss" {
		t.Fatalf("decoded cleanup bound lost: %v", err)
	}
}

func TestRunCleanupUsesExistingExactRowIndex(t *testing.T) {
	db, ctx := fixture(t)
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+runCleanupQuery, 2*model.MaxRunCleanupErrorsBytes, testScope, "owner")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SEARCH runs") || !strings.Contains(plan, "scope=? AND id=?") || strings.Contains(plan, "SCAN ") {
		t.Fatalf("cleanup scanned instead of selecting exact row: %s", plan)
	}
}
