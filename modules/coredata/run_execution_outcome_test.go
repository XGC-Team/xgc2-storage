package coredata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestExecutionOutcomeSkipsUnrelatedCorruptPayloads(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	request := model.NewRun{ID: "outcome", TargetID: "agent", WorkflowResourceID: "wf", WorkflowCommitID: "c1", DefinitionDigest: strings.Repeat("d", 64), ActionID: "run",
		Inputs: json.RawMessage(`{"input":"private"}`), Trigger: json.RawMessage(`{"kind":"manual"}`), SourceObservation: json.RawMessage(`{"invocation":{"instanceId":"exact"}}`)}
	if _, _, err := createRun(ctx, tx, testScope, request); err != nil {
		t.Fatal(err)
	}
	finish := model.RunFinish{ID: request.ID, Status: model.RunFailed, Error: json.RawMessage(`{"code":"exact-error"}`),
		Result: json.RawMessage(`{"exact":9007199254740993}`), Nodes: json.RawMessage(`[{"id":"private"}]`), CleanupErrors: json.RawMessage(`[{"message":"cleanup"}]`)}
	if _, err := finishRun(ctx, tx, testScope, finish); err != nil {
		t.Fatal(err)
	}
	full, err := getRun(ctx, tx, testScope, request.ID)
	if err != nil || !bytes.Equal(full.Nodes, finish.Nodes) || !bytes.Equal(full.Result, finish.Result) {
		t.Fatalf("ordinary complete Run changed: %+v %v", full, err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE runs SET inputs_json=x'01',trigger_json=x'02',nodes_json=x'03',cleanup_errors_json=x'04' WHERE id=?", request.ID); err != nil {
		t.Fatal(err)
	}
	outcome, err := getRunExecutionOutcome(ctx, tx, testScope, request.ID)
	if err != nil || outcome.ID != request.ID || outcome.TargetID != request.TargetID || outcome.Projection != model.RunExecutionOutcome || !outcome.HasResult ||
		!bytes.Equal(outcome.Result, finish.Result) || !bytes.Equal(outcome.Error, finish.Error) ||
		!bytes.Equal(outcome.SourceObservation, request.SourceObservation) || outcome.CleanupFailed != 1 ||
		len(outcome.Inputs)+len(outcome.Trigger)+len(outcome.Nodes)+len(outcome.CleanupErrors) != 0 {
		t.Fatalf("exact outcome selected unrelated data: %+v %v", outcome, err)
	}
	if _, err := getRun(ctx, tx, testScope, request.ID); err == nil {
		t.Fatal("ordinary full Get concealed corrupt execution detail")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE runs SET result_json=x'01' WHERE id=?", request.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunExecutionOutcome(ctx, tx, testScope, request.ID); err == nil {
		t.Fatal("corrupt selected outcome was accepted")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE runs SET result_json=zeroblob(?) WHERE id=?", 2*model.MaxRunResultBytes+1, request.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunExecutionOutcome(ctx, tx, testScope, request.ID); err == nil {
		t.Fatal("oversized stored outcome was accepted")
	}
	if _, err := getRunExecutionOutcome(ctx, tx, testScope, "missing"); err == nil {
		t.Fatal("missing Run returned an outcome")
	}
}

func TestExecutionOutcomeUsesExactRunIndex(t *testing.T) {
	db, ctx := fixture(t)
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT "+executionOutcomeColumns+" FROM runs WHERE scope=? AND id=?",
		2*model.MaxRunErrorBytes, 2*model.MaxRunResultBytes, model.MaxRunSourceObservationBytes, model.MaxRunErrorSummaryBytes, testScope, "child")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details string
	for rows.Next() {
		var id, parent, unused int
		var step string
		if err := rows.Scan(&id, &parent, &unused, &step); err != nil {
			t.Fatal(err)
		}
		details += step + "; "
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(details, "SEARCH") || !strings.Contains(details, "sqlite_autoindex_runs_1") || strings.Contains(details, "SCAN ") || strings.Contains(details, "TEMP B-TREE") {
		t.Fatalf("exact outcome scanned or sorted history: %s", details)
	}
}
