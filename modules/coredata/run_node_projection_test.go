package coredata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunNodeProjectionKeepsExecutionFieldsAndSkipsUnrequestedPayloads(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	q := model.NewRun{ID: "selected", TargetID: "agent", RootRunID: "root", ParentRunID: "parent", CallNodeID: "call", Depth: 2, SessionID: "session",
		WorkflowResourceID: "wf", WorkflowCommitID: "pin", DefinitionDigest: strings.Repeat("a", 64), ActionID: "run",
		Inputs: json.RawMessage(`{"input":{"exact":9007199254740993}}`), Trigger: json.RawMessage(`{"kind":"manual"}`),
		SourceObservation: json.RawMessage(`{"invocation":{"instanceId":"owned","presetId":"run"},"actionControl":{"kind":"service","stop":true}}`)}
	if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	f := model.RunFinish{ID: q.ID, Status: model.RunFailed, Termination: "failed",
		Error:         json.RawMessage(`{"code":"exact","message":"原错误","details":{"n":9007199254740993}}`),
		Result:        json.RawMessage(`{"result":9007199254740993}`),
		Nodes:         json.RawMessage(`[{"id":"owner","resources":[{"kind":"process","id":"native","meta":{"join":{"incarnation":"accepted"}}}]}]`),
		CleanupErrors: json.RawMessage(`[{"message":"physical teardown failed"}]`)}
	if _, err := finishRun(ctx, tx, testScope, f); err != nil {
		t.Fatal(err)
	}
	for _, input := range []bool{false, true} {
		r, err := getRunNodeProjection(ctx, tx, testScope, q.ID, input)
		if err != nil || r.ID != q.ID || r.TargetID != q.TargetID || r.ParentRunID != q.ParentRunID || r.RootRunID != q.RootRunID || r.CallNodeID != q.CallNodeID || r.Depth != 2 || r.SessionID != q.SessionID || r.WorkflowCommitID != q.WorkflowCommitID || r.DefinitionDigest != q.DefinitionDigest || r.Status != f.Status || r.Termination != f.Termination || !r.HasResult || r.CleanupFailed != 1 ||
			!bytes.Equal(r.Error, f.Error) || !bytes.Equal(r.Result, f.Result) || !bytes.Equal(r.Nodes, f.Nodes) || !bytes.Equal(r.SourceObservation, q.SourceObservation) || len(r.ErrorSummary) == 0 || len(r.Trigger)+len(r.CleanupErrors) != 0 || input && !bytes.Equal(r.Inputs, q.Inputs) || !input && len(r.Inputs) != 0 {
			t.Fatalf("selected execution fields: input=%v row=%+v err=%v", input, r, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET trigger_json=x'01',cleanup_errors_json=x'02' WHERE id=?`, q.ID); err != nil {
		t.Fatal(err)
	}
	for _, input := range []bool{false, true} {
		if _, err := getRunNodeProjection(ctx, tx, testScope, q.ID, input); err != nil {
			t.Fatalf("unused payload was decoded: input=%v err=%v", input, err)
		}
	}
	if _, err := getRun(ctx, tx, testScope, q.ID); err == nil {
		t.Fatal("full Get concealed corrupt unused detail")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET inputs_json=x'03' WHERE id=?`, q.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunNodeProjection(ctx, tx, testScope, q.ID, false); err != nil {
		t.Fatalf("unrequested input decoded: %v", err)
	}
	if _, err := getRunNodeProjection(ctx, tx, testScope, q.ID, true); err == nil {
		t.Fatal("explicit input corruption was hidden")
	}
	for _, field := range []string{"result_json", "error_json", "nodes_json"} {
		t.Run(field, func(t *testing.T) {
			var saved []byte
			if err := tx.QueryRowContext(ctx, "SELECT "+field+" FROM runs WHERE id=?", q.ID).Scan(&saved); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, "UPDATE runs SET "+field+"=x'01' WHERE id=?", q.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := getRunNodeProjection(ctx, tx, testScope, q.ID, false); err == nil {
				t.Fatal("selected payload corruption was hidden")
			}
			if _, err := tx.ExecContext(ctx, "UPDATE runs SET "+field+"=? WHERE id=?", saved, q.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := getRunNodeProjection(ctx, tx, "foreign", q.ID, false); errorCode(err) != "not_found" {
		t.Fatalf("foreign scope: %v", err)
	}
	if _, err := getRunNodeProjection(ctx, tx, testScope, "missing", false); errorCode(err) != "not_found" {
		t.Fatalf("missing: %v", err)
	}
	if _, err := getRunNodeProjection(ctx, tx, testScope, "", false); errorCode(err) != "invalid_argument" {
		t.Fatalf("empty ID: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET nodes_json=zeroblob(?) WHERE id=?`, 2*model.MaxRunNodesBytes+1, q.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunNodeProjection(ctx, tx, testScope, q.ID, false); errorCode(err) != "data_loss" {
		t.Fatalf("encoded node bound: %v", err)
	}
}
