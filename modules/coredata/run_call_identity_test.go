package coredata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunCallIdentityUsesExactCanonicalTriggerAndRejectsOversize(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	trigger := json.RawMessage(`{"kind":"workflow-call","parentSource":{"targetId":"local","runId":"parent","workflow":{"resourceId":"wf","commitId":"c1","digest":"digest","name":""}}}`)
	q := model.NewRun{ID: "child", TargetID: "agent", RootRunID: "root", ParentRunID: "parent", CallNodeID: "call", Depth: 2,
		WorkflowResourceID: "wf", WorkflowCommitID: "c1", DefinitionDigest: "digest", ActionID: "run", Inputs: json.RawMessage(`{}`), Trigger: trigger}
	if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Result: json.RawMessage(`{"exact":true}`), Nodes: json.RawMessage(`[]`)}); err != nil {
		t.Fatal(err)
	}
	// Malformed execution detail and forged derived observation cannot become authority.
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET inputs_json=x'01',result_json=x'02',nodes_json=x'03',error_json=x'04',cleanup_errors_json=x'05',source_observation_json=? WHERE id=?`, []byte(`{"parentSource":{"targetId":"forged"}}`), q.ID); err != nil {
		t.Fatal(err)
	}
	got, err := getRunCallIdentity(ctx, tx, testScope, q.ID)
	if err != nil || got.ID != q.ID || got.TargetID != q.TargetID || got.RootRunID != q.RootRunID || got.ParentRunID != q.ParentRunID || got.CallNodeID != q.CallNodeID || got.Depth != q.Depth || !bytes.Equal(got.Trigger, trigger) {
		t.Fatalf("exact call identity: %+v %v", got, err)
	}
	if _, err := getRun(ctx, tx, testScope, q.ID); err == nil {
		t.Fatal("full detail must still detect the corrupt execution payload")
	}
	if _, err := getRunCallIdentity(ctx, tx, testScope, "absent"); err == nil {
		t.Fatal("missing Run selected another row")
	}
	// Inspect the actual SQLite plan: the operation is an exact primary-key
	// lookup, not a newest-first scan or a selected history page.
	rows, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+runCallIdentityQuery, model.MaxStoredRunCallTriggerBytes, testScope, q.ID)
	if err != nil {
		t.Fatal(err)
	}
	var plan string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SEARCH runs") || !strings.Contains(plan, "scope=? AND id=?") {
		t.Fatalf("not an exact indexed lookup: %s", plan)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET trigger_json=zeroblob(?) WHERE id=?`, model.MaxStoredRunCallTriggerBytes+1, q.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunCallIdentity(ctx, tx, testScope, q.ID); err == nil {
		t.Fatal("oversized stored trigger was accepted")
	}
	encoded, err := encodeRunPayload([]byte(`{"x":"` + strings.Repeat("a", model.MaxRunCallTriggerBytes) + `"}`))
	if err != nil || len(encoded) > model.MaxStoredRunCallTriggerBytes {
		t.Fatalf("test compression: %d %v", len(encoded), err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET trigger_json=? WHERE id=?`, encoded, q.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := getRunCallIdentity(ctx, tx, testScope, q.ID); err == nil {
		t.Fatal("compressed trigger exceeded the decoded bound")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET parent_run_id='',call_node_id='',depth=0,root_run_id=id,trigger_json=zeroblob(?) WHERE id=?`, model.MaxStoredRunCallTriggerBytes+1, q.ID); err != nil {
		t.Fatal(err)
	}
	root, err := getRunCallIdentity(ctx, tx, testScope, q.ID)
	if err != nil || len(root.Trigger) != 0 {
		t.Fatalf("root trigger should not be selected: %+v %v", root, err)
	}
	q.ID = "too-large"
	q.Trigger = json.RawMessage(`{"x":"` + strings.Repeat("a", model.MaxRunCallTriggerBytes) + `"}`)
	if _, _, err := createRun(ctx, tx, testScope, q); err == nil {
		t.Fatal("oversized child trigger was admitted")
	}
}
