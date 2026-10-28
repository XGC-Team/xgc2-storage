package coredata

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAcceptedExperimentInvocationUsesItsExactCurrentIndex(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, row := range []struct {
		id, target, experiment, instance string
		closed                           bool
	}{
		{"selected-1", "local", "experiment", "instance", false},
		{"selected-2", "local", "experiment", "instance", false},
		{"other-target", "agent", "experiment", "instance", false},
		{"other-experiment", "local", "other", "instance", false},
		{"other-instance", "local", "experiment", "other", false},
		{"closed", "local", "experiment", "instance", true},
	} {
		source, _ := json.Marshal(map[string]any{"experimentInvocation": map[string]string{"experimentId": row.experiment, "workflowInstanceId": row.instance}})
		q := model.NewRun{ID: row.id, TargetID: row.target, WorkflowResourceID: "same-workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: json.RawMessage(`{}`), Trigger: json.RawMessage(`{}`), SourceObservation: source}
		if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if row.closed {
			if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: row.id, Status: model.RunSucceeded}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A scoped observation must not touch any source/execution detail blob.
	if _, err := tx.ExecContext(ctx, "UPDATE runs SET inputs_json=x'01',trigger_json=x'02',nodes_json=x'03'"); err != nil {
		t.Fatal(err)
	}
	filter := model.RunFilter{TargetID: "local", ExperimentResourceID: "experiment", WorkflowInstanceID: "instance", Statuses: []model.RunStatus{model.RunQueued, model.RunRunning, model.RunStopping}, Projection: model.RunObservation, Limit: 1}
	seen := map[string]bool{}
	for {
		page, err := listRuns(ctx, tx, testScope, filter)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Runs {
			if seen[r.ID] || !strings.HasPrefix(r.ID, "selected-") {
				t.Fatal("scoped query crossed accepted source", r.ID)
			}
			seen[r.ID] = true
		}
		if page.Next == "" {
			break
		}
		filter.Cursor = page.Next
	}
	if len(seen) != 2 {
		t.Fatalf("selected current Runs lost: %v", seen)
	}
	query, args, _, err := runQuery(testScope, filter)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	plan := strings.Join(details, " ")
	if !strings.Contains(plan, "runs_experiment_current") || strings.Contains(plan, "SCAN runs") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("scoped current source scans unrelated history: %s", plan)
	}
}
