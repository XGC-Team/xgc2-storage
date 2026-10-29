package coredata

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunFaultProbeSeeksOnlyChangedIDsAndPolicy(t *testing.T) {
	var selectedBytes [2]int
	var createBytes [2]int
	for trial, unrelated := range []int{0, 128} {
		rec := &headerSQLRecorder{}
		db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "fault.db") + "?_pragma=foreign_keys(1)", rec: rec})
		db.SetMaxOpenConns(1)
		ctx := t.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := Initialize(ctx, tx); err != nil {
			t.Fatal(err)
		}
		at := time.Unix(10000, 0).UTC()
		for i := 0; i <= unrelated; i++ {
			id := "subject"
			if i > 0 {
				id = fmt.Sprintf("unrelated-%03d", i)
			}
			q := model.NewRun{ID: id, TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`), At: at}
			if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: "subject", Status: model.RunSucceeded, At: at.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rec.statements, rec.bytes = nil, 0
		state := runFaultState{scope: testScope, policy: model.RunRetentionPolicy{OlderThan: at.Add(time.Hour)}, finished: []string{"subject"}, attempt: model.RunRetentionEffect{ChangedFinish: true, EligibilityReleased: true}}
		if err := state.probe(ctx, tx, true); err != nil {
			t.Fatal(err)
		}
		if !state.fact.Retention.ChangedFinish || !state.fact.Retention.HasVictim || len(rec.statements) != 2 {
			t.Fatalf("exact finish and policy queries: fact=%+v SQL=%v", state.fact, rec.statements)
		}
		for _, query := range rec.statements {
			lower := strings.ToLower(query)
			for _, body := range []string{"inputs_json", "trigger_json", "result_json", "nodes_json", "cleanup_errors_json"} {
				if strings.Contains(lower, body) {
					t.Fatalf("fault probe selected %s: %s", body, query)
				}
			}
		}
		selectedBytes[trial] = rec.bytes
		rows, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+runFaultFinishedQuery, testScope, `["subject"]`)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "; ")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !strings.Contains(plan.String(), "SEARCH runs") || strings.Contains(plan.String(), "SCAN runs") {
			t.Fatalf("fault Finish IDs were not individually sought: %s", plan.String())
		}
		rec.statements, rec.bytes = nil, 0
		created := runFaultState{scope: testScope, created: []model.CreatedRun{{Run: model.Run{ID: "subject"}, Created: true}}}
		if err := created.probe(ctx, tx, true); err != nil || len(created.fact.Creates) != 1 || !created.fact.Creates[0].CurrentKnown || !created.fact.Creates[0].Present || created.fact.Creates[0].Current.Projection != model.RunIdentity || len(rec.statements) != 1 {
			t.Fatalf("exact created identity probe: %+v %v SQL=%v", created.fact, err, rec.statements)
		}
		for _, field := range []string{"inputs_json", "trigger_json", "nodes_json", "cleanup_errors_json"} {
			if strings.Contains(strings.ToLower(rec.statements[0]), field) {
				t.Fatalf("created identity probe selected frozen %s", field)
			}
		}
		createBytes[trial] = rec.bytes
		rec.statements, rec.bytes = nil, 0
		noop := runFaultState{scope: testScope, policy: state.policy}
		if err := noop.probe(ctx, tx, true); err != nil || len(rec.statements) != 0 || noop.fact.Retention != (model.RunRetentionEffect{}) {
			t.Fatalf("no-change probe manufactured work: %+v %v SQL=%v", noop.fact, err, rec.statements)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	if selectedBytes[0] != selectedBytes[1] {
		t.Fatalf("unrelated Runs changed selected driver bytes: %v", selectedBytes)
	}
	if createBytes[0] != createBytes[1] {
		t.Fatalf("unrelated Runs changed selected create identity bytes: %v", createBytes)
	}
	t.Logf("0/128 unrelated Runs: changed Finish two indexed reads %d/%d bytes; created identity one indexed read %d/%d bytes; no-op probe zero reads", selectedBytes[0], selectedBytes[1], createBytes[0], createBytes[1])
}

func TestRemoteReserveFaultProbeSeeksOnlyValidatedRelation(t *testing.T) {
	var selectedBytes [2]int
	for trial, unrelated := range []int{0, 128} {
		rec := &headerSQLRecorder{}
		db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "reserve-fault.db") + "?_pragma=foreign_keys(1)", rec: rec})
		db.SetMaxOpenConns(1)
		ctx := t.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = Initialize(ctx, tx); err != nil {
			t.Fatal(err)
		}
		var selected model.RemoteRunLink
		for i := 0; i <= unrelated; i++ {
			id := "owner"
			if i > 0 {
				id = fmt.Sprintf("unrelated-%03d", i)
			}
			q := model.NewRun{ID: id, TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}
			if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
				t.Fatal(err)
			}
			link := model.RemoteRunLink{OwnerRunID: id, RootRunID: id, TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "boot"}
			if changed, err := changeRemoteRun(ctx, tx, testScope, link, false, nil); err != nil || !changed {
				t.Fatalf("seed relation=%t %v", changed, err)
			}
			if i == 0 {
				selected = link
			}
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rec.statements, rec.bytes = nil, 0
		state := remoteReserveFaultState{scope: testScope, requested: selected, detail: remoteRunChangeDetail{reserveValidated: true, reserveAction: "existing", reserveBeforePresent: true, reserveBefore: selected}}
		if err = state.probe(ctx, tx, true); err != nil || !state.fact.CurrentKnown || !state.fact.Present || state.fact.Current != selected || len(rec.statements) != 1 {
			t.Fatalf("exact reserve fact=%+v %v SQL=%v", state.fact, err, rec.statements)
		}
		selectedBytes[trial] = rec.bytes
		rows, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+remoteReserveRevalidateQuery, testScope, selected.OwnerRunID, selected.TargetID, selected.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "; ")
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !strings.Contains(plan.String(), "SEARCH run_remote_pending") || strings.Contains(plan.String(), "SCAN run_remote_pending") {
			t.Fatalf("reserve fact missed its exact relation index: %s", plan.String())
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	if selectedBytes[0] != selectedBytes[1] {
		t.Fatalf("unrelated links changed reserve driver bytes: %v", selectedBytes)
	}
	t.Logf("0/128 unrelated links: one exact indexed reserve read and %d/%d selected driver bytes", selectedBytes[0], selectedBytes[1])
}
