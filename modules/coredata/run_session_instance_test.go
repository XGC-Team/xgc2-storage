package coredata

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSessionActionInstanceIndexSelectsOnlyAcceptedOpenInstance(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "sessions.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	create := func(id, session, trigger, source string, terminal bool) {
		t.Helper()
		q := model.NewRun{ID: id, TargetID: "local", SessionID: session, WorkflowResourceID: "same-workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{"large":"` + strings.Repeat("x", 8192) + `"}`), Trigger: []byte(trigger), SourceObservation: []byte(source), At: time.Unix(123, 0)}
		if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if terminal {
			if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: id, Status: model.RunSucceeded}); err != nil {
				t.Fatal(err)
			}
		}
	}
	selectedTrigger := `{"kind":"session-action","instanceId":"selected","actionId":"run"}`
	selectedSource := `{"invocation":{"instanceId":"selected","actionId":"run"}}`
	create("selected-1", "session", selectedTrigger, selectedSource, false)
	create("selected-2", "session", selectedTrigger, selectedSource, false)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	filter := model.RunFilter{TargetID: "local", SessionID: "session", SessionInstanceID: "selected", Statuses: []model.RunStatus{model.RunQueued, model.RunRunning, model.RunStopping}, Projection: model.RunObservation, Limit: 1}
	read := func(f model.RunFilter) (model.RunPage, int, string) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		page, err := listRuns(ctx, tx, testScope, f)
		tx.Rollback()
		if err != nil || len(rec.statements) != 1 {
			t.Fatalf("page=%+v reads=%d err=%v", page, len(rec.statements), err)
		}
		query := strings.ToLower(rec.statements[0])
		for _, forbidden := range []string{"inputs_json", "trigger_json", "nodes_json", "run_remote_pending"} {
			if strings.Contains(query, forbidden) {
				t.Fatalf("observation selected %s: %s", forbidden, query)
			}
		}
		return page, rec.bytes, query
	}
	first, before, _ := read(filter)
	if len(first.Runs) != 1 || first.Runs[0].ID != "selected-2" || first.Next == "" {
		t.Fatalf("selected first page: %+v", first)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 512 {
		create(fmt.Sprintf("other-instance-%04d", i), "session", fmt.Sprintf(`{"kind":"session-action","instanceId":"other-%04d"}`, i), selectedSource, false)
		create(fmt.Sprintf("other-session-%04d", i), fmt.Sprintf("session-%04d", i), selectedTrigger, selectedSource, false)
		create(fmt.Sprintf("closed-selected-%04d", i), "session", selectedTrigger, selectedSource, true)
	}
	// A standalone Experiment and an unrelated trigger do not become Session
	// Actions merely because the observation claims that instance.
	create("standalone", "session", `{"kind":"experiment-action","instanceId":"selected"}`, `{"experimentInvocation":{"experimentId":"e","workflowInstanceId":"selected"}}`, false)
	create("forged-source", "session", `{"kind":"session-action","instanceId":"actual"}`, selectedSource, false)
	create("foreign-target", "session", selectedTrigger, selectedSource, false)
	if _, err := tx.ExecContext(ctx, "UPDATE runs SET target_id='agent' WHERE id='foreign-target'"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	afterPage, after, _ := read(filter)
	if after != before || len(afterPage.Runs) != 1 || afterPage.Runs[0].ID != first.Runs[0].ID {
		t.Fatalf("unrelated Session/instance/history changed selected read: bytes %d/%d page=%+v", before, after, afterPage)
	}
	filter.Cursor = afterPage.Next
	second, _, _ := read(filter)
	if len(second.Runs) != 1 || second.Runs[0].ID != "selected-1" || second.Next != "" {
		t.Fatalf("selected next page: %+v", second)
	}
	filter.Cursor = ""
	filter.SessionInstanceID = "missing"
	empty, emptyBytes, _ := read(filter)
	if len(empty.Runs) != 0 || empty.Next != "" || emptyBytes != 0 {
		t.Fatalf("missing instance read other rows: %+v bytes=%d", empty, emptyBytes)
	}
	filter.SessionInstanceID = "selected"
	for _, tc := range []struct {
		name   string
		filter model.RunFilter
		index  string
	}{
		{"open", filter, "runs_session_instance_open"},
		{"history", model.RunFilter{SessionID: "session", SessionInstanceID: "selected", Projection: model.RunObservation}, "runs_session_instance"},
	} {
		query, args, _, err := runQuery(testScope, tc.filter)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var a, b, c int
			var detail string
			if err := rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "; ")
		}
		rows.Close()
		if !strings.Contains(plan.String(), tc.index+" (") || !strings.Contains(plan.String(), "session_instance_id=?") || strings.Contains(plan.String(), "SCAN runs") || strings.Contains(plan.String(), "TEMP B-TREE") {
			t.Fatalf("%s selector is not exact and ordered: %s", tc.name, plan.String())
		}
		t.Logf("%s plan: %s", tc.name, plan.String())
	}
	t.Logf("selected observation driver bytes before/after unrelated Session, instance and same-instance history=%d/%d; empty instance=%d bytes", before, after, emptyBytes)
}

func TestSessionActionTriggerDerivationMatchesCanonicalShape(t *testing.T) {
	for _, tc := range []struct {
		trigger string
		want    string
	}{
		{`{"kind":"session-action","instanceId":"first","instanceId":"last","extra":true}`, "last"},
		{`{"kind":"session-action","instanceId":"selected","presetId":7}`, ""},
		{`{"kind":"session-action","instanceId":7}`, ""},
		{`{"kind":"experiment-action","instanceId":"selected"}`, ""},
		{`{"invocation":{"instanceId":"selected"}}`, ""},
	} {
		got, err := sessionInstanceFromTrigger("session", []byte(tc.trigger))
		if err != nil || got != tc.want {
			t.Fatalf("trigger %s: got %q, err %v; want %q", tc.trigger, got, err, tc.want)
		}
	}
	// The SDK's source is only a projection; it cannot override the trigger.
	_, err := sessionInstanceFromTrigger("session", json.RawMessage(`{"kind":"session-action","instanceId":" bad "}`))
	if err == nil {
		t.Fatal("invalid indexed identifier silently admitted")
	}
	if got, err := sessionInstanceFromTrigger("", []byte(`{"kind":"session-action","instanceId":"selected"}`)); err != nil || got != "" {
		t.Fatalf("unowned trigger became a Session selector: %q %v", got, err)
	}
}
