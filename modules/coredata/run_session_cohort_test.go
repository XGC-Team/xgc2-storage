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

func TestSessionActionCohortUsesOneSelectedPage(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "cohort.db") + "?_pragma=foreign_keys(1)", rec: rec})
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
	create := func(id, session, instance, action string, terminal bool) {
		t.Helper()
		q := model.NewRun{ID: id, TargetID: "local", SessionID: session, WorkflowResourceID: "shared-workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: action,
			Inputs: []byte(`{"large":"` + strings.Repeat("x", 8192) + `"}`), Trigger: []byte(fmt.Sprintf(`{"kind":"session-action","instanceId":%q,"actionId":%q}`, instance, action)), At: time.Unix(123, 0)}
		if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if terminal {
			if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: id, Status: model.RunSucceeded}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, x := range []struct{ id, instance, action string }{{"00-a-run", "a", "run"}, {"01-a-inspect", "a", "inspect"}, {"02-b-run", "b", "run"}, {"03-b-inspect", "b", "inspect"}} {
		create(x.id, "session", x.instance, x.action, false)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f := model.RunFilter{TargetID: "local", SessionID: "session", SessionInstanceIDs: []string{"a", "b", "a"}, Statuses: []model.RunStatus{model.RunQueued, model.RunRunning, model.RunStopping}, Projection: model.RunObservation, Limit: 2}
	read := func(f model.RunFilter) (model.RunPage, int) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		page, err := listRuns(ctx, tx, testScope, f)
		tx.Rollback()
		if err != nil || len(rec.statements) != 1 {
			t.Fatalf("page=%+v queries=%d error=%v", page, len(rec.statements), err)
		}
		query := strings.ToLower(rec.statements[0])
		if !strings.Contains(query, "session_instance_id in (") && len(f.SessionInstanceIDs) > 1 {
			t.Fatalf("not one selected SQL set: %s", query)
		}
		for _, wide := range []string{"inputs_json", "trigger_json", "nodes_json", "run_remote_pending"} {
			if strings.Contains(query, wide) {
				t.Fatalf("cohort materialized discarded %s", wide)
			}
		}
		return page, rec.bytes
	}
	first, before := read(f)
	if len(first.Runs) != 2 || first.Runs[0].ID != "03-b-inspect" || first.Runs[1].ID != "02-b-run" || first.Next == "" {
		t.Fatalf("cohort first page/auxiliary Actions: %+v", first)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 128 {
		create(fmt.Sprintf("noise-instance-%03d", i), "session", "other", "run", false)
		create(fmt.Sprintf("noise-session-%03d", i), "other-session", "a", "run", false)
		create(fmt.Sprintf("noise-history-%03d", i), "session", "a", "run", true)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after, bytes := read(f)
	if bytes != before || after.Runs[0].ID != first.Runs[0].ID {
		t.Fatalf("unselected history/instances changed cohort work: %d/%d", before, bytes)
	}
	f.Cursor = after.Next
	second, _ := read(f)
	if len(second.Runs) != 2 || second.Runs[0].ID != "01-a-inspect" || second.Runs[1].ID != "00-a-run" || second.Next != "" {
		t.Fatalf("cohort pagination/duplicate selection: %+v", second)
	}
	f.Cursor = ""
	f.SessionInstanceIDs = []string{}
	empty, _ := read(f)
	if len(empty.Runs) != 0 || empty.Next != "" {
		t.Fatalf("explicit empty broadened: %+v", empty)
	}
	for _, bad := range []model.RunFilter{{SessionInstanceIDs: []string{"a"}}, {SessionID: "session", SessionInstanceIDs: []string{"a", ""}}, {SessionID: "session", SessionInstanceIDs: []string{" bad "}}} {
		if _, _, _, err := runQuery(testScope, bad); err == nil {
			t.Fatalf("invalid cohort admitted: %+v", bad)
		}
	}
	t.Logf("one SELECT per cohort page; selected observation driver bytes before/after unrelated instances/sessions/history=%d/%d", before, bytes)
}
