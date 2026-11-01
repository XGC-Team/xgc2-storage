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

func TestRunNativeQuerySelectsActionTerminalOverlapWithoutPayloads(t *testing.T) {
	recorder := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "native.db") + "?_pragma=foreign_keys(1)", rec: recorder})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	epoch := time.Unix(1000, 0)
	for i := 0; i < 25; i++ {
		q := model.NewRun{ID: fmt.Sprintf("record-%02d", i), TargetID: "local", SessionID: "session", WorkflowResourceID: "recording", WorkflowCommitID: "commit", DefinitionDigest: "digest", Trigger: []byte(`{}`), ActionID: "record-session", Inputs: []byte(`{"input":{"typed":9007199254740993}}`), At: epoch.Add(time.Duration(i) * time.Second)}
		if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if _, err = finishRun(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: q.At.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 64; i++ {
		q := model.NewRun{ID: fmt.Sprintf("wrong-%03d", i), TargetID: "local", SessionID: "session", WorkflowResourceID: "recording", WorkflowCommitID: "commit", DefinitionDigest: "digest", Trigger: []byte(`{}`), ActionID: "record-member", Inputs: []byte(`{"large":"` + strings.Repeat("x", 8192) + `"}`), At: epoch.Add(time.Duration(i+100) * time.Second)}
		if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if _, err = finishRun(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: q.At.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runs SET nodes_json=x'00' WHERE scope=? AND id=?", testScope, "record-24"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	recorder.statements = nil
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	filter := model.RunFilter{SessionID: "session", WorkflowResourceID: "recording", ActionID: "record-session", TerminalOnly: true, Projection: model.RunObservation, Limit: 20}
	page, err := listRuns(ctx, tx, testScope, filter)
	if err != nil || len(page.Runs) != 20 || page.Runs[0].ID != "record-24" || page.Next == "" {
		t.Fatal(page, err)
	}
	query := strings.ToLower(recorder.statements[len(recorder.statements)-1])
	for _, column := range []string{"inputs_json", "trigger_json", "nodes_json", "cleanup_errors_json", "result_json,"} {
		if strings.Contains(query, column) {
			t.Fatal("native summary query selected execution payload", column, query)
		}
	}
	filter.OverlapSince = epoch.Add(10 * time.Second)
	filter.OverlapUntil = epoch.Add(11 * time.Second)
	filter.Limit = 2
	page, err = listRuns(ctx, tx, testScope, filter)
	if err != nil || len(page.Runs) != 1 || page.Runs[0].ID != "record-10" {
		t.Fatal("half-open overlap included adjacent interval", page, err)
	}
	exact, err := getRunExecutionInput(ctx, tx, testScope, "record-24")
	if err != nil || exact.Projection != model.RunExecutionInput || !json.Valid(exact.Inputs) || !strings.Contains(string(exact.Inputs), "9007199254740993") || len(exact.Nodes)+len(exact.Trigger)+len(exact.Result)+len(exact.CleanupErrors) != 0 {
		t.Fatal("input query was not exact/thin/lossless", exact, err)
	}

	if _, err = getRunExecutionInput(ctx, tx, testScope, "record-24"); err != nil {
		t.Fatal("thin input read touched unrelated corrupt node detail", err)
	}
	if _, err := getRun(ctx, tx, testScope, "record-24"); err == nil {
		t.Fatal("corrupt full node detail fixture was not effective")
	}
}
