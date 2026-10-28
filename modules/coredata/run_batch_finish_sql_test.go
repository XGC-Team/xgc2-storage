package coredata

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The batch and direct paths use the same SQLite transaction and finish
// helper. Count actual driver-returned values, including every SELECT.
func TestBatchFinishDriverReadsOnlyStatusOnChangedRun(t *testing.T) {
	rec := &headerSQLRecorder{}
	dsn := "file:" + filepath.Join(t.TempDir(), "finish.db") + "?_pragma=foreign_keys(1)"
	db := sql.OpenDB(headerCountingConnector{dsn: dsn, rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 256<<10)
	if _, err := io.ReadFull(rand.New(rand.NewSource(1)), data); err != nil {
		t.Fatal(err)
	}
	large := json.RawMessage(`{"data":"` + base64.StdEncoding.EncodeToString(data) + `"}`)
	for _, test := range []struct {
		id     string
		inputs json.RawMessage
	}{{"small", json.RawMessage(`{}`)}, {"large", large}, {"direct", large}} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		q := model.NewRun{ID: test.id, TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: test.inputs, Trigger: json.RawMessage(`{}`)}
		if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	selected := make([]int, 0, 2)
	for _, id := range []string{"small", "large"} {
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = finishRunWithDetail(ctx, tx, testScope, model.RunFinish{ID: id, Status: model.RunSucceeded}, false); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if len(rec.statements) != 1 || rec.statements[0] != "SELECT status FROM runs WHERE scope=? AND id=?" {
			t.Fatalf("changed batch selected detail: %q", rec.statements)
		}
		selected = append(selected, rec.bytes)
	}
	if selected[0] != len(model.RunQueued) || selected[1] != selected[0] {
		t.Fatalf("selected driver bytes small/large=%v", selected)
	}
	// The direct return and an exact replay actually need the stored detail.
	for _, id := range []string{"direct", "large"} {
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		finish := model.RunFinish{ID: id, Status: model.RunSucceeded}
		if id == "direct" {
			_, err = finishRun(ctx, tx, testScope, finish)
		} else {
			_, err = finishRunWithDetail(ctx, tx, testScope, finish, false)
		}
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if len(rec.statements) != 2 || !strings.Contains(rec.statements[1], "inputs_json") || rec.bytes < 200<<10 {
			t.Fatalf("%s did not read required detail: statements=%q bytes=%d", id, rec.statements, rec.bytes)
		}
	}
	t.Logf("changed finish selected driver bytes with small/large existing payloads: %d/%d; direct return and replay each select full detail", selected[0], selected[1])
}
