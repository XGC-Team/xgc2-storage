package coredata

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSessionActivationUnsafeSubsetPlans(t *testing.T) {
	db, ctx := fixture(t)
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+sessionUnsafeActivation, testScope, "session")
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var a, b, c int
		var detail string
		if err = rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "; ")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, index := range []string{"runs_session_instance_open", "runs_session_pending", "run_remote_pending_session"} {
		if !strings.Contains(plan.String(), index) {
			t.Fatalf("missing %s: %s", index, plan.String())
		}
	}
	// Failed cleanup is already part of the predecessor Session read. Do not
	// repeat the same fact as a Run-index probe on each clean activation.
	if strings.Contains(plan.String(), "runs_session_failed") {
		t.Fatalf("repeated failed-cleanup read: %s", plan.String())
	}
	if strings.Contains(plan.String(), "SCAN runs") || strings.Contains(plan.String(), "SCAN run_remote_pending") {
		t.Fatalf("unsafe subset scans history: %s", plan.String())
	}
	t.Logf("actual activation unsafe plan: %s", plan.String())
}

func TestRetentionEffectZeroCountSkipsOldest(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "effect.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	q := model.NewRun{ID: "owner", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}
	if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: q.ID, RootRunID: q.ID, TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "boot"}
	if _, err = changeRemoteRun(ctx, tx, testScope, link, remoteReserve, nil); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rec.statements, rec.bytes = nil, 0
	var cut retentionChanges
	if _, err = finishRunTransition(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunSucceeded}, false, &cut, "", map[string]struct{}{q.ID: {}}); err != nil {
		t.Fatal(err)
	}
	if err = cut.finish(ctx, tx, testScope); err != nil {
		t.Fatal(err)
	}
	effect, err := retentionEffect(ctx, tx, testScope, &cut, model.RunRetentionPolicy{OlderThan: time.Now().Add(time.Hour), KeepNewest: 10})
	if err != nil || !effect.ChangedFinish || effect.EligibilityReleased || effect.HasVictim {
		t.Fatalf("effect=%+v %v", effect, err)
	}
	if len(rec.statements) != 3 || !strings.Contains(rec.statements[2], "retention_eligible_count") {
		t.Fatalf("unexpected queries: %q", rec.statements)
	}
	for _, q := range rec.statements {
		if q == pruneOldest {
			t.Fatalf("empty eligible count still sought oldest: %q", rec.statements)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	t.Logf("changed protected finish policy work: selected queries=%d driver bytes=%d, oldest seeks=0", len(rec.statements), rec.bytes)
}
