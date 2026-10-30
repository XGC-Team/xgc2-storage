package coredata

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestPendingRemoteTargetSQLReadsOnlySelectedRelations(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "pending.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	makeOwner := func(id string) {
		t.Helper()
		_, _, err := createRun(ctx, tx, testScope, model.NewRun{ID: id, TargetID: "core", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
	add := func(owner, target, child string) {
		t.Helper()
		link := model.RemoteRunLink{OwnerRunID: owner, RootRunID: owner, TargetID: target, RunID: child, CallNodeID: "call", BootID: "boot"}
		if _, err := changeRemoteRun(ctx, tx, testScope, link, remoteReserve, nil); err != nil {
			t.Fatal(err)
		}
	}
	makeOwner("mixed-owner")
	add("mixed-owner", "selected", "child-1")
	add("mixed-owner", "selected", "child-2")
	for i := 0; i < 128; i++ {
		add("mixed-owner", fmt.Sprintf("mixed-other-%03d", i), fmt.Sprintf("child-%03d", i))
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	read := func(target string) (model.RemoteRunPage, int, string) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		page, err := pendingRemotePage(ctx, tx, testScope, target, "", 1, remoteRunCursor{})
		tx.Rollback()
		if err != nil || len(rec.statements) != 1 {
			t.Fatalf("target=%q statements=%d page=%+v err=%v", target, len(rec.statements), page, err)
		}
		query := rec.statements[0]
		for _, forbidden := range []string{"pending_remote_json", "inputs_json", "trigger_json", "nodes_json", "result_json"} {
			if strings.Contains(strings.ToLower(query), forbidden) {
				t.Fatalf("selected target materialized %s", forbidden)
			}
		}
		return page, rec.bytes, query
	}
	first, baseline, query := read("selected")
	if len(first.Links) != 1 || first.Links[0].RunID != "child-1" || first.Next == "" || baseline > 1024 {
		t.Fatalf("first=%+v driver bytes=%d", first, baseline)
	}
	empty, emptyBytes, _ := read("absent")
	if len(empty.Links) != 0 || empty.Next != "" || emptyBytes != 0 {
		t.Fatalf("empty=%+v bytes=%d", empty, emptyBytes)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 512; i++ {
		owner := fmt.Sprintf("other-owner-%04d", i)
		makeOwner(owner)
		for j := 0; j < 4; j++ {
			add(owner, fmt.Sprintf("other-target-%04d-%d", i, j), fmt.Sprintf("other-child-%d", j))
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	second, after, _ := read("selected")
	if after != baseline || len(second.Links) != 1 || second.Links[0] != first.Links[0] {
		t.Fatalf("unrelated rows changed selected read: %d/%d %+v", baseline, after, second)
	}
	empty, emptyBytes, _ = read("absent")
	if len(empty.Links) != 0 || empty.Next != "" || emptyBytes != 0 {
		t.Fatalf("unrelated rows changed empty read: %+v bytes=%d", empty, emptyBytes)
	}
	plans, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, testScope, "selected", "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	var details strings.Builder
	for plans.Next() {
		var a, b, c int
		var detail string
		if err = plans.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		details.WriteString(detail + "; ")
	}
	plans.Close()
	plan := details.String()
	if !strings.Contains(plan, "run_remote_pending_target") || !strings.Contains(plan, "SEARCH p") || !strings.Contains(plan, "SEARCH r") || strings.Contains(plan, "SCAN p") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("target lookup is not bounded by its index: %s", plan)
	}
	for _, projection := range []model.RunProjection{model.RunIdentity, model.RunObservation, model.RunComplete} {
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if projection == model.RunComplete {
			_, err = getRun(ctx, tx, testScope, "mixed-owner")
		} else {
			_, err = listRuns(ctx, tx, testScope, model.RunFilter{ID: "mixed-owner", Projection: projection, Limit: 1})
		}
		tx.Rollback()
		if err != nil || len(rec.statements) != 1 || strings.Contains(strings.ToLower(rec.statements[0]), "run_remote_pending") {
			t.Fatalf("Run %s materialized remote relations: statements=%v err=%v", projection, rec.statements, err)
		}
	}
	t.Logf("selected driver bytes 1/N targets=%d/%d; mixed owner has 128 other targets; empty target uses one query and %d bytes; plan: %s", baseline, after, emptyBytes, plan)
}
