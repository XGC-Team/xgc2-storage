package coredata

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRemoteResolutionUsesOnlyCausalIndexedFacts(t *testing.T) {
	var bytesByPopulation [2][3]int
	for trial, unrelated := range []int{0, 128} {
		rec := &headerSQLRecorder{}
		db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "resolution.db") + "?_pragma=foreign_keys(1)", rec: rec})
		db.SetMaxOpenConns(1)
		ctx := t.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = Initialize(ctx, tx); err != nil {
			t.Fatal(err)
		}
		create := func(id, parent, root string, depth int) {
			t.Helper()
			q := model.NewRun{ID: id, TargetID: "core", ParentRunID: parent, RootRunID: root, Depth: depth, WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}
			if parent != "" {
				q.CallNodeID = "call"
			}
			if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
				t.Fatal(err)
			}
		}
		add := func(link model.RemoteRunLink) {
			t.Helper()
			if _, err := changeRemoteRun(ctx, tx, testScope, link, false, nil); err != nil {
				t.Fatal(err)
			}
		}
		create("root", "", "", 0)
		create("owner-a", "root", "root", 1)
		create("owner-b", "root", "root", 1)
		links := []model.RemoteRunLink{
			{OwnerRunID: "owner-a", RootRunID: "root", TargetID: "agent", RunID: "child-1", CallNodeID: "call", BootID: "boot"},
			{OwnerRunID: "owner-a", RootRunID: "root", TargetID: "agent", RunID: "child-2", CallNodeID: "call", BootID: "boot"},
			{OwnerRunID: "owner-b", RootRunID: "root", TargetID: "agent", RunID: "child-3", CallNodeID: "call", BootID: "boot"},
		}
		for _, link := range links {
			add(link)
		}
		for i := 0; i < unrelated; i++ {
			id := fmt.Sprintf("unrelated-%03d", i)
			create(id, "", "", 0)
			add(model.RemoteRunLink{OwnerRunID: id, RootRunID: id, TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"})
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for i, link := range links {
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			rec.statements, rec.bytes = nil, 0
			out, err := resolveRemoteRun(ctx, tx, testScope, link)
			if err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			wantQueries := 3
			if i == 0 {
				wantQueries = 2
			}
			if len(rec.statements) != wantQueries || !out.Changed || out.LastForRoot != (i == 2) {
				t.Fatalf("release %d queries=%d result=%+v statements=%v", i, len(rec.statements), out, rec.statements)
			}
			for _, query := range rec.statements {
				for _, forbidden := range []string{"inputs_json", "trigger_json", "result_json", "nodes_json", "cleanup_errors_json"} {
					if strings.Contains(strings.ToLower(query), forbidden) {
						t.Fatalf("resolution selected %s: %s", forbidden, query)
					}
				}
			}
			if i > 0 && rec.statements[2] != remoteRunLastResolutionQuery {
				t.Fatalf("last-owner check changed: %s", rec.statements[2])
			}
			bytesByPopulation[trial][i] = rec.bytes
		}
		plans, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+remoteRunLastResolutionQuery, testScope, "root", "root", "owner-b", testScope, "owner-b", testScope, "root")
		if err != nil {
			t.Fatal(err)
		}
		var details strings.Builder
		for plans.Next() {
			var a, b, c int
			var detail string
			if err := plans.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			details.WriteString(detail + "; ")
		}
		if err := plans.Err(); err != nil {
			t.Fatal(err)
		}
		plans.Close()
		plan := details.String()
		if !strings.Contains(plan, "SEARCH run_remote_pending USING COVERING INDEX run_remote_pending_root") || !strings.Contains(plan, "SEARCH runs USING INDEX") || strings.Contains(plan, "SCAN run_remote_pending") || strings.Contains(plan, "SCAN runs") || strings.Contains(plan, "TEMP B-TREE") {
			t.Fatalf("root and finish lookups lack their indexes: %s", plan)
		}
		db.Close()
	}
	if bytesByPopulation[0] != bytesByPopulation[1] {
		t.Fatalf("unrelated roots changed driver bytes: %v", bytesByPopulation)
	}
	t.Logf("same-owner nonfinal / other-owner nonfinal / final driver bytes (0/128 unrelated roots): %v; query counts 2/3/3", bytesByPopulation)
}

func TestRemoteResolutionRollbackRestoresLinkAndQuota(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "rollback.db") + "?_pragma=foreign_keys(1)", rec: rec})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "root", TargetID: "core", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "root", RootRunID: "root", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if _, err = changeRemoteRun(ctx, tx, testScope, link, false, nil); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var beforeRows, beforeBytes int64
	if err = db.QueryRowContext(ctx, `SELECT rows,bytes FROM core_data_usage WHERE scope=?`, testScope).Scan(&beforeRows, &beforeBytes); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := resolveRemoteRun(ctx, tx, testScope, link)
	if err != nil || !out.Changed || !out.LastForRoot {
		t.Fatalf("in-transaction result=%+v %v", out, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var rows, bytes, links int64
	if err = db.QueryRowContext(ctx, `SELECT rows,bytes FROM core_data_usage WHERE scope=?`, testScope).Scan(&rows, &bytes); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM run_remote_pending WHERE scope=? AND root_run_id=?`, testScope, "root").Scan(&links); err != nil {
		t.Fatal(err)
	}
	if rows != beforeRows || bytes != beforeBytes || links != 1 {
		t.Fatalf("rollback changed authority: rows=%d/%d bytes=%d/%d links=%d", rows, beforeRows, bytes, beforeBytes, links)
	}
}

func TestRemoteResolutionSameRootOwnerDoesNotReadFinishTwice(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "same-root.db") + "?_pragma=foreign_keys(1)", rec: rec})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	for _, q := range []model.NewRun{
		{ID: "root", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)},
		{ID: "owner", ParentRunID: "root", RootRunID: "root", Depth: 1, CallNodeID: "call", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)},
	} {
		if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"root", "owner"} {
		rec.statements, rec.bytes = nil, 0
		var rootAt, ownerAt sql.NullInt64
		err = db.QueryRowContext(ctx, remoteRunLastResolutionQuery, testScope, "root", "root", owner, testScope, owner, testScope, "root").Scan(&rootAt, &ownerAt)
		wantBytes := 16
		if owner == "root" {
			wantBytes = 8
		}
		if err != nil || !rootAt.Valid || ownerAt.Valid != (owner != "root") || rec.bytes != wantBytes || len(rec.statements) != 1 {
			t.Fatalf("owner=%s root=%+v owner=%+v bytes=%d statements=%d err=%v", owner, rootAt, ownerAt, rec.bytes, len(rec.statements), err)
		}
	}
}
