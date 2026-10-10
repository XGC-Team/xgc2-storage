package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	_ "modernc.org/sqlite"
)

const testScope = `{"namespace":"core","user":"operator","workspace":"station"}`

func fixture(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "core.db")+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}
func execSQL(t *testing.T, db *sql.DB, ctx context.Context, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		t.Fatal(err)
	}
}
func run(t *testing.T, db *sql.DB, ctx context.Context, operation string, request any) (json.RawMessage, error) {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	result, err := Execute(ctx, tx, testScope, operation, raw)
	// Intentionally commit even on operation error. Its savepoint must prevent
	// partial data independently of the required outer-owner rollback.
	if e := tx.Commit(); e != nil {
		t.Fatal(e)
	}
	return result, err
}
func count(t *testing.T, db *sql.DB, ctx context.Context, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func errorCode(err error) string {
	var e *api.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// seedTree stores a two-level robot namespace tree with one resource per level.
func seedTree(t *testing.T, db *sql.DB, ctx context.Context) {
	t.Helper()
	for _, n := range []struct{ id, parent, name string }{{"source", "", "source"}, {"child", "source", "child"}, {"other", "", "other"}} {
		var parent any
		if n.parent != "" {
			parent = n.parent
		}
		execSQL(t, db, ctx, "INSERT INTO core_namespaces VALUES(?,?,?,?,?,?,3,0,'{}')", testScope, "robot", n.id, parent, n.name, n.name)
	}
	for i, namespace := range []string{"source", "child"} {
		id, commit, branch := fmt.Sprintf("resource-%d", i), fmt.Sprintf("commit-%d", i), fmt.Sprintf("branch-%d", i)
		payload, manifest := json.RawMessage(`{"typed_name":"Source","kind":"robot","private":"preserved"}`), json.RawMessage(`{"root_digest":"root","nodes":[{"id":"root"}]}`)
		refs := []model.Reference{{Slot: "configuration", TargetDomain: "robot", TargetResourceID: "reference-target", TargetCommitID: "reference-pin", Body: json.RawMessage(`{"mode":"pinned","component":"action","schema":2}`)}}
		pin, err := model.SnapshotDigest(payload, manifest, refs)
		if err != nil {
			t.Fatal(err)
		}
		execSQL(t, db, ctx, "INSERT INTO core_resources VALUES(?,?,?,?,?,?,5,?,0,'','', '{}')", testScope, "robot", id, namespace, id, id, commit)
		execSQL(t, db, ctx, "INSERT INTO core_branches VALUES(?,?,?,?,'main',?,4,'{}')", testScope, "robot", branch, id, commit)
		execSQL(t, db, ctx, "INSERT INTO core_snapshots VALUES(?,?,?,?,?,7,'',?,?,?,?)", testScope, "robot", commit, id, branch, pin, []byte(payload), []byte(manifest), []byte(`{"actor":"original"}`))
		for _, f := range refs {
			execSQL(t, db, ctx, "INSERT INTO core_references VALUES(?,?,?,?,?,?,?,?)", testScope, "robot", commit, f.Slot, f.TargetDomain, f.TargetResourceID, f.TargetCommitID, []byte(f.Body))
		}
	}
}

func TestNamespaceSnapshotReturnsCurrentMainTreeAndVerifiesDigests(t *testing.T) {
	db, ctx := fixture(t)
	seedTree(t, db, ctx)
	raw, err := run(t, db, ctx, model.NamespaceSnapshotOperation, model.NamespaceRead{Domain: "robot", ID: "source"})
	if err != nil {
		t.Fatal(err)
	}
	var tree model.NamespaceTree
	if err = json.Unmarshal(raw, &tree); err != nil || len(tree.Namespaces) != 2 || len(tree.Resources) != 2 || len(tree.Resources[0].References) != 1 || !strings.Contains(string(tree.Resources[0].Payload), "private") {
		t.Fatalf("snapshot=%s error=%v", raw, err)
	}
	// A changed frozen payload no longer matches its immutable content digest.
	execSQL(t, db, ctx, "UPDATE core_snapshots SET payload=? WHERE id='commit-0'", []byte(`{"typed_name":"Tampered"}`))
	if _, err = run(t, db, ctx, model.NamespaceSnapshotOperation, model.NamespaceRead{Domain: "robot", ID: "source"}); errorCode(err) != "failed_precondition" {
		t.Fatalf("tampered content accepted: %v", err)
	}
}

func TestRelationalIndexesAndScopeIsolation(t *testing.T) {
	db, ctx := fixture(t)
	seedTree(t, db, ctx)
	for _, q := range []string{
		"SELECT id FROM core_namespaces WHERE scope='s' AND domain='d' AND parent_id='p' AND archived=0",
		"SELECT id FROM core_resources WHERE scope='s' AND domain='d' AND namespace_id='n' AND archived=0",
		"SELECT commit_id FROM core_references WHERE scope='s' AND target_domain='d' AND target_resource_id='r' AND target_commit_id='c'",
	} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q)
		if err != nil {
			t.Fatal(err)
		}
		var plans string
		for rows.Next() {
			var a, b, c int
			var detail string
			if err = rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plans += detail
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !strings.Contains(plans, "INDEX") || strings.Contains(plans, "SCAN ") {
			t.Fatalf("unindexed access %s: %s", q, plans)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	raw, _ := json.Marshal(model.NamespaceRead{Domain: "robot", ID: "source"})
	if _, err = Execute(ctx, tx, `{"namespace":"core","user":"other","workspace":"station"}`, model.NamespaceSnapshotOperation, raw); errorCode(err) != "not_found" {
		t.Fatalf("scope leaked source: %v", err)
	}
}

func TestNamespaceSnapshotBoundsTheSubtree(t *testing.T) {
	db, ctx := fixture(t)
	seedTree(t, db, ctx)
	execSQL(t, db, ctx, `WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i<1024)
 INSERT INTO core_namespaces SELECT ?,'robot','extra-'||i,'other','extra-'||i,'extra-'||i,1,0,'{}' FROM seq`, testScope)
	if _, err := run(t, db, ctx, model.NamespaceSnapshotOperation, model.NamespaceRead{Domain: "robot", ID: "other"}); errorCode(err) != "resource_exhausted" {
		t.Fatalf("subtree bound=%v", err)
	}
}
