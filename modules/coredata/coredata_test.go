package coredata

import (
	"context"
	"database/sql"

	"encoding/json"
	"errors"
	"fmt"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
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
	// This is the engine's existing document table, used by exact data guards.
	if _, err = tx.ExecContext(ctx, `CREATE TABLE records(scope TEXT,collection TEXT,key TEXT,version INTEGER,deleted INTEGER,data BLOB,PRIMARY KEY(scope,collection,key))`); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct{ collection, key string }{{"runs", "parent"}, {"invocations", "invocation"}, {"definitions", "pin"}} {
		body := []byte(`{}`)
		parent, producer := preparationStates()
		if v.collection == "runs" {
			body, err = json.Marshal(parent)
		} else if v.collection == "invocations" {
			body, err = json.Marshal(producer)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,1,0,?)", testScope, v.collection, v.key, body); err != nil {
			t.Fatal(err)
		}
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
func group(n int) model.GroupPrepare {
	r := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "fan-out", ParentGuard: model.RecordGuard{"runs", "parent", "1"}, InvocationGuard: model.RecordGuard{"invocations", "invocation", "1"}, PinGuard: model.RecordGuard{"definitions", "pin", "1"}, Body: json.RawMessage(`{"complete_pin":{"config":"c","execution_plan":"e","registry":"r","definition":"d"},"policy":{"max_concurrency":4,"remaining":"retain"}}`)}
	parent, producer := preparationStates()
	r.Condition = model.GroupPrepareCondition{Parent: parent, Producer: producer, Ancestors: []model.RecordGuard{r.ParentGuard}}
	for i := 0; i < n; i++ {
		r.Members = append(r.Members, model.GroupMember{ItemKey: fmt.Sprintf("item-%d", i), ChildID: fmt.Sprintf("child-%d", i), EventID: fmt.Sprintf("event-%d", i), Parameters: json.RawMessage(`{"value":1}`), Event: json.RawMessage(`{"trigger_kind":"rpc","target_version":3}`), Link: json.RawMessage(`{"definition_version":3,"owner_run_id":"parent","relation":"attached","cancel_policy":"cascade"}`), Body: json.RawMessage(`{"state":"queued","revision":1}`)})
	}
	return r
}

func preparationStates() (model.RunPrepareState, model.ProducerPrepareState) {
	return model.RunPrepareState{ID: "parent", TargetID: "local", RootRunID: "parent", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"}, model.ProducerPrepareState{ID: "invocation", RunID: "parent", NodeID: "call", Kind: "automation-call", Status: "running", ChildRunProducer: true}
}

func TestGroupPrepare86And1000AtomicIndexedAndReplay(t *testing.T) {
	for _, n := range []int{0, 86, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db, ctx := fixture(t)
			r := group(n)
			first, err := run(t, db, ctx, "group.prepare", r)
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"core_group_members", "core_group_events", "core_child_links"} {
				if got := count(t, db, ctx, table); got != n {
					t.Fatalf("%s=%d, want %d", table, got, n)
				}
			}
			var sealed int
			var body string
			if err = db.QueryRowContext(ctx, "SELECT sealed,body FROM core_groups").Scan(&sealed, &body); err != nil || sealed != 1 || body != string(r.Body) {
				t.Fatalf("sealed/body %d %s %v", sealed, body, err)
			}
			// Simulate a later parent lifecycle update: durable replay is the
			// original preparation, not a second admission or new dispatch.
			execSQL(t, db, ctx, "UPDATE records SET version=2 WHERE collection='runs'")
			again, err := run(t, db, ctx, "group.prepare", r)
			if err != nil || string(first) != string(again) {
				t.Fatalf("replay=%s first=%s error=%v", again, first, err)
			}
			r.Members = append(r.Members, model.GroupMember{ItemKey: "other", ChildID: "other", EventID: "other", Parameters: json.RawMessage(`{}`), Event: json.RawMessage(`{}`), Link: json.RawMessage(`{}`), Body: json.RawMessage(`{}`)})
			if _, err = run(t, db, ctx, "group.prepare", r); err == nil {
				t.Fatal("changed immutable group accepted")
			}
		})
	}
}

func TestGroupPrepareFinalMemberFailureLeavesNoFactsOrQuota(t *testing.T) {
	db, ctx := fixture(t)
	execSQL(t, db, ctx, `CREATE TRIGGER fail_final BEFORE INSERT ON core_group_members WHEN NEW.ordinal=999 BEGIN SELECT RAISE(ABORT,'final member fault'); END`)
	if _, err := run(t, db, ctx, "group.prepare", group(1000)); err == nil || !strings.Contains(err.Error(), "final member fault") {
		t.Fatalf("missing late injection: %v", err)
	}
	for _, table := range []string{"core_groups", "core_group_members", "core_group_events", "core_child_links", "core_data_usage"} {
		if got := count(t, db, ctx, table); got != 0 {
			t.Fatalf("partial %s=%d", table, got)
		}
	}
	execSQL(t, db, ctx, "DROP TRIGGER fail_final")
	if _, err := run(t, db, ctx, "group.prepare", group(1000)); err != nil {
		t.Fatal(err)
	}
}

func TestGroupEightMiBParametersStoredOnceAndOverflowRejected(t *testing.T) {
	db, ctx := fixture(t)
	r := group(1000)
	used := 0
	for i := 1; i < len(r.Members); i++ {
		used += len(r.Members[i].Parameters)
	}
	padding := MaxParameterBytes - used - len(`{"padding":""}`)
	r.Members[0].Parameters = json.RawMessage(`{"padding":"` + strings.Repeat("a", padding) + `"}`)
	if _, err := run(t, db, ctx, "group.prepare", r); err != nil {
		t.Fatal(err)
	}
	var bytes int
	if err := db.QueryRowContext(ctx, "SELECT sum(length(parameters)) FROM core_group_members").Scan(&bytes); err != nil || bytes != MaxParameterBytes {
		t.Fatalf("bytes=%d error=%v", bytes, err)
	}
	r.ID = "overflow"
	r.GroupKey = "overflow"
	r.Members[0].Parameters = json.RawMessage(`{"padding":"` + strings.Repeat("a", padding+1) + `"}`)
	if _, err := run(t, db, ctx, "group.prepare", r); errorCode(err) != "resource_exhausted" {
		t.Fatalf("overflow: %v", err)
	}
	if count(t, db, ctx, "core_groups") != 1 {
		t.Fatal("overflow created facts")
	}
}

func TestGroupHTMLParametersPreserveEightMiBCapacity(t *testing.T) {
	db, ctx := fixture(t)
	r := group(1000)
	remaining := MaxParameterBytes
	for i := range r.Members {
		size := remaining / (len(r.Members) - i)
		r.Members[i].Parameters = []byte(`{"html":"` + strings.Repeat("<", size-len(`{"html":""}`)) + `"}`)
		remaining -= len(r.Members[i].Parameters)
	}
	wire, err := json.Marshal(r)
	if err != nil || len(wire) > MaxRequestBytes || remaining != 0 {
		t.Fatalf("wire=%d remaining=%d error=%v", len(wire), remaining, err)
	}
	if _, err = run(t, db, ctx, "group.prepare", r); err != nil {
		t.Fatal(err)
	}
	raw, err := run(t, db, ctx, "group.snapshot", model.GroupRead{"group"})
	if err != nil {
		t.Fatal(err)
	}
	var recovered model.GroupSnapshot
	if err = json.Unmarshal(raw, &recovered); err != nil || len(recovered.Members) != 1000 {
		t.Fatalf("recovered members=%d error=%v", len(recovered.Members), err)
	}
	for i, m := range recovered.Members {
		if string(m.Parameters) != string(r.Members[i].Parameters) {
			t.Fatalf("parameter bytes changed at ordinal %d", i)
		}
	}
}

func TestGroupPointGuardConflictsButUnrelatedCommitDoesNot(t *testing.T) {
	db, ctx := fixture(t)
	execSQL(t, db, ctx, "INSERT INTO records VALUES(?,'runs','unrelated',9,0,'{}')", testScope)
	if _, err := run(t, db, ctx, "group.prepare", group(1)); err != nil {
		t.Fatal(err)
	}
	r := group(1)
	r.ID = "second"
	r.GroupKey = "second"
	r.Members[0].ChildID = "second"
	r.Members[0].EventID = "second"
	execSQL(t, db, ctx, "UPDATE records SET version=2 WHERE collection='invocations'")
	if _, err := run(t, db, ctx, "group.prepare", r); errorCode(err) != "conflict" {
		t.Fatalf("stale invocation: %v", err)
	}
	r.InvocationGuard.Version = "2"
	r.PinGuard.Version = "0"
	if _, err := run(t, db, ctx, "group.prepare", r); errorCode(err) != "invalid_argument" {
		t.Fatalf("missing live pin guard: %v", err)
	}
}

func seedTree(t *testing.T, db *sql.DB, ctx context.Context) model.NamespaceClone {
	t.Helper()
	for _, n := range []struct{ id, parent, name string }{{"source", "", "source"}, {"child", "source", "child"}, {"target-parent", "", "target-parent"}} {
		var parent any
		if n.parent != "" {
			parent = n.parent
		}
		execSQL(t, db, ctx, "INSERT INTO core_namespaces VALUES(?,?,?,?,?,?,3,0,'{}')", testScope, "robot", n.id, parent, n.name, n.name)
	}
	r := model.NamespaceClone{Domain: "robot", SourceID: "source", ExpectedRevision: "3", TargetParentID: "target-parent", ExpectedTargetParentRevision: "3", TargetID: "copy", Name: "Copy", NameKey: "copy", ChangeID: "copy-change", Change: json.RawMessage(`{"operation":"copy","actor":"operator","reason":"test"}`), Namespaces: []model.NamespaceCopy{{"source", "3", "copy", json.RawMessage(`{"owner":"operator"}`)}, {"child", "3", "copy-child", json.RawMessage(`{}`)}}}
	for i, namespace := range []string{"source", "child"} {
		id, commit, branch := fmt.Sprintf("resource-%d", i), fmt.Sprintf("commit-%d", i), fmt.Sprintf("branch-%d", i)
		payload, manifest := json.RawMessage(`{"typed_name":"Source","kind":"robot","private":"preserved"}`), json.RawMessage(`{"root_digest":"root","nodes":[{"id":"root"}]}`)
		refs := []model.Reference{{"configuration", "robot", "reference-target", "reference-pin", json.RawMessage(`{"mode":"pinned","component":"action","schema":2}`)}}
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
		r.Resources = append(r.Resources, model.ResourceCopy{SourceID: id, ExpectedRevision: "5", ExpectedBranchRevision: "4", SourceCommitID: commit, SourceContentDigest: pin, TargetID: "copy-" + id, TargetBranchID: "copy-" + branch, TargetCommitID: "copy-" + commit, Body: json.RawMessage(`{"system":false}`), BranchBody: json.RawMessage(`{"name":"main"}`), CommitBody: json.RawMessage(`{"actor":"operator","schema":2}`), Payload: payload, Manifest: manifest, References: refs})
	}
	return r
}

func TestNamespaceCloneRecursiveCurrentMainOriginAndReferences(t *testing.T) {
	db, ctx := fixture(t)
	r := seedTree(t, db, ctx)
	// Historical source commits never become cloned history.
	execSQL(t, db, ctx, "INSERT INTO core_snapshots VALUES(?,?,?,?,?,1,'','old','{}','{}','{}')", testScope, "robot", "old", "resource-0", "branch-0")
	r.Resources[0].Payload = json.RawMessage(`{"typed_name":"Rewritten","kind":"robot","private":"preserved"}`)
	if _, err := run(t, db, ctx, "namespace.clone", r); err != nil {
		t.Fatal(err)
	}
	if count(t, db, ctx, "core_namespaces") != 5 || count(t, db, ctx, "core_resources") != 4 || count(t, db, ctx, "core_snapshots") != 5 || count(t, db, ctx, "core_references") != 4 || count(t, db, ctx, "core_changes") != 1 {
		t.Fatal("incomplete clone or copied history")
	}
	var parent, origin, originCommit string
	if err := db.QueryRowContext(ctx, "SELECT parent_id FROM core_namespaces WHERE id='copy-child'").Scan(&parent); err != nil || parent != "copy" {
		t.Fatalf("parent=%s %v", parent, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT origin_resource_id,origin_commit_id FROM core_resources WHERE id='copy-resource-0'").Scan(&origin, &originCommit); err != nil || origin != "resource-0" || originCommit != "commit-0" {
		t.Fatalf("origin=%s/%s %v", origin, originCommit, err)
	}
	raw, err := run(t, db, ctx, "namespace.snapshot", model.NamespaceRead{"robot", "copy"})
	if err != nil {
		t.Fatal(err)
	}
	var tree model.NamespaceTree
	if err = json.Unmarshal(raw, &tree); err != nil || len(tree.Namespaces) != 2 || len(tree.Resources) != 2 || len(tree.Resources[0].References) != 1 || !strings.Contains(string(tree.Resources[0].Payload), "Rewritten") {
		t.Fatalf("snapshot=%s error=%v", raw, err)
	}
}

func TestNamespaceLateReferenceFailureRollsBackAllRelations(t *testing.T) {
	db, ctx := fixture(t)
	r := seedTree(t, db, ctx)
	execSQL(t, db, ctx, `CREATE TRIGGER fail_reference BEFORE INSERT ON core_references WHEN NEW.commit_id='copy-commit-1' BEGIN SELECT RAISE(ABORT,'last reference fault'); END`)
	before := map[string]int{}
	for _, table := range []string{"core_namespaces", "core_resources", "core_branches", "core_snapshots", "core_references", "core_changes", "core_data_usage"} {
		before[table] = count(t, db, ctx, table)
	}
	if _, err := run(t, db, ctx, "namespace.clone", r); err == nil || !strings.Contains(err.Error(), "last reference fault") {
		t.Fatalf("late fault=%v", err)
	}
	for table, n := range before {
		if got := count(t, db, ctx, table); got != n {
			t.Fatalf("partial %s=%d before=%d", table, got, n)
		}
	}
	execSQL(t, db, ctx, "DROP TRIGGER fail_reference")
	if _, err := run(t, db, ctx, "namespace.clone", r); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceGuardAndPhantomFailures(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, context.Context, *model.NamespaceClone)
		code   string
	}{
		{"root-revision", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) { r.ExpectedRevision = "4" }, "conflict"},
		{"child-revision", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "UPDATE core_namespaces SET revision=4 WHERE id='child'")
		}, "conflict"},
		{"resource-revision", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "UPDATE core_resources SET revision=6 WHERE id='resource-1'")
		}, "conflict"},
		{"branch-revision", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "UPDATE core_branches SET revision=6 WHERE id='branch-1'")
		}, "conflict"},
		{"pin-digest", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			r.Resources[0].SourceContentDigest = "different"
		}, "conflict"},
		{"omitted-resource", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			r.Resources = r.Resources[:1]
		}, "conflict"},
		{"new-child-phantom", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "INSERT INTO core_namespaces VALUES(?,'robot','phantom','child','phantom','phantom',1,0,'{}')", testScope)
		}, "conflict"},
		{"missing-current-main", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "UPDATE core_resources SET main_commit_id='absent' WHERE id='resource-0'")
		}, "failed_precondition"},
		{"source-cycle", func(t *testing.T, db *sql.DB, ctx context.Context, r *model.NamespaceClone) {
			execSQL(t, db, ctx, "UPDATE core_namespaces SET parent_id='child' WHERE id='source'")
		}, "failed_precondition"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, ctx := fixture(t)
			r := seedTree(t, db, ctx)
			c.mutate(t, db, ctx, &r)
			before := count(t, db, ctx, "core_namespaces")
			if _, err := run(t, db, ctx, "namespace.clone", r); errorCode(err) != c.code {
				t.Fatalf("error=%v want=%s", err, c.code)
			}
			if count(t, db, ctx, "core_namespaces") != before || count(t, db, ctx, "core_data_usage") != 0 {
				t.Fatal("guard failure created partial data")
			}
		})
	}
}

func TestNamespaceNameConflictAndRevisionPrecision(t *testing.T) {
	db, ctx := fixture(t)
	r := seedTree(t, db, ctx)
	execSQL(t, db, ctx, "UPDATE core_namespaces SET revision=9007199254740993 WHERE id='source'")
	r.ExpectedRevision = "9007199254740993"
	r.Namespaces[0].ExpectedRevision = r.ExpectedRevision
	raw, err := run(t, db, ctx, "namespace.snapshot", model.NamespaceRead{"robot", "source"})
	if err != nil || !strings.Contains(string(raw), `"revision":"9007199254740993"`) {
		t.Fatalf("revision precision=%s error=%v", raw, err)
	}
	execSQL(t, db, ctx, "INSERT INTO core_namespaces VALUES(?,'robot','conflicting','target-parent','Copy','copy',1,0,'{}')", testScope)
	before := count(t, db, ctx, "core_namespaces")
	if _, err = run(t, db, ctx, "namespace.clone", r); err == nil {
		t.Fatal("sibling name conflict accepted")
	}
	if count(t, db, ctx, "core_namespaces") != before || count(t, db, ctx, "core_data_usage") != 0 {
		t.Fatal("name conflict left partial targets")
	}
}

func TestGroupSnapshotCompleteEmptyAndQuotaAdmission(t *testing.T) {
	for _, n := range []int{0, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db, ctx := fixture(t)
			if _, err := run(t, db, ctx, "group.prepare", group(n)); err != nil {
				t.Fatal(err)
			}
			raw, err := run(t, db, ctx, "group.snapshot", model.GroupRead{"group"})
			if err != nil {
				t.Fatal(err)
			}
			var snapshot model.GroupSnapshot
			if err = json.Unmarshal(raw, &snapshot); err != nil || len(snapshot.Members) != n {
				t.Fatalf("snapshot=%s error=%v", raw, err)
			}
		})
	}
	db, ctx := fixture(t)
	execSQL(t, db, ctx, "INSERT INTO core_data_usage VALUES(?,?,?)", testScope, MaxScopeRows, MaxScopeBytes)
	if _, err := run(t, db, ctx, "group.prepare", group(1)); errorCode(err) != "resource_exhausted" {
		t.Fatalf("quota bypassed=%v", err)
	}
	if count(t, db, ctx, "core_groups") != 0 {
		t.Fatal("quota failure left facts")
	}
}

func TestRelationalIndexesAndScopeIsolation(t *testing.T) {
	db, ctx := fixture(t)
	seedTree(t, db, ctx)
	for _, q := range []string{
		"SELECT id FROM core_namespaces WHERE scope='s' AND domain='d' AND parent_id='p' AND archived=0",
		"SELECT id FROM core_resources WHERE scope='s' AND domain='d' AND namespace_id='n' AND archived=0",
		"SELECT ordinal FROM core_group_members WHERE scope='s' AND group_id='g' ORDER BY ordinal",
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
	raw, _ := json.Marshal(model.NamespaceRead{"robot", "source"})
	if _, err = Execute(ctx, tx, `{"namespace":"core","user":"other","workspace":"station"}`, "namespace.snapshot", raw); errorCode(err) != "not_found" {
		t.Fatalf("scope leaked source: %v", err)
	}
}

func TestNamespaceBoundedSubtreeAndLargeDestinationMetadata(t *testing.T) {
	db, ctx := fixture(t)
	r := seedTree(t, db, ctx)
	execSQL(t, db, ctx, `WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i<1024)
 INSERT INTO core_namespaces SELECT ?,'robot','extra-'||i,'target-parent','extra-'||i,'extra-'||i,1,0,'{}' FROM seq`, testScope)
	if _, err := run(t, db, ctx, "namespace.snapshot", model.NamespaceRead{"robot", "target-parent"}); errorCode(err) != "resource_exhausted" {
		t.Fatalf("subtree bound=%v", err)
	}
	raw, err := run(t, db, ctx, "namespace.get", model.NamespaceRead{"robot", "target-parent"})
	if err != nil || !strings.Contains(string(raw), `"revision":"3"`) {
		t.Fatalf("destination metadata=%s error=%v", raw, err)
	}
	if _, err = run(t, db, ctx, "namespace.clone", r); err != nil {
		t.Fatal(err)
	}
}
