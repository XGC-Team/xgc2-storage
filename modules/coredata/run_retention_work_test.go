package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

// Measure the real SQLite statements and maintained indexes, including VM
// instructions and transient SQLite allocation. The fixture is diagnostic SQL;
// the typed Store lifecycle and rollback tests are separate.
func retentionVM(t *testing.T, path, query string, args ...string) (int32, int32, int32, int64) {
	t.Helper()
	tls := libc.NewTLS()
	defer tls.Close()
	cpath, err := libc.CString(path)
	if err != nil {
		t.Fatal(err)
	}
	defer libc.Xfree(tls, cpath)
	pp := libc.Xmalloc(tls, 8)
	defer libc.Xfree(tls, pp)
	if rc := sqlite3.Xsqlite3_open_v2(tls, cpath, pp, sqlite3.SQLITE_OPEN_READWRITE, 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("open: %d", rc)
	}
	db := libc.AtomicLoadPUintptr(pp)
	defer sqlite3.Xsqlite3_close_v2(tls, db)
	sqltext, err := libc.CString(query)
	if err != nil {
		t.Fatal(err)
	}
	defer libc.Xfree(tls, sqltext)
	pstmt := libc.Xmalloc(tls, 8)
	defer libc.Xfree(tls, pstmt)
	if rc := sqlite3.Xsqlite3_prepare_v2(tls, db, sqltext, -1, pstmt, 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("prepare: %d", rc)
	}
	stmt := libc.AtomicLoadPUintptr(pstmt)
	var bindings []uintptr
	defer func() {
		for _, p := range bindings {
			libc.Xfree(tls, p)
		}
	}()
	defer sqlite3.Xsqlite3_finalize(tls, stmt)
	for i, s := range args {
		p, e := libc.CString(s)
		if e != nil {
			t.Fatal(e)
		}
		bindings = append(bindings, p)
		if rc := sqlite3.Xsqlite3_bind_text(tls, stmt, int32(i+1), p, -1, 0); rc != sqlite3.SQLITE_OK {
			t.Fatalf("bind: %d", rc)
		}
	}
	values := libc.Xmalloc(tls, 16)
	defer libc.Xfree(tls, values)
	cache := libc.Xmalloc(tls, 8)
	defer libc.Xfree(tls, cache)
	if rc := sqlite3.Xsqlite3_db_status(tls, db, sqlite3.SQLITE_DBSTATUS_CACHE_WRITE, cache, cache+4, 1); rc != sqlite3.SQLITE_OK {
		t.Fatalf("cache write reset: %d", rc)
	}
	if rc := sqlite3.Xsqlite3_status64(tls, sqlite3.SQLITE_STATUS_MEMORY_USED, values, values+8, 1); rc != sqlite3.SQLITE_OK {
		t.Fatalf("memory reset: %d", rc)
	}
	baseline := libc.AtomicLoadPInt64(values)
	if rc := sqlite3.Xsqlite3_step(tls, stmt); rc != sqlite3.SQLITE_DONE {
		t.Fatalf("step: %d", rc)
	}
	steps := sqlite3.Xsqlite3_stmt_status(tls, stmt, sqlite3.SQLITE_STMTSTATUS_VM_STEP, 0)
	changed := sqlite3.Xsqlite3_changes(tls, db)
	if rc := sqlite3.Xsqlite3_db_status(tls, db, sqlite3.SQLITE_DBSTATUS_CACHE_WRITE, cache, cache+4, 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("cache write read: %d", rc)
	}
	writtenPages := libc.AtomicLoadPInt32(cache)
	if rc := sqlite3.Xsqlite3_status64(tls, sqlite3.SQLITE_STATUS_MEMORY_USED, values, values+8, 0); rc != sqlite3.SQLITE_OK {
		t.Fatalf("memory read: %d", rc)
	}
	peak := libc.AtomicLoadPInt64(values+8) - baseline
	if baseline == 0 && peak == 0 {
		peak = -1 // SQLite memory statistics are disabled in the normal driver.
	}
	return steps, changed, writtenPages, peak
}

// The separate process enables SQLite memory accounting before initialization.
// Normal-driver VM and row counts are measured independently below.
func TestRetentionMemoryProbe(t *testing.T) {
	path := os.Getenv("XGC_RETENTION_MEMORY_PATH")
	if path == "" {
		t.Skip("subprocess probe")
	}
	tls := libc.NewTLS()
	defer tls.Close()
	args := libc.Xmalloc(tls, 8)
	defer libc.Xfree(tls, args)
	if rc := sqlite3.Xsqlite3_config(tls, sqlite3.SQLITE_CONFIG_MEMSTATUS, libc.VaList(args, 1)); rc != sqlite3.SQLITE_OK {
		t.Fatalf("enable SQLite memory accounting: %d", rc)
	}
	query, params := retentionRootRelease, []string{"scope", "root"}
	if os.Getenv("XGC_RETENTION_MEMORY_DIRECTION") == "session" {
		query, params = retentionSessionRelease, []string{"scope", "core", "old"}
	}
	steps, changed, pages, peak := retentionVM(t, path, query, params...)
	if peak < 0 {
		t.Fatal("SQLite memory statistics unavailable")
	}
	b, _ := json.Marshal([]int64{int64(steps), int64(changed), int64(pages), peak})
	fmt.Printf("RETENTION_MEMORY=%s\n", b)
}

func retentionMemory(t *testing.T, path, direction string) int64 {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRetentionMemoryProbe$")
	cmd.Env = append(os.Environ(), "XGC_RETENTION_MEMORY_PATH="+path, "XGC_RETENTION_MEMORY_DIRECTION="+direction)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("memory probe: %v: %s", err, out)
	}
	line := strings.SplitN(string(out), "RETENTION_MEMORY=", 2)
	if len(line) != 2 {
		t.Fatalf("missing memory result: %s", out)
	}
	var values []int64
	if err = json.Unmarshal([]byte(strings.SplitN(line[1], "\n", 2)[0]), &values); err != nil || len(values) != 4 {
		t.Fatalf("invalid memory result %s: %v", out, err)
	}
	return values[3]
}

func retentionVMFixture(t *testing.T, direction string, affected, protected, unrelated int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "work.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	const cols = `scope,id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,session_instance_id,idempotency_key,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,source_observation_json,status,termination,error_json,error_summary_json,result_json,nodes_json,cleanup_errors_json,cleanup_failed,created_at,started_at,finished_at,revision,bytes,retention_local_ok,retention_eligible`
	// Identical synthetic finished rows, differentiated only by indexed guard keys.
	const insert = `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<?) INSERT INTO runs(` + cols + `) SELECT 'scope',printf('%s-%08d',?,n),'core',?,'parent','call',1,?,'','','wf','c','d','run',x'7b7d',x'7b7d',x'7b7d','succeeded','',x'',x'',x'',x'',x'',0,n,0,n+1,1,1,1,? FROM seq`
	add := func(n int, prefix, root, session string, eligible int) {
		if n > 0 {
			if _, err = tx.ExecContext(ctx, insert, n, prefix, root, session, eligible); err != nil {
				t.Fatal(err)
			}
		}
	}
	if direction == "root" {
		add(affected, "allowed", "root", "", 0)
		add(protected, "blocked", "root", "old", 0)
		add(unrelated, "unrelated", "other", "", 1)
		if protected > 0 {
			_, err = tx.ExecContext(ctx, `INSERT INTO sessions(scope,id,target_id,predecessor_session_id,experiment_resource_id,experiment_commit_id,run_mode,experiment_name,root_run_id,status,stop_intent_json,outcome,error_json,opened_at,closed_at,revision,bytes) VALUES('scope','next','core','old','e','c','sim','','','queued',x'','',x'',1,0,1,1)`)
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		add(affected, "allowed", "done", "old", 0)
		add(protected, "blocked", "live", "old", 0)
		add(unrelated, "unrelated", "other", "other-session", 1)
		// One live root guards the entire blocked Session group.
		_, err = tx.ExecContext(ctx, `INSERT INTO runs(`+cols+`) VALUES('scope','live','core','live','','',0,'','','','wf','c','d','run',x'7b7d',x'7b7d',x'7b7d','queued','',x'',x'',x'',x'',x'',0,1,0,0,1,1,0,0)`)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRetentionGroupedSelectorsActualSQLiteWork(t *testing.T) {
	for _, direction := range []string{"root", "session"} {
		t.Run(direction, func(t *testing.T) {
			query, args, index := retentionRootRelease, []string{"scope", "root"}, "runs_retention_root_group"
			if direction == "session" {
				query, args, index = retentionSessionRelease, []string{"scope", "core", "old"}, "runs_retention_session_group"
			}
			cases := []struct {
				name                           string
				affected, protected, unrelated int
			}{{"one", 1, 0, 0}, {"protected-10000", 1, 10000, 0}, {"unrelated-10000", 1, 0, 10000}, {"changed-257", 257, 0, 0}, {"changed-10000", 10000, 0, 0}}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					path := retentionVMFixture(t, direction, tc.affected, tc.protected, tc.unrelated)
					db, err := sql.Open("sqlite", "file:"+path)
					if err != nil {
						t.Fatal(err)
					}
					bound := make([]any, len(args))
					for i, s := range args {
						bound[i] = s
					}
					rows, err := db.Query("EXPLAIN QUERY PLAN "+query, bound...)
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
					db.Close()
					if !strings.Contains(plan.String(), index) {
						t.Fatalf("selector missed %s: %s", index, plan.String())
					}
					memory := retentionMemory(t, path, direction)
					// The memory subprocess executed the mutation; use a fresh
					// identical fixture for normal-driver VM measurement.
					path = retentionVMFixture(t, direction, tc.affected, tc.protected, tc.unrelated)
					steps, changed, pages, _ := retentionVM(t, path, query, args...)
					if int(changed) != tc.affected {
						t.Fatalf("changed %d want %d", changed, tc.affected)
					}
					if tc.affected == 1 && steps > 3000 {
						t.Fatalf("one changed row traversed protected or unrelated rows: %d VM", steps)
					}
					t.Logf("%s changed=%d protected=%d unrelated=%d vm=%d sqlite_pages_written=%d sqlite_tracked_transient_peak_bytes=%d", direction, changed, tc.protected, tc.unrelated, steps, pages, memory)
				})
			}
		})
	}
}

func sessionBootVMFixture(t *testing.T, history int, status string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "boot.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if history > 0 {
		_, err = tx.ExecContext(ctx, `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<?)
        INSERT INTO sessions(scope,id,target_id,predecessor_session_id,experiment_resource_id,experiment_commit_id,run_mode,experiment_name,root_run_id,status,stop_intent_json,outcome,error_json,opened_at,closed_at,revision,bytes)
        SELECT 'scope',printf('closed-%08d',n),'target','', 'e','c','mode','','','closed',x'','stopped',x'',1,2,1,1 FROM seq`, history)
		if err != nil {
			t.Fatal(err)
		}
	}
	if status != "" {
		predecessor := ""
		if status == "queued" {
			predecessor = "previous"
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO sessions(scope,id,target_id,predecessor_session_id,experiment_resource_id,experiment_commit_id,run_mode,experiment_name,root_run_id,status,stop_intent_json,outcome,error_json,opened_at,closed_at,revision,bytes) VALUES('scope','current','target',?,'e','c','mode','','',?,x'','',x'',1,0,1,1)`, predecessor, status)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSessionBootSQLiteWorkSkipsRetainedHistory(t *testing.T) {
	// The prior combined statement is retained here only as a measured
	// diagnostic of the discarded scan, never as a runtime fallback.
	old := "UPDATE sessions SET status='interrupted',closed_at=?,revision=revision+1 WHERE scope=? AND status IN ('queued','open','stopping')"
	path := sessionBootVMFixture(t, 10000, "open")
	oldSteps, oldChanged, oldPages, _ := retentionVM(t, path, old, "3", "scope")
	if oldChanged != 1 || oldSteps < 10000 {
		t.Fatalf("old combined selector did not expose history traversal: changed=%d vm=%d", oldChanged, oldSteps)
	}
	t.Logf("previous combined Session boot retained=10000 changed=%d vm=%d sqlite_pages_written=%d", oldChanged, oldSteps, oldPages)
	for _, tc := range []struct {
		name, status, query string
		history, want       int
	}{
		{"live/one", "open", interruptLiveSessions, 0, 1},
		{"live/one-plus-history", "open", interruptLiveSessions, 10000, 1},
		{"live/zero-plus-history", "", interruptLiveSessions, 10000, 0},
		{"queued/one", "queued", interruptQueuedSessions, 0, 1},
		{"queued/one-plus-history", "queued", interruptQueuedSessions, 10000, 1},
		{"queued/zero-plus-history", "", interruptQueuedSessions, 10000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := sessionBootVMFixture(t, tc.history, tc.status)
			steps, changed, pages, _ := retentionVM(t, path, tc.query, "3", "scope")
			if int(changed) != tc.want || steps > 1000 {
				t.Fatalf("history=%d changed=%d want=%d vm=%d", tc.history, changed, tc.want, steps)
			}
			t.Logf("Session boot status=%s retained=%d changed=%d vm=%d sqlite_pages_written=%d", tc.status, tc.history, changed, steps, pages)
		})
	}
}
