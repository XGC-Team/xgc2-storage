package coredata

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRemotePhaseChecksRejectImpossibleRows(t *testing.T) {
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "phase-checks.db") + "?_pragma=foreign_keys(1)", rec: &headerSQLRecorder{}})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "owner", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO run_remote_pending(scope,owner_run_id,root_run_id,target_id,child_run_id,call_node_id,boot_id,encoded_bytes,phase,settled_disposition,settled_at,released_at,owner_session_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'')`
	cases := []struct {
		name                  string
		phase, disposition    string
		settledAt, releasedAt int64
		ok                    bool
	}{
		{"open clean", "open", "", 0, 0, true},
		{"open with settled evidence", "open", "succeeded", 1, 0, false},
		{"open with release", "open", "", 0, 1, false},
		{"settled", "settled", "succeeded", 1, 0, true},
		{"settled without disposition", "settled", "", 1, 0, false},
		{"settled without instant", "settled", "succeeded", 0, 0, false},
		{"released", "released", "succeeded", 1, 2, true},
		{"released without release instant", "released", "succeeded", 1, 0, false},
		{"unknown phase", "final", "", 0, 0, false},
	}
	for i, tc := range cases {
		child := "child-" + tc.name
		_, err = tx.ExecContext(ctx, insert, testScope, "owner", "owner", "agent", child, "call", "boot", 1, tc.phase, tc.disposition, tc.settledAt, tc.releasedAt)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: accepted=%v", tc.name, err == nil)
		}
		if err == nil {
			if _, err = tx.ExecContext(ctx, `DELETE FROM run_remote_pending WHERE scope=? AND child_run_id=?`, testScope, child); err != nil {
				t.Fatal(err)
			}
		}
		_ = i
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestRunIncomingHoldChecksRequirePairs(t *testing.T) {
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "hold-checks.db") + "?_pragma=foreign_keys(1)", rec: &headerSQLRecorder{}})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "root", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	mk := func(id, target, principal string, released int) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO runs(scope,id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,session_instance_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,source_observation_json,status,termination,error_json,error_summary_json,result_json,nodes_json,cleanup_errors_json,cleanup_failed,created_at,started_at,finished_at,revision,bytes,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released)
			VALUES(?,?,?,?,?,?,0,'','','',?,?,?,?,?,x'',x'',x'','running','',x'',x'',x'',x'',x'',0,1,0,0,1,0,?,?,?)`,
			testScope, id, "local", "root", "root", "call", "", "wf", "commit", "digest", "run", target, principal, released)
		return err
	}
	for _, tc := range []struct {
		name      string
		target    string
		principal string
		released  int
		ok        bool
	}{
		{"plain child", "", "", 0, true},
		{"hold pair", "core", "principal", 0, true},
		{"target without principal", "core", "", 0, false},
		{"principal without target", "", "principal", 0, false},
		{"released hold", "core", "principal", 1, true},
		{"released without hold", "", "", 1, false},
		{"hold equals own target", "local", "principal", 0, false},
	} {
		err := mk(tc.name, tc.target, tc.principal, tc.released)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: accepted=%v %v", tc.name, err == nil, err)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestRemotePhaseFaultProbeSeesCommittedPhase(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "phase-fault.db") + "?_pragma=foreign_keys(1)", rec: rec})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "owner", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if _, err = changeRemoteRun(ctx, tx, testScope, link, remoteReserve, nil); err != nil {
		t.Fatal(err)
	}
	link.Phase = model.RemoteRunOpen
	settled := model.RemoteRunSettlement{Disposition: "succeeded", FinishedAt: time.Unix(1, 0)}
	if _, err = settleRemoteRun(ctx, tx, testScope, link, settled, nil); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	state := &remotePhaseFaultState{scope: testScope, link: link, detail: remotePhaseChangeDetail{validated: true}, applied: true}
	if err = state.probe(ctx, tx, true); err != nil || !state.fact.Present || state.fact.Phase != model.RemoteRunSettled || state.fact.Current.SettledDisposition != "succeeded" {
		t.Fatalf("committed phase fact=%+v %v", state.fact, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// A writer that never reached validation reads nothing on revalidation.
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rec.statements, rec.bytes = nil, 0
	state = &remotePhaseFaultState{scope: testScope, link: link}
	if err = state.probe(ctx, tx, true); err != nil || state.fact.Validated || state.fact.Present || len(rec.statements) != 0 {
		t.Fatalf("unvalidated probe read facts: %+v SQL=%v", state.fact, rec.statements)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// A non-committing writer reports its last validated cut, nothing durable.
	state = &remotePhaseFaultState{scope: testScope, link: link, detail: remotePhaseChangeDetail{validated: true, before: link}}
	if err = state.probe(ctx, tx, false); err != nil || !state.fact.NonCommitted || !state.fact.Present || state.fact.Phase != model.RemoteRunOpen {
		t.Fatalf("non-commit fact=%+v %v", state.fact, err)
	}
}

func TestIncomingReleaseFaultProbeSeesHoldState(t *testing.T) {
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "hold-fault.db") + "?_pragma=foreign_keys(1)", rec: &headerSQLRecorder{}})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	child := model.NewRun{ID: "child", TargetID: "local", ParentRunID: "parent", RootRunID: "parent", CallNodeID: "call", Depth: 1, WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`), IncomingHold: model.IncomingRemoteHold{TargetID: "core", PrincipalID: "principal-c"}}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "parent", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, child); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE runs SET status='succeeded',finished_at=1 WHERE scope=? AND id=?`, testScope, "child"); err != nil {
		t.Fatal(err)
	}
	want := model.IncomingRunRelease{ParentRunID: "parent", RootRunID: "parent", CallNodeID: "call", TargetID: "core", PrincipalID: "principal-c"}
	var detail incomingReleaseChangeDetail
	changed, err := releaseIncomingRun(ctx, tx, testScope, "child", want, &detail)
	if err != nil || !changed || !detail.validated {
		t.Fatalf("release changed=%t detail=%+v %v", changed, detail, err)
	}
	var eligible int
	if err = tx.QueryRowContext(ctx, `SELECT retention_eligible FROM runs WHERE scope=? AND id=?`, testScope, "child").Scan(&eligible); err != nil {
		t.Fatal(err)
	}
	// The release itself is not the whole retention verdict — the live root
	// still guards this child — but the hold must no longer block it.
	var held int
	if err = tx.QueryRowContext(ctx, `SELECT incoming_hold_released FROM runs WHERE scope=? AND id=?`, testScope, "child").Scan(&held); err != nil || held != 1 {
		t.Fatalf("released flag=%d %v", held, err)
	}
	state := &incomingReleaseFaultState{scope: testScope, runID: "child", want: want, detail: detail, applied: true}
	if err = state.probe(ctx, tx, true); err != nil || !state.fact.RunPresent || !state.fact.HoldPresent || !state.fact.Released || state.fact.Current != want {
		t.Fatalf("committed hold fact=%+v %v", state.fact, err)
	}
	nonCommitted := &incomingReleaseFaultState{scope: testScope, runID: "child", want: want, detail: detail}
	if err = nonCommitted.probe(ctx, tx, false); err != nil || !nonCommitted.fact.NonCommitted || nonCommitted.fact.Released {
		t.Fatalf("non-commit hold fact=%+v %v", nonCommitted.fact, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestIncomingReleaseFaultProbeSeesRefusedHold(t *testing.T) {
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "hold-refused.db") + "?_pragma=foreign_keys(1)", rec: &headerSQLRecorder{}})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	child := model.NewRun{ID: "child", TargetID: "local", ParentRunID: "parent", RootRunID: "parent", CallNodeID: "call", Depth: 1, WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`), IncomingHold: model.IncomingRemoteHold{TargetID: "core", PrincipalID: "principal-c"}}
	if _, _, err = createRun(ctx, tx, testScope, model.NewRun{ID: "parent", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = createRun(ctx, tx, testScope, child); err != nil {
		t.Fatal(err)
	}
	want := model.IncomingRunRelease{ParentRunID: "parent", RootRunID: "parent", CallNodeID: "call", TargetID: "core", PrincipalID: "principal-c"}

	// Unclean dispositions refuse inside the same scalar read — no UPDATE
	// lands, but the validated hold facts still populate the fault detail.
	for _, tc := range []struct {
		name string
		set  string
	}{
		{"interrupted", `UPDATE runs SET status='interrupted',finished_at=1`},
		{"cleanup failed", `UPDATE runs SET status='succeeded',finished_at=1,cleanup_failed=2`},
	} {
		if _, err = tx.ExecContext(ctx, tc.set+` WHERE scope=? AND id=?`, testScope, "child"); err != nil {
			t.Fatal(err)
		}
		var detail incomingReleaseChangeDetail
		changed, err := releaseIncomingRun(ctx, tx, testScope, "child", want, &detail)
		if err == nil || changed || !detail.validated {
			t.Fatalf("%s: changed=%t detail=%+v err=%v", tc.name, changed, detail, err)
		}
		var held int
		if err = tx.QueryRowContext(ctx, `SELECT incoming_hold_released FROM runs WHERE scope=? AND id=?`, testScope, "child").Scan(&held); err != nil || held != 0 {
			t.Fatalf("%s: hold released=%d %v", tc.name, held, err)
		}
		// A non-committing refused writer still reports the durable hold
		// state it validated before the guard refused.
		state := &incomingReleaseFaultState{scope: testScope, runID: "child", want: want, detail: detail}
		if err = state.probe(ctx, tx, false); err != nil || !state.fact.NonCommitted || !state.fact.HoldPresent || state.fact.Released || state.fact.Current != want {
			t.Fatalf("%s: refused fact=%+v %v", tc.name, state.fact, err)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE runs SET status='queued',finished_at=0,cleanup_failed=0 WHERE scope=? AND id=?`, testScope, "child"); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestPendingRemoteRootOpenIndexCoversRestartFence(t *testing.T) {
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "open-index.db") + "?_pragma=foreign_keys(1)", rec: &headerSQLRecorder{}})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT EXISTS(SELECT 1 FROM run_remote_pending INDEXED BY run_remote_pending_root_open WHERE scope=? AND root_run_id=? AND phase='open')`, "s", "r")
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "; ")
	}
	rows.Close()
	if !strings.Contains(plan.String(), "run_remote_pending_root_open") || strings.Contains(plan.String(), "SCAN run_remote_pending") {
		t.Fatalf("restart fence lost its covering index: %s", plan.String())
	}
}
