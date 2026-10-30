package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func sessionFailedCleanup(t *testing.T, f *store, id string) bool {
	t.Helper()
	var failed bool
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT failed_cleanup FROM sessions WHERE scope=? AND id=?", engine.ScopeID(f.scope), id).Scan(&failed)
	}); err != nil {
		t.Fatal(err)
	}
	return failed
}

func TestRestartSessionCleanupSurvivesHistoryClear(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, queueFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("failed=%t/queue-first=%t", failed, queueFirst), func(t *testing.T) {
				f := openStore(t)
				p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
				old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "simulation", At: epoch}
				if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
					t.Fatal(err)
				}
				q := newRun("run")
				q.SessionID = old.ID
				if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
					t.Fatal(err)
				}
				finish := model.RunFinish{ID: q.ID, Status: model.RunStopped, At: epoch}
				if failed {
					finish.CleanupErrors = []byte(`[{"message":"physical cleanup failed"}]`)
				}
				if _, err := f.core.FinishRun(f.ctx, finish, p); err != nil {
					t.Fatal(err)
				}
				if got := sessionFailedCleanup(t, f, old.ID); got != failed {
					t.Fatalf("failed disposition=%t want=%t", got, failed)
				}
				// Replaying the same outcome cannot erase or duplicate its disposition.
				if _, err := f.core.FinishRun(f.ctx, finish, p); err != nil {
					t.Fatal(err)
				}
				next := old
				next.ID = "next"
				next.PredecessorSessionID = old.ID
				if queueFirst {
					if _, created, err := f.core.QueueRestartSession(f.ctx, next); err != nil || !created {
						t.Fatalf("queue: %t %v", created, err)
					}
				}
				pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
				wantDeleted := 1
				if queueFirst {
					wantDeleted = 0
				}
				if err != nil || pruned.Deleted != wantDeleted {
					t.Fatalf("clear: %+v %v", pruned, err)
				}
				f.reopen(t)
				if !queueFirst {
					if _, created, err := f.core.QueueRestartSession(f.ctx, next); err != nil || !created {
						t.Fatalf("queue after clear: %t %v", created, err)
					}
				}
				if _, err = f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
				if _, err = f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
					t.Fatal(err)
				}
				f.reopen(t)
				got, err := f.core.ActivateRestartSession(f.ctx, next.ID, p)
				if failed {
					if errCode(err) != "failed_precondition" {
						t.Fatalf("failed cleanup promoted: %+v %v", got, err)
					}
					if !sessionFailedCleanup(t, f, old.ID) {
						t.Fatal("cleanup failure lost after reopen")
					}
					if _, err = f.core.CloseSession(f.ctx, next.ID, model.SessionClose{Outcome: model.SessionFailed, At: epoch.Add(2 * time.Minute)}, p); err != nil {
						t.Fatal(err)
					}
					released, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
					if err != nil || released.Deleted != 1-wantDeleted {
						t.Fatalf("failed Session history after cancel: %+v %v", released, err)
					}
				} else if err != nil || got.Status != model.SessionOpen {
					t.Fatalf("clean activation: %+v %v", got, err)
				}
				assertRetentionIndex(t, f)
			})
		}
	}
}

func TestRestartSessionCleanupFailureRollbackAndRaceWithClear(t *testing.T) {
	for i := 0; i < 8; i++ {
		f := openStore(t)
		p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
		old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
		if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
			t.Fatal(err)
		}
		q := newRun("run")
		q.SessionID = old.ID
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
		finish := model.RunFinish{ID: q.ID, Status: model.RunStopped, CleanupErrors: []byte(`[{"message":"cleanup failed"}]`), At: epoch}
		_, err := f.core.ApplyRunOps(f.ctx, nil, nil, []model.RunFinish{finish, {ID: "missing", Status: model.RunFailed}}, p)
		if errCode(err) != "not_found" || sessionFailedCleanup(t, f, old.ID) {
			t.Fatalf("rollback leaked failure: %v", err)
		}
		run, err := f.core.GetRun(f.ctx, q.ID)
		if err != nil || run.Status != model.RunQueued {
			t.Fatalf("rollback leaked finish: %+v %v", run, err)
		}
		var finishErr, clearErr error
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() { defer wg.Done(); <-start; _, finishErr = f.core.FinishRun(f.ctx, finish, p) }()
		go func() {
			defer wg.Done()
			<-start
			_, clearErr = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
		}()
		close(start)
		wg.Wait()
		if finishErr != nil || clearErr != nil || !sessionFailedCleanup(t, f, old.ID) {
			t.Fatalf("race failure=%v clear=%v", finishErr, clearErr)
		}
		next := old
		next.ID = "next"
		next.PredecessorSessionID = old.ID
		if _, _, err = f.core.QueueRestartSession(f.ctx, next); err != nil {
			t.Fatal(err)
		}
		if _, err = f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if _, err = f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
			t.Fatal(err)
		}
		if _, err = f.core.ActivateRestartSession(f.ctx, next.ID, p); errCode(err) != "failed_precondition" {
			t.Fatalf("failed cleanup promoted after race: %v", err)
		}
		assertRetentionIndex(t, f)
	}
}

func TestRestartSessionDetectsActualPendingRelationWithInconsistentCounter(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	q := newRun("run")
	q.SessionID = old.ID
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	link := remoteLink(q.ID, q.ID, "remote")
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunStopped, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	next := old
	next.ID = "next"
	next.PredecessorSessionID = old.ID
	if _, _, err := f.core.QueueRestartSession(f.ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
		t.Fatal(err)
	}
	// Test-only corruption: retain the authoritative relation but hide it
	// from the counter subset. Activation must still see the indexed link.
	f.exec(t, `UPDATE runs SET pending_remote_count=0,pending_remote_bytes=0 WHERE id=?`, q.ID)
	if _, err := f.core.ActivateRestartSession(f.ctx, next.ID, p); errCode(err) != "failed_precondition" {
		t.Fatalf("actual pending link was ignored: %v", err)
	}
}

func TestRemoteSessionProjectionChargedAndReleasedExactly(t *testing.T) {
	f := openStore(t)
	q := newRun("owner")
	q.SessionID = "session-测试"
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	beforeRows, beforeBytes := usageRows(t, f)
	link := remoteLink(q.ID, q.ID, "remote")
	link.Phase = model.RemoteRunOpen
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]model.RemoteRunLink{fundedRemoteLink(link)})
	if err != nil {
		t.Fatal(err)
	}
	afterRows, afterBytes := usageRows(t, f)
	expected := int64(len(encoded) + 256 + len(q.SessionID))
	if afterRows-beforeRows != 1 || afterBytes-beforeBytes != expected {
		t.Fatalf("relation quota rows=%d bytes=%d want=%d", afterRows-beforeRows, afterBytes-beforeBytes, expected)
	}
	var stored string
	if err = f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT owner_session_id FROM run_remote_pending WHERE scope=? AND owner_run_id=?`, engine.ScopeID(f.scope), q.ID).Scan(&stored)
	}); err != nil || stored != q.SessionID {
		t.Fatalf("relation Session selector=%q %v", stored, err)
	}
	releaseBoundRemote(t, f, link)
	if _, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	finalRows, finalBytes := usageRows(t, f)
	if finalRows != beforeRows || finalBytes != beforeBytes {
		t.Fatalf("quota did not return after join: %d/%d -> %d/%d", beforeRows, beforeBytes, finalRows, finalBytes)
	}
}

func TestRestartSessionClaimsExistingFailedRunBeforeClear(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	q := newRun("run")
	q.SessionID = "later-session"
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunStopped, CleanupErrors: []byte(`[{"message":"cleanup failed"}]`), At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	old := model.NewSession{ID: q.SessionID, TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	if !sessionFailedCleanup(t, f, old.ID) {
		t.Fatal("Session did not claim its retained failed outcome")
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("clear: %+v %v", pruned, err)
	}
	next := old
	next.ID = "next"
	next.PredecessorSessionID = old.ID
	if _, _, err = f.core.QueueRestartSession(f.ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if _, err = f.core.ActivateRestartSession(f.ctx, next.ID, p); errCode(err) != "failed_precondition" {
		t.Fatalf("lost claimed failure: %v", err)
	}
}

func TestRestartSessionOpenRunGuardAndCleanupCompletion(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	q := newRun("run")
	q.SessionID = old.ID
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	next := old
	next.ID = "next"
	next.PredecessorSessionID = old.ID
	if _, _, err := f.core.QueueRestartSession(f.ctx, next); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ActivateRestartSession(f.ctx, next.ID, p); errCode(err) != "failed_precondition" {
		t.Fatalf("open Run did not protect activation: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunStopped, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	active, err := f.core.ActivateRestartSession(f.ctx, next.ID, p)
	if err != nil || active.Status != model.SessionOpen {
		t.Fatalf("clean join did not release activation: %+v %v", active, err)
	}
	assertRetentionIndex(t, f)
}

func TestRestartSessionFailedFinishRacesActivation(t *testing.T) {
	for i := 0; i < 8; i++ {
		f := openStore(t)
		p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
		old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
		if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
			t.Fatal(err)
		}
		q := newRun("run")
		q.SessionID = old.ID
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
		next := old
		next.ID = "next"
		next.PredecessorSessionID = old.ID
		if _, _, err := f.core.QueueRestartSession(f.ctx, next); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
			t.Fatal(err)
		}
		var finishErr, activationErr error
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() {
			defer wg.Done()
			<-start
			_, finishErr = f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunStopped, CleanupErrors: []byte(`[{"message":"failed"}]`), At: epoch}, p)
		}()
		go func() { defer wg.Done(); <-start; _, activationErr = f.core.ActivateRestartSession(f.ctx, next.ID, p) }()
		close(start)
		wg.Wait()
		if finishErr != nil || errCode(activationErr) != "failed_precondition" || !sessionFailedCleanup(t, f, old.ID) {
			t.Fatalf("finish=%v activation=%v", finishErr, activationErr)
		}
		assertRetentionIndex(t, f)
	}
}
