package coredata_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// This deliberately recomputes the old authoritative predicate in a test
// snapshot. The production transitions never scan all history for this check.
func assertRetentionIndex(t *testing.T, f *store) {
	t.Helper()
	const local = `r.finished_at>0 AND r.pending_remote_count=0
      AND NOT EXISTS(SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.owner_run_id=r.id)
      AND NOT EXISTS(SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.root_run_id=r.id)
      AND NOT EXISTS(SELECT 1 FROM runs s WHERE s.scope=r.scope AND s.predecessor_run_id=r.id AND s.predecessor_run_id<>'' AND s.finished_at=0)`
	const root = `NOT EXISTS(SELECT 1 FROM runs p WHERE p.scope=r.scope AND p.id=r.root_run_id AND p.finished_at=0)`
	const session = `NOT EXISTS(SELECT 1 FROM sessions s WHERE s.scope=r.scope AND s.target_id=r.target_id AND s.predecessor_session_id=r.session_id AND s.predecessor_session_id<>'' AND s.status='queued')`
	err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		scope := engine.ScopeID(f.scope)
		var bad, actual, stored int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM runs r WHERE r.scope=? AND (r.retention_local_ok<>(`+local+`) OR r.retention_eligible<>(`+local+` AND `+root+` AND `+session+`))`, scope).Scan(&bad); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(retention_eligible),0) FROM runs WHERE scope=?`, scope).Scan(&actual); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT retention_eligible_count FROM core_data_usage WHERE scope=?),0)`, scope).Scan(&stored); err != nil {
			return err
		}
		if bad != 0 || actual != stored {
			return fmt.Errorf("invalid rows=%d actual count=%d stored=%d", bad, actual, stored)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetentionIndexMixedBatchesAndMissingRoot(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint("reverse=", reverse), func(t *testing.T) {
			f := openStore(t)
			p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
			c := childRun("child", "missing", "missing", 1)
			_, _, err := f.core.CreateRun(f.ctx, c)
			if err != nil {
				t.Fatal(err)
			}
			finish := model.RunFinish{ID: c.ID, Status: model.RunSucceeded, At: epoch}
			out, err := f.core.FinishRun(f.ctx, finish, p)
			if err != nil || !out.Retention.HasVictim {
				t.Fatalf("missing root is not a guard: %+v %v", out, err)
			}
			assertRetentionIndex(t, f)
			_, _, err = f.core.CreateRun(f.ctx, newRun("missing"))
			if err != nil {
				t.Fatal(err)
			}
			assertRetentionIndex(t, f)
			pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
			if err != nil || pruned.Deleted != 0 {
				t.Fatalf("recreated live root: %+v %v", pruned, err)
			}
			out, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: "missing", Status: model.RunSucceeded, At: epoch.Add(2 * time.Hour)}, p)
			if err != nil || !out.Retention.HasVictim || !out.Retention.EligibilityReleased {
				t.Fatalf("root release: %+v %v", out, err)
			}
			assertRetentionIndex(t, f)
			_, _, err = f.core.CreateRun(f.ctx, newRun("batch-root"))
			if err != nil {
				t.Fatal(err)
			}
			child := childRun("batch-child", "batch-root", "batch-root", 1)
			finishes := []model.RunFinish{{ID: "batch-root", Status: model.RunSucceeded, At: epoch}, {ID: child.ID, Status: model.RunSucceeded, At: epoch}}
			if reverse {
				finishes[0], finishes[1] = finishes[1], finishes[0]
			}
			result, err := f.core.ApplyRunOps(f.ctx, []model.NewRun{child}, nil, finishes, p)
			if err != nil || !result.Retention.ChangedFinish || !result.Retention.HasVictim {
				t.Fatalf("mixed batch: %+v %v", result, err)
			}
			assertRetentionIndex(t, f)
			replay, err := f.core.ApplyRunOps(f.ctx, nil, nil, finishes, p)
			if err != nil || replay.Retention != (model.RunRetentionEffect{}) {
				t.Fatalf("replay: %+v %v", replay, err)
			}
			assertRetentionIndex(t, f)
			_, _, err = f.core.CreateRun(f.ctx, newRun("rollback"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.core.ApplyRunOps(f.ctx, nil, nil, []model.RunFinish{{ID: "rollback", Status: model.RunSucceeded}, {ID: "rollback", Status: model.RunFailed}}, p)
			if err == nil {
				t.Fatal("conflicting duplicate batch committed")
			}
			assertRetentionIndex(t, f)
			r, err := f.core.GetRun(f.ctx, "rollback")
			if err != nil || r.Status != model.RunQueued {
				t.Fatalf("rollback: %+v %v", r, err)
			}
			old := newRun("old")
			_, _, err = f.core.CreateRun(f.ctx, old)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: old.ID, Status: model.RunSucceeded, At: epoch}, p)
			if err != nil {
				t.Fatal(err)
			}
			next := newRun("successor")
			next.PredecessorRunID = old.ID
			_, err = f.core.ApplyRunOps(f.ctx, []model.NewRun{next}, nil, []model.RunFinish{{ID: next.ID, Status: model.RunSucceeded, At: epoch}}, p)
			if err != nil {
				t.Fatal(err)
			}
			assertRetentionIndex(t, f)
			f.reopen(t)
			assertRetentionIndex(t, f)
		})
	}
}

func TestRetentionIndexSessionReleaseAndBootCuts(t *testing.T) {
	for _, boot := range []bool{false, true} {
		t.Run(fmt.Sprint("boot=", boot), func(t *testing.T) {
			f := openStore(t)
			p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
			old := model.NewSession{ID: "old-session", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "simulation", At: epoch}
			_, _, err := f.core.OpenSession(f.ctx, old)
			if err != nil {
				t.Fatal(err)
			}
			root := newRun("root")
			root.SessionID = old.ID
			child := childRun("child", root.ID, root.ID, 1)
			child.SessionID = old.ID
			_, _, err = f.core.CreateRun(f.ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = f.core.CreateRun(f.ctx, child)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch}, p)
			if err != nil {
				t.Fatal(err)
			}
			next := old
			next.ID = "next"
			next.PredecessorSessionID = old.ID
			_, created, err := f.core.QueueRestartSession(f.ctx, next)
			if err != nil || !created {
				t.Fatalf("queue %t %v", created, err)
			}
			_, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunSucceeded, At: epoch}, p)
			if err != nil {
				t.Fatal(err)
			}
			assertRetentionIndex(t, f)
			if boot {
				closed, err := f.core.CloseOpenSessions(f.ctx, epoch.Add(time.Minute), p)
				if err != nil || closed.Count != 2 || !closed.Retention.EligibilityReleased || !closed.Retention.HasVictim {
					t.Fatalf("boot: %+v %v", closed, err)
				}
				again, err := f.core.CloseOpenSessions(f.ctx, epoch.Add(time.Minute), p)
				if err != nil || again.Count != 0 || again.Retention != (model.RunRetentionEffect{}) {
					t.Fatalf("boot replay: %+v %v", again, err)
				}
			} else {
				if _, err = f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
				if _, err = f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p); err != nil {
					t.Fatal(err)
				}
				activated, err := f.core.ActivateRestartSession(f.ctx, next.ID, p)
				if err != nil || !activated.Retention.EligibilityReleased || !activated.Retention.HasVictim {
					t.Fatalf("activate: %+v %v", activated, err)
				}
				again, err := f.core.ActivateRestartSession(f.ctx, next.ID, p)
				if err != nil || again.Retention != (model.RunRetentionEffect{}) {
					t.Fatalf("activate replay: %+v %v", again, err)
				}
			}
			assertRetentionIndex(t, f)
			f.reopen(t)
			assertRetentionIndex(t, f)
		})
	}
}

func TestRetentionIndexNonfinalRemoteOwnerAndMissingRoot(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprint("missing=", missing), func(t *testing.T) {
			f := openStore(t)
			p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
			if !missing {
				_, _, err := f.core.CreateRun(f.ctx, newRun("root"))
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, id := range []string{"a", "b"} {
				_, _, err := f.core.CreateRun(f.ctx, childRun(id, "root", "root", 1))
				if err != nil {
					t.Fatal(err)
				}
			}
			a, b := remoteLink("a", "root", "remote-a"), remoteLink("b", "root", "remote-b")
			for _, link := range []model.RemoteRunLink{a, b} {
				if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
					t.Fatal(err)
				}
			}
			for _, id := range []string{"a", "b"} {
				if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunSucceeded, At: epoch}, p); err != nil {
					t.Fatal(err)
				}
			}
			if !missing {
				if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunSucceeded, At: epoch}, p); err != nil {
					t.Fatal(err)
				}
			}
			assertRetentionIndex(t, f)
			releaseBoundRemote(t, f, a)
			first, err := f.core.ResolveRemoteRun(f.ctx, a, p)
			if err != nil || !first.Changed || first.LastForRoot || !first.Retention.EligibilityReleased || !first.Retention.HasVictim {
				t.Fatalf("nonfinal release: %+v %v", first, err)
			}
			again, err := f.core.ResolveRemoteRun(f.ctx, a, p)
			if err != nil || again.Changed || again.Retention != (model.RunRetentionEffect{}) {
				t.Fatalf("duplicate: %+v %v", again, err)
			}
			assertRetentionIndex(t, f)
			pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
			if err != nil || pruned.Deleted != 1 || pruned.HasVictim {
				t.Fatalf("prune released owner: %+v %v", pruned, err)
			}
			if _, err = f.core.GetRun(f.ctx, "b"); err != nil {
				t.Fatal(err)
			}
			releaseBoundRemote(t, f, b)
			last, err := f.core.ResolveRemoteRun(f.ctx, b, p)
			if err != nil || !last.LastForRoot || !last.Retention.HasVictim {
				t.Fatalf("last: %+v %v", last, err)
			}
			assertRetentionIndex(t, f)
		})
	}
}

func TestRetentionIndexOldestCountContinuationAndStrictAge(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{KeepNewest: 2}
	for _, id := range []string{"e", "c", "a", "d", "b"} {
		if _, _, err := f.core.CreateRun(f.ctx, newRun(id)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunSucceeded, At: epoch}, p); err != nil {
			t.Fatal(err)
		}
	}
	assertRetentionIndex(t, f)
	first, err := f.core.PruneRuns(f.ctx, model.RunRetention{KeepNewest: 2, Limit: 2})
	if err != nil || first.Deleted != 2 || !first.HasVictim {
		t.Fatalf("first: %+v %v", first, err)
	}
	for _, id := range []string{"a", "b"} {
		if _, err = f.core.GetRun(f.ctx, id); errCode(err) != "not_found" {
			t.Fatalf("%s survived: %v", id, err)
		}
	}
	second, err := f.core.PruneRuns(f.ctx, model.RunRetention{KeepNewest: 2, Limit: 2})
	if err != nil || second.Deleted != 1 || second.HasVictim {
		t.Fatalf("second: %+v %v", second, err)
	}
	for _, id := range []string{"d", "e"} {
		if _, err = f.core.GetRun(f.ctx, id); err != nil {
			t.Fatalf("%s removed: %v", id, err)
		}
	}
	same, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch, Limit: 2})
	if err != nil || same.Deleted != 0 || same.HasVictim {
		t.Fatalf("strict age: %+v %v", same, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexBootOverlapAndNegativeFinish(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "simulation", At: epoch}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	root := newRun("root")
	root.SessionID = old.ID
	child := childRun("child", root.ID, root.ID, 1)
	child.SessionID = old.ID
	for _, r := range []model.NewRun{root, child} {
		if _, _, err := f.core.CreateRun(f.ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	link := remoteLink(root.ID, root.ID, "remote")
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	next := old
	next.ID = "next"
	next.PredecessorSessionID = old.ID
	if _, _, err := f.core.QueueRestartSession(f.ctx, next); err != nil {
		t.Fatal(err)
	}
	boot, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Minute), p)
	if err != nil || boot.Count != 1 || !boot.Retention.ChangedFinish || boot.Retention.HasVictim {
		t.Fatalf("Run boot with guards: %+v %v", boot, err)
	}
	assertRetentionIndex(t, f)
	sessions, err := f.core.CloseOpenSessions(f.ctx, epoch.Add(time.Minute), p)
	if err != nil || sessions.Count != 2 || !sessions.Retention.HasVictim {
		t.Fatalf("Session boot release: %+v %v", sessions, err)
	}
	assertRetentionIndex(t, f)
	releaseBoundRemote(t, f, link)
	released, err := f.core.ResolveRemoteRun(f.ctx, link, p)
	if err != nil || !released.LastForRoot || !released.Retention.EligibilityReleased || !released.Retention.HasVictim {
		t.Fatalf("remote boot release: %+v %v", released, err)
	}
	assertRetentionIndex(t, f)
	// The historic predicate only admits positive finished timestamps, but a
	// negative terminal timestamp still ceases to be an open-root guard.
	if _, _, err = f.core.CreateRun(f.ctx, newRun("negative-root")); err != nil {
		t.Fatal(err)
	}
	q := childRun("positive-child", "negative-root", "negative-root", 1)
	if _, _, err = f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	negative, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "negative-root", Status: model.RunSucceeded, At: time.Unix(-1, 0)}, p)
	if err != nil || !negative.Retention.EligibilityReleased {
		t.Fatalf("negative root release: %+v %v", negative, err)
	}
	assertRetentionIndex(t, f)
	// The exactly-zero stored timestamp cannot satisfy the existing terminal
	// status constraint; the attempted batch must leave the open Run intact.
	if _, _, err = f.core.CreateRun(f.ctx, newRun("zero-root")); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: "zero-root", Status: model.RunSucceeded, At: time.Unix(0, 0)}, p); err == nil {
		t.Fatal("zero terminal timestamp accepted")
	}
	assertRetentionIndex(t, f)
	r, err := f.core.GetRun(f.ctx, "zero-root")
	if err != nil || r.Status != model.RunQueued {
		t.Fatalf("zero rollback: %+v %v", r, err)
	}
}

func TestRetentionIndexQueuedCancelSkipsLiveRoot(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "simulation", At: epoch}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	root := newRun("live")
	root.SessionID = old.ID
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	blocked := childRun("blocked", "live", "live", 1)
	blocked.SessionID = old.ID
	free := newRun("free")
	free.SessionID = old.ID
	for _, q := range []model.NewRun{blocked, free} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: epoch}, p); err != nil {
			t.Fatal(err)
		}
	}
	next := old
	next.ID = "next"
	next.PredecessorSessionID = old.ID
	if _, _, err := f.core.QueueRestartSession(f.ctx, next); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
	out, err := f.core.CloseSession(f.ctx, next.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(time.Minute)}, p)
	if err != nil || !out.Retention.EligibilityReleased || !out.Retention.HasVictim {
		t.Fatalf("cancel: %+v %v", out, err)
	}
	assertRetentionIndex(t, f)
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("cancel pruned live-root child: %+v %v", pruned, err)
	}
	if _, err = f.core.GetRun(f.ctx, blocked.ID); err != nil {
		t.Fatal(err)
	}
	out, err = f.core.CloseSession(f.ctx, next.ID, model.SessionClose{Outcome: model.SessionStopped}, p)
	if err != nil || out.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("cancel replay: %+v %v", out, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexFullBatchThenRecentHasNoContinuation(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	var creates []model.NewRun
	var finishes []model.RunFinish
	for i := 0; i < 257; i++ {
		id := fmt.Sprintf("run-%03d", i)
		creates = append(creates, newRun(id))
		at := epoch
		if i == 256 {
			at = epoch.Add(2 * time.Hour)
		}
		finishes = append(finishes, model.RunFinish{ID: id, Status: model.RunSucceeded, At: at})
	}
	out, err := f.core.ApplyRunOps(f.ctx, creates, nil, finishes, p)
	if err != nil || !out.Retention.HasVictim {
		t.Fatalf("batch: %+v %v", out.Retention, err)
	}
	assertRetentionIndex(t, f)
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan, Limit: 256})
	if err != nil || pruned.Deleted != 256 || pruned.HasVictim {
		t.Fatalf("unnecessary continuation: %+v %v", pruned, err)
	}
	if _, err = f.core.GetRun(f.ctx, "run-256"); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexScopesAndConcurrentProtection(t *testing.T) {
	for i := 0; i < 8; i++ {
		f := openStore(t)
		otherScope := api.Scope{Namespace: "core", User: "other", Workspace: "station"}
		other, err := coredata.New(f.db, otherScope)
		if err != nil {
			t.Fatal(err)
		}
		otherFixture := *f
		otherFixture.scope = otherScope
		otherFixture.core = other
		for _, owner := range []*coredata.Store{f.core, other} {
			if _, _, err = owner.CreateRun(f.ctx, newRun("run")); err != nil {
				t.Fatal(err)
			}
			if _, err = owner.FinishRun(f.ctx, model.RunFinish{ID: "run", Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
		// Session and Run target/session identities are scoped in both guards.
		old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation", At: epoch}
		if _, _, err = f.core.OpenSession(f.ctx, old); err != nil {
			t.Fatal(err)
		}
		run := newRun("session-run")
		run.SessionID = old.ID
		if _, _, err = f.core.CreateRun(f.ctx, run); err != nil {
			t.Fatal(err)
		}
		if _, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
		// Synchronize the starts, not the outcome. Both complete on the same
		// real Storage writer, so either serialized cut is valid.
		next := old
		next.ID = "next"
		next.PredecessorSessionID = old.ID
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var queued bool
		var queueErr, pruneErr error
		var pruned model.RunPruneResult
		go func() { defer wg.Done(); <-start; _, queued, queueErr = f.core.QueueRestartSession(f.ctx, next) }()
		go func() {
			defer wg.Done()
			<-start
			pruned, pruneErr = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
		}()
		close(start)
		wg.Wait()
		if queueErr != nil || !queued || pruneErr != nil {
			t.Fatalf("race queue=%t %v prune=%+v %v", queued, queueErr, pruned, pruneErr)
		}
		_, err = f.core.GetRun(f.ctx, run.ID)
		if pruned.Deleted == 1 && err != nil || pruned.Deleted == 2 && errCode(err) != "not_found" || pruned.Deleted < 1 || pruned.Deleted > 2 {
			t.Fatalf("inconsistent serialized cut: deleted=%d run err=%v", pruned.Deleted, err)
		}
		if _, err = other.GetRun(f.ctx, "run"); err != nil {
			t.Fatalf("cross-scope deletion: %v", err)
		}
		assertRetentionIndex(t, f)
		assertRetentionIndex(t, &otherFixture)
	}
}

func TestRetentionIndexOneIntentAndReplayOwnerWork(t *testing.T) {
	f := openStore(t)
	q := newRun("one")
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	finish := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: epoch}
	before := stats(t, f)
	out, err := f.core.FinishRun(f.ctx, finish, p)
	if err != nil || !out.Retention.HasVictim || !out.Retention.ChangedFinish {
		t.Fatalf("finish: %+v %v", out.Retention, err)
	}
	after := stats(t, f)
	if after.Calls-before.Calls != 1 || after.CommitsDurable-before.CommitsDurable != 1 || after.CommitsRelaxed != before.CommitsRelaxed || after.Conflicts != before.Conflicts || after.Timeouts != before.Timeouts || after.WritersQueued != 0 || after.ReadersActive != 0 {
		t.Fatalf("finish owner work: before=%+v after=%+v", before, after)
	}
	replay, err := f.core.FinishRun(f.ctx, finish, p)
	if err != nil || replay.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("replay: %+v %v", replay.Retention, err)
	}
	joined := stats(t, f)
	if joined.Calls-after.Calls != 1 || joined.CommitsDurable-after.CommitsDurable != 1 || joined.CommitsRelaxed != after.CommitsRelaxed || joined.Conflicts != after.Conflicts || joined.Timeouts != after.Timeouts || joined.WritersQueued != 0 || joined.ReadersActive != 0 {
		t.Fatalf("replay owner work: after=%+v joined=%+v", after, joined)
	}
	t.Logf("one changed finish: calls=+%d durable=+%d relaxed=+%d conflicts=+%d timeouts=+%d; replay: calls=+%d durable=+%d relaxed=+%d; joined writers/readers=%d/%d", after.Calls-before.Calls, after.CommitsDurable-before.CommitsDurable, after.CommitsRelaxed-before.CommitsRelaxed, after.Conflicts-before.Conflicts, after.Timeouts-before.Timeouts, joined.Calls-after.Calls, joined.CommitsDurable-after.CommitsDurable, joined.CommitsRelaxed-after.CommitsRelaxed, joined.WritersQueued, joined.ReadersActive)
	assertRetentionIndex(t, f)
}

func TestRetentionIndexPruneFailsClosedOnProtectionOrQuotaMismatch(t *testing.T) {
	for _, corrupt := range []string{"guard", "count"} {
		t.Run(corrupt, func(t *testing.T) {
			f := openStore(t)
			p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
			if corrupt == "guard" {
				if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
					t.Fatal(err)
				}
				child := childRun("child", "root", "root", 1)
				if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
					t.Fatal(err)
				}
				if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch}, p); err != nil {
					t.Fatal(err)
				}
				// Test-only corruption: make the derived flag and count agree even
				// though the real live-root guard still prohibits this deletion.
				f.exec(t, `UPDATE runs SET retention_eligible=1 WHERE id='child'`)
				f.exec(t, `UPDATE core_data_usage SET retention_eligible_count=1`)
			} else {
				if _, _, err := f.core.CreateRun(f.ctx, newRun("child")); err != nil {
					t.Fatal(err)
				}
				if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "child", Status: model.RunSucceeded, At: epoch}, p); err != nil {
					t.Fatal(err)
				}
				f.exec(t, `UPDATE core_data_usage SET retention_eligible_count=0`)
			}
			rowsBefore, bytesBefore := usageRows(t, f)
			out, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
			if errCode(err) != "data_loss" || out != (model.RunPruneResult{}) {
				t.Fatalf("mismatch: %+v %v", out, err)
			}
			if _, err = f.core.GetRun(f.ctx, "child"); err != nil {
				t.Fatalf("failed prune deleted Run: %v", err)
			}
			rowsAfter, bytesAfter := usageRows(t, f)
			if rowsAfter != rowsBefore || bytesAfter != bytesBefore {
				t.Fatal("failed prune changed quota")
			}
		})
	}
}

func TestRetentionIndexInvalidPolicyHasNoOwnerCall(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("run")); err != nil {
		t.Fatal(err)
	}
	before := stats(t, f)
	for _, p := range []model.RunRetentionPolicy{{KeepNewest: -1}, {OlderThan: time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)}} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "run", Status: model.RunSucceeded}, p); errCode(err) != "invalid_argument" {
			t.Fatalf("invalid policy: %+v %v", p, err)
		}
	}
	after := stats(t, f)
	if after.Calls != before.Calls || after.CommitsDurable != before.CommitsDurable {
		t.Fatal("invalid policy entered the owner")
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexRemoteOwnerStillPendingAndLiveRoot(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.CreateRun(f.ctx, childRun("owner", "root", "root", 1)); err != nil {
		t.Fatal(err)
	}
	a, b := remoteLink("owner", "root", "remote-a"), remoteLink("owner", "root", "remote-b")
	for _, link := range []model.RemoteRunLink{a, b} {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "owner", Status: model.RunSucceeded, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	releaseBoundRemote(t, f, a)
	first, err := f.core.ResolveRemoteRun(f.ctx, a, p)
	if err != nil || !first.Changed || first.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("other own link: %+v %v", first, err)
	}
	releaseBoundRemote(t, f, b)
	second, err := f.core.ResolveRemoteRun(f.ctx, b, p)
	if err != nil || !second.LastForRoot || second.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("live root: %+v %v", second, err)
	}
	assertRetentionIndex(t, f)
	finish, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunSucceeded, At: epoch}, p)
	if err != nil || !finish.Retention.HasVictim {
		t.Fatalf("root release: %+v %v", finish, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexFirstRemoteRelationRevokesFinishedRoot(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.CreateRun(f.ctx, childRun("owner", "root", "root", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunSucceeded, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
	a, b := remoteLink("owner", "root", "remote-a"), remoteLink("owner", "root", "remote-b")
	if err := f.core.ReserveRemoteRun(f.ctx, a); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
	for _, link := range []model.RemoteRunLink{a, b} {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	assertRetentionIndex(t, f)
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("pending root pruned: %+v %v", pruned, err)
	}
	releaseBoundRemote(t, f, a)
	if _, err = f.core.ResolveRemoteRun(f.ctx, a, p); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
	releaseBoundRemote(t, f, b)
	last, err := f.core.ResolveRemoteRun(f.ctx, b, p)
	if err != nil || !last.Retention.EligibilityReleased || !last.Retention.HasVictim {
		t.Fatalf("root released: %+v %v", last, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexPlannedFinishFailureAndCreateReplay(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	root := newRun("root")
	root.IdempotencyKey = "start"
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	child := childRun("child", root.ID, root.ID, 1)
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	_, err := f.core.ApplyRunOps(f.ctx, nil, nil, []model.RunFinish{{ID: child.ID, Status: model.RunSucceeded, At: epoch}, {ID: root.ID, Status: model.RunRunning, At: epoch}}, p)
	if errCode(err) != "invalid_argument" {
		t.Fatalf("invalid planned root: %v", err)
	}
	for _, id := range []string{root.ID, child.ID} {
		r, err := f.core.GetRun(f.ctx, id)
		if err != nil || r.Status != model.RunQueued {
			t.Fatalf("partial finish %s: %+v %v", id, r, err)
		}
	}
	assertRetentionIndex(t, f)
	replay := newRun("different-id")
	replay.IdempotencyKey = root.IdempotencyKey
	out, err := f.core.ApplyRunOps(f.ctx, []model.NewRun{replay}, nil, []model.RunFinish{{ID: root.ID, Status: model.RunSucceeded, At: epoch}, {ID: child.ID, Status: model.RunSucceeded, At: epoch}}, p)
	if err != nil || len(out.Created) != 1 || out.Created[0].Created || out.Created[0].Run.ID != root.ID || !out.Retention.HasVictim {
		t.Fatalf("replayed create with planned finish: %+v %v", out, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexAnyRunIdentityCanGuardReferencedRoot(t *testing.T) {
	f := openStore(t)
	p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
	dependent := childRun("dependent", "parent", "child-identity", 1)
	if _, _, err := f.core.CreateRun(f.ctx, dependent); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: dependent.ID, Status: model.RunSucceeded, At: epoch}, p); err != nil {
		t.Fatal(err)
	}
	// validateNewRun has no root foreign key or requirement that a referenced
	// root identity itself be a root-shaped Run.
	childIdentity := childRun("child-identity", "upstream", "other-root", 1)
	if _, _, err := f.core.CreateRun(f.ctx, childIdentity); err != nil {
		t.Fatal(err)
	}
	assertRetentionIndex(t, f)
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: p.OlderThan})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("live referenced child identity: %+v %v", pruned, err)
	}
	out, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: childIdentity.ID, Status: model.RunSucceeded, At: epoch}, p)
	if err != nil || !out.Retention.HasVictim {
		t.Fatalf("referenced child release: %+v %v", out, err)
	}
	assertRetentionIndex(t, f)
}

func TestRetentionIndexPlannedSuccessorsAcrossTargets(t *testing.T) {
	for _, finishBoth := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("finish-both=%t/reverse=%t", finishBoth, reverse), func(t *testing.T) {
				f := openStore(t)
				p := model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)}
				predecessor := newRun("predecessor")
				one := newRun("one")
				one.PredecessorRunID = predecessor.ID
				two := newRun("two")
				two.PredecessorRunID = predecessor.ID
				two.TargetID = "other-target"
				for _, q := range []model.NewRun{predecessor, one, two} {
					if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
						t.Fatal(err)
					}
				}
				finishes := []model.RunFinish{{ID: predecessor.ID, Status: model.RunStopped, At: epoch}, {ID: one.ID, Status: model.RunStopped, At: epoch}}
				if finishBoth {
					finishes = append(finishes, model.RunFinish{ID: two.ID, Status: model.RunStopped, At: epoch})
				}
				if reverse {
					for i, j := 0, len(finishes)-1; i < j; i, j = i+1, j-1 {
						finishes[i], finishes[j] = finishes[j], finishes[i]
					}
				}
				if _, err := f.core.ApplyRunOps(f.ctx, nil, nil, finishes, p); err != nil {
					t.Fatal(err)
				}
				assertRetentionIndex(t, f)
				var eligible bool
				if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, `SELECT retention_eligible FROM runs WHERE scope=? AND id=?`, engine.ScopeID(f.scope), predecessor.ID).Scan(&eligible)
				}); err != nil {
					t.Fatal(err)
				}
				if eligible != finishBoth {
					t.Fatalf("predecessor eligible=%t while finish-both=%t", eligible, finishBoth)
				}
				if !finishBoth {
					if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: two.ID, Status: model.RunStopped, At: epoch}, p); err != nil {
						t.Fatal(err)
					}
					assertRetentionIndex(t, f)
				}
			})
		}
	}
}
