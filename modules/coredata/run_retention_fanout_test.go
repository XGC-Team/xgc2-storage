package coredata_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Each child is created and completed before the next. A live root can
// therefore protect more durable history than the 256-row prune batch.
func TestRootRetentionFanoutAcrossReopenAndProtectionChanges(t *testing.T) {
	f := openStore(t)
	old := model.NewSession{ID: "session", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "simulation", At: epoch}
	if _, created, err := f.core.OpenSession(f.ctx, old); err != nil || !created {
		t.Fatalf("open session: created=%t err=%v", created, err)
	}
	root := newRun("root")
	root.SessionID = old.ID
	if _, created, err := f.core.CreateRun(f.ctx, root); err != nil || !created {
		t.Fatalf("create root: created=%t err=%v", created, err)
	}
	const children = 257
	for i := range children {
		child := childRun(fmt.Sprintf("child-%03d", i), root.ID, root.ID, 1)
		child.SessionID = old.ID
		finish := model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch.Add(time.Second + time.Duration(i))}
		created, err := f.core.ApplyRunOps(f.ctx, []model.NewRun{child}, nil, []model.RunFinish{finish})
		if err != nil || len(created) != 1 || !created[0].Created {
			t.Fatalf("child %d: %+v %v", i, created, err)
		}
	}
	policy := model.RunRetention{OlderThan: epoch.Add(2 * time.Hour), Limit: 256}
	prune := func(want int) {
		t.Helper()
		got, err := f.core.PruneRuns(f.ctx, policy)
		if err != nil || got.Deleted != want {
			t.Fatalf("prune deleted=%d want=%d err=%v", got.Deleted, want, err)
		}
	}
	prune(0)
	f.reopen(t)
	prune(0)
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunSucceeded, At: epoch.Add(3 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	prune(256)
	if got := f.count(t, "runs"); got != 2 {
		t.Fatalf("after first batch got %d Runs, want root plus one child", got)
	}
	// A real new protection can arrive between deletion batches and must be
	// rechecked after reopen, rather than trusted from an earlier finish cut.
	next := old
	next.ID, next.PredecessorSessionID = "next", old.ID
	if _, created, err := f.core.QueueRestartSession(f.ctx, next); err != nil || !created {
		t.Fatalf("queue successor: created=%t err=%v", created, err)
	}
	f.reopen(t)
	prune(0)
	if _, err := f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(4 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ActivateRestartSession(f.ctx, next.ID); err != nil {
		t.Fatal(err)
	}
	prune(1)
	if got := f.count(t, "runs"); got != 1 {
		t.Fatalf("after releasing protection got %d Runs, want recent root", got)
	}
	t.Logf("%d sequential descendants; prune counts live/reopen/root finish/queued/release: 0/0/256/0/1", children)
}
