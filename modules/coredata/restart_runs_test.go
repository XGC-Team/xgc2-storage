package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRestartRunIdentityReservationAndRetention(t *testing.T) {
	f := openStore(t)
	predecessor := newRun("old")
	if _, _, err := f.core.CreateRun(f.ctx, predecessor); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "old", Status: model.RunStopped, At: epoch.Add(time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	successor := newRun("new")
	successor.IdempotencyKey = "restart-key"
	successor.PredecessorRunID = "old"
	accepted, created, err := f.core.CreateRun(f.ctx, successor)
	if err != nil || !created || accepted.PredecessorRunID != "old" || accepted.Status != model.RunQueued {
		t.Fatalf("accept: %+v %t %v", accepted, created, err)
	}
	for _, projection := range []model.RunProjection{model.RunIdentity, model.RunObservation} {
		page, err := f.core.ListRuns(f.ctx, model.RunFilter{ID: "new", Limit: 1, Projection: projection})
		if err != nil || len(page.Runs) != 1 || page.Runs[0].PredecessorRunID != "old" {
			t.Fatalf("projection %v: %+v %v", projection, page, err)
		}
	}
	replay, created, err := f.core.CreateRun(f.ctx, successor)
	if err != nil || created || replay.ID != "new" || replay.PredecessorRunID != "old" {
		t.Fatalf("replay: %+v %t %v", replay, created, err)
	}
	competing := newRun("different")
	competing.IdempotencyKey, competing.PredecessorRunID = "different-key", "old"
	winner, created, err := f.core.CreateRun(f.ctx, competing)
	if err != nil || created || winner.ID != "new" || winner.IdempotencyKey != "restart-key" {
		t.Fatalf("winner: %+v %t %v", winner, created, err)
	}
	child := childRun("child", "old", "old", 1)
	child.PredecessorRunID = "old"
	if _, _, err := f.core.CreateRun(f.ctx, child); errCode(err) != "invalid_argument" {
		t.Fatalf("child accepted: %v", err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("pruned live dependency: %+v %v", pruned, err)
	}
	f.reopen(t)
	got, err := f.core.GetRun(f.ctx, "new")
	if err != nil || got.PredecessorRunID != "old" {
		t.Fatalf("reopen: %+v %v", got, err)
	}
	interrupted, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(2*time.Second), model.RunRetentionPolicy{})
	if err != nil || interrupted.Count != 1 {
		t.Fatalf("boot interruption: %d %v", interrupted.Count, err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 2 {
		t.Fatalf("terminal history: %+v %v", pruned, err)
	}
}
