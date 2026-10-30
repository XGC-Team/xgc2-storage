package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRestartSessionReservationPromotionAndRemoteRetention(t *testing.T) {
	f := openStore(t)
	old := model.NewSession{ID: "old-session", TargetID: "local", ExperimentResourceID: "experiment", ExperimentCommitID: "old-commit", RunMode: "simulation", At: epoch}
	if _, created, err := f.core.OpenSession(f.ctx, old); err != nil || !created {
		t.Fatalf("open: %t %v", created, err)
	}
	run := newRun("old-run")
	run.SessionID = old.ID
	if _, _, err := f.core.CreateRun(f.ctx, run); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: run.ID, RootRunID: run.ID, TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunStopped, At: epoch.Add(time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	next := model.NewSession{ID: "next", TargetID: "local", PredecessorSessionID: old.ID, ExperimentResourceID: "experiment", ExperimentCommitID: "frozen-next", RunMode: "simulation", At: epoch.Add(time.Second)}
	queued, created, err := f.core.QueueRestartSession(f.ctx, next)
	if err != nil || !created || queued.Status != model.SessionQueued || queued.PredecessorSessionID != old.ID {
		t.Fatalf("queued: %+v %t %v", queued, created, err)
	}
	other := next
	other.ID = "other"
	winner, created, err := f.core.QueueRestartSession(f.ctx, other)
	if err != nil || created || winner.ID != next.ID {
		t.Fatalf("single winner: %+v %t %v", winner, created, err)
	}
	if _, err = f.core.ActivateRestartSession(f.ctx, next.ID, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("premature promotion: %v", err)
	}
	if _, err = f.core.RequestStop(f.ctx, old.ID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped, At: epoch.Add(2 * time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("queued dependency pruned: %+v %v", pruned, err)
	}
	if _, err = f.core.ActivateRestartSession(f.ctx, next.ID, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("pending remote promoted: %v", err)
	}
	releaseBoundRemote(t, f, link)
	if _, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	active, err := f.core.ActivateRestartSession(f.ctx, next.ID, model.RunRetentionPolicy{})
	if err != nil || active.Status != model.SessionOpen || active.PredecessorSessionID != old.ID || active.ExperimentCommitID != "frozen-next" {
		t.Fatalf("active: %+v %v", active, err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("joined history retained: %+v %v", pruned, err)
	}
}

func TestRestartSessionCancelAndBootInterruption(t *testing.T) {
	f := openStore(t)
	q := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation"}
	if _, _, err := f.core.OpenSession(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	q.ID = "queued"
	q.PredecessorSessionID = "old"
	if _, _, err := f.core.QueueRestartSession(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if n, err := f.core.CloseOpenSessions(f.ctx, epoch, model.RunRetentionPolicy{}); err != nil || n.Count != 2 {
		t.Fatalf("boot: %d %v", n.Count, err)
	}
	got, err := f.core.GetSession(f.ctx, "queued")
	if err != nil || got.Status != model.SessionInterrupted || got.PredecessorSessionID != "old" {
		t.Fatalf("interrupted: %+v %v", got, err)
	}
}

func TestRestartSessionRejectsFailedPredecessorCleanup(t *testing.T) {
	f := openStore(t)
	old := model.NewSession{ID: "old", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation"}
	if _, _, err := f.core.OpenSession(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	run := newRun("dirty")
	run.SessionID = old.ID
	if _, _, err := f.core.CreateRun(f.ctx, run); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunStopped, CleanupErrors: []byte(`[{"message":"teardown failed"}]`)}, model.RunRetentionPolicy{}); err != nil {
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
	if _, err := f.core.CloseSession(f.ctx, old.ID, model.SessionClose{Outcome: model.SessionStopped}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ActivateRestartSession(f.ctx, next.ID, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("dirty Session promoted: %v", err)
	}
}
