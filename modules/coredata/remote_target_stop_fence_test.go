package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestStoppedRemoteOwnerFencesOnlyItsTargetUntilJoined(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"old", "next", "concurrent"} {
		if _, _, err := f.core.CreateRun(f.ctx, newRun(id)); err != nil {
			t.Fatal(err)
		}
	}
	old := model.RemoteRunLink{OwnerRunID: "old", RootRunID: "old", TargetID: "offline", RunID: "old-child", CallNodeID: "call", BootID: "old-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, old); err != nil {
		t.Fatal(err)
	}
	// Independent concurrent callers are still valid before Stop.
	concurrent := model.RemoteRunLink{OwnerRunID: "concurrent", RootRunID: "concurrent", TargetID: "offline", RunID: "parallel-child", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, concurrent); err != nil {
		t.Fatalf("live independent call: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "old", Status: model.RunStopped, At: epoch.Add(time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	next := model.RemoteRunLink{OwnerRunID: "next", RootRunID: "next", TargetID: "offline", RunID: "new-child", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, next); errCode(err) != "failed_precondition" {
		t.Fatalf("offline target accepted a conflicting new call: %v", err)
	}
	other := next
	other.TargetID, other.RunID = "independent-docker", "docker-child"
	if err := f.core.ReserveRemoteRun(f.ctx, other); err != nil {
		t.Fatalf("unrelated target was fenced: %v", err)
	}
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "offline", "", 20)
	if err != nil || len(page.Links) != 2 {
		t.Fatalf("offline relations lost: %+v %v", page, err)
	}
	releaseBoundRemote(t, f, old)
	if err := f.core.ReserveRemoteRun(f.ctx, next); errCode(err) != "failed_precondition" {
		t.Fatalf("retained relation was ignored before owner resolution: %v", err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, old, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.ReserveRemoteRun(f.ctx, next); err != nil {
		t.Fatalf("joined target remained fenced: %v", err)
	}
}

func TestRootStopIntentSurvivesCoreInterruption(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"stopped", "only-interrupted"} {
		session := model.NewSession{ID: id, TargetID: "core-" + id, ExperimentResourceID: "e", ExperimentCommitID: "c", RunMode: "simulation"}
		if _, _, err := f.core.OpenSession(f.ctx, session); err != nil {
			t.Fatal(err)
		}
		run := newRun("root-" + id)
		run.SessionID = id
		if _, _, err := f.core.CreateRun(f.ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.core.RequestStop(f.ctx, "stopped", []byte(`{"outcome":"stopped"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.CloseOpenSessions(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	for _, tc := range []struct {
		root string
		want bool
	}{{"root-stopped", true}, {"root-only-interrupted", false}} {
		got, err := f.core.HasRootStopIntent(f.ctx, tc.root)
		if err != nil || got != tc.want {
			t.Fatalf("root %s stop=%v want=%v err=%v", tc.root, got, tc.want, err)
		}
	}
}
