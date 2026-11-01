package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestInterruptedNestedRemoteCallsSealOnlyAfterEveryExactChildJoins(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	parent := incomingChild("agent-parent", "root", "root", "core", "principal")
	parent.RecoveryAgentBootID = "original-agent-boot"
	if _, _, err := f.core.CreateRun(f.ctx, parent); err != nil {
		t.Fatal(err)
	}
	for _, admission := range []model.RunResourceAdmission{
		{ID: "serve", NodeID: "serve", Kind: "process"},
		{ID: "call-owned", NodeID: "call", Kind: "workflow-call"},
	} {
		if err := f.core.AdmitRunResource(f.ctx, parent.ID, parent.RecoveryAgentBootID, admission); err != nil {
			t.Fatal(err)
		}
	}
	links := []model.RemoteRunLink{
		{OwnerRunID: parent.ID, RootRunID: "root", TargetID: "agent-1", RunID: "remote-1", CallNodeID: "call", BootID: "old-1"},
		{OwnerRunID: parent.ID, RootRunID: "root", TargetID: "agent-2", RunID: "remote-2", CallNodeID: "call", BootID: "old-2"},
	}
	for _, link := range links {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t)
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	proof := model.RunResourceJoin{RunID: parent.ID, NodeID: "serve", ResourceID: "serve", Kind: "process", AgentBootID: parent.RecoveryAgentBootID,
		Incarnation: "old-process", HostBootID: "os", PID: 23, PGID: 23, StartTicks: 31, OwnerPID: 1, OwnerStartTicks: 1}
	if err := f.core.JoinRunResource(f.ctx, proof); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, parent.ID, parent.RecoveryAgentBootID); errCode(err) != "failed_precondition" {
		t.Fatalf("live children were skipped: %v", err)
	}
	for index, link := range links {
		disposition := []string{"failed", "canceled"}[index]
		if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: disposition, FinishedAt: epoch.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
			t.Fatalf("settlement without exact child release resolved: %v", err)
		}
		if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		result, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 && result.RecoveredOwner != nil || index == len(links)-1 && (result.RecoveredOwner == nil || result.RecoveredOwner.ID != parent.ID || result.RecoveredOwner.Status != model.RunInterrupted || len(result.RecoveredOwner.Nodes) != 0) {
			t.Fatalf("resolution returned the wrong bounded owner notification: %+v", result.RecoveredOwner)
		}
		state, found, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
		if err != nil || !found || state.Joined != (index == len(links)-1) {
			t.Fatalf("resolved %d child calls: joined=%v found=%v error=%v", index+1, state.Joined, found, err)
		}
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !found || !state.Joined {
		t.Fatalf("complete exact-child receipt did not survive restart: %+v %v %v", state, found, err)
	}
	run, err := f.core.GetRun(f.ctx, parent.ID)
	if err != nil || run.Status != model.RunInterrupted {
		t.Fatalf("business interruption was changed: %+v %v", run, err)
	}
}

func TestNestedCallDoesNotHideUnjoinedLocalChild(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	parent := incomingChild("parent", "root", "root", "core", "principal")
	parent.RecoveryAgentBootID = "boot"
	if _, _, err := f.core.CreateRun(f.ctx, parent); err != nil {
		t.Fatal(err)
	}
	for _, a := range []model.RunResourceAdmission{{ID: "call", NodeID: "call", Kind: "workflow-call"}, {ID: "local-child", NodeID: "call", Kind: "uncovered-effect"}} {
		if err := f.core.AdmitRunResource(f.ctx, parent.ID, "boot", a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, parent.ID, "boot"); errCode(err) != "failed_precondition" {
		t.Fatalf("unknown local child became joined merely because no remote relation remains: %v", err)
	}
}

func TestIncomingReleaseWaitsForExactNestedRemoteClosure(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	parent := incomingChild("parent", "root", "root", "core", "principal")
	if _, _, err := f.core.CreateRun(f.ctx, parent); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: parent.ID, RootRunID: "root", TargetID: "other-agent", RunID: "nested", CallNodeID: "nested-call", BootID: "other-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: parent.ID, Status: model.RunCanceled, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, parent.ID, releaseWant(parent)); changed || errCode(err) != "failed_precondition" {
		t.Fatalf("terminal parent released while nested child remained owned: changed=%v err=%v", changed, err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "canceled", FinishedAt: epoch.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, parent.ID, releaseWant(parent)); !changed || err != nil {
		t.Fatalf("parent could not release after exact nested child joined: changed=%v err=%v", changed, err)
	}
}
