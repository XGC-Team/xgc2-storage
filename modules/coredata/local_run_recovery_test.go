package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func admittedLocalFixture(t *testing.T) (*store, model.NewRun, model.NewRun) {
	t.Helper()
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	parent := incomingChild("parent", "root", "root", "core", "principal")
	parent.RecoveryAgentBootID = "agent-boot"
	if _, _, err := f.core.CreateRun(f.ctx, parent); err != nil {
		t.Fatal(err)
	}
	child := childRun("local-child", parent.ID, parent.RootRunID, 2)
	child.RecoveryAgentBootID = parent.RecoveryAgentBootID
	if _, _, err := f.core.CreateRun(f.ctx, child); errCode(err) != "failed_precondition" {
		t.Fatalf("child accepted before its exact admission: %v", err)
	}
	if err := f.core.AdmitRunResource(f.ctx, parent.ID, parent.RecoveryAgentBootID,
		model.RunResourceAdmission{ID: child.ID, NodeID: child.CallNodeID, Kind: "local-call"}); err != nil {
		t.Fatal(err)
	}
	wrong := child
	wrong.Depth++
	if _, _, err := f.core.CreateRun(f.ctx, wrong); errCode(err) != "failed_precondition" {
		t.Fatalf("child with wrong depth accepted under the exact admission: %v", err)
	}
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	return f, parent, child
}

func TestLocalChildJoinRequiresActualTrackedCleanChild(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	if _, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("still-running child was joined: %v", err)
	}
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID, model.RunResourceAdmission{ID: "command", NodeID: "node", Kind: "command"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunInterrupted, At: epoch, RecoveryAgentBootID: child.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	proof := model.RunResourceJoin{RunID: child.ID, NodeID: "node", ResourceID: "command", Kind: "command", AgentBootID: child.RecoveryAgentBootID,
		Incarnation: "old", HostBootID: "os", PID: 23, PGID: 23, StartTicks: 31, OwnerPID: 1, OwnerStartTicks: 1}
	if _, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, "foreign-boot", model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign boot accepted: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: parent.ID, Status: model.RunInterrupted, At: epoch.Add(time.Second), RecoveryAgentBootID: parent.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if wake, err := f.core.JoinRunResource(f.ctx, proof, model.RunRetentionPolicy{}); err != nil || len(wake.Recovered) != 0 {
		t.Fatalf("resource alone became physical child join: %+v %v", wake, err)
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || state.Joined || state.Resources[0].LocalPhysicalJoined {
		t.Fatalf("terminal and resource proof fabricated local owner join: %+v %v", state, err)
	}
	wake, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{})
	if err != nil || len(wake.Recovered) != 1 || wake.Recovered[0].ID != parent.ID || wake.Recovered[0].Status != model.RunInterrupted {
		t.Fatalf("exact physically joined child did not seal its parent: %+v %v", wake, err)
	}
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !found || !state.Joined || len(state.Resources) != 1 || !state.Resources[0].LocalJoined {
		t.Fatalf("local join not durable: %+v %v %v", state, found, err)
	}
	replay, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{})
	if err != nil || len(replay.Recovered) != 1 || replay.Recovered[0].ID != parent.ID {
		t.Fatalf("settled exact join replay did not return the already sealed owner: %+v %v", replay, err)
	}
	f.reopen(t)
	state, found, err = f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !found || !state.Joined || !state.Resources[0].LocalJoined {
		t.Fatalf("local join lost across reopen: %+v %v %v", state, found, err)
	}
}

func TestLocalChildUnknownEffectDoesNotBecomeJoined(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID, model.RunResourceAdmission{ID: "unknown", NodeID: "node", Kind: "uncovered-effect"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch, RecoveryAgentBootID: child.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if wake, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{}); err != nil || len(wake.Recovered) != 0 {
		t.Fatalf("physical join could not be retained: %+v %v", wake, err)
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !state.Resources[0].LocalPhysicalJoined || state.Resources[0].LocalJoined {
		t.Fatalf("unknown child was recorded joined: %+v %v", state, err)
	}
}

func TestPhysicallyJoinedLocalChildWaitsForItsExactRemoteRelation(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID,
		model.RunResourceAdmission{ID: "remote-call", NodeID: "call", Kind: "workflow-call"}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: child.ID, RootRunID: parent.RootRunID, TargetID: "other-agent", RunID: "remote-child", CallNodeID: "call", BootID: "other-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	for _, run := range []model.NewRun{child, parent} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunInterrupted, At: epoch, RecoveryAgentBootID: run.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	// The real local owner has joined, but its independently owned remote
	// relation has not. Record the former without claiming the latter joined.
	if wake, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{}); err != nil || len(wake.Recovered) != 0 {
		t.Fatalf("physical child join was not retained while remote proof is pending: wake=%+v err=%v", wake, err)
	}
	f.reopen(t)
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || state.Joined || !state.Resources[0].LocalPhysicalJoined || state.Resources[0].LocalJoined {
		t.Fatalf("pending remote relation was hidden: %+v %v", state, err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "canceled", FinishedAt: epoch.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.RecoveredAncestors == nil || len(*result.RecoveredAncestors) != 1 || (*result.RecoveredAncestors)[0].ID != parent.ID || (*result.RecoveredAncestors)[0].Status != model.RunInterrupted {
		t.Fatalf("remote closure did not report its exact newly joined local caller: %+v", result.RecoveredAncestors)
	}
	state, _, err = f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !state.Joined || !state.Resources[0].LocalJoined {
		t.Fatalf("exact remote closure failed to complete the earlier physical join: %+v %v", state, err)
	}
}

func TestPhysicallyJoinedLocalChildWaitsForExactProcessReceipt(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID, model.RunResourceAdmission{ID: "p", NodeID: "node", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	for _, run := range []model.NewRun{child, parent} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunInterrupted, At: epoch, RecoveryAgentBootID: run.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if wake, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{}); err != nil || len(wake.Recovered) != 0 {
		t.Fatalf("physical join hid a missing process receipt: %+v %v", wake, err)
	}
	f.reopen(t)
	proof := model.RunResourceJoin{RunID: child.ID, NodeID: "node", ResourceID: "p", Kind: "process", AgentBootID: child.RecoveryAgentBootID,
		Incarnation: "old", HostBootID: "os", PID: 23, PGID: 23, StartTicks: 31, OwnerPID: 1, OwnerStartTicks: 1}
	wake, err := f.core.JoinRunResource(f.ctx, proof, model.RunRetentionPolicy{})
	if err != nil || len(wake.Recovered) != 1 || wake.Recovered[0].ID != parent.ID {
		t.Fatalf("exact process receipt did not complete the earlier physical join: %+v %v", wake, err)
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, parent.ID)
	if err != nil || !state.Joined || !state.Resources[0].LocalJoined {
		t.Fatalf("local caller remains incomplete: %+v %v", state, err)
	}
}

func TestLocalPhysicalJoinsCompleteOutOfOrderAcrossExactLineage(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	grandchild := childRun("grandchild", child.ID, parent.RootRunID, 3)
	grandchild.RecoveryAgentBootID = parent.RecoveryAgentBootID
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID,
		model.RunResourceAdmission{ID: grandchild.ID, NodeID: grandchild.CallNodeID, Kind: "local-call"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.CreateRun(f.ctx, grandchild); err != nil {
		t.Fatal(err)
	}
	for _, run := range []model.NewRun{grandchild, child, parent} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunInterrupted, At: epoch, RecoveryAgentBootID: run.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	if wake, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{}); err != nil || len(wake.Recovered) != 0 {
		t.Fatalf("child hid its unjoined descendant: %+v %v", wake, err)
	}
	wake, err := f.core.JoinLocalRun(f.ctx, child.ID, grandchild.CallNodeID, grandchild.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{})
	if err != nil || len(wake.Recovered) != 2 || wake.Recovered[0].ID != child.ID || wake.Recovered[1].ID != parent.ID {
		t.Fatalf("exact child-to-parent completion did not cascade: %+v %v", wake, err)
	}
}

func TestLocalChildRetentionWaitsForItsExactOwnerJoin(t *testing.T) {
	f, parent, child := admittedLocalFixture(t)
	// The root may be on another target, or already finished. The incoming
	// parent is still live and must retain the selected child until its join.
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: parent.RootRunID, Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: child.ID, Status: model.RunSucceeded, At: epoch, RecoveryAgentBootID: child.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.GetRun(f.ctx, child.ID); err != nil {
		t.Fatalf("unjoined local child was removed before its exact owner could join it: %v", err)
	}
	joined, err := f.core.JoinLocalRun(f.ctx, parent.ID, child.CallNodeID, child.ID, parent.RecoveryAgentBootID, model.RunRetentionPolicy{OlderThan: epoch.Add(time.Hour)})
	if err != nil || !joined.Retention.EligibilityReleased || !joined.Retention.HasVictim {
		t.Fatalf("child join did not report the actual newly eligible retention victim: %+v %v", joined.Retention, err)
	}
	if _, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.GetRun(f.ctx, child.ID); errCode(err) != "not_found" {
		t.Fatalf("fully joined child remained unnecessarily retained: %v", err)
	}
}
