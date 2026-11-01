package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The tracked create committed before any possible effect. An empty covered
// admission list therefore differs from a legacy row with no coverage fact.
func TestInterruptedTrackedRunBeforeFirstEffectCanRelease(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("before-effect", "parent", "parent", "core", "principal")
	child.RecoveryAgentBootID = "original-agent"
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	legacy := incomingChild("legacy", "parent", "parent", "core", "principal")
	if _, _, err := f.core.CreateRun(f.ctx, legacy); err != nil {
		t.Fatal(err)
	}
	pending := incomingChild("pending", "parent", "parent", "core", "principal")
	pending.RecoveryAgentBootID = child.RecoveryAgentBootID
	if _, _, err := f.core.CreateRun(f.ctx, pending); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: pending.ID, RootRunID: "parent", TargetID: "other-agent", RunID: "grandchild", CallNodeID: "call-out"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, child.ID)
	if err != nil || !found || !state.Joined || len(state.Resources) != 0 || state.AgentBootID != child.RecoveryAgentBootID {
		t.Fatalf("covered no-effect interruption did not join: %+v found=%v err=%v", state, found, err)
	}
	want := releaseWant(child)
	want.AgentBootID = child.RecoveryAgentBootID
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, child.ID, want); err != nil || !changed {
		t.Fatalf("covered no-effect incoming hold did not release: changed=%v err=%v", changed, err)
	}
	legacyWant := releaseWant(legacy)
	legacyWant.AgentBootID = child.RecoveryAgentBootID
	if _, err := f.core.ReleaseIncomingRun(f.ctx, legacy.ID, legacyWant); errCode(err) != "failed_precondition" {
		t.Fatalf("legacy Run without coverage was treated as empty: %v", err)
	}
	pendingWant := releaseWant(pending)
	pendingWant.AgentBootID = pending.RecoveryAgentBootID
	if _, err := f.core.ReleaseIncomingRun(f.ctx, pending.ID, pendingWant); errCode(err) != "failed_precondition" {
		t.Fatalf("empty local coverage erased a real outbound obligation: %v", err)
	}
	if _, err := f.core.ResolveUnsentRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, pending.ID, pending.RecoveryAgentBootID); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, pending.ID, pendingWant); err != nil || !changed {
		t.Fatalf("actually joined outbound obligation did not release: changed=%v err=%v", changed, err)
	}
	old, err := f.core.GetRun(f.ctx, child.ID)
	if err != nil || old.Status != model.RunInterrupted {
		t.Fatalf("recovery changed the business outcome: %+v %v", old, err)
	}
}
