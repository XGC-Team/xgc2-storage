package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A replacement kernel ended the local execution scope even if the agent
// crashed after admitting a process but before publishing its PID record.
// Remote work and resources whose scope was never recorded remain separate.
func TestInterruptedKernelScopeCompletesAdmittedProcess(t *testing.T) {
	f := openStore(t)
	child := incomingChild("child", "parent", "parent", "core", "core-principal")
	child.RecoveryAgentBootID = "old-agent"
	child.RecoveryKernelBootID = "old-kernel"
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID, model.RunResourceAdmission{ID: "process", NodeID: "start", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	want := releaseWant(child)
	want.AgentBootID = child.RecoveryAgentBootID
	if _, err := f.core.ReleaseIncomingRun(f.ctx, child.ID, want); errCode(err) != "failed_precondition" {
		t.Fatalf("unjoined process released: %v", err)
	}
	for _, bad := range []struct{ agent, kernel string }{{"foreign-agent", "new-kernel"}, {"old-agent", "old-kernel"}, {"old-agent", ""}} {
		badWant := want
		badWant.AgentBootID = bad.agent
		if _, err := f.core.JoinRunKernelScope(f.ctx, child.ID, badWant, bad.kernel, model.RunRetentionPolicy{}); err == nil {
			t.Fatalf("accepted non-replacement scope %+v", bad)
		}
	}
	foreign := want
	foreign.PrincipalID = "foreign"
	if _, err := f.core.JoinRunKernelScope(f.ctx, child.ID, foreign, "new-kernel", model.RunRetentionPolicy{}); err == nil {
		t.Fatal("foreign caller joined the incoming scope")
	}
	if _, err := f.core.JoinRunKernelScope(f.ctx, child.ID, want, "new-kernel", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, child.ID)
	if err != nil || !found || !state.Joined || state.KernelBootID != "old-kernel" || state.KernelReplacedBy != "new-kernel" || state.Resources[0].Join != nil {
		t.Fatalf("expected durable scope join without invented PID: %+v found=%v err=%v", state, found, err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, child.ID, want); err != nil || !changed {
		t.Fatalf("normal owner release: %v %v", changed, err)
	}
}

func TestInterruptedKernelScopeDoesNotGuessHistoryOrRemoteJoin(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"unrecorded", "remote"} {
		child := incomingChild(id, "parent", "parent", "core", "core-principal")
		child.RecoveryAgentBootID = "old-agent"
		if id == "remote" {
			child.RecoveryKernelBootID = "old-kernel"
		}
		if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
			t.Fatal(err)
		}
		if err := f.core.AdmitRunResource(f.ctx, id, "old-agent", model.RunResourceAdmission{ID: "process", NodeID: "start", Kind: "process"}); err != nil {
			t.Fatal(err)
		}
	}
	link := model.RemoteRunLink{OwnerRunID: "remote", RootRunID: "parent", TargetID: "peer", RunID: "remote-child", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"unrecorded", "remote"} {
		child := incomingChild(id, "parent", "parent", "core", "core-principal")
		want := releaseWant(child)
		want.AgentBootID = "old-agent"
		if id == "unrecorded" {
			if _, err := f.core.JoinRunKernelScope(f.ctx, id, want, "new-kernel", model.RunRetentionPolicy{}); err == nil {
				t.Fatal("inferred missing originating kernel")
			}
		} else if _, err := f.core.JoinRunKernelScope(f.ctx, id, want, "new-kernel", model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, "remote")
	if err != nil || state.Joined {
		t.Fatalf("open remote child was ignored: %+v %v", state, err)
	}
}

func TestWitnessedInterruptedScopePreservesIncomingHold(t *testing.T) {
	f := openStore(t)
	child := incomingChild("selected", "parent", "parent", "core", "core-principal")
	child.RecoveryAgentBootID = "old-agent"
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, child.ID, "old-agent", model.RunResourceAdmission{ID: "p", NodeID: "start", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct{ agent, before, after string }{
		{"foreign", "old-kernel", "new-kernel"}, {"old-agent", "old-kernel", "old-kernel"}, {"old-agent", "", "new-kernel"},
	} {
		if _, err := f.core.WitnessRunKernelScope(f.ctx, child.ID, bad.agent, bad.before, bad.after, model.RunRetentionPolicy{}); err == nil {
			t.Fatalf("accepted invalid witness %+v", bad)
		}
	}
	if _, err := f.core.WitnessRunKernelScope(f.ctx, child.ID, "old-agent", "old-kernel", "new-kernel", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, child.ID)
	if err != nil || !found || !state.Joined || state.KernelBootID != "old-kernel" || state.KernelReplacedBy != "new-kernel" || state.Resources[0].Join != nil {
		t.Fatalf("wrong scope fact: %+v %v %v", state, found, err)
	}
	row, err := f.core.GetRun(f.ctx, child.ID)
	if err != nil || !row.IncomingHold.Active() || row.Status != model.RunInterrupted {
		t.Fatalf("witness released or changed outcome: %+v %v", row, err)
	}
	if _, err := f.core.WitnessRunKernelScope(f.ctx, child.ID, "old-agent", "other-kernel", "new-kernel", model.RunRetentionPolicy{}); err == nil {
		t.Fatal("replaced recorded origin")
	}
}
