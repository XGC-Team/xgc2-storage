package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A directly accepted root has the same execution owner as an incoming Run.
// Its persisted terminal outcome alone must not claim that owner joined.
func TestAcceptedRootHasDurableResourceOwnership(t *testing.T) {
	f := openStore(t)
	root := newRun("record-root")
	root.RecoveryAgentBootID = "core-process-boot"
	if _, created, err := f.core.CreateRun(f.ctx, root); err != nil || !created {
		t.Fatalf("accepted root lacks durable resource coverage: created=%v err=%v", created, err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunSucceeded, At: epoch.Add(time.Second), RecoveryAgentBootID: root.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || !found || state.Joined {
		t.Fatalf("terminal status fabricated physical completion: state=%+v found=%v err=%v", state, found, err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("retention erased the still-owned root: %+v %v", pruned, err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, "other-process", "joined", model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign execution owner joined root: %v", err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, root.RecoveryAgentBootID, "joined", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err = f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || !found || !state.Joined || state.LocalDriver != "joined" {
		t.Fatalf("actual local owner join was not retained: state=%+v found=%v err=%v", state, found, err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("joined root did not return to normal retention: %+v %v", pruned, err)
	}
}

func TestRootDriverJoinDoesNotHideActualResourceOrRemoteChild(t *testing.T) {
	f := openStore(t)
	root := newRun("retained-root")
	root.RecoveryAgentBootID = "core-process-boot"
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, root.ID, root.RecoveryAgentBootID, model.RunResourceAdmission{ID: "process", NodeID: "node", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: root.ID, RootRunID: root.ID, TargetID: "other-agent", RunID: "child", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunStopped, At: epoch.Add(time.Second), RecoveryAgentBootID: root.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, root.RecoveryAgentBootID, "joined", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	proof := model.RunResourceJoin{RunID: root.ID, NodeID: "node", ResourceID: "process", Kind: "process", AgentBootID: root.RecoveryAgentBootID,
		Incarnation: "actual", HostBootID: "kernel", PID: 23, PGID: 23, StartTicks: 31, OwnerPID: 1, OwnerStartTicks: 1}
	if _, err := f.core.JoinRunResource(f.ctx, proof, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("late exact physical join after terminal was refused: %v", err)
	}
	f.reopen(t)
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || state.Joined || state.LocalDriver != "joined" || state.Resources[0].Join == nil {
		t.Fatalf("remote obligation was hidden or local facts lost: %+v %v", state, err)
	}
	if _, err := f.core.ResolveUnsentRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, _, err = f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || !state.Joined {
		t.Fatalf("last exact child resolution did not complete root: %+v %v", state, err)
	}
	run, err := f.core.GetRun(f.ctx, root.ID)
	if err != nil || run.Status != model.RunStopped {
		t.Fatalf("resource cleanup changed business outcome: %+v %v", run, err)
	}
}

func TestRootFailedDriverDoesNotBecomeACompletionReceipt(t *testing.T) {
	f := openStore(t)
	root := newRun("failed-driver")
	root.RecoveryAgentBootID = "core-boot"
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunFailed, At: epoch, RecoveryAgentBootID: root.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, root.RecoveryAgentBootID, "failed", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, root.RecoveryAgentBootID, "joined", model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("failed cleanup changed into a successful join: %v", err)
	}
	f.reopen(t)
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || state.Joined || state.LocalDriver != "failed" {
		t.Fatalf("failed driver was treated as fully joined: %+v %v", state, err)
	}
}

func TestRootDriverJoinBeforeOutcomeIsRetainedAndPrunableAfterFinish(t *testing.T) {
	f := openStore(t)
	root := newRun("early-driver-join")
	root.RecoveryAgentBootID = "core-boot"
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.JoinRunDriver(f.ctx, root.ID, root.RecoveryAgentBootID, "joined", model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunSucceeded, At: epoch, RecoveryAgentBootID: root.RecoveryAgentBootID}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || !state.Joined {
		t.Fatalf("late durable outcome did not complete the prior physical join: %+v %v", state, err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("normal retention did not see the joined root: %+v %v", pruned, err)
	}
}

func TestRootRestartJoinsOnlyPreviousDriverUntilItsActualProcessJoins(t *testing.T) {
	f := openStore(t)
	root := newRun("root-with-process")
	root.RecoveryAgentBootID = "original-process"
	if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, root.ID, root.RecoveryAgentBootID, model.RunResourceAdmission{ID: "child-process", NodeID: "node", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	// Open the same exclusively owned database after the old owner ends.
	f.reopen(t)
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	state, _, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || state.LocalDriver != "lost" || state.Joined || state.Resources[0].Join != nil {
		t.Fatalf("replaced driver fabricated a process join: %+v %v", state, err)
	}
	proof := model.RunResourceJoin{RunID: root.ID, NodeID: "node", ResourceID: "child-process", Kind: "process", AgentBootID: root.RecoveryAgentBootID,
		Incarnation: "actual", HostBootID: "kernel", PID: 23, PGID: 23, StartTicks: 31, OwnerPID: 1, OwnerStartTicks: 1}
	if _, err := f.core.JoinRunResource(f.ctx, proof, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, _, err = f.core.GetRunResourceRecovery(f.ctx, root.ID)
	if err != nil || !state.Joined {
		t.Fatalf("actual reaped process did not complete interrupted root: %+v %v", state, err)
	}
	run, err := f.core.GetRun(f.ctx, root.ID)
	if err != nil || run.Status != model.RunInterrupted {
		t.Fatalf("recovery changed interrupted business outcome: %+v %v", run, err)
	}
}

func TestRootEffectCompletionRequiresExactSuccessfulReturn(t *testing.T) {
	f := openStore(t)
	for _, tc := range []struct {
		id       string
		complete bool
	}{{"remote-canceled", false}, {"effect-returned", true}} {
		root := newRun(tc.id)
		root.RecoveryAgentBootID = "owner"
		if _, _, err := f.core.CreateRun(f.ctx, root); err != nil {
			t.Fatal(err)
		}
		admission := model.RunResourceAdmission{ID: "@effect:transfer", NodeID: "transfer", Kind: "uncovered-effect"}
		if err := f.core.AdmitRunResource(f.ctx, root.ID, "owner", admission); err != nil {
			t.Fatal(err)
		}
		if tc.complete {
			if err := f.core.CompleteRunEffect(f.ctx, root.ID, "other-owner", admission.ID, admission.NodeID); errCode(err) != "failed_precondition" {
				t.Fatalf("foreign owner completed effect: %v", err)
			}
			if err := f.core.CompleteRunEffect(f.ctx, root.ID, "owner", admission.ID, admission.NodeID); err != nil {
				t.Fatal(err)
			}
			if err := f.core.CompleteRunEffect(f.ctx, root.ID, "owner", admission.ID, admission.NodeID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: root.ID, Status: model.RunStopped, At: epoch, RecoveryAgentBootID: "owner"}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.core.JoinRunDriver(f.ctx, root.ID, "owner", "joined", model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
		state, _, err := f.core.GetRunResourceRecovery(f.ctx, root.ID)
		if err != nil || state.Joined != tc.complete {
			t.Fatalf("effect %s joined=%v want=%v state=%+v err=%v", root.ID, state.Joined, tc.complete, state, err)
		}
	}
}
