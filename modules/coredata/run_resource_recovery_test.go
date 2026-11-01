package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestInterruptedResourceCoverageNeedsAllExactJoinsAndOutboundResolution(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("child", "parent", "parent", "core", "core-principal")
	child.RecoveryAgentBootID = "original-agent-boot"
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	for _, a := range []model.RunResourceAdmission{
		{ID: "provider", NodeID: "serve-process", Kind: "process"},
		{ID: "command", NodeID: "prepare", Kind: "command"},
	} {
		if err := f.core.AdmitRunResource(f.ctx, "child", child.RecoveryAgentBootID, a); err != nil {
			t.Fatal(err)
		}
	}
	link := model.RemoteRunLink{OwnerRunID: "child", RootRunID: "parent", TargetID: "other-agent", RunID: "grandchild", CallNodeID: "call-out"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, "child")
	if err != nil || !found || state.AgentBootID != child.RecoveryAgentBootID || len(state.Resources) != 2 || state.Joined {
		t.Fatalf("persisted admission: %+v found=%v err=%v", state, found, err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, "child", child.RecoveryAgentBootID); errCode(err) != "failed_precondition" {
		t.Fatalf("unjoined resources produced a receipt: %v", err)
	}
	proof := func(id, node string, pid int) model.RunResourceJoin {
		return model.RunResourceJoin{RunID: "child", NodeID: node, ResourceID: id, AgentBootID: child.RecoveryAgentBootID,
			Incarnation: "inc-" + id, HostBootID: "os-boot", PID: pid, PGID: pid, StartTicks: uint64(pid * 4), OwnerPID: 11, OwnerStartTicks: 22}
	}
	provider := proof("provider", "serve-process", 21)
	for _, wrong := range []model.RunResourceJoin{
		func() model.RunResourceJoin { p := provider; p.NodeID = "foreign"; return p }(),
		func() model.RunResourceJoin { p := provider; p.AgentBootID = "replacement"; return p }(),
		func() model.RunResourceJoin { p := provider; p.StartTicks = 0; return p }(),
	} {
		if err := f.core.JoinRunResource(f.ctx, wrong); errCode(err) != "failed_precondition" && errCode(err) != "invalid_argument" {
			t.Fatalf("foreign or incomplete proof accepted: %+v %v", wrong, err)
		}
	}
	if err := f.core.JoinRunResource(f.ctx, provider); err != nil {
		t.Fatal(err)
	}
	if err := f.core.JoinRunResource(f.ctx, provider); err != nil {
		t.Fatalf("exact receipt replay: %v", err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, "child", child.RecoveryAgentBootID); errCode(err) != "failed_precondition" {
		t.Fatalf("one joined resource covered another: %v", err)
	}
	if err := f.core.JoinRunResource(f.ctx, proof("command", "prepare", 23)); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, "child", child.RecoveryAgentBootID); errCode(err) != "failed_precondition" {
		t.Fatalf("open outbound child was ignored: %v", err)
	}
	if _, err := f.core.ResolveUnsentRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, "child", child.RecoveryAgentBootID); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err = f.core.GetRunResourceRecovery(f.ctx, "child")
	if err != nil || !found || !state.Joined || state.Resources[0].Join == nil || state.Resources[1].Join == nil {
		t.Fatalf("positive receipt not durable: %+v found=%v err=%v", state, found, err)
	}
	stored, err := f.core.GetRun(f.ctx, "child")
	if err != nil || stored.Status != model.RunInterrupted || !stored.IncomingHold.Active() {
		t.Fatalf("receipt rewrote business status or incoming hold: %+v %v", stored, err)
	}
}

func TestRecoveryDoesNotInferEmptyOrUncoveredEffects(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"empty", "uncovered"} {
		child := incomingChild(id, "parent", "parent", "core", "core-principal")
		child.RecoveryAgentBootID = "boot"
		if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.core.AdmitRunResource(f.ctx, "uncovered", "boot", model.RunResourceAdmission{ID: "effect", NodeID: "remote-shell", Kind: "uncovered-effect"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"empty", "uncovered"} {
		if err := f.core.SealRunResourceRecovery(f.ctx, id, "boot"); errCode(err) != "failed_precondition" {
			t.Fatalf("%s acquired a fabricated complete join: %v", id, err)
		}
	}
	p := model.RunResourceJoin{RunID: "uncovered", NodeID: "remote-shell", ResourceID: "effect", AgentBootID: "boot", Incarnation: "i", HostBootID: "os", PID: 2, PGID: 2, StartTicks: 1, OwnerPID: 1, OwnerStartTicks: 1}
	if err := f.core.JoinRunResource(f.ctx, p); errCode(err) != "failed_precondition" {
		t.Fatalf("an uncovered effect accepted a process proof: %v", err)
	}
}

func TestInterruptedFinishKeepsNodeDetailAndActualQuota(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("tracked", "parent", "parent", "core", "core-principal")
	child.RecoveryAgentBootID = "boot"
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, child.ID, "boot", model.RunResourceAdmission{ID: "p", NodeID: "node", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	nodes := json.RawMessage(`[{"id":"node","status":"running"}]`)
	finish := model.RunFinish{ID: child.ID, Status: model.RunInterrupted, Termination: "interrupted", Nodes: nodes, At: epoch.Add(time.Second)}
	if _, err := f.core.FinishRun(f.ctx, finish, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, finish, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("exact finish replay: %v", err)
	}
	f.reopen(t)
	run, err := f.core.GetRun(f.ctx, child.ID)
	state, found, stateErr := f.core.GetRunResourceRecovery(f.ctx, child.ID)
	if err != nil || stateErr != nil || string(run.Nodes) != string(nodes) || !found || len(state.Resources) != 1 {
		t.Fatalf("node detail or recovery lost: nodes=%s state=%+v found=%v errors=%v/%v", run.Nodes, state, found, err, stateErr)
	}
	_, accounted := usageRows(t, f)
	var actual int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT coalesce(sum(bytes),0) FROM runs").Scan(&actual)
	}); err != nil || actual != accounted {
		t.Fatalf("Run bytes=%d scope bytes=%d error=%v", actual, accounted, err)
	}
}

// FinishRun historically accepts any valid JSON in Nodes. A marker embedded
// there by itself is therefore not evidence that the new admission writer ran.
func TestHistoricalNodesCannotForgeNewRecoveryCoverage(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("old-shaped", "parent", "parent", "core", "core-principal")
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	legacy := json.RawMessage(`{"$runResourceRecovery":"xgc2-run-resource-recovery-v1","recovery":{"agentBootId":"boot","resources":[{"id":"r","nodeId":"n","kind":"process","join":{"runId":"old-shaped","nodeId":"n","resourceId":"r","agentBootId":"boot","incarnation":"i","hostBootId":"host","pid":2,"pgid":2,"startTicks":1,"ownerPid":1,"ownerStartTicks":1}}],"joined":true}}`)
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "old-shaped", Status: model.RunInterrupted, Nodes: legacy, At: epoch.Add(time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	stored, err := f.core.GetRun(f.ctx, "old-shaped")
	if err != nil || string(stored.Nodes) != string(legacy) {
		t.Fatalf("historical node detail was interpreted or changed: %s %v", stored.Nodes, err)
	}
	if _, found, err := f.core.GetRunResourceRecovery(f.ctx, "old-shaped"); err != nil || found {
		t.Fatalf("legacy node bytes became recovery proof: found=%v err=%v", found, err)
	}
}

func TestInterruptedHistoricalRunDoesNotAcquireResourceCoverage(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("old", "parent", "parent", "core", "core-principal")
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.core.GetRunResourceRecovery(f.ctx, "old"); err != nil || found {
		t.Fatalf("old missing admissions became known: found=%v err=%v", found, err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, "old", "replacement-agent-boot"); errCode(err) != "failed_precondition" {
		t.Fatalf("old missing admissions were sealed: %v", err)
	}
}
