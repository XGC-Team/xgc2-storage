package coredata_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestConcurrentRemoteProviderSelectionsPreserveEveryPendingSlot(t *testing.T) {
	f := openStore(t)
	owner := newRun("owner")
	owner.SessionID, owner.TrackRemoteProviderRecovery = "session", true
	if _, _, err := f.core.CreateRun(f.ctx, owner); err != nil {
		t.Fatal(err)
	}
	var selections []model.RunRemoteProviderSelection
	for i := 0; i < 8; i++ {
		link := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: fmt.Sprintf("agent-%d", i),
			RunID: fmt.Sprintf("child-%d", i), CallNodeID: "call", BootID: fmt.Sprintf("boot-%d", i)}
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
		selections = append(selections, model.RunRemoteProviderSelection{Link: link, SessionID: "session", NodeID: "serve",
			ResourceID: fmt.Sprintf("resource-%d", i), Incarnation: fmt.Sprintf("incarnation-%d", i),
			WorkflowResourceID: "workflow", WorkflowCommitID: "commit", WorkflowDigest: "digest"})
	}
	var wg sync.WaitGroup
	errors := make(chan error, len(selections))
	for _, item := range selections {
		wg.Add(1)
		go func() { defer wg.Done(); errors <- f.core.AdmitRemoteProviderRecovery(f.ctx, item) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t)
	for _, expected := range selections {
		got, found, err := f.core.GetRemoteProviderRecovery(f.ctx, expected.Link)
		if err != nil || !found || got != expected {
			t.Fatalf("selection %s lost: %+v found=%v error=%v", expected.Link.TargetID, got, found, err)
		}
	}
}

func TestRemoteProviderSelectionSurvivesRunFinishAndReopen(t *testing.T) {
	f := openStore(t)
	owner := newRun("owner")
	owner.SessionID = "session"
	owner.TrackRemoteProviderRecovery = true
	if _, _, err := f.core.CreateRun(f.ctx, owner); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "old-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	selection := model.RunRemoteProviderSelection{Link: link, SessionID: "session", NodeID: "serve", ResourceID: "resource", Incarnation: "incarnation",
		WorkflowResourceID: "workflow", WorkflowCommitID: "commit", WorkflowDigest: "digest"}
	wrong := selection
	wrong.SessionID = "foreign"
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, wrong); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign Session selected a provider: %v", err)
	}
	wrong = selection
	wrong.Link.BootID = "foreign"
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, wrong); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign boot selected a provider: %v", err)
	}
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, selection); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, selection); err != nil {
		t.Fatalf("identical admission not idempotent: %v", err)
	}
	wrong = selection
	wrong.Incarnation = "foreign"
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, wrong); errCode(err) != "failed_precondition" {
		t.Fatalf("the selected incarnation was silently replaced: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: owner.ID, Status: model.RunStopped, Nodes: []byte(`[{"id":"call"}]`), TrackRemoteProviderRecovery: true, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	stored, found, err := f.core.GetRemoteProviderRecovery(f.ctx, link)
	if err != nil || !found || stored != selection {
		t.Fatalf("exact selected provider was lost: %+v found=%v error=%v", stored, found, err)
	}
	run, err := f.core.GetRun(f.ctx, owner.ID)
	if err != nil || string(run.Nodes) != `[{"id":"call"}]` || run.Status != model.RunStopped {
		t.Fatalf("control evidence changed public nodes or status: %+v error=%v", run, err)
	}
}

func TestRemoteProviderSelectionSurvivesInterruptionButOldMissingIsUnknown(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"new", "old"} {
		owner := newRun(id)
		owner.SessionID = "session"
		owner.TrackRemoteProviderRecovery = id == "new"
		if _, _, err := f.core.CreateRun(f.ctx, owner); err != nil {
			t.Fatal(err)
		}
		link := model.RemoteRunLink{OwnerRunID: id, RootRunID: id, TargetID: "agent", RunID: id + "-child", CallNodeID: "call", BootID: "old-boot"}
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
		if id == "new" {
			selection := model.RunRemoteProviderSelection{Link: link, SessionID: "session", NodeID: "serve", ResourceID: "resource", Incarnation: "incarnation",
				WorkflowResourceID: "workflow", WorkflowCommitID: "commit", WorkflowDigest: "digest"}
			if err := f.core.AdmitRemoteProviderRecovery(f.ctx, selection); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	// A legacy row's unrelated node payload is never fetched or decoded to
	// invent a selection merely because its relation remains pending.
	f.exec(t, `UPDATE runs SET nodes_json=x'02' WHERE id=?`, "old")
	f.reopen(t)
	for _, id := range []string{"new", "old"} {
		link := model.RemoteRunLink{OwnerRunID: id, RootRunID: id, TargetID: "agent", RunID: id + "-child", CallNodeID: "call", BootID: "old-boot"}
		selection, found, err := f.core.GetRemoteProviderRecovery(f.ctx, link)
		if err != nil || found != (id == "new") || found && selection.Incarnation != "incarnation" {
			t.Fatalf("%s provider selection: %+v found=%v error=%v", id, selection, found, err)
		}
	}
}

func TestHistoricalNodeJSONCannotForgeRemoteProviderSelection(t *testing.T) {
	f := openStore(t)
	owner := newRun("old-owner")
	owner.SessionID = "session"
	if _, _, err := f.core.CreateRun(f.ctx, owner); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "child", CallNodeID: "call", BootID: "old-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	// Before this implementation, FinishRun accepted arbitrary valid Nodes
	// JSON. Even a byte-perfect apparent envelope must not become authority.
	legacy := []byte(`{"$runResourceRecovery":"xgc2-run-resource-recovery-v1","recovery":{"agentBootId":"","resources":null,"joined":false,"trackRemoteProviders":true,"remoteProviders":[{"link":{"ownerRunId":"old-owner","rootRunId":"old-owner","targetId":"agent","runId":"child","callNodeId":"call","bootId":"old-boot","phase":""},"sessionId":"session","nodeId":"serve","resourceId":"resource","incarnation":"incarnation","workflowResourceId":"workflow","workflowCommitId":"commit","workflowDigest":"digest"}]}}`)
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: owner.ID, Status: model.RunInterrupted, Nodes: legacy, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if selected, found, err := f.core.GetRemoteProviderRecovery(f.ctx, link); err != nil || found {
		t.Fatalf("historical JSON forged a selected provider: %+v found=%v error=%v", selected, found, err)
	}
	stored, err := f.core.GetRun(f.ctx, owner.ID)
	if err != nil || string(stored.Nodes) != string(legacy) {
		t.Fatalf("historical node value changed: %s %v", stored.Nodes, err)
	}
}

func TestRemoteSelectionAndLocalResourceShareExactRunOwner(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("owner", "root", "root", "core", "principal")
	child.SessionID, child.RecoveryAgentBootID, child.TrackRemoteProviderRecovery = "session", "owner-boot", true
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := f.core.AdmitRunResource(f.ctx, child.ID, child.RecoveryAgentBootID, model.RunResourceAdmission{ID: "local-process", NodeID: "local", Kind: "process"}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: child.ID, RootRunID: "root", TargetID: "other-agent", RunID: "grandchild", CallNodeID: "call", BootID: "other-boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	selection := model.RunRemoteProviderSelection{Link: link, SessionID: "session", NodeID: "serve", ResourceID: "remote-process", Incarnation: "remote-incarnation",
		WorkflowResourceID: "workflow", WorkflowCommitID: "commit", WorkflowDigest: "digest"}
	if err := f.core.AdmitRemoteProviderRecovery(f.ctx, selection); err != nil {
		t.Fatal(err)
	}
	proof := model.RunResourceJoin{RunID: child.ID, NodeID: "local", ResourceID: "local-process", Kind: "process", AgentBootID: child.RecoveryAgentBootID,
		Incarnation: "local-incarnation", HostBootID: "os", PID: 2, PGID: 2, StartTicks: 3, OwnerPID: 1, OwnerStartTicks: 1}
	if err := f.core.JoinRunResource(f.ctx, proof); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, child.ID, child.RecoveryAgentBootID); errCode(err) != "failed_precondition" {
		t.Fatalf("open remote relation was ignored: %v", err)
	}
	got, found, err := f.core.GetRemoteProviderRecovery(f.ctx, link)
	if err != nil || !found || got != selection {
		t.Fatalf("remote provider was lost during local join: %+v %v %v", got, found, err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "stopped", FinishedAt: epoch}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := f.core.SealRunResourceRecovery(f.ctx, child.ID, child.RecoveryAgentBootID); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	state, found, err := f.core.GetRunResourceRecovery(f.ctx, child.ID)
	if err != nil || !found || !state.Joined || len(state.Resources) != 1 || state.Resources[0].Join == nil || *state.Resources[0].Join != proof {
		t.Fatalf("local joined proof did not survive: %+v %v %v", state, found, err)
	}
}
