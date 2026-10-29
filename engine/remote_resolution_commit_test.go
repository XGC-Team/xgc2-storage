package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRemoteResolutionCancellationRetainsExactCommitAttempt(t *testing.T) {
	ctx := t.Context()
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "storage.db"), Create: true, Manifest: manifest, Modules: []engine.Module{coredata.Module()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := coredata.New(db, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateRun(ctx, model.NewRun{ID: "root", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "root", RootRunID: "root", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := store.ReserveRemoteRun(ctx, link); err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC)
	if _, err := store.FinishRun(ctx, model.RunFinish{ID: "root", Status: model.RunCanceled, At: finishedAt}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	entered, release := engine.GateNextCommitForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		result model.RemoteRunResolveResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := store.ResolveRemoteRun(callCtx, link, model.RunRetentionPolicy{})
		done <- outcome{result, err}
	}()
	select {
	case <-entered:
	case early := <-done:
		t.Fatalf("resolution returned before COMMIT gate: %+v %v", early.result, early.err)
	case <-time.After(5 * time.Second):
		t.Fatal("resolution did not reach COMMIT")
	}
	cancel()
	out := <-done
	var disposition *engine.WriteError
	var resolution *coredata.RemoteRunResolveError
	if !errors.As(out.err, &disposition) || disposition.Disposition != engine.WriteUnknown || disposition.Settled || !disposition.MayHaveCommitted || !errors.As(out.err, &resolution) || !resolution.ValidatedAttempt || resolution.AttemptedLink != link || out.result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("in-flight resolution lost its exact owner: %+v %v", out.result, out.err)
	}
	if pending, err := store.HasPendingRemoteRoot(ctx, "root"); err == nil || pending {
		t.Fatalf("unjoined resolution allowed a revalidation: pending=%v %v", pending, err)
	}
	release()
	if err := disposition.WaitSettlement(ctx); err != nil {
		t.Fatalf("real COMMIT did not join: %v", err)
	}
	if pending, err := store.HasPendingRemoteRoot(ctx, "root"); err != nil || pending {
		t.Fatalf("committed result disagrees with durable relation: pending=%v %v", pending, err)
	}
}

func TestAgentLinkCancellationRetainsExactCommitAttempt(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
	db, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "storage.db"), Create: true, Manifest: manifest, Modules: []engine.Module{coredata.Module()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := coredata.New(db, scope)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.ListAgentLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := model.AgentLinkRecord{AgentID: "agent", Enrollment: "known", AgentLinkFacts: model.AgentLinkFacts{CapabilityManifest: []byte(`{}`)}}
	q := model.AgentLinkWrite{Expected: model.AgentLinkGuard{Identity: initial.Identity, AgentID: record.AgentID, Version: "0"}, Record: record}
	entered, release := engine.GateNextCommitForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		result model.AgentLinkWriteResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := store.WriteAgentLink(callCtx, q); done <- outcome{result, err} }()
	select {
	case <-entered:
	case early := <-done:
		t.Fatalf("write returned before COMMIT gate: %+v %v", early.result, early.err)
	case <-time.After(5 * time.Second):
		t.Fatal("write did not reach COMMIT")
	}
	cancel()
	out := <-done
	var disposition *engine.WriteError
	if !errors.As(out.err, &disposition) || disposition.Disposition != engine.WriteUnknown || disposition.Settled || !disposition.MayHaveCommitted || out.result.Changed {
		t.Fatalf("in-flight enrollment was reported as committed: %+v %v", out.result, out.err)
	}
	release()
	if err := disposition.WaitSettlement(ctx); err != nil {
		t.Fatalf("real enrollment COMMIT did not join: %v", err)
	}
	after, err := store.ListAgentLinks(ctx)
	if err != nil || len(after.Records) != 1 || after.Records[0].Guard.Version != "1" || after.Records[0].Guard.Identity != initial.Identity {
		t.Fatalf("joined current enrollment state: %+v %v", after, err)
	}
}
