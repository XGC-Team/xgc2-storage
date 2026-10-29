package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestTypedCreateHeldCallbackRetainsInputsAndHidesResult(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	at := time.Now().UTC()
	request := faultRun("held-create", at)
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "INSERT INTO runs(", "exec")
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	type answer struct {
		run     model.Run
		created bool
		err     error
	}
	done := make(chan answer, 1)
	go func() { run, created, err := store.CreateRun(ctx, request); done <- answer{run, created, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real Run insert did not reach held driver")
	}
	var got answer
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("typed CreateRun blocked beyond deadline")
	}
	var outcome *engine.WriteError
	var fault *coredata.RunWriteFaultError
	if !errors.As(got.err, &outcome) || !errors.As(got.err, &fault) || !outcome.CallbackPending || outcome.MayHaveCommitted || outcome.Settled || got.created || got.run.ID != "" {
		t.Fatalf("unjoined callback leaked result: %+v error=%v", got.run, got.err)
	}
	for i := range request.Inputs {
		request.Inputs[i] = 'x'
	}
	release()
	if err := outcome.WaitSettlement(t.Context()); err != nil {
		t.Fatalf("Run callback rollback: %v", err)
	}
	fact, err := fault.WaitRevalidation(t.Context())
	if err != nil || !fact.NonCommitted || len(fact.Creates) != 1 || !fact.Creates[0].Attempted.Created || !fact.Creates[0].CurrentKnown || fact.Creates[0].Present {
		t.Fatalf("joined rollback lost evaluated insertion or its absent before-state: %+v %v", fact, err)
	}
	if _, err := store.GetRun(t.Context(), request.ID); err == nil {
		t.Fatal("canceled create committed")
	}
	stats, err := db.Stats()
	if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 {
		t.Fatalf("typed owner not idle: %+v %v", stats, err)
	}
}

func TestTypedRemoteHeldDeleteDoesNotExposeAttemptBeforeJoin(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "DELETE FROM run_remote_pending", "exec")
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.ResolveRemoteRun(ctx, link, model.RunRetentionPolicy{OlderThan: time.Now().Add(time.Hour)})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real remote DELETE did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("typed Resolve blocked beyond deadline")
	}
	var outcome *engine.WriteError
	var fault *coredata.RemoteRunResolveError
	if !errors.As(err, &outcome) || !outcome.CallbackPending || outcome.MayHaveCommitted || !errors.As(err, &fault) || fault.ValidatedAttempt || fault.AttemptedLink != (model.RemoteRunLink{}) {
		t.Fatalf("unjoined callback exposed validation: %+v %v", fault, err)
	}
	release()
	fact, err := fault.WaitRevalidation(t.Context())
	if err != nil || !fact.NonCommitted || fact.Released || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("joined callback fact: %+v %v", fact, err)
	}
	if pending, err := store.HasPendingRemoteRoot(t.Context(), link.RootRunID); err != nil || !pending {
		t.Fatalf("rolled-back remote link: %t %v", pending, err)
	}
}

func TestTypedRemoteHeldDeleteAndFailedCloseRetainOriginalOwner(t *testing.T) {
	db, store, cfg, scope, link := remoteSettlementFixture(t)
	ctx := t.Context()
	entered, release := engine.GateCallbackSQLWithFailedCloseForModuleTest(t, db, "DELETE FROM run_remote_pending", "exec")
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.ResolveRemoteRun(callCtx, link, model.RunRetentionPolicy{OlderThan: time.Now().Add(time.Hour)})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real remote DELETE did not enter")
	}
	err := <-done
	var fault *coredata.RemoteRunResolveError
	if !errors.As(err, &fault) {
		t.Fatalf("missing original remote owner: %v", err)
	}
	release()
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if _, err := fault.WaitRevalidation(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed physical close exposed a fact: %v", err)
	}
	waitCancel()
	if err := db.Close(); err != nil {
		t.Fatalf("later physical join: %v", err)
	}
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || fact.Released || fact.Link != link {
		t.Fatalf("original remote callback settlement: %+v %v", fact, err)
	}
	cfg.Create = false
	reopened, err := engine.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, err := coredata.New(reopened, scope)
	if err != nil {
		t.Fatal(err)
	}
	if pending, err := current.HasPendingRemoteRoot(ctx, link.RootRunID); err != nil || !pending {
		t.Fatalf("joined rollback lost relation: pending=%t %v", pending, err)
	}
}

func TestTypedFinishHeldRowRetainsInputAndPolicy(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-time.Hour)
	q := faultRun("held-finish-input", at)
	if _, _, err := store.CreateRun(ctx, q); err != nil {
		t.Fatal(err)
	}
	finish := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: at, Result: []byte(`{"value":"original"}`)}
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "SELECT status,root_run_id FROM runs", "row")
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.ApplyRunOps(callCtx, nil, nil, []model.RunFinish{finish}, model.RunRetentionPolicy{OlderThan: time.Now()})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real Finish row did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("Finish blocked beyond deadline")
	}
	var outcome *engine.WriteError
	var fault *coredata.RunWriteFaultError
	if !errors.As(err, &outcome) || !outcome.CallbackPending || outcome.MayHaveCommitted || !errors.As(err, &fault) {
		t.Fatalf("unjoined Finish did not retain its attempt: %v", err)
	}
	for i := range finish.Result {
		finish.Result[i] = 'x'
	}
	release()
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("rolled-back Finish manufactured policy event: %+v %v", fact, err)
	}
	current, err := store.GetRun(ctx, q.ID)
	if err != nil || !current.Status.Open() {
		t.Fatalf("held Finish committed: %+v %v", current, err)
	}
}

func TestTypedAgentLinkHeldCallbackOwnsInputAndResult(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	initial, err := store.ListAgentLinks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := model.AgentLinkRecord{AgentID: "agent", Enrollment: "known", AgentLinkFacts: model.AgentLinkFacts{CapabilityManifest: []byte(`{}`), Capabilities: []string{"camera"}}}
	request := model.AgentLinkWrite{Expected: model.AgentLinkGuard{Identity: initial.Identity, AgentID: record.AgentID, Version: "0"}, Record: record}
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "INSERT INTO core_agent_links", "exec")
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	type answer struct {
		result model.AgentLinkWriteResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := store.WriteAgentLink(callCtx, request); done <- answer{result, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real AgentLink insert did not enter")
	}
	var got answer
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("AgentLink callback blocked beyond deadline")
	}
	var outcome *engine.WriteError
	if !errors.As(got.err, &outcome) || !outcome.CallbackPending || outcome.MayHaveCommitted || got.result.Changed {
		t.Fatalf("unjoined AgentLink exposed a result: %+v %v", got.result, got.err)
	}
	request.Record.CapabilityManifest[0] = 'x'
	request.Record.Capabilities[0] = "changed"
	release()
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("AgentLink rollback: %v", err)
	}
	after, err := store.ListAgentLinks(ctx)
	if err != nil || len(after.Records) != 0 {
		t.Fatalf("canceled AgentLink committed: %+v %v", after, err)
	}
}

func TestTypedMixedHeldCallbackKeepsEvaluatedCreateReplay(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-time.Hour)
	prior, other := faultRun("prior-create", at), faultRun("held-finish", at)
	for _, q := range []model.NewRun{prior, other} {
		if _, _, err := store.CreateRun(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "SELECT status,root_run_id FROM runs", "row")
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := store.ApplyRunOps(callCtx, []model.NewRun{prior}, nil, []model.RunFinish{{ID: other.ID, Status: model.RunSucceeded, At: at}}, model.RunRetentionPolicy{OlderThan: time.Now()})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("mixed Finish callback did not enter")
	}
	err := <-done
	var fault *coredata.RunWriteFaultError
	if !errors.As(err, &fault) {
		t.Fatalf("missing mixed callback owner: %v", err)
	}
	release()
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || len(fact.Creates) != 1 || fact.Creates[0].Attempted.Created || fact.Creates[0].Attempted.Run.ID != prior.ID || !fact.Creates[0].CurrentKnown || !fact.Creates[0].Present || fact.Creates[0].Current.ID != prior.ID || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("evaluated replay was lost or promoted: %+v %v", fact, err)
	}
}

func TestTypedReserveHeldBindKeepsValidatedPriorBoot(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	owner := faultRun("held-bind-owner", time.Now().UTC())
	if _, _, err := store.CreateRun(ctx, owner); err != nil {
		t.Fatal(err)
	}
	unbound := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "child", CallNodeID: "call"}
	if err := store.ReserveRemoteRun(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	bound := unbound
	bound.BootID = "agent-boot"
	entered, release := engine.GateCallbackSQLForModuleTest(t, db, "UPDATE run_remote_pending SET boot_id", "exec")
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- store.ReserveRemoteRun(callCtx, bound) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real boot binding did not enter")
	}
	err := <-done
	var outcome *engine.WriteError
	var fault *coredata.RemoteRunReserveError
	if !errors.As(err, &outcome) || !outcome.CallbackPending || outcome.MayHaveCommitted || !errors.As(err, &fault) || fault.Requested != bound {
		t.Fatalf("held bind lost original owner: %v", err)
	}
	release()
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || !fact.Validated || fact.Action != "bind" || !fact.BeforePresent || fact.Before != unbound || !fact.CurrentKnown || !fact.Present || fact.Current != unbound {
		t.Fatalf("rolled-back bind lost validated prior boot: %+v %v", fact, err)
	}
}
