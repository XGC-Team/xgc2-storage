package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func faultRun(id string, at time.Time) model.NewRun {
	return model.NewRun{ID: id, TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", IdempotencyKey: "key-" + id, Inputs: []byte(`{}`), Trigger: []byte(`{}`), At: at}
}

func waitRunFact(t *testing.T, err error) coredata.RunFaultFact {
	t.Helper()
	var fault *coredata.RunWriteFaultError
	var disposition *engine.WriteError
	if !errors.As(err, &fault) || !errors.As(err, &disposition) || disposition.Disposition != engine.WriteUnknown {
		t.Fatalf("expected original unknown Run fault: %v", err)
	}
	fact, e := fault.WaitRevalidation(t.Context())
	if e != nil {
		t.Fatalf("same-attempt Run revalidation: %v", e)
	}
	return fact
}

func TestRunFaultFinishRevalidatesOriginalPolicyAndReplay(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct_%t", direct), func(t *testing.T) {
			db, store, _, _, _ := remoteSettlementFixture(t)
			ctx := t.Context()
			old := time.Now().UTC().Add(-2 * time.Hour)
			q := faultRun("old-finish", old)
			if _, _, err := store.CreateRun(ctx, q); err != nil {
				t.Fatal(err)
			}
			finish := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: old.Add(time.Minute)}
			policy := model.RunRetentionPolicy{OlderThan: old.Add(time.Hour)}
			engine.InjectCommitAfterForModuleTest(t, db)
			var err error
			if direct {
				_, err = store.FinishRun(ctx, finish, policy)
			} else {
				_, err = store.ApplyRunOps(ctx, nil, nil, []model.RunFinish{finish}, policy)
			}
			fact := waitRunFact(t, err)
			if fact.NonCommitted || !fact.Retention.ChangedFinish || !fact.Retention.EligibilityReleased || !fact.Retention.HasVictim {
				t.Fatalf("lost Finish omitted exact policy effect: %+v", fact)
			}
			engine.InjectCommitAfterForModuleTest(t, db)
			if direct {
				_, err = store.FinishRun(ctx, finish, policy)
			} else {
				_, err = store.ApplyRunOps(ctx, nil, nil, []model.RunFinish{finish}, policy)
			}
			replay := waitRunFact(t, err)
			if replay.Retention != (model.RunRetentionEffect{}) {
				t.Fatalf("replay manufactured a policy event: %+v", replay)
			}
			pruned, err := store.PruneRuns(ctx, model.RunRetention{OlderThan: policy.OlderThan})
			if err != nil || pruned.Deleted != 1 {
				t.Fatalf("actual policy victim: %+v %v", pruned, err)
			}
		})
	}
}

func TestRunFaultMixedBatchKeepsOriginalFinishPolicy(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-2 * time.Hour)
	old := faultRun("mixed-old", at)
	if _, _, err := store.CreateRun(ctx, old); err != nil {
		t.Fatal(err)
	}
	newRun := faultRun("mixed-create", at)
	finish := model.RunFinish{ID: old.ID, Status: model.RunSucceeded, At: at.Add(time.Minute)}
	policy := model.RunRetentionPolicy{OlderThan: at.Add(time.Hour)}
	engine.InjectCommitAfterForModuleTest(t, db)
	_, err := store.ApplyRunOps(ctx, []model.NewRun{newRun}, nil, []model.RunFinish{finish}, policy)
	fact := waitRunFact(t, err)
	if !fact.Retention.ChangedFinish || !fact.Retention.HasVictim || len(fact.Creates) != 1 || !fact.Creates[0].Attempted.Created || !fact.Creates[0].CurrentKnown || !fact.Creates[0].Present {
		t.Fatalf("mixed Finish lost original policy: %+v", fact)
	}
	pruned, err := store.PruneRuns(ctx, model.RunRetention{OlderThan: policy.OlderThan})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("mixed original policy victim: %+v %v", pruned, err)
	}
}

func TestRunFaultCreateDistinguishesReplayFromInsertedGeneration(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprintf("direct_%t", direct), func(t *testing.T) {
			db, store, _, _, _ := remoteSettlementFixture(t)
			ctx := t.Context()
			at := time.Now().UTC().Add(-time.Hour)
			q := faultRun("same-id", at)
			if _, _, err := store.CreateRun(ctx, q); err != nil {
				t.Fatal(err)
			}
			attempt := func() error {
				engine.InjectCommitAfterForModuleTest(t, db)
				if direct {
					_, _, err := store.CreateRun(ctx, q)
					return err
				}
				_, err := store.ApplyRunOps(ctx, []model.NewRun{q}, nil, nil, model.RunRetentionPolicy{})
				return err
			}
			replay := waitRunFact(t, attempt())
			if len(replay.Creates) != 1 || replay.Creates[0].Attempted.Created || !replay.Creates[0].CurrentKnown || !replay.Creates[0].Present {
				t.Fatalf("existing replay was attributed to a new insert: %+v", replay)
			}
			finish := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: at.Add(time.Minute)}
			if _, err := store.FinishRun(ctx, finish, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
			pruned, err := store.PruneRuns(ctx, model.RunRetention{OlderThan: at.Add(time.Hour)})
			if err != nil || pruned.Deleted != 1 {
				t.Fatalf("first generation prune: %+v %v", pruned, err)
			}
			faultErr := attempt()
			created := waitRunFact(t, faultErr)
			if len(created.Creates) != 1 || !created.Creates[0].Attempted.Created || !created.Creates[0].CurrentKnown || !created.Creates[0].Present || created.Creates[0].Current.Projection != model.RunIdentity {
				t.Fatalf("inserted candidate missing exact same-cut identity: %+v", created)
			}
			created.Creates[0].Attempted.Run.Inputs[0] = 'x'
			again := waitRunFact(t, faultErr)
			if string(again.Creates[0].Attempted.Run.Inputs) != `{}` {
				t.Fatalf("returned fact mutated original attempt: %+v", again)
			}
			if _, err := store.FinishRun(ctx, finish, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
			pruned, err = store.PruneRuns(ctx, model.RunRetention{OlderThan: at.Add(time.Hour)})
			if err != nil || pruned.Deleted != 1 {
				t.Fatalf("second generation prune: %+v %v", pruned, err)
			}
			recreated, wasCreated, err := store.CreateRun(ctx, q)
			if err != nil || !wasCreated || recreated.Revision != again.Creates[0].Current.Revision || !recreated.CreatedAt.Equal(again.Creates[0].Current.CreatedAt) {
				t.Fatalf("same-frozen ABA setup: %+v created=%t %v", recreated, wasCreated, err)
			}
			if !again.Creates[0].Attempted.Created || replay.Creates[0].Attempted.Created {
				t.Fatal("later same-ID generation rewrote the original provenance")
			}
		})
	}
}

func TestRunFaultFailedCloseRevalidatesBeforeOwnerUnlock(t *testing.T) {
	db, store, cfg, scope, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-2 * time.Hour)
	q := faultRun("held-finish", at)
	if _, _, err := store.CreateRun(ctx, q); err != nil {
		t.Fatal(err)
	}
	finish := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: at.Add(time.Minute)}
	policy := model.RunRetentionPolicy{OlderThan: at.Add(time.Hour)}
	engine.InjectCommitAfterFailedCloseForModuleTest(t, db)
	_, err := store.ApplyRunOps(ctx, nil, nil, []model.RunFinish{finish}, policy)
	var fault *coredata.RunWriteFaultError
	if !errors.As(err, &fault) {
		t.Fatalf("missing pending Run fault: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if _, err := fault.WaitRevalidation(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined fault supplied a fact: %v", err)
	}
	cancel()
	if err := db.Close(); err != nil {
		t.Fatalf("later physical join and exact probe: %v", err)
	}
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.Retention.ChangedFinish || !fact.Retention.HasVictim {
		t.Fatalf("later-join policy fact: %+v %v", fact, err)
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
	pruned, err := current.PruneRuns(ctx, model.RunRetention{OlderThan: policy.OlderThan})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("reopened policy victim: %+v %v", pruned, err)
	}
}

func TestRunFaultConfirmedRollbackAndPreflightNeverSignalPolicy(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-time.Hour)
	q := faultRun("rollback", at)
	if _, _, err := store.CreateRun(ctx, q); err != nil {
		t.Fatal(err)
	}
	engine.InjectCommitBeforeForModuleTest(t, db)
	_, err := store.ApplyRunOps(ctx, nil, nil, []model.RunFinish{{ID: q.ID, Status: model.RunSucceeded, At: at}}, model.RunRetentionPolicy{OlderThan: time.Now()})
	if !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("confirmed rollback: %v", err)
	}
	var fault *coredata.RunWriteFaultError
	if errors.As(err, &fault) {
		t.Fatal("confirmed rollback manufactured a fault policy event")
	}
	before, _ := db.Stats()
	_, err = store.ApplyRunOps(ctx, nil, nil, nil, model.RunRetentionPolicy{KeepNewest: -1})
	if !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("policy preflight not typed: %v", err)
	}
	_, err = store.ApplyRunOps(ctx, make([]model.NewRun, model.MaxPageSize+1), nil, nil, model.RunRetentionPolicy{})
	if !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("batch preflight not typed: %v", err)
	}
	if err = store.ReserveRemoteRun(ctx, model.RemoteRunLink{}); !engine.DefinitelyNotCommitted(err) {
		t.Fatalf("reserve preflight not typed: %v", err)
	}
	after, _ := db.Stats()
	if after.Calls != before.Calls {
		t.Fatalf("preflight entered writer: calls %d -> %d", before.Calls, after.Calls)
	}
}

func TestRunFaultCanceledCommitConfirmedRollbackHasNoPolicyEvent(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-time.Hour)
	q := faultRun("canceled-rollback", at)
	if _, _, err := store.CreateRun(ctx, q); err != nil {
		t.Fatal(err)
	}
	entered, release := engine.GateConfirmedRollbackForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := store.ApplyRunOps(callCtx, nil, nil, []model.RunFinish{{ID: q.ID, Status: model.RunSucceeded, At: at}}, model.RunRetentionPolicy{OlderThan: time.Now()})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical COMMIT did not enter")
	}
	cancel()
	err := <-done
	var fault *coredata.RunWriteFaultError
	if !errors.As(err, &fault) {
		t.Fatalf("canceled physical attempt: %v", err)
	}
	release()
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("confirmed rollback manufactured a wake: %+v %v", fact, err)
	}
	current, err := store.GetRun(ctx, q.ID)
	if err != nil || !current.Status.Open() {
		t.Fatalf("rollback changed Run: %+v %v", current, err)
	}
}

func TestRunFaultRolledBackInBatchReplayKeepsOriginalAbsence(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	q := faultRun("in-batch-replay", time.Now().UTC())
	entered, release := engine.GateConfirmedRollbackForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := store.ApplyRunOps(callCtx, []model.NewRun{q, q}, nil, nil, model.RunRetentionPolicy{})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("batch COMMIT did not enter")
	}
	cancel()
	err := <-done
	release()
	fact := waitRunFact(t, err)
	if !fact.NonCommitted || len(fact.Creates) != 2 || !fact.Creates[0].Attempted.Created || fact.Creates[1].Attempted.Created ||
		!fact.Creates[0].CurrentKnown || fact.Creates[0].Present || !fact.Creates[1].CurrentKnown || fact.Creates[1].Present {
		t.Fatalf("in-batch replay was mistaken for a preexisting row: %+v", fact)
	}
	if _, err := store.GetRun(ctx, q.ID); err == nil {
		t.Fatal("rolled-back in-batch create survived")
	}
}

func TestRemoteFaultNonfinalOwnerRevalidatesOnlyReleasedPolicy(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	at := time.Now().UTC().Add(-2 * time.Hour)
	root := faultRun("group-root", at)
	a, b := faultRun("group-a", at), faultRun("group-b", at)
	for _, child := range []*model.NewRun{&a, &b} {
		child.ParentRunID, child.RootRunID, child.Depth, child.CallNodeID = root.ID, root.ID, 1, "call"
	}
	for _, q := range []model.NewRun{root, a, b} {
		if _, _, err := store.CreateRun(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	linkA := model.RemoteRunLink{OwnerRunID: a.ID, RootRunID: root.ID, TargetID: "agent", RunID: "remote-a", CallNodeID: "call", BootID: "boot"}
	linkB := model.RemoteRunLink{OwnerRunID: b.ID, RootRunID: root.ID, TargetID: "agent", RunID: "remote-b", CallNodeID: "call", BootID: "boot"}
	for _, link := range []model.RemoteRunLink{linkA, linkB} {
		if err := store.ReserveRemoteRun(ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{root.ID, a.ID, b.ID} {
		if _, err := store.FinishRun(ctx, model.RunFinish{ID: id, Status: model.RunSucceeded, At: at.Add(time.Minute)}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	policy := model.RunRetentionPolicy{OlderThan: at.Add(time.Hour)}
	engine.InjectCommitAfterForModuleTest(t, db)
	_, err := store.ResolveRemoteRun(ctx, linkA, policy)
	var fault *coredata.RemoteRunResolveError
	if !errors.As(err, &fault) || !fault.ValidatedAttempt || fault.AttemptedLink != linkA {
		t.Fatalf("lost exact attempt: %v", err)
	}
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.Released || !fact.LastForOwner || fact.LastForRoot || !fact.Retention.EligibilityReleased || !fact.Retention.HasVictim {
		t.Fatalf("nonfinal owner fact: %+v %v", fact, err)
	}
	if pending, err := store.HasPendingRemoteRoot(ctx, root.ID); err != nil || !pending {
		t.Fatalf("sibling protection lost: %t %v", pending, err)
	}
	pruned, err := store.PruneRuns(ctx, model.RunRetention{OlderThan: policy.OlderThan})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("only owner A is eligible: %+v %v", pruned, err)
	}
}

func TestRemoteFaultFailedCloseRetainsExactLinkUntilJoin(t *testing.T) {
	db, store, cfg, scope, link := remoteSettlementFixture(t)
	ctx := t.Context()
	policy := model.RunRetentionPolicy{OlderThan: time.Now().Add(time.Hour)}
	engine.InjectCommitAfterFailedCloseForModuleTest(t, db)
	_, err := store.ResolveRemoteRun(ctx, link, policy)
	var fault *coredata.RemoteRunResolveError
	if !errors.As(err, &fault) || !fault.ValidatedAttempt || fault.AttemptedLink != link {
		t.Fatalf("lost exact remote attempt: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if _, err := fault.WaitRevalidation(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined remote produced a fact: %v", err)
	}
	cancel()
	if err := db.Close(); err != nil {
		t.Fatalf("later remote physical join and probe: %v", err)
	}
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || fact.Link != link || !fact.Released || !fact.LastForOwner || !fact.LastForRoot || !fact.Retention.HasVictim {
		t.Fatalf("later-join exact remote fact: %+v %v", fact, err)
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
	replay, err := current.ResolveRemoteRun(ctx, link, policy)
	if err != nil || replay.Changed || replay.Retention.HasVictim {
		t.Fatalf("post-join replay manufactured a release: %+v %v", replay, err)
	}
}

func TestRemoteFaultCanceledCommitConfirmedRollbackHasNoRelease(t *testing.T) {
	db, store, _, _, link := remoteSettlementFixture(t)
	ctx := t.Context()
	entered, release := engine.GateConfirmedRollbackForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := store.ResolveRemoteRun(callCtx, link, model.RunRetentionPolicy{OlderThan: time.Now().Add(time.Hour)})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("remote physical COMMIT did not enter")
	}
	cancel()
	err := <-done
	var fault *coredata.RemoteRunResolveError
	if !errors.As(err, &fault) || !fault.ValidatedAttempt {
		t.Fatalf("canceled exact attempt: %v", err)
	}
	release()
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || fact.Released || fact.Retention != (model.RunRetentionEffect{}) {
		t.Fatalf("rolled-back release manufactured a wake: %+v %v", fact, err)
	}
	if pending, err := store.HasPendingRemoteRoot(ctx, link.RootRunID); err != nil || !pending {
		t.Fatalf("rolled-back release lost exact relation: pending=%t %v", pending, err)
	}
}

func waitReserveFact(t *testing.T, err error) coredata.RemoteRunReserveFact {
	t.Helper()
	var fault *coredata.RemoteRunReserveError
	var outcome *engine.WriteError
	if !errors.As(err, &fault) || !errors.As(err, &outcome) || outcome.Disposition != engine.WriteUnknown {
		t.Fatalf("expected original reserve unknown: %v", err)
	}
	fact, e := fault.WaitRevalidation(t.Context())
	if e != nil {
		t.Fatalf("same-attempt reserve revalidation: %v", e)
	}
	return fact
}

func TestReserveFaultDistinguishesInsertBindAndBoundReplay(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	owner := faultRun("reservation-owner", time.Now().UTC())
	if _, _, err := store.CreateRun(ctx, owner); err != nil {
		t.Fatal(err)
	}
	unbound := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "remote-child", CallNodeID: "call"}
	attempt := func(link model.RemoteRunLink) coredata.RemoteRunReserveFact {
		t.Helper()
		engine.InjectCommitAfterForModuleTest(t, db)
		return waitReserveFact(t, store.ReserveRemoteRun(ctx, link))
	}
	inserted := attempt(unbound)
	if !inserted.Validated || inserted.Action != "insert" || !inserted.Applied || inserted.BeforePresent || !inserted.CurrentKnown || !inserted.Present || inserted.Current != unbound {
		t.Fatalf("insert provenance: %+v", inserted)
	}
	existing := attempt(unbound)
	if !existing.Validated || existing.Action != "existing" || existing.Applied || !existing.BeforePresent || existing.Before != unbound || existing.Current != unbound {
		t.Fatalf("unbound replay provenance: %+v", existing)
	}
	bound := unbound
	bound.BootID = "agent-boot"
	binding := attempt(bound)
	if !binding.Validated || binding.Action != "bind" || !binding.Applied || !binding.BeforePresent || binding.Before != unbound || binding.Current != bound {
		t.Fatalf("new binding provenance: %+v", binding)
	}
	boundReplay := attempt(bound)
	if !boundReplay.Validated || boundReplay.Action != "existing" || boundReplay.Applied || boundReplay.Before != bound || boundReplay.Current != bound {
		t.Fatalf("already-bound replay provenance: %+v", boundReplay)
	}
	emptyReplay := attempt(unbound)
	if !emptyReplay.Validated || emptyReplay.Action != "existing" || emptyReplay.Applied || emptyReplay.Current != bound {
		t.Fatalf("unbound replay invented an unbind: %+v", emptyReplay)
	}
}

func TestReserveFaultCanceledBindRollbackPreservesPriorBoot(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	owner := faultRun("bind-rollback-owner", time.Now().UTC())
	if _, _, err := store.CreateRun(ctx, owner); err != nil {
		t.Fatal(err)
	}
	unbound := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "child", CallNodeID: "call"}
	if err := store.ReserveRemoteRun(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	bound := unbound
	bound.BootID = "new-boot"
	entered, release := engine.GateConfirmedRollbackForModuleTest(t, db)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- store.ReserveRemoteRun(callCtx, bound) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bind COMMIT did not enter")
	}
	cancel()
	err := <-done
	release()
	fact := waitReserveFact(t, err)
	if !fact.NonCommitted || !fact.Validated || fact.Action != "bind" || !fact.BeforePresent || fact.Before != unbound || !fact.CurrentKnown || !fact.Present || fact.Current != unbound {
		t.Fatalf("rolled-back bind lost prior exact boot: %+v", fact)
	}
}

func TestReserveFaultFailedCloseJoinsBoundFactBeforeUnlock(t *testing.T) {
	db, store, cfg, scope, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	owner := faultRun("bind-held-owner", time.Now().UTC())
	if _, _, err := store.CreateRun(ctx, owner); err != nil {
		t.Fatal(err)
	}
	unbound := model.RemoteRunLink{OwnerRunID: owner.ID, RootRunID: owner.ID, TargetID: "agent", RunID: "child", CallNodeID: "call"}
	if err := store.ReserveRemoteRun(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	bound := unbound
	bound.BootID = "agent-boot"
	engine.InjectCommitAfterFailedCloseForModuleTest(t, db)
	err := store.ReserveRemoteRun(ctx, bound)
	var fault *coredata.RemoteRunReserveError
	if !errors.As(err, &fault) {
		t.Fatalf("missing retained reserve owner: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if _, err := fault.WaitRevalidation(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined bind exposed fact: %v", err)
	}
	cancel()
	if err := db.Close(); err != nil {
		t.Fatalf("later physical join: %v", err)
	}
	fact, err := fault.WaitRevalidation(ctx)
	if err != nil || !fact.Validated || fact.Action != "bind" || !fact.Applied || fact.Before != unbound || !fact.CurrentKnown || !fact.Present || fact.Current != bound {
		t.Fatalf("later-join bound fact: %+v %v", fact, err)
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
	result, err := current.ResolveRemoteRun(ctx, fact.Current, model.RunRetentionPolicy{})
	if err != nil || !result.Changed {
		t.Fatalf("actual bound relation cannot resolve: %+v %v", result, err)
	}
}
