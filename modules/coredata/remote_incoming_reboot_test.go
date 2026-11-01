package coredata_test

import (
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// An owner-verified local join must still wait for all durable remote children
// and the exact incoming principal before releasing an interrupted Run.
func TestIncomingReleaseAfterStartupInterrupt(t *testing.T) {
	f := openStore(t)
	clean := incomingChild("clean", "parent", "parent", "core", "principal")
	blocked := incomingChild("blocked", "parent", "parent", "core", "principal")
	nested := incomingChild("nested", "parent", "parent", "core", "principal")
	local := childRun("local", "nested", "parent", 2)
	for _, q := range []model.NewRun{newRun("parent"), clean, blocked, nested, local} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	link := model.RemoteRunLink{OwnerRunID: "blocked", RootRunID: "parent", TargetID: "other-agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	nestedLink := link
	nestedLink.OwnerRunID, nestedLink.RunID = local.ID, "nested-remote"
	if err := f.core.ReserveRemoteRun(f.ctx, nestedLink); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, releaseWant(clean)); errCode(err) != "failed_precondition" {
		t.Fatalf("open incoming child released: %v", err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	want := func(q model.NewRun) model.IncomingRunRelease {
		w := releaseWant(q)
		w.LocalJoined = true
		return w
	}
	if _, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, releaseWant(clean)); errCode(err) != "failed_precondition" {
		t.Fatalf("interrupted child with no local join proof released: %v", err)
	}
	foreign := want(clean)
	foreign.PrincipalID = "foreign"
	if _, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, foreign); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign caller released child: %v", err)
	}
	if _, err := f.core.ReleaseIncomingRun(f.ctx, blocked.ID, want(blocked)); errCode(err) != "failed_precondition" {
		t.Fatalf("unjoined remote descendant released: %v", err)
	}
	if _, err := f.core.ReleaseIncomingRun(f.ctx, nested.ID, want(nested)); errCode(err) != "failed_precondition" {
		t.Fatalf("unjoined remote of local descendant released: %v", err)
	}
	openChild := childRun("still-open", clean.ID, "parent", 2)
	if _, _, err := f.core.CreateRun(f.ctx, openChild); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, want(clean)); errCode(err) != "failed_precondition" {
		t.Fatalf("open local descendant released: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: openChild.ID, Status: model.RunCanceled, At: epoch.Add(2 * time.Second)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, want(clean)); err != nil || !changed {
		t.Fatalf("clean interrupted child not released: changed=%v err=%v", changed, err)
	}
	if changed, err := f.core.ReleaseIncomingRun(f.ctx, clean.ID, want(clean)); err != nil || changed {
		t.Fatalf("release retry: changed=%v err=%v", changed, err)
	}
	// The actual durable descendant relations are joined through their own
	// settlement order, after which the incoming holds can be released.
	for _, pending := range []model.RemoteRunLink{link, nestedLink} {
		releaseBoundRemote(t, f, pending)
		if _, err := f.core.ResolveRemoteRun(f.ctx, pending, model.RunRetentionPolicy{}); err != nil {
			t.Fatalf("resolve descendant %s: %v", pending.RunID, err)
		}
	}
	for _, q := range []model.NewRun{blocked, nested} {
		if changed, err := f.core.ReleaseIncomingRun(f.ctx, q.ID, want(q)); err != nil || !changed {
			t.Fatalf("joined descendant %s still pinned: changed=%v err=%v", q.ID, changed, err)
		}
	}
}

// A completed local ancestor can be normally pruned while a deeper remote
// owner is retained. The missing ancestry is unknown, not proof of a join.
func TestInterruptedIncomingKeepsUnknownPrunedRemoteAncestry(t *testing.T) {
	f := openStore(t)
	incoming := incomingChild("incoming", "root", "root", "core", "principal")
	for _, q := range []model.NewRun{newRun("root"), incoming,
		childRun("intermediate", incoming.ID, "root", 2), childRun("remote-owner", "intermediate", "root", 3)} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	link := model.RemoteRunLink{OwnerRunID: "remote-owner", RootRunID: "root", TargetID: "other-agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.InterruptOpenRuns(f.ctx, epoch.Add(time.Second), model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.GetRun(f.ctx, "intermediate"); errCode(err) != "not_found" {
		t.Fatalf("fixture did not prune the intermediate Run: %v", err)
	}
	want := releaseWant(incoming)
	want.LocalJoined = true
	if _, err := f.core.ReleaseIncomingRun(f.ctx, incoming.ID, want); errCode(err) != "failed_precondition" {
		t.Fatalf("missing ancestry hid retained remote work: %v", err)
	}
}
