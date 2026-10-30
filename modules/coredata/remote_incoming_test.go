package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func incomingChild(id, parent, root, callerTarget, principal string) model.NewRun {
	q := childRun(id, parent, root, 1)
	q.IncomingHold = model.IncomingRemoteHold{TargetID: callerTarget, PrincipalID: principal}
	return q
}

func releaseWant(run model.NewRun) model.IncomingRunRelease {
	return model.IncomingRunRelease{
		ParentRunID: run.ParentRunID, RootRunID: run.RootRunID, CallNodeID: run.CallNodeID,
		TargetID: run.IncomingHold.TargetID, PrincipalID: run.IncomingHold.PrincipalID,
	}
}

func TestIncomingHoldStampsAtomicallyAndBlocksRetention(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("child", "parent", "parent", "core", "principal-c")
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	stored, err := f.core.GetRun(f.ctx, "child")
	if err != nil || stored.IncomingHold.TargetID != "core" || stored.IncomingHold.PrincipalID != "principal-c" || stored.IncomingHold.Released {
		t.Fatalf("hold not stamped with child: %+v %v", stored.IncomingHold, err)
	}
	for _, id := range []string{"child", "parent"} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("unreleased hold did not pin exactly the child: %+v %v", pruned, err)
	}
	if _, err = f.core.GetRun(f.ctx, "child"); err != nil {
		t.Fatal("held child pruned")
	}
	changed, err := f.core.ReleaseIncomingRun(f.ctx, "child", releaseWant(child))
	if err != nil || !changed {
		t.Fatalf("release: %t %v", changed, err)
	}
	stored, err = f.core.GetRun(f.ctx, "child")
	if err != nil || !stored.IncomingHold.Released {
		t.Fatalf("released flag not durable: %+v %v", stored.IncomingHold, err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("released hold still pinned: %+v %v", pruned, err)
	}
}

func TestIncomingHoldValidationRejectsMalformedAdmission(t *testing.T) {
	f := openStore(t)
	for _, tc := range []struct {
		name string
		edit func(*model.NewRun)
	}{
		{"no parent", func(q *model.NewRun) { q.ParentRunID = "" }},
		{"no call", func(q *model.NewRun) { q.CallNodeID = "" }},
		{"self target", func(q *model.NewRun) { q.IncomingHold.TargetID = q.TargetID }},
		{"principal without target", func(q *model.NewRun) { q.IncomingHold.TargetID = "" }},
		{"admitted released", func(q *model.NewRun) { q.IncomingHold.Released = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := incomingChild("child", "parent", "parent", "core", "principal-c")
			tc.edit(&q)
			if _, _, err := f.core.CreateRun(f.ctx, q); errCode(err) != "invalid_argument" {
				t.Fatalf("admitted malformed hold: %v", err)
			}
		})
	}
}

func TestReleaseIncomingRunExactIdempotentAndStaged(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("child", "parent", "parent", "core", "principal-c")
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.core.CreateRun(f.ctx, childRun("local-child", "parent", "parent", 1)); err != nil {
		t.Fatal(err)
	}
	want := releaseWant(child)

	// A live child refuses: the caller may only release after the local run
	// reached a terminal state (physical join proof precedes release).
	if _, err := f.core.ReleaseIncomingRun(f.ctx, "child", want); errCode(err) != "failed_precondition" {
		t.Fatalf("live child released: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "child", Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}

	// Foreign or stale facts never release this child's hold.
	for _, tc := range []struct {
		name string
		edit func(*model.IncomingRunRelease)
	}{
		{"foreign caller identity", func(w *model.IncomingRunRelease) { w.PrincipalID = "other-principal" }},
		{"foreign target", func(w *model.IncomingRunRelease) { w.TargetID = "other" }},
		{"stale call", func(w *model.IncomingRunRelease) { w.CallNodeID = "other-call" }},
		{"stale parent", func(w *model.IncomingRunRelease) { w.ParentRunID = "other-parent" }},
		{"stale root", func(w *model.IncomingRunRelease) { w.RootRunID = "other-root" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := want
			tc.edit(&w)
			if _, err := f.core.ReleaseIncomingRun(f.ctx, "child", w); errCode(err) != "failed_precondition" {
				t.Fatalf("%s released another hold: %v", tc.name, err)
			}
		})
	}
	stored, err := f.core.GetRun(f.ctx, "child")
	if err != nil || stored.IncomingHold.Released {
		t.Fatal("foreign attempt released the hold")
	}

	// A run that never admitted a remote caller is not an idempotent success.
	if _, err := f.core.ReleaseIncomingRun(f.ctx, "local-child", want); errCode(err) != "failed_precondition" {
		t.Fatalf("hold-free run released: %v", err)
	}

	changed, err := f.core.ReleaseIncomingRun(f.ctx, "child", want)
	if err != nil || !changed {
		t.Fatalf("release: %t %v", changed, err)
	}
	// An exact repeat is idempotent, including after the child is pruned:
	// the hold cannot outlive its Run row.
	changed, err = f.core.ReleaseIncomingRun(f.ctx, "child", want)
	if err != nil || changed {
		t.Fatalf("repeat release changed: %t %v", changed, err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "parent", Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10})
	if err != nil || pruned.Deleted != 2 {
		t.Fatalf("released child still pinned: %+v %v", pruned, err)
	}
	changed, err = f.core.ReleaseIncomingRun(f.ctx, "child", want)
	if err != nil || changed {
		t.Fatalf("post-prune release: %t %v", changed, err)
	}
}

func TestReleaseIncomingRunRefusesUncleanDispositions(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("parent")); err != nil {
		t.Fatal(err)
	}
	child := incomingChild("child", "parent", "parent", "core", "principal-c")
	if _, _, err := f.core.CreateRun(f.ctx, child); err != nil {
		t.Fatal(err)
	}
	want := releaseWant(child)

	// An interrupted child may still own remote work: its hold refuses
	// release even though the row is no longer open.
	f.exec(t, `UPDATE runs SET status='interrupted',finished_at=? WHERE id=?`, epoch.Unix(), "child")
	if _, err := f.core.ReleaseIncomingRun(f.ctx, "child", want); errCode(err) != "failed_precondition" {
		t.Fatalf("interrupted child released: %v", err)
	}
	// A clean-terminal child whose cleanup failed is not a join proof either.
	f.exec(t, `UPDATE runs SET status='succeeded',cleanup_failed=1 WHERE id=?`, "child")
	if _, err := f.core.ReleaseIncomingRun(f.ctx, "child", want); errCode(err) != "failed_precondition" {
		t.Fatalf("cleanup-failed child released: %v", err)
	}
	// Refusals never touch the hold, and the hold still pins retention.
	stored, err := f.core.GetRun(f.ctx, "child")
	if err != nil || stored.IncomingHold.Released {
		t.Fatalf("refused release mutated the hold: %+v %v", stored.IncomingHold, err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "parent", Status: model.RunSucceeded, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("unclean child's hold failed to pin: %+v %v", pruned, err)
	}
	if _, err = f.core.GetRun(f.ctx, "child"); err != nil {
		t.Fatal("unclean child pruned")
	}

	// Once cleanup clears, the same exact release lands.
	f.exec(t, `UPDATE runs SET cleanup_failed=0 WHERE id=?`, "child")
	changed, err := f.core.ReleaseIncomingRun(f.ctx, "child", want)
	if err != nil || !changed {
		t.Fatalf("clean release blocked: %t %v", changed, err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 10})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("released hold still pinned: %+v %v", pruned, err)
	}
}

func TestSettleReleaseConsumeOnlyPrefundedEscrow(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	link.Phase = model.RemoteRunOpen
	funded, err := json.Marshal(fundedRemoteLink(link))
	if err != nil {
		t.Fatal(err)
	}
	pending := func() (count int, total int64) {
		if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT pending_remote_count,pending_remote_bytes FROM runs WHERE scope=? AND id=?`, engine.ScopeID(f.scope), "owner").Scan(&count, &total)
		}); err != nil {
			t.Fatal(err)
		}
		return count, total
	}
	if count, total := pending(); count != 1 || total != int64(len(funded)+2) {
		t.Fatalf("reservation did not prefund the phase bound: count=%d total=%d", count, total)
	}

	// Saturate the scope quota after admission: every later phase write must
	// fit entirely inside what the reservation already prefunded.
	f.exec(t, `UPDATE core_data_usage SET bytes=? WHERE scope=?`, coredata.MaxScopeBytes, engine.ScopeID(f.scope))
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "succeeded", FinishedAt: epoch}); err != nil {
		t.Fatalf("settle needed new capacity: %v", err)
	}
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch); err != nil {
		t.Fatalf("release needed new capacity: %v", err)
	}
	// The actual encoding grew inside the prefund; the funded counter and the
	// exhausted quota never moved.
	var encoded int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT encoded_bytes FROM run_remote_pending WHERE scope=? AND owner_run_id=?`, engine.ScopeID(f.scope), "owner").Scan(&encoded)
	}); err != nil {
		t.Fatal(err)
	}
	if encoded <= 0 || encoded > int64(len(funded)) {
		t.Fatalf("released encoding %d outside prefund %d", encoded, len(funded))
	}
	if count, total := pending(); count != 1 || total != int64(len(funded)+2) {
		t.Fatalf("phase writes moved the funded counter: count=%d total=%d", count, total)
	}
	// Resolution returns the whole prefund — still with zero new capacity.
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("resolve needed new capacity: %v", err)
	}
	if count, total := pending(); count != 0 || total != 0 {
		t.Fatalf("resolved relation left counters: %d/%d", count, total)
	}
}

func TestSettleRemoteRunRequiresCleanDisposition(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	// Anything that is not a clean terminal status can never be a join proof
	// and would break the prefunded encoding bound.
	for _, d := range []string{"", "settled", "interrupted", "queued", "running", "unknown"} {
		if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: d, FinishedAt: epoch}); errCode(err) != "invalid_argument" {
			t.Fatalf("disposition %q accepted: %v", d, err)
		}
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "stopped", FinishedAt: epoch}); err != nil {
		t.Fatalf("clean disposition refused: %v", err)
	}
}

func TestSettleRemoteRunGatesBoundRelation(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	settled := model.RemoteRunSettlement{Disposition: "succeeded", FinishedAt: epoch}

	// Bound relations refuse a bare resolve; settlement must come first.
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("open bound relation resolved: %v", err)
	}
	// Release before settle is impossible.
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch); errCode(err) != "failed_precondition" {
		t.Fatalf("unsettled relation released: %v", err)
	}
	// Identity conflicts settle nothing.
	bad := link
	bad.BootID = "foreign"
	if _, err := f.core.SettleRemoteRun(f.ctx, bad, settled); errCode(err) != "failed_precondition" {
		t.Fatalf("foreign boot settled: %v", err)
	}
	// An unbound reservation has no remote process to settle against.
	unbound := link
	unbound.RunID, unbound.BootID = "remote-2", ""
	if err := f.core.ReserveRemoteRun(f.ctx, unbound); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, unbound, settled); errCode(err) != "invalid_argument" {
		t.Fatalf("unbound reservation settled: %v", err)
	}
	// The unbound reservation still resolves directly; it provably sent no Start.
	if _, err := f.core.ResolveRemoteRun(f.ctx, unbound, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("unsent reservation blocked: %v", err)
	}

	changed, err := f.core.SettleRemoteRun(f.ctx, link, settled)
	if err != nil || !changed {
		t.Fatalf("settle: %t %v", changed, err)
	}
	// Settled alone does not permit resolution; the callee release must land.
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("settled-but-held relation resolved: %v", err)
	}
	// Same evidence repeats idempotently; conflicting evidence refuses.
	changed, err = f.core.SettleRemoteRun(f.ctx, link, settled)
	if err != nil || changed {
		t.Fatalf("settle replay changed: %t %v", changed, err)
	}
	conflict := settled
	conflict.Disposition = "failed"
	if _, err := f.core.SettleRemoteRun(f.ctx, link, conflict); errCode(err) != "failed_precondition" {
		t.Fatalf("conflicting settle accepted: %v", err)
	}
	// The restart-from-settled path: release marks once and repeats safely.
	changed, err = f.core.MarkRemoteRunReleased(f.ctx, link, epoch)
	if err != nil || !changed {
		t.Fatalf("release mark: %t %v", changed, err)
	}
	changed, err = f.core.MarkRemoteRunReleased(f.ctx, link, epoch)
	if err != nil || changed {
		t.Fatalf("release replay changed: %t %v", changed, err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, settled); errCode(err) != "failed_precondition" {
		t.Fatalf("released relation re-settled: %v", err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatalf("released relation blocked: %v", err)
	}
}

func TestResolveUnsentRemoteRunRefusesBoundProcess(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	bound := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "bound", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, bound); err != nil {
		t.Fatal(err)
	}
	// Neither an unbooted claim nor the exact bound boot can pretend unsent.
	if _, err := f.core.ResolveUnsentRemoteRun(f.ctx, bound, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatalf("bound relation resolved as unsent: %v", err)
	}
	unbound := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "unsent", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, unbound); err != nil {
		t.Fatal(err)
	}
	out, err := f.core.ResolveUnsentRemoteRun(f.ctx, unbound, model.RunRetentionPolicy{})
	if err != nil || !out.Changed {
		t.Fatalf("unsent reservation blocked: %+v %v", out, err)
	}
}

func TestHasPendingRemoteRootCountsOnlyOpenRelations(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "owner"); err != nil || !pending {
		t.Fatalf("open relation invisible: %v %v", pending, err)
	}
	if _, err := f.core.SettleRemoteRun(f.ctx, link, model.RemoteRunSettlement{Disposition: "succeeded", FinishedAt: epoch}); err != nil {
		t.Fatal(err)
	}
	// A settled relation still exists, but it no longer fences restart:
	// restart-from-settled retries the release instead of stalling.
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "owner"); err != nil || pending {
		t.Fatalf("settled relation still fences restart: %v %v", pending, err)
	}
	if _, err := f.core.MarkRemoteRunReleased(f.ctx, link, epoch); err != nil {
		t.Fatal(err)
	}
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "owner"); err != nil || pending {
		t.Fatalf("released relation still fences restart: %v %v", pending, err)
	}
	// The row still lists until the final resolve removes it.
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 4)
	if err != nil || len(page.Links) != 1 || page.Links[0].Phase != model.RemoteRunReleased {
		t.Fatalf("released relation unlisted: %+v %v", page, err)
	}
}
