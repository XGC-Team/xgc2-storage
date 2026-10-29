package coredata_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func remoteLink(owner, root, run string) model.RemoteRunLink {
	return model.RemoteRunLink{OwnerRunID: owner, RootRunID: root, TargetID: "agent", RunID: run, CallNodeID: "call", BootID: "boot"}
}

func TestResolveRemoteRunReturnsOnlyTheCommittedFinalRootCut(t *testing.T) {
	f := openStore(t)
	for _, run := range []model.NewRun{newRun("root"), childRun("owner-a", "root", "root", 1), childRun("owner-b", "root", "root", 1), newRun("other-root")} {
		if _, _, err := f.core.CreateRun(f.ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	a, b, other := remoteLink("owner-a", "root", "remote-a"), remoteLink("owner-b", "root", "remote-b"), remoteLink("other-root", "other-root", "remote-other")
	for _, link := range []model.RemoteRunLink{a, b, other} {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	rootAt, aAt, bAt := epoch.Add(20*time.Minute), epoch.Add(time.Minute), epoch.Add(2*time.Minute)
	for id, at := range map[string]time.Time{"root": rootAt, "owner-a": aAt, "owner-b": bAt} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunCanceled, At: at}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t)
	// Malformed frozen Run payloads cannot affect this fixed identity/finish projection.
	f.exec(t, `UPDATE runs SET inputs_json=x'01',trigger_json=x'02',result_json=x'03',nodes_json=x'04' WHERE id IN ('root','owner-a','owner-b')`)
	rows, bytes := usageRows(t, f)
	for _, bad := range []model.RemoteRunLink{
		func() model.RemoteRunLink { x := a; x.BootID = "wrong"; return x }(),
		func() model.RemoteRunLink { x := a; x.CallNodeID = "wrong"; return x }(),
		func() model.RemoteRunLink { x := a; x.RootRunID = "other-root"; return x }(),
	} {
		result, err := f.core.ResolveRemoteRun(f.ctx, bad, model.RunRetentionPolicy{})
		if errCode(err) != "failed_precondition" || result != (model.RemoteRunResolveResult{}) {
			t.Fatalf("identity mismatch returned committed facts: %+v %v", result, err)
		}
	}
	if gotRows, gotBytes := usageRows(t, f); gotRows != rows || gotBytes != bytes {
		t.Fatal("rejected resolution changed quota")
	}
	first, err := f.core.ResolveRemoteRun(f.ctx, a, model.RunRetentionPolicy{})
	if err != nil || first != (model.RemoteRunResolveResult{RootRunID: "root", Changed: true, LastForOwner: true, Retention: model.RunRetentionEffect{EligibilityReleased: true}}) {
		t.Fatalf("nonfinal owner: %+v %v", first, err)
	}
	rowsAfterFirst, bytesAfterFirst := usageRows(t, f)
	if rowsAfterFirst != rows-1 || bytesAfterFirst >= bytes {
		t.Fatalf("first release quota: rows %d->%d bytes %d->%d", rows, rowsAfterFirst, bytes, bytesAfterFirst)
	}
	replay, err := f.core.ResolveRemoteRun(f.ctx, a, model.RunRetentionPolicy{})
	if err != nil || replay != (model.RemoteRunResolveResult{RootRunID: "root"}) {
		t.Fatalf("nonfinal replay: %+v %v", replay, err)
	}
	if gotRows, gotBytes := usageRows(t, f); gotRows != rowsAfterFirst || gotBytes != bytesAfterFirst {
		t.Fatal("replay changed quota")
	}
	last, err := f.core.ResolveRemoteRun(f.ctx, b, model.RunRetentionPolicy{})
	if err != nil || !last.Changed || !last.LastForRoot || !last.RootFound || last.RootRunID != "root" || !last.RootFinishedAt.Equal(rootAt) || !last.OwnerFinishedAt.Equal(bAt) {
		t.Fatalf("final owner: %+v %v", last, err)
	}
	replay, err = f.core.ResolveRemoteRun(f.ctx, b, model.RunRetentionPolicy{})
	if err != nil || replay != (model.RemoteRunResolveResult{RootRunID: "root"}) {
		t.Fatalf("final replay: %+v %v", replay, err)
	}
	if gotRows, gotBytes := usageRows(t, f); gotRows != rows-2 || gotBytes >= bytesAfterFirst {
		t.Fatal("last resolution did not release exactly one relation")
	}
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "other-root"); err != nil || !pending {
		t.Fatalf("an unrelated root lost protection: pending=%v %v", pending, err)
	}
	// The root is newer than the owner; the resulting cut permits the eligible
	// older children to be considered without pretending that the root is old.
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(10 * time.Minute), Limit: 100})
	if err != nil || pruned.Deleted != 2 {
		t.Fatalf("child eligibility under newer terminal root: %+v %v", pruned, err)
	}
}

func TestResolveRemoteRunDistinguishesLiveAndMissingRoots(t *testing.T) {
	for _, exists := range []bool{true, false} {
		t.Run(fmt.Sprintf("root_found_%t", exists), func(t *testing.T) {
			f := openStore(t)
			if exists {
				if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := f.core.CreateRun(f.ctx, childRun("owner", "root", "root", 1)); err != nil {
				t.Fatal(err)
			}
			link := remoteLink("owner", "root", "remote")
			if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
				t.Fatal(err)
			}
			at := epoch.Add(time.Minute)
			if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "owner", Status: model.RunCanceled, At: at}, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
			f.reopen(t)
			result, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
			if err != nil || !result.Changed || !result.LastForRoot || result.RootFound != exists || !result.RootFinishedAt.IsZero() || !result.OwnerFinishedAt.Equal(at) {
				t.Fatalf("root status: %+v %v", result, err)
			}
			if exists {
				pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
				if err != nil || pruned.Deleted != 0 {
					t.Fatalf("live root lost child: %+v %v", pruned, err)
				}
			}
		})
	}
}

func TestResolveRemoteRunConcurrentOwnersAndDuplicateJoin(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("duplicate_%t", duplicate), func(t *testing.T) {
			f := openStore(t)
			for _, q := range []model.NewRun{newRun("root"), childRun("a", "root", "root", 1), childRun("b", "root", "root", 1)} {
				if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			a, b := remoteLink("a", "root", "a-remote"), remoteLink("b", "root", "b-remote")
			if err := f.core.ReserveRemoteRun(f.ctx, a); err != nil {
				t.Fatal(err)
			}
			if duplicate {
				b = a
			} else if err := f.core.ReserveRemoteRun(f.ctx, b); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var results [2]model.RemoteRunResolveResult
			var errs [2]error
			for i, link := range []model.RemoteRunLink{a, b} {
				wg.Add(1)
				go func(i int, link model.RemoteRunLink) {
					defer wg.Done()
					<-start
					results[i], errs[i] = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
				}(i, link)
			}
			close(start)
			wg.Wait()
			changed, last := 0, 0
			for i, result := range results {
				if errs[i] != nil || result.RootRunID != "root" {
					t.Fatalf("result[%d]=%+v %v", i, result, errs[i])
				}
				if result.Changed {
					changed++
				}
				if result.LastForRoot {
					last++
					if !result.Changed || !result.RootFound || !result.RootFinishedAt.IsZero() || !result.OwnerFinishedAt.IsZero() {
						t.Fatalf("invalid last edge: %+v", result)
					}
				}
			}
			wantChanges := 2
			if duplicate {
				wantChanges = 1
			}
			if changed != wantChanges || last != 1 {
				t.Fatalf("changed=%d last=%d results=%+v", changed, last, results)
			}
		})
	}
}

func TestResolveRemoteRunScopeAndCancellationLeaveNoFalseCommit(t *testing.T) {
	f := openStore(t)
	otherScope := f.scope
	otherScope.Workspace = "other"
	other, err := coredata.New(f.db, otherScope)
	if err != nil {
		t.Fatal(err)
	}
	link := remoteLink("root", "root", "remote")
	for _, owner := range []*coredata.Store{f.core, other} {
		if _, _, err := owner.CreateRun(f.ctx, newRun("root")); err != nil {
			t.Fatal(err)
		}
		if err := owner.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	result, err := f.core.ResolveRemoteRun(ctx, link, model.RunRetentionPolicy{})
	if errCode(err) != "cancelled" || result != (model.RemoteRunResolveResult{}) {
		t.Fatalf("canceled result=%+v %v", result, err)
	}
	for _, owner := range []*coredata.Store{f.core, other} {
		pending, err := owner.HasPendingRemoteRoot(f.ctx, "root")
		if err != nil || !pending {
			t.Fatalf("canceled or foreign mutation removed protection: %v %v", pending, err)
		}
	}
	result, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || !result.LastForRoot {
		t.Fatalf("scope release=%+v %v", result, err)
	}
	if pending, err := other.HasPendingRemoteRoot(f.ctx, "root"); err != nil || !pending {
		t.Fatalf("other scope lost protection: %v %v", pending, err)
	}
}

func TestResolveRemoteRunUsesTransactionCutWhenRootFinishesConcurrently(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	link := remoteLink("root", "root", "remote")
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	at := epoch.Add(time.Hour)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var resolved model.RemoteRunResolveResult
	var resolveErr, finishErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		resolved, resolveErr = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, finishErr = f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunCanceled, At: at}, model.RunRetentionPolicy{})
	}()
	close(start)
	wg.Wait()
	if finishErr != nil || resolveErr != nil || !resolved.Changed || !resolved.LastForRoot || !resolved.RootFound {
		t.Fatalf("finish=%v resolution=%+v %v", finishErr, resolved, resolveErr)
	}
	if !resolved.RootFinishedAt.IsZero() && !resolved.RootFinishedAt.Equal(at) {
		t.Fatalf("root finish is not its durable cut: %+v", resolved)
	}
	if !resolved.RootFinishedAt.Equal(resolved.OwnerFinishedAt) {
		t.Fatalf("same Run has inconsistent finish facts: %+v", resolved)
	}
	if run, err := f.core.GetRun(f.ctx, "root"); err != nil || !run.FinishedAt.Equal(at) {
		t.Fatalf("finish not durable: %+v %v", run, err)
	}
}

func TestResolveRemoteRunNewReservationReprotectsTerminalRoot(t *testing.T) {
	f := openStore(t)
	for _, q := range []model.NewRun{newRun("root"), childRun("owner", "root", "root", 1)} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	a, b := remoteLink("owner", "root", "a"), remoteLink("owner", "root", "b")
	if err := f.core.ReserveRemoteRun(f.ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunCanceled, At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	last, err := f.core.ResolveRemoteRun(f.ctx, a, model.RunRetentionPolicy{})
	if err != nil || !last.Changed || !last.LastForRoot || !last.RootFound || last.RootFinishedAt.IsZero() {
		t.Fatalf("last cut=%+v %v", last, err)
	}
	if err := f.core.ReserveRemoteRun(f.ctx, b); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("later pending relation failed to reprotect root: %+v %v", pruned, err)
	}
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "root"); err != nil || !pending {
		t.Fatalf("later relation disappeared: %v %v", pending, err)
	}
}

func TestResolveRemoteRunReportsCancellationVersusActualDurableEffect(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	var committed, canceled int
	for i := range 20 {
		link := remoteLink("root", "root", fmt.Sprintf("remote-%02d", i))
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(f.ctx)
		start := make(chan struct{})
		done := make(chan struct{})
		var result model.RemoteRunResolveResult
		var err error
		go func() {
			<-start
			result, err = f.core.ResolveRemoteRun(ctx, link, model.RunRetentionPolicy{})
			close(done)
		}()
		close(start)
		if i%2 == 0 {
			cancel()
		} else {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}
		<-done
		pending, pendingErr := f.core.HasPendingRemoteRoot(f.ctx, "root")
		if pendingErr != nil {
			t.Fatal(pendingErr)
		}
		if err == nil {
			committed++
			if !result.Changed || !result.LastForRoot || pending {
				t.Fatalf("successful commit disagrees with stored relation: %+v pending=%v", result, pending)
			}
		} else {
			canceled++
			if errCode(err) != "cancelled" || result != (model.RemoteRunResolveResult{}) || !pending {
				t.Fatalf("canceled request changed relation or returned facts: %+v pending=%v err=%v", result, pending, err)
			}
			if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("deliberate cancellation race: committed=%d canceled-before-commit=%d", committed, canceled)
}

func TestResolveRemoteRunOneIntentReturnsOwnershipToBaseline(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("root")); err != nil {
		t.Fatal(err)
	}
	rows, bytes := usageRows(t, f)
	link := remoteLink("root", "root", "remote")
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	before := stats(t, f)
	result, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || !result.LastForRoot || !result.RootFound {
		t.Fatalf("one resolution=%+v %v", result, err)
	}
	if durable, relaxed := commits(t, f, before); durable != 1 || relaxed != 0 {
		t.Fatalf("one intent committed durable=%d relaxed=%d", durable, relaxed)
	}
	if gotRows, gotBytes := usageRows(t, f); gotRows != rows || gotBytes != bytes {
		t.Fatalf("joined quota did not return to baseline: rows=%d/%d bytes=%d/%d", gotRows, rows, gotBytes, bytes)
	}
	if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "root"); err != nil || pending {
		t.Fatalf("joined root still pending: %v %v", pending, err)
	}
	t.Logf("one changed intent: 1 durable transaction; after join rows=%d bytes=%d and no pending relation", rows, bytes)
}

func TestResolveRemoteRunRacesNewReservationWithoutLosingProtection(t *testing.T) {
	f := openStore(t)
	for _, q := range []model.NewRun{newRun("root"), childRun("owner", "root", "root", 1)} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "root", Status: model.RunCanceled, At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		a := remoteLink("owner", "root", fmt.Sprintf("old-%d", i))
		b := remoteLink("owner", "root", fmt.Sprintf("new-%d", i))
		if err := f.core.ReserveRemoteRun(f.ctx, a); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var resolved model.RemoteRunResolveResult
		var resolveErr, reserveErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			resolved, resolveErr = f.core.ResolveRemoteRun(f.ctx, a, model.RunRetentionPolicy{})
		}()
		go func() { defer wg.Done(); <-start; reserveErr = f.core.ReserveRemoteRun(f.ctx, b) }()
		close(start)
		wg.Wait()
		if resolveErr != nil || reserveErr != nil || !resolved.Changed || resolved.RootRunID != "root" {
			t.Fatalf("resolve=%+v %v reserve=%v", resolved, resolveErr, reserveErr)
		}
		if resolved.LastForRoot && (!resolved.RootFound || resolved.RootFinishedAt.IsZero()) {
			t.Fatalf("final edge lost terminal root fact: %+v", resolved)
		}
		if pending, err := f.core.HasPendingRemoteRoot(f.ctx, "root"); err != nil || !pending {
			t.Fatalf("new reservation not protected: %v %v", pending, err)
		}
		if _, err := f.core.ResolveRemoteRun(f.ctx, b, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
}
