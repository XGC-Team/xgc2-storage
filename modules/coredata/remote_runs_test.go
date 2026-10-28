package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestPendingRemoteRunProtectsOwnerAndRootAcrossReopenUntilExactJoin(t *testing.T) {
	f := openStore(t)
	for _, q := range []model.NewRun{newRun("root"), childRun("owner", "root", "root", 1), newRun("unrelated")} {
		if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "root", TargetID: "agent", RunID: "remote-run", CallNodeID: "call"}
	_, before := usageRows(t, f)
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	_, reserved := usageRows(t, f)
	if reserved <= before {
		t.Fatal("pending ownership was not charged")
	}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	_, replayed := usageRows(t, f)
	if replayed != reserved {
		t.Fatal("idempotent reservation charged twice")
	}
	link.BootID = "boot-a"
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	bad := link
	bad.BootID = "boot-b"
	if errCode(f.core.ReserveRemoteRun(f.ctx, bad)) != "failed_precondition" {
		t.Fatal("remote process binding changed")
	}
	for _, id := range []string{"root", "owner", "unrelated"} {
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunCanceled, At: epoch.Add(time.Second)}, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("cleanup deleted unresolved authority: %+v %v", pruned, err)
	}
	f.reopen(t)
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 128)
	if err != nil || len(page.Links) != 1 || page.Links[0] != link || page.Next != "" {
		t.Fatalf("reopen lost owned remote identity: %+v %v", page, err)
	}
	// Prove pending control and its pruning protection do not decode Run detail.
	if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE runs SET inputs_json=x'01',trigger_json=x'02',error_json=x'03',result_json=x'04',nodes_json=x'05',cleanup_errors_json=x'06' WHERE id IN ('root','owner')")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.GetRun(f.ctx, "owner"); err == nil {
		t.Fatal("fixture failed to corrupt unused execution data")
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, bad, model.RunRetentionPolicy{}); errCode(err) != "failed_precondition" {
		t.Fatal("foreign boot joined another process")
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 0 {
		t.Fatal("failed join freed authority", err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal("exact join retry failed", err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 2 {
		t.Fatalf("joined development history remained pinned: %+v %v", pruned, err)
	}
}

func TestPendingRemotePageAdvancesAcrossUnmatchedOwnersAndMutations(t *testing.T) {
	f := openStore(t)
	// Unrelated owners must not produce empty scans before the selected target.
	for i := range 35 {
		id := fmt.Sprintf("owner-%02d", i)
		if _, _, err := f.core.CreateRun(f.ctx, newRun(id)); err != nil {
			t.Fatal(err)
		}
		target := "other"
		if i == 34 {
			target = "agent"
		}
		for j := range 4 {
			link := model.RemoteRunLink{OwnerRunID: id, RootRunID: id, TargetID: target, RunID: fmt.Sprintf("remote-%d", j), CallNodeID: "call", BootID: "boot"}
			if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
				t.Fatal(err)
			}
		}
	}
	cursor := ""
	seen := map[string]bool{}
	emptyPages := 0
	for attempts := 0; attempts < 20; attempts++ {
		page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Links) > 1 {
			t.Fatal("link page exceeded limit")
		}
		if len(page.Links) == 0 {
			emptyPages++
		}
		for _, link := range page.Links {
			if seen[link.RunID] || link.TargetID != "agent" {
				t.Fatal("cursor repeated/changed identity")
			}
			seen[link.RunID] = true
			// Removal changes array offsets; the stable identity cursor must not skip.
			if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor {
			t.Fatal("cursor made no progress")
		}
		cursor = page.Next
	}
	if len(seen) != 4 || emptyPages != 0 {
		t.Fatalf("selected-only pages lost or scanned unrelated links: %v %d", seen, emptyPages)
	}
	for _, cursor := range []string{"bad", strings.Repeat("x", 17000)} {
		if _, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", cursor, 1); errCode(err) != "invalid_argument" {
			t.Fatal("invalid cursor admitted")
		}
	}
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "other", "", 1)
	if err != nil || page.Next == "" {
		t.Fatal("missing foreign-target cursor")
	}
	if _, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", page.Next, 1); errCode(err) != "invalid_argument" {
		t.Fatal("cursor crossed target identity")
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.core.ListPendingRemoteRuns(ctx, "agent", "", 1); err == nil {
		t.Fatal("canceled query continued")
	}
}

func TestPendingRemoteMutationsUseTheirIndexedRowAndRollbackQuota(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "remote", CallNodeID: "call", BootID: "boot"}
	rows, before := usageRows(t, f)
	bad := link
	bad.RootRunID = "not-owner"
	if errCode(f.core.ReserveRemoteRun(f.ctx, bad)) != "failed_precondition" {
		t.Fatal("lineage mismatch admitted")
	}
	gotRows, after := usageRows(t, f)
	if rows != gotRows || before != after {
		t.Fatal("rejection changed scope quota")
	}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	gotRows, after = usageRows(t, f)
	if rows != gotRows || before != after {
		t.Fatal("join failed to return exact quota")
	}
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		queries := map[string]string{
			"run_remote_pending_root": "SELECT 1 FROM run_remote_pending INDEXED BY run_remote_pending_root WHERE scope=? AND root_run_id=?",
		}
		for index, query := range queries {
			plans, err := tx.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, `{"namespace":"core","user":"operator","workspace":"station"}`, "owner")
			if err != nil {
				return err
			}
			var details []string
			for plans.Next() {
				var a, b, c int
				var detail string
				if err := plans.Scan(&a, &b, &c, &detail); err != nil {
					plans.Close()
					return err
				}
				details = append(details, detail)
			}
			plans.Close()
			joined := strings.Join(details, " ")
			if !strings.Contains(joined, index) || strings.Contains(joined, "TEMP B-TREE") || strings.Contains(joined, "SCAN runs") {
				return fmt.Errorf("pending authority is not indexed: %v", details)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 128)
	if err != nil || !reflect.DeepEqual(page.Links, []model.RemoteRunLink(nil)) {
		t.Fatal("joined relation still pending", err)
	}
}

func TestPendingRemoteMixedOwnerCursorAndExactQuota(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	baseRows, baseBytes := usageRows(t, f)
	links := []model.RemoteRunLink{
		{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent-a", RunID: "a<1", CallNodeID: "call"},
		{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent-a", RunID: "a-2", CallNodeID: "call", BootID: "boot"},
		{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent-a", RunID: "a-3", CallNodeID: "call", BootID: "boot"},
		{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent-b", RunID: "b-1", CallNodeID: "call", BootID: "boot"},
		{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent-b", RunID: "b-2", CallNodeID: "call", BootID: "boot"},
	}
	for _, link := range links {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(links)
	if err != nil {
		t.Fatal(err)
	}
	rows, charged := usageRows(t, f)
	if rows-baseRows != int64(len(links)) || charged-baseBytes != int64(len(encoded)+256*len(links)) {
		t.Fatalf("reservation quota rows=%d bytes=%d encoded=%d", rows-baseRows, charged-baseBytes, len(encoded))
	}
	var count int
	var logical int64
	if err = f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT pending_remote_count,pending_remote_bytes FROM runs WHERE scope=? AND id=?`, engine.ScopeID(f.scope), "owner").Scan(&count, &logical)
	}); err != nil {
		t.Fatal(err)
	}
	if count != len(links) || logical != int64(len(encoded)) {
		t.Fatalf("derived count/bytes=%d/%d want %d/%d", count, logical, len(links), len(encoded))
	}
	first, err := f.core.ListPendingRemoteRuns(f.ctx, "agent-a", "", 1)
	if err != nil || len(first.Links) != 1 || first.Next == "" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	otherScope, err := coredata.New(f.db, api.Scope{Namespace: "core", User: "another", Workspace: "station"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = otherScope.ListPendingRemoteRuns(f.ctx, "agent-a", first.Next, 1); errCode(err) != "invalid_argument" {
		t.Fatalf("cursor crossed scope: %v", err)
	}
	f.reopen(t)
	cursor := ""
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent-a", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Links) != 1 || seen[page.Links[0].RunID] {
			t.Fatalf("selected page=%+v", page)
		}
		link := page.Links[0]
		seen[link.RunID] = true
		if _, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if len(seen) != 3 {
		t.Fatalf("selected links=%v", seen)
	}
	other, err := f.core.ListPendingRemoteRuns(f.ctx, "agent-b", "", 10)
	if err != nil || len(other.Links) != 2 || other.Next != "" {
		t.Fatalf("other target lost: %+v %v", other, err)
	}
	for _, link := range other.Links {
		if _, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
	rows, charged = usageRows(t, f)
	if rows != baseRows || charged != baseBytes {
		t.Fatalf("resolved quota rows=%d/%d bytes=%d/%d", rows, baseRows, charged, baseBytes)
	}
}

func TestPendingRemoteCancellationAndQuotaRollback(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: "child", CallNodeID: "call"}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if err := f.core.ReserveRemoteRun(ctx, link); err == nil {
		t.Fatal("canceled reserve succeeded")
	}
	page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 1)
	if err != nil || len(page.Links) != 0 {
		t.Fatalf("canceled reserve left a relation: %+v %v", page, err)
	}
	rows, bytes := usageRows(t, f)
	f.exec(t, `UPDATE core_data_usage SET rows=?,bytes=? WHERE scope=?`, coredata.MaxScopeRows, bytes, engine.ScopeID(f.scope))
	if err = f.core.ReserveRemoteRun(f.ctx, link); errCode(err) != "resource_exhausted" {
		t.Fatalf("row quota reserve: %v", err)
	}
	gotRows, gotBytes := usageRows(t, f)
	if gotRows != coredata.MaxScopeRows || gotBytes != bytes {
		t.Fatal("failed row quota changed counters")
	}
	f.exec(t, `UPDATE core_data_usage SET rows=?,bytes=? WHERE scope=?`, rows, bytes, engine.ScopeID(f.scope))
	if err = f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	rows, bytes = usageRows(t, f)
	f.exec(t, `UPDATE core_data_usage SET bytes=? WHERE scope=?`, coredata.MaxScopeBytes, engine.ScopeID(f.scope))
	bound := link
	bound.BootID = "boot"
	if err = f.core.ReserveRemoteRun(f.ctx, bound); errCode(err) != "resource_exhausted" {
		t.Fatalf("boot binding quota: %v", err)
	}
	page, err = f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 1)
	if err != nil || len(page.Links) != 1 || page.Links[0].BootID != "" {
		t.Fatalf("failed boot binding changed authority: %+v %v", page, err)
	}
	f.exec(t, `UPDATE core_data_usage SET bytes=? WHERE scope=?`, bytes, engine.ScopeID(f.scope))
	if _, err = f.core.ResolveRemoteRun(ctx, link, model.RunRetentionPolicy{}); err == nil {
		t.Fatal("canceled resolve succeeded")
	}
	page, err = f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 1)
	if err != nil || len(page.Links) != 1 {
		t.Fatalf("canceled resolve removed relation: %+v %v", page, err)
	}
	if _, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	gotRows, gotBytes = usageRows(t, f)
	if gotRows != rows-1 || gotBytes >= bytes {
		t.Fatalf("resolved quota not released: %d/%d %d/%d", gotRows, rows, gotBytes, bytes)
	}
}

func TestPendingRemotePerOwnerCountAndByteLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		large bool
	}{{"count", false}, {"bytes", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := openStore(t)
			owner := "owner"
			if tc.large {
				owner = strings.Repeat("o", 240)
			}
			if _, _, err := f.core.CreateRun(f.ctx, newRun(owner)); err != nil {
				t.Fatal(err)
			}
			target, call, boot := "agent", "call", "boot"
			if tc.large {
				target = strings.Repeat("t", 240)
				call = strings.Repeat("c", 240)
				boot = strings.Repeat("b", 240)
			}
			accepted := 0
			for i := 0; i <= model.MaxRemoteRunLinks; i++ {
				id := fmt.Sprintf("child-%04d", i)
				if tc.large {
					id = fmt.Sprintf("%04d-%s", i, strings.Repeat("r", 240))
				}
				link := model.RemoteRunLink{OwnerRunID: owner, RootRunID: owner, TargetID: target, RunID: id, CallNodeID: call, BootID: boot}
				beforeRows, beforeBytes := usageRows(t, f)
				err := f.core.ReserveRemoteRun(f.ctx, link)
				if err != nil {
					if errCode(err) != "resource_exhausted" {
						t.Fatal(err)
					}
					afterRows, afterBytes := usageRows(t, f)
					if beforeRows != afterRows || beforeBytes != afterBytes {
						t.Fatal("failed capacity reservation changed quota")
					}
					break
				}
				accepted++
			}
			if tc.large && (accepted == 0 || accepted >= model.MaxRemoteRunLinks) || !tc.large && accepted != model.MaxRemoteRunLinks {
				t.Fatalf("%s accepted=%d", tc.name, accepted)
			}
			page, err := f.core.ListPendingRemoteRuns(f.ctx, target, "", model.MaxRemoteRunPageSize)
			if err != nil || len(page.Links) != model.MaxRemoteRunPageSize || page.Next == "" {
				t.Fatalf("bounded page=%+v err=%v", page, err)
			}
		})
	}
}

func TestPendingRemoteTargetPagesAcrossOwnerCapacityAndReopen(t *testing.T) {
	f := openStore(t)
	const owners, perOwner = 9, 128
	for i := range owners {
		owner := fmt.Sprintf("owner-%02d", i)
		if _, _, err := f.core.CreateRun(f.ctx, newRun(owner)); err != nil {
			t.Fatal(err)
		}
		for j := range perOwner {
			link := model.RemoteRunLink{OwnerRunID: owner, RootRunID: owner, TargetID: "selected", RunID: fmt.Sprintf("remote-%03d", j), CallNodeID: "call", BootID: "boot"}
			if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
				t.Fatal(err)
			}
		}
		other := model.RemoteRunLink{OwnerRunID: owner, RootRunID: owner, TargetID: "other", RunID: "other", CallNodeID: "other-call"}
		if err := f.core.ReserveRemoteRun(f.ctx, other); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	cursor, pages := "", 0
	for {
		page, err := f.core.ListPendingRemoteRuns(f.ctx, "selected", cursor, 128)
		if err != nil || len(page.Links) == 0 || len(page.Links) > 128 {
			t.Fatalf("page %d: %+v %v", pages, page, err)
		}
		pages++
		for _, link := range page.Links {
			key := link.OwnerRunID + "/" + link.RunID
			if seen[key] || link.TargetID != "selected" {
				t.Fatalf("duplicate or foreign link: %+v", link)
			}
			seen[key] = true
			if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
				t.Fatal(err)
			}
		}
		if page.Next == "" {
			break
		}
		if page.Next == cursor || pages > 20 {
			t.Fatal("cursor failed to advance")
		}
		cursor = page.Next
		if pages == 1 {
			f.reopen(t)
		}
	}
	if len(seen) != owners*perOwner || pages != 9 {
		t.Fatalf("draining selected rows lost links: %d across %d pages", len(seen), pages)
	}
	other, err := f.core.ListPendingRemoteRuns(f.ctx, "other", "", 128)
	if err != nil || len(other.Links) != owners || other.Next != "" {
		t.Fatalf("draining selected target changed other targets: %+v %v", other, err)
	}
}

func TestPendingRemoteBootBindRacesUnboundResolve(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		unbound := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "owner", TargetID: "agent", RunID: fmt.Sprintf("remote-%d", i), CallNodeID: "call"}
		if err := f.core.ReserveRemoteRun(f.ctx, unbound); err != nil {
			t.Fatal(err)
		}
		bound := unbound
		bound.BootID = "boot"
		start := make(chan struct{})
		var bindErr, resolveErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; bindErr = f.core.ReserveRemoteRun(f.ctx, bound) }()
		go func() {
			defer wg.Done()
			<-start
			_, resolveErr = f.core.ResolveRemoteRun(f.ctx, unbound, model.RunRetentionPolicy{})
		}()
		close(start)
		wg.Wait()
		if bindErr != nil || resolveErr != nil && errCode(resolveErr) != "failed_precondition" {
			t.Fatalf("bind=%v resolve=%v", bindErr, resolveErr)
		}
		page, err := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 1)
		if err != nil || len(page.Links) != 1 || page.Links[0] != bound {
			t.Fatalf("old resolve erased bound child: %+v %v", page, err)
		}
		if _, err := f.core.ResolveRemoteRun(f.ctx, bound, model.RunRetentionPolicy{}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPendingRemoteMissingRootStillProtectsOwner(t *testing.T) {
	f := openStore(t)
	if _, _, err := f.core.CreateRun(f.ctx, childRun("owner", "missing-root", "missing-root", 1)); err != nil {
		t.Fatal(err)
	}
	link := model.RemoteRunLink{OwnerRunID: "owner", RootRunID: "missing-root", TargetID: "agent", RunID: "remote", CallNodeID: "call"}
	if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	pending, err := f.core.HasPendingRemoteRoot(f.ctx, "missing-root")
	if err != nil || !pending {
		t.Fatalf("missing root lost pending authority: %t %v", pending, err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "owner", Status: model.RunCanceled, At: epoch}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 0 {
		t.Fatalf("missing root permitted owner retirement: %+v %v", pruned, err)
	}
	if _, err := f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("joined missing-root owner not retired: %+v %v", pruned, err)
	}
}
