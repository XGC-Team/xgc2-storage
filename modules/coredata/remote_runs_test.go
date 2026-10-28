package coredata_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
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
		if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: id, Status: model.RunCanceled, At: epoch.Add(time.Second)}); err != nil {
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
	if errCode(f.core.ResolveRemoteRun(f.ctx, bad)) != "failed_precondition" {
		t.Fatal("foreign boot joined another process")
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 0 {
		t.Fatal("failed join freed authority", err)
	}
	if err := f.core.ResolveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	if err := f.core.ResolveRemoteRun(f.ctx, link); err != nil {
		t.Fatal("exact join retry failed", err)
	}
	pruned, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour), Limit: 100})
	if err != nil || pruned.Deleted != 2 {
		t.Fatalf("joined development history remained pinned: %+v %v", pruned, err)
	}
}

func TestPendingRemotePageAdvancesAcrossUnmatchedOwnersAndMutations(t *testing.T) {
	f := openStore(t)
	// More than one bounded owner window contains no requested target.
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
			if err := f.core.ResolveRemoteRun(f.ctx, link); err != nil {
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
	if len(seen) != 4 || emptyPages < 2 {
		t.Fatalf("bounded unmatched scans lost matching links: %v %d", seen, emptyPages)
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
	if err := f.core.ResolveRemoteRun(f.ctx, link); err != nil {
		t.Fatal(err)
	}
	gotRows, after = usageRows(t, f)
	if rows != gotRows || before != after {
		t.Fatal("join failed to return exact quota")
	}
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		queries := map[string]string{
			"runs_pending_remote":      "SELECT id,pending_remote_json,pending_remote_count FROM runs INDEXED BY runs_pending_remote WHERE scope=? AND pending_remote_count>0 AND id>=? ORDER BY id LIMIT 17",
			"runs_pending_remote_root": "SELECT 1 FROM runs WHERE scope=? AND root_run_id=? AND pending_remote_count>0",
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
