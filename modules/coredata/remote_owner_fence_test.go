package coredata_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRemoteOwnerFenceUsesExactAcceptedOwnerAndFinalChangedRelease(t *testing.T) {
	f := openStore(t)
	for _, run := range []model.NewRun{newRun("root"), childRun("callback", "root", "root", 1), newRun("other-root")} {
		if _, _, err := f.core.CreateRun(f.ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	outer := remoteLink("root", "root", "outer")
	inner1 := remoteLink("callback", "root", "inner-1")
	inner2 := remoteLink("callback", "root", "inner-2")
	unrelated := remoteLink("other-root", "other-root", "other")
	for _, link := range []model.RemoteRunLink{outer, inner1, inner2, unrelated} {
		if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t)
	// The owner predicate must not decode detail or trust an undercounted row.
	f.exec(t, `UPDATE runs SET inputs_json=x'01',trigger_json=x'02',nodes_json=x'03',pending_remote_count=0,pending_remote_bytes=0 WHERE id=?`, "callback")
	if _, err := f.core.GetRun(f.ctx, "callback"); err == nil {
		t.Fatal("control: corrupt Run detail unexpectedly decoded")
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "callback"); err != nil || !pending {
		t.Fatalf("actual relation missed with corrupt summary: pending=%v err=%v", pending, err)
	}
	// Restore only the injected summary from the authoritative relation rows.
	f.exec(t, `UPDATE runs SET pending_remote_count=(SELECT count(*) FROM run_remote_pending WHERE scope=runs.scope AND owner_run_id=runs.id),pending_remote_bytes=(SELECT sum(encoded_bytes)+count(*)+1 FROM run_remote_pending WHERE scope=runs.scope AND owner_run_id=runs.id) WHERE id=?`, "callback")
	other, err := coredata.New(f.db, api.Scope{Namespace: "core", User: "another", Workspace: "station"})
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"missing", "callback"} {
		selected := f.core
		if owner == "callback" {
			selected = other
		}
		if pending, err := selected.HasPendingRemoteOwner(f.ctx, owner); pending || errCode(err) != "not_found" {
			t.Fatalf("unknown owner %q: pending=%v err=%v", owner, pending, err)
		}
	}
	if _, err := f.core.HasPendingRemoteOwner(f.ctx, " "); errCode(err) != "invalid_argument" {
		t.Fatalf("invalid owner: %v", err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.core.HasPendingRemoteOwner(ctx, "callback"); err == nil {
		t.Fatal("canceled owner check succeeded")
	}

	result, err := f.core.ResolveRemoteRun(f.ctx, inner1, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || result.LastForOwner || result.LastForRoot {
		t.Fatalf("owner nonfinal: %+v %v", result, err)
	}
	result, err = f.core.ResolveRemoteRun(f.ctx, inner1, model.RunRetentionPolicy{})
	if err != nil || result.Changed || result.LastForOwner || result.LastForRoot {
		t.Fatalf("duplicate: %+v %v", result, err)
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "callback"); err != nil || !pending {
		t.Fatalf("inner still pending=%v err=%v", pending, err)
	}
	result, err = f.core.ResolveRemoteRun(f.ctx, inner2, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || !result.LastForOwner || result.LastForRoot {
		t.Fatalf("callback final while ancestor pending: %+v %v", result, err)
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "callback"); err != nil || pending {
		t.Fatalf("callback after join=%v %v", pending, err)
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "root"); err != nil || !pending {
		t.Fatalf("ancestor released with callback: %v %v", pending, err)
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "other-root"); err != nil || !pending {
		t.Fatalf("unrelated owner released: %v %v", pending, err)
	}
	result, err = f.core.ResolveRemoteRun(f.ctx, outer, model.RunRetentionPolicy{})
	if err != nil || !result.Changed || !result.LastForOwner || !result.LastForRoot {
		t.Fatalf("ancestor final: %+v %v", result, err)
	}
	result, err = f.core.ResolveRemoteRun(f.ctx, outer, model.RunRetentionPolicy{})
	if err != nil || result.Changed || result.LastForOwner || result.LastForRoot {
		t.Fatalf("ancestor replay: %+v %v", result, err)
	}
	if pending, err := f.core.HasPendingRemoteOwner(f.ctx, "other-root"); err != nil || !pending {
		t.Fatalf("unrelated owner changed: %v %v", pending, err)
	}
}

func TestRemoteOwnerFenceCorruptCountRollsBackRelease(t *testing.T) {
	for _, undercount := range []bool{true, false} {
		t.Run(fmt.Sprintf("undercount_%v", undercount), func(t *testing.T) {
			f := openStore(t)
			if _, _, err := f.core.CreateRun(f.ctx, newRun("owner")); err != nil {
				t.Fatal(err)
			}
			first := remoteLink("owner", "owner", "first")
			second := remoteLink("owner", "owner", "second")
			links := []model.RemoteRunLink{first}
			if undercount {
				links = append(links, second)
			}
			for _, link := range links {
				if err := f.core.ReserveRemoteRun(f.ctx, link); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			var bytes, encoded int64
			if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
				if err := tx.QueryRowContext(ctx, `SELECT pending_remote_count,pending_remote_bytes FROM runs WHERE scope=? AND id=?`, engine.ScopeID(f.scope), "owner").Scan(&count, &bytes); err != nil {
					return err
				}
				return tx.QueryRowContext(ctx, `SELECT encoded_bytes FROM run_remote_pending WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`, engine.ScopeID(f.scope), first.OwnerRunID, first.TargetID, first.RunID).Scan(&encoded)
			}); err != nil {
				t.Fatal(err)
			}
			badCount := 2
			if undercount {
				badCount = 1
			}
			f.exec(t, `UPDATE runs SET pending_remote_count=?,pending_remote_bytes=? WHERE scope=? AND id=?`, badCount, encoded+2, engine.ScopeID(f.scope), "owner")
			rowsBefore, bytesBefore := usageRows(t, f)
			result, err := f.core.ResolveRemoteRun(f.ctx, first, model.RunRetentionPolicy{})
			if errCode(err) != "data_loss" || result != (model.RemoteRunResolveResult{}) {
				t.Fatalf("corrupt summary produced committed facts: %+v %v", result, err)
			}
			rowsAfter, bytesAfter := usageRows(t, f)
			page, pageErr := f.core.ListPendingRemoteRuns(f.ctx, "agent", "", 10)
			pending, fenceErr := f.core.HasPendingRemoteOwner(f.ctx, "owner")
			if rowsBefore != rowsAfter || bytesBefore != bytesAfter || pageErr != nil || len(page.Links) != len(links) || fenceErr != nil || !pending {
				t.Fatalf("fault did not rollback: usage=%d/%d -> %d/%d page=%+v err=%v fence=%v/%v", rowsBefore, bytesBefore, rowsAfter, bytesAfter, page, pageErr, pending, fenceErr)
			}
			f.exec(t, `UPDATE runs SET pending_remote_count=?,pending_remote_bytes=? WHERE scope=? AND id=?`, count, bytes, engine.ScopeID(f.scope), "owner")
			for index, link := range links {
				result, err = f.core.ResolveRemoteRun(f.ctx, link, model.RunRetentionPolicy{})
				last := index == len(links)-1
				if err != nil || !result.Changed || result.LastForOwner != last || result.LastForRoot != last {
					t.Fatalf("repaired release %d: %+v %v", index, result, err)
				}
			}
		})
	}
}

func TestRemoteOwnerFenceUsesOnlyIndexedIdentity(t *testing.T) {
	f := openStore(t)
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT EXISTS(SELECT 1 FROM run_remote_pending WHERE scope=r.scope AND owner_run_id=r.id) FROM runs AS r WHERE r.scope=? AND r.id=?`, engine.ScopeID(f.scope), "owner")
		if err != nil {
			return err
		}
		defer rows.Close()
		var parts []string
		for rows.Next() {
			var a, b, c int
			var detail string
			if err := rows.Scan(&a, &b, &c, &detail); err != nil {
				return err
			}
			parts = append(parts, detail)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		plan := strings.Join(parts, " ")
		if !strings.Contains(plan, "SEARCH r USING") || !strings.Contains(plan, "SEARCH run_remote_pending USING") || strings.Contains(plan, "SCAN ") || strings.Contains(plan, "TEMP B-TREE") {
			return fmt.Errorf("not two indexed point/prefix reads: %s", plan)
		}
		t.Log(plan)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
