package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func storePreparationState(t *testing.T, db *sql.DB, ctx context.Context, collection, key string, state any) {
	t.Helper()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, ctx, "INSERT INTO records VALUES(?,?,?,1,0,?) ON CONFLICT(scope,collection,key) DO UPDATE SET data=excluded.data", testScope, collection, key, raw)
}

func TestGroupSameVersionWrongLifecycleProducerAndPredicateFail(t *testing.T) {
	cases := map[string]func(*model.RunPrepareState, *model.ProducerPrepareState){
		"stopping parent":     func(p *model.RunPrepareState, _ *model.ProducerPrepareState) { p.Status = "stopping" },
		"terminal parent":     func(p *model.RunPrepareState, _ *model.ProducerPrepareState) { p.Status = "succeeded" },
		"wrong model":         func(p *model.RunPrepareState, _ *model.ProducerPrepareState) { p.ExecutionModel = "different" },
		"wrong root":          func(p *model.RunPrepareState, _ *model.ProducerPrepareState) { p.RootRunID = "different" },
		"wrong target":        func(p *model.RunPrepareState, _ *model.ProducerPrepareState) { p.TargetID = "different" },
		"waiting producer":    func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.Status = "waiting" },
		"terminal producer":   func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.Status = "succeeded" },
		"not call capable":    func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.ChildRunProducer = false },
		"wrong producer run":  func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.RunID = "different" },
		"wrong producer node": func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.NodeID = "different" },
		"wrong producer kind": func(_ *model.RunPrepareState, p *model.ProducerPrepareState) { p.Kind = "different" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			db, ctx := fixture(t)
			parent, producer := preparationStates()
			change(&parent, &producer)
			// The point version remains 1. A version-only check would accept it.
			storePreparationState(t, db, ctx, "runs", "parent", parent)
			storePreparationState(t, db, ctx, "invocations", "invocation", producer)
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, group(1)); errorCode(err) != "conflict" {
				t.Fatalf("ineligible data at exact version accepted: %v", err)
			}
			if count(t, db, ctx, "core_groups") != 0 || count(t, db, ctx, "core_data_usage") != 0 {
				t.Fatal("predicate failure published preparation")
			}
		})
	}
	for name, change := range map[string]func(*model.GroupPrepare){
		"missing predicate":   func(r *model.GroupPrepare) { r.Condition = model.GroupPrepareCondition{} },
		"weak parent":         func(r *model.GroupPrepare) { r.Condition.Parent.Status = "stopping" },
		"weak producer":       func(r *model.GroupPrepare) { r.Condition.Producer.Status = "waiting" },
		"optional capability": func(r *model.GroupPrepare) { r.Condition.Producer.ChildRunProducer = false },
		"no ancestry":         func(r *model.GroupPrepare) { r.Condition.Ancestors = nil },
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := fixture(t)
			r := group(1)
			change(&r)
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, r); errorCode(err) != "invalid_argument" {
				t.Fatalf("caller weakened required condition: %v", err)
			}
		})
	}
}

func stopSet(t *testing.T, db *sql.DB, ctx context.Context, target, action, status, scope string) {
	t.Helper()
	err := commandTx(t, db, ctx, scope, func(tx *sql.Tx) error {
		r := commandIntent()
		r.CommandID, r.IdempotencyKey, r.Target, r.Action = "stop", "stop", target, action
		if _, _, err := acceptCommand(ctx, tx, scope, r); err != nil {
			return err
		}
		if status != "accepted" {
			_, err := completeCommand(ctx, tx, scope, model.CommandCompletion{CommandID: "stop", Result: model.CommandResult{Status: status}})
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGroupStopReceiptPhantomAtUnchangedRunVersion(t *testing.T) {
	for _, status := range []string{"accepted", "succeeded", "failed", "rejected"} {
		t.Run(status, func(t *testing.T) {
			db, ctx := fixture(t)
			r := group(2) // Core's complete preparation read, before the stop command.
			stopSet(t, db, ctx, model.RunCommandTargetPrefix+"parent", model.StopSetAction, status, testScope)
			var version int
			if err := db.QueryRowContext(ctx, "SELECT version FROM records WHERE scope=? AND collection='runs' AND key='parent'", testScope).Scan(&version); err != nil || version != 1 {
				t.Fatal("test modified parent point version")
			}
			_, err := run(t, db, ctx, model.GroupPrepareOperation, r)
			if status == "accepted" || status == "succeeded" {
				if errorCode(err) != "conflict" || count(t, db, ctx, "core_groups") != 0 {
					t.Fatalf("durable stop escaped prepare: %v", err)
				}
			} else if err != nil {
				t.Fatalf("unsuccessful stop fenced new children: %v", err)
			}
		})
	}
	for name, values := range map[string][3]string{
		"unrelated target": {model.RunCommandTargetPrefix + "different", model.StopSetAction, testScope},
		"unrelated action": {model.RunCommandTargetPrefix + "parent", "workflowruntime.start", testScope},
		"other scope":      {model.RunCommandTargetPrefix + "parent", model.StopSetAction, "other"},
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := fixture(t)
			stopSet(t, db, ctx, values[0], values[1], "accepted", values[2])
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, group(1)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func ancestorPlan(t *testing.T, db *sql.DB, ctx context.Context) model.GroupPrepare {
	t.Helper()
	r := group(1)
	r.Condition.Parent.RootRunID, r.Condition.Parent.ParentRunID = "root", "root"
	storePreparationState(t, db, ctx, "runs", "parent", r.Condition.Parent)
	root := model.RunPrepareState{ID: "root", TargetID: "local", RootRunID: "root", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"}
	storePreparationState(t, db, ctx, "runs", "root", root)
	r.Condition.Ancestors = append(r.Condition.Ancestors, model.RecordGuard{Collection: "runs", Key: "root", Version: "1"})
	return r
}

func TestGroupCompleteAncestorChainAndRootStopFence(t *testing.T) {
	for _, kind := range []string{"valid", "root stop", "omission", "stale ancestor", "cycle", "cross target", "wrong edge"} {
		t.Run(kind, func(t *testing.T) {
			db, ctx := fixture(t)
			r := ancestorPlan(t, db, ctx)
			switch kind {
			case "root stop":
				stopSet(t, db, ctx, model.RunCommandTargetPrefix+"root", model.StopSetAction, "accepted", testScope)
			case "omission":
				r.Condition.Ancestors = r.Condition.Ancestors[:1]
			case "stale ancestor":
				execSQL(t, db, ctx, "UPDATE records SET version=2 WHERE collection='runs' AND key='root'")
			case "cycle":
				storePreparationState(t, db, ctx, "runs", "root", model.RunPrepareState{ID: "root", TargetID: "local", RootRunID: "root", ParentRunID: "parent", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"})
			case "cross target":
				storePreparationState(t, db, ctx, "runs", "root", model.RunPrepareState{ID: "root", TargetID: "different", RootRunID: "root", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"})
			case "wrong edge":
				r.Condition.Ancestors[1].Key = "different"
			}
			_, err := run(t, db, ctx, model.GroupPrepareOperation, r)
			if kind == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if errorCode(err) != "conflict" {
				t.Fatalf("bad ancestry accepted: %v", err)
			}
		})
	}
}

func TestGroupMemberSnapshotIsSameSealedAuthorityAndIndexed(t *testing.T) {
	db, ctx := fixture(t)
	plan := group(1000)
	if _, err := run(t, db, ctx, model.GroupPrepareOperation, plan); err != nil {
		t.Fatal(err)
	}
	for _, read := range []model.GroupMemberRead{{ChildID: "child-999"}, {EventID: "event-999"}} {
		raw, err := run(t, db, ctx, model.GroupMemberSnapshotOperation, read)
		if err != nil {
			t.Fatal(err)
		}
		var got model.GroupMemberSnapshot
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.Ordinal != 999 || got.Group.ID != plan.ID || got.Group.MemberCount != 1000 || got.ParentID != plan.ParentID || got.InvocationID != plan.InvocationID || string(got.Body) != string(plan.Body) || !reflect.DeepEqual(got.Member, plan.Members[999]) {
			t.Fatal("child/event reader received different preparation facts")
		}
	}
	for _, column := range []string{"child_id", "event_id"} {
		query := "EXPLAIN QUERY PLAN SELECT m.parameters,g.body FROM core_group_members m JOIN core_groups g ON g.scope=m.scope AND g.id=m.group_id WHERE m.scope='s' AND m." + column + "='id' AND g.sealed=1"
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		var details strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			details.WriteString(detail)
		}
		rows.Close()
		if !strings.Contains(details.String(), "SEARCH m USING INDEX") || strings.Contains(details.String(), "SCAN m") {
			t.Fatalf("unindexed prepared member lookup: %s", details.String())
		}
	}
	for _, read := range []model.GroupMemberRead{{}, {ChildID: "child-0", EventID: "event-0"}} {
		if _, err := run(t, db, ctx, model.GroupMemberSnapshotOperation, read); errorCode(err) != "invalid_argument" {
			t.Fatalf("nonexact member lookup accepted: %v", err)
		}
	}
	execSQL(t, db, ctx, "UPDATE core_groups SET sealed=0")
	if _, err := run(t, db, ctx, model.GroupMemberSnapshotOperation, model.GroupMemberRead{ChildID: "child-0"}); errorCode(err) != "not_found" {
		t.Fatalf("provisional member became visible: %v", err)
	}
}

func TestGroupMemberSnapshotRejectsCorruptFactsAndOtherScope(t *testing.T) {
	changes := map[string]string{
		"parameter bytes": "UPDATE core_group_members SET parameters='{}' WHERE ordinal=0",
		"event facts":     "UPDATE core_group_members SET event_body='{\"changed\":true}' WHERE ordinal=0",
		"link facts":      "UPDATE core_group_members SET link_body='{\"changed\":true}' WHERE ordinal=0",
		"member facts":    "UPDATE core_group_members SET member_body='{\"changed\":true}' WHERE ordinal=0",
		"group body":      "UPDATE core_groups SET body='{\"changed\":true}'",
		"parent identity": "UPDATE core_groups SET parent_id='other'",
		"group digest":    "UPDATE core_groups SET membership_digest='other'",
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			db, ctx := fixture(t)
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, group(1)); err != nil {
				t.Fatal(err)
			}
			execSQL(t, db, ctx, change)
			if _, err := run(t, db, ctx, model.GroupMemberSnapshotOperation, model.GroupMemberRead{ChildID: "child-0"}); errorCode(err) != "data_loss" {
				t.Fatalf("corrupt sealed fact published: %v", err)
			}
		})
	}
	t.Run("scope isolation", func(t *testing.T) {
		db, ctx := fixture(t)
		if _, err := run(t, db, ctx, model.GroupPrepareOperation, group(1)); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		for _, read := range []model.GroupMemberRead{{ChildID: "child-0"}, {EventID: "event-0"}} {
			if _, err := groupMemberSnapshot(ctx, tx, "other", read); errorCode(err) != "not_found" {
				t.Fatalf("other scope accessed preparation: %v", err)
			}
		}
	})
}

func TestGroupReplayRefreshesCommitGuardsAndPredicatesWithoutChangingFacts(t *testing.T) {
	db, ctx := fixture(t)
	original := ancestorPlan(t, db, ctx)
	first, err := run(t, db, ctx, model.GroupPrepareOperation, original)
	if err != nil {
		t.Fatal(err)
	}
	snapshotBefore, err := run(t, db, ctx, model.GroupSnapshotOperation, model.GroupRead{ID: original.ID})
	if err != nil {
		t.Fatal(err)
	}
	refreshed := original
	refreshed.ParentGuard.Version, refreshed.InvocationGuard.Version, refreshed.PinGuard.Version = "2", "3", "4"
	refreshed.Condition.Ancestors = []model.RecordGuard{refreshed.ParentGuard, {Collection: "runs", Key: "root", Version: "5"}}
	refreshed.Condition.Parent.Status = "succeeded"
	refreshed.Condition.Producer.Status = "waiting"
	refreshed.Condition.Producer.ChildRunProducer = false
	storePreparationState(t, db, ctx, "runs", "parent", refreshed.Condition.Parent)
	storePreparationState(t, db, ctx, "invocations", "invocation", refreshed.Condition.Producer)
	execSQL(t, db, ctx, "UPDATE records SET version=2 WHERE collection='runs' AND key='parent'")
	execSQL(t, db, ctx, "UPDATE records SET version=3 WHERE collection='invocations'")
	execSQL(t, db, ctx, "UPDATE records SET version=4 WHERE collection='definitions'")
	execSQL(t, db, ctx, "UPDATE records SET version=5 WHERE collection='runs' AND key='root'")
	// A durable stop fences new admission; it must not erase or invalidate a
	// confirmation of preparation that was already sealed before this stop.
	stopSet(t, db, ctx, model.RunCommandTargetPrefix+"root", model.StopSetAction, "accepted", testScope)
	var beforeRows, beforeBytes int64
	if err := db.QueryRowContext(ctx, "SELECT rows,bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&beforeRows, &beforeBytes); err != nil {
		t.Fatal(err)
	}
	for _, plan := range []model.GroupPrepare{refreshed, func() model.GroupPrepare { p := refreshed; p.Condition = model.GroupPrepareCondition{}; return p }()} {
		again, err := run(t, db, ctx, model.GroupPrepareOperation, plan)
		if err != nil || string(again) != string(first) {
			t.Fatalf("refreshed admission changed immutable replay: %s %v", again, err)
		}
	}
	snapshotAfter, err := run(t, db, ctx, model.GroupSnapshotOperation, model.GroupRead{ID: original.ID})
	if err != nil || string(snapshotAfter) != string(snapshotBefore) {
		t.Fatal("replay rewrote sealed preparation")
	}
	var afterRows, afterBytes int64
	if err := db.QueryRowContext(ctx, "SELECT rows,bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&afterRows, &afterBytes); err != nil || afterRows != beforeRows || afterBytes != beforeBytes {
		t.Fatal("replay charged another preparation")
	}
	for _, table := range []string{"core_groups", "core_group_members", "core_group_events", "core_child_links"} {
		if count(t, db, ctx, table) != 1 {
			t.Fatalf("replay duplicated %s", table)
		}
	}
}

func TestGroupImmutableReplayComparesCompletePreparation(t *testing.T) {
	changes := map[string]func(*model.GroupPrepare){
		"parent": func(p *model.GroupPrepare) { p.ParentID = "other-parent"; p.ParentGuard.Key = p.ParentID },
		"invocation": func(p *model.GroupPrepare) {
			p.InvocationID = "other-invocation"
			p.InvocationGuard.Key = p.InvocationID
		},
		"group key": func(p *model.GroupPrepare) { p.GroupKey = "other-key" },
		"group pin and policy body": func(p *model.GroupPrepare) {
			p.Body = json.RawMessage(`{"complete_pin":{"config":"other"},"policy":{"max_concurrency":1}}`)
		},
		"item key":     func(p *model.GroupPrepare) { p.Members[0].ItemKey = "other-item" },
		"child id":     func(p *model.GroupPrepare) { p.Members[0].ChildID = "other-child" },
		"event id":     func(p *model.GroupPrepare) { p.Members[0].EventID = "other-event" },
		"parameters":   func(p *model.GroupPrepare) { p.Members[0].Parameters = []byte(`{"p":"other"}`) },
		"event facts":  func(p *model.GroupPrepare) { p.Members[0].Event = json.RawMessage(`{"ingress":"other"}`) },
		"link facts":   func(p *model.GroupPrepare) { p.Members[0].Link = json.RawMessage(`{"policy":"other"}`) },
		"member facts": func(p *model.GroupPrepare) { p.Members[0].Body = json.RawMessage(`{"fact":"other"}`) },
		"member count": func(p *model.GroupPrepare) { p.Members = p.Members[:1] },
		"member order": func(p *model.GroupPrepare) { p.Members[0], p.Members[1] = p.Members[1], p.Members[0] },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			db, ctx := fixture(t)
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, group(2)); err != nil {
				t.Fatal(err)
			}
			plan := group(2)
			plan.ParentGuard.Version, plan.InvocationGuard.Version, plan.PinGuard.Version = "2", "3", "4"
			change(&plan)
			if _, err := run(t, db, ctx, model.GroupPrepareOperation, plan); errorCode(err) != "conflict" {
				t.Fatalf("changed immutable %s accepted: %v", name, err)
			}
			if count(t, db, ctx, "core_groups") != 1 || count(t, db, ctx, "core_group_members") != 2 {
				t.Fatal("immutable conflict changed stored preparation")
			}
		})
	}
}
