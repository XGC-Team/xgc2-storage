package coredata_test

import (
	"context"
	"database/sql"

	"encoding/json"
	"errors"
	"fmt"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
)

func TestEngineNamedGroupReceiptRestartAndFailure(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
			spec := coredata.Spec()
			n := api.Namespace{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []api.Module{spec}}
			for _, id := range []string{"runs", "invocations", "definitions"} {
				n.Collections = append(n.Collections, api.Collection{ID: id, MaxRecordBytes: 4096, MaxRecords: 100, MaxBytes: 1 << 20, Retention: "Core current/recovery facts; explicit owner cleanup", Recovery: "consistent storage backup"})
			}
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			initialize := func(ctx context.Context, tx *sql.Tx) error {
				if err := coredata.Initialize(ctx, tx); err != nil {
					return err
				}
				if fault {
					_, err := tx.ExecContext(ctx, `CREATE TRIGGER final_member_failure BEFORE INSERT ON core_group_members WHEN NEW.ordinal=999 BEGIN SELECT RAISE(ABORT,'late group fault'); END`)
					return err
				}
				return nil
			}
			config := engine.Config{Path: filepath.Join(dir, "storage.db"), Create: true, Manifest: api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{n}}, Modules: []engine.DataModule{{Spec: spec, Initialize: initialize, Execute: coredata.Execute}}}
			store, err := engine.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { store.Close() }()
			read, err := store.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"parent"}}}})
			if err != nil {
				t.Fatal(err)
			}
			mutations := []api.Mutation{}
			parentState := model.RunPrepareState{ID: "parent", TargetID: "local", RootRunID: "parent", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"}
			producerState := model.ProducerPrepareState{ID: "invocation", RunID: "parent", NodeID: "call", Kind: "automation-call", Status: "running", ChildRunProducer: true}
			for _, id := range []struct{ collection, key string }{{"runs", "parent"}, {"invocations", "invocation"}, {"definitions", "pin"}} {
				data := []byte(`{"validated":true}`)
				if id.collection == "runs" {
					data, err = json.Marshal(parentState)
				} else if id.collection == "invocations" {
					data, err = json.Marshal(producerState)
				}
				if err != nil {
					t.Fatal(err)
				}
				mutations = append(mutations, api.Mutation{Collection: id.collection, Key: id.key, ExpectedVersion: "0", Data: data})
			}
			if _, err = store.Batch(ctx, api.BatchRequest{Scope: scope, Expected: read.Token, RequestID: "seed-current-data", Mutations: mutations}); err != nil {
				t.Fatal(err)
			}
			r := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "fan-out", ParentGuard: model.RecordGuard{Collection: "runs", Key: "parent", Version: "1"}, InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: "1"}, PinGuard: model.RecordGuard{Collection: "definitions", Key: "pin", Version: "1"}, Body: json.RawMessage(`{"complete_pin":{"config":"c","execution":"e","registry":"r","definition":"d"}}`)}
			r.Condition = model.GroupPrepareCondition{Parent: parentState, Producer: producerState, Ancestors: []model.RecordGuard{r.ParentGuard}}
			for i := 0; i < 1000; i++ {
				r.Members = append(r.Members, model.GroupMember{ItemKey: fmt.Sprint(i), ChildID: fmt.Sprintf("child-%d", i), EventID: fmt.Sprintf("event-%d", i), Parameters: json.RawMessage(`{"p":1}`), Event: json.RawMessage(`{"trigger":"rpc"}`), Link: json.RawMessage(`{"relation":"attached","owner":"parent","definition_version":3}`), Body: json.RawMessage(`{"state":"queued"}`)})
			}
			payload, _ := json.Marshal(r)
			request := api.NamedRequest{Scope: scope, DatabaseID: read.Token.DatabaseID, Schema: coredata.Schema, Module: "coredata", Operation: "group.prepare", RequestID: "prepare-1000", Payload: payload}
			first, err := store.Named(ctx, request)
			if fault {
				if err == nil {
					t.Fatal("late fault committed")
				}
				_, receiptError := store.Receipt(ctx, api.ReceiptRequest{Scope: scope, RequestID: request.RequestID})
				var wireError *api.Error
				if !errors.As(receiptError, &wireError) || wireError.Code != "not_found" {
					t.Fatalf("failed transaction receipt=%v", receiptError)
				}
				read, err = store.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"parent"}}}})
				if err != nil || read.Token.Revision != "1" {
					t.Fatalf("failed transaction revision=%s error=%v", read.Token.Revision, err)
				}
				return
			}
			if err != nil || first.Receipt == nil || first.Receipt.Token.Revision != "2" {
				t.Fatalf("named result=%+v error=%v", first, err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			config.Create = false
			store, err = engine.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			again, err := store.Named(ctx, request)
			if err != nil || again.Receipt == nil || again.Receipt.Digest != first.Receipt.Digest || string(again.Result) != string(first.Result) {
				t.Fatalf("restart replay=%+v error=%v", again, err)
			}
			stored, err := store.Receipt(ctx, api.ReceiptRequest{Scope: scope, RequestID: request.RequestID})
			if err != nil || stored.Digest != first.Receipt.Digest {
				t.Fatalf("retained receipt=%+v error=%v", stored, err)
			}
			request.Operation = "group.snapshot"
			request.RequestID = "read-group"
			request.Payload = json.RawMessage(`{"id":"group"}`)
			out, err := store.Named(ctx, request)
			if err != nil || out.Receipt != nil {
				t.Fatalf("readonly snapshot=%+v error=%v", out, err)
			}
			request.Operation = model.GroupMemberSnapshotOperation
			request.RequestID = "read-prepared-child"
			request.Payload = json.RawMessage(`{"child_id":"child-999"}`)
			memberResult, err := store.Named(ctx, request)
			if err != nil || memberResult.Receipt != nil {
				t.Fatalf("named member snapshot=%+v error=%v", memberResult, err)
			}
			var member model.GroupMemberSnapshot
			if err := json.Unmarshal(memberResult.Result, &member); err != nil || member.Ordinal != 999 || member.Member.EventID != "event-999" || member.Group.ID != "group" {
				t.Fatalf("recovered child/event/link authority=%+v error=%v", member, err)
			}
			var group model.GroupSnapshot
			if err = json.Unmarshal(out.Result, &group); err != nil || group.MemberCount != 1000 || len(group.Members) != 1000 || string(group.Members[999].Link) != `{"definition_version":3,"owner":"parent","relation":"attached"}` {
				t.Fatalf("recovered group members=%d error=%v", len(group.Members), err)
			}
		})
	}
}
