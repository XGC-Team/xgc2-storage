package coredata_test

import (
	"encoding/json"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestEngineGroupImmutableReplayRefreshesGuardsButTransportRemainsExact(t *testing.T) {
	f := newExecutionFixture(t, "", 100)
	parent := model.RunPrepareState{ID: "parent", TargetID: "local", RootRunID: "parent", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"}
	producer := model.ProducerPrepareState{ID: "invocation", RunID: "parent", NodeID: "call", Kind: "automation-call", Status: "running", ChildRunProducer: true}
	parentJSON, _ := json.Marshal(parent)
	producerJSON, _ := json.Marshal(producer)
	seed, _ := committedExecution(t, f, "seed", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: parentJSON}, {Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: producerJSON}, {Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"pinned":1}`)}}})
	guard := model.RecordGuard{Collection: "runs", Key: "parent", Version: seed.Receipt.Token.Revision}
	plan := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "group-key", ParentGuard: guard, InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: guard.Version}, PinGuard: model.RecordGuard{Collection: "definitions", Key: "pin", Version: guard.Version}, Condition: model.GroupPrepareCondition{Parent: parent, Producer: producer, Ancestors: []model.RecordGuard{guard}}, Body: json.RawMessage(`{"pin":"immutable","policy":"attached"}`), Members: []model.GroupMember{{ItemKey: "item", ChildID: "child", EventID: "event", Parameters: []byte(`{"p":1}`), Event: json.RawMessage(`{"ingress":"rpc"}`), Link: json.RawMessage(`{"relation":"attached"}`), Body: json.RawMessage(`{"prepared":true}`)}}}
	first, err := f.named("prepare", model.GroupPrepareOperation, plan)
	if err != nil || first.Receipt == nil {
		t.Fatalf("first preparation %v", err)
	}
	snapshotBefore, err := f.named("snapshot-before", model.GroupSnapshotOperation, model.GroupRead{ID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	// Reloaded versions and lifecycle predicates are commit conditions, not
	// a change to the immutable preparation that was already sealed.
	parent.Status, producer.Status, producer.ChildRunProducer = "succeeded", "waiting", false
	parentJSON, _ = json.Marshal(parent)
	producerJSON, _ = json.Marshal(producer)
	updated, _ := committedExecution(t, f, "advance-sources", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "parent", ExpectedVersion: guard.Version, Data: parentJSON}, {Collection: "invocations", Key: "invocation", ExpectedVersion: guard.Version, Data: producerJSON}, {Collection: "definitions", Key: "pin", ExpectedVersion: guard.Version, Data: json.RawMessage(`{"pinned":2}`)}}})
	plan.ParentGuard.Version, plan.InvocationGuard.Version, plan.PinGuard.Version = updated.Receipt.Token.Revision, updated.Receipt.Token.Revision, updated.Receipt.Token.Revision
	plan.Condition = model.GroupPrepareCondition{Parent: parent, Producer: producer, Ancestors: []model.RecordGuard{plan.ParentGuard}}
	if _, err = f.named("prepare", model.GroupPrepareOperation, plan); executionErrorCode(err) != "conflict" {
		t.Fatalf("transport request identity ignored changed guard content: %v", err)
	}
	retained, err := f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: "prepare"})
	if err != nil || retained.Digest != first.Receipt.Digest {
		t.Fatal("conflict replaced original transport receipt")
	}
	again, err := f.named("prepare-fresh-guards", model.GroupPrepareOperation, plan)
	if err != nil || again.Receipt == nil || string(again.Result) != string(first.Result) {
		t.Fatalf("refreshed guards changed immutable replay: %+v %v", again, err)
	}
	if again.Receipt.Digest == first.Receipt.Digest {
		t.Fatal("new transport intent lost precise content digest")
	}
	stop := commandFor("stop-after-seal")
	stop.Target, stop.Action = model.RunCommandTargetPrefix+parent.ID, model.StopSetAction
	committedExecution(t, f, "stop-after-seal", model.ExecutionCommit{Command: stop})
	stoppedReplay, err := f.named("confirm-after-stop", model.GroupPrepareOperation, plan)
	if err != nil || string(stoppedReplay.Result) != string(first.Result) {
		t.Fatalf("stop invalidated old immutable preparation: %v", err)
	}
	snapshotAfter, err := f.named("snapshot-after", model.GroupSnapshotOperation, model.GroupRead{ID: plan.ID})
	if err != nil || string(snapshotBefore.Result) != string(snapshotAfter.Result) {
		t.Fatal("replay rewrote stored event/link/member facts")
	}
	// Restore eligible parent/producer data and obtain exact new point guards.
	// The independently inserted stop receipt still prevents a NEW group.
	parent.Status, producer.Status, producer.ChildRunProducer = "running", "running", true
	parentJSON, _ = json.Marshal(parent)
	producerJSON, _ = json.Marshal(producer)
	active, _ := committedExecution(t, f, "restore-active-sources", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "parent", ExpectedVersion: plan.ParentGuard.Version, Data: parentJSON}, {Collection: "invocations", Key: "invocation", ExpectedVersion: plan.InvocationGuard.Version, Data: producerJSON}}})
	plan.ID, plan.GroupKey = "new-group", "new-key"
	plan.Members[0].ChildID, plan.Members[0].EventID = "new-child", "new-event"
	plan.ParentGuard.Version, plan.InvocationGuard.Version = active.Receipt.Token.Revision, active.Receipt.Token.Revision
	plan.Condition = model.GroupPrepareCondition{Parent: parent, Producer: producer, Ancestors: []model.RecordGuard{plan.ParentGuard}}
	if _, err := f.named("new-group-after-stop", model.GroupPrepareOperation, plan); executionErrorCode(err) != "conflict" {
		t.Fatalf("new admission escaped durable stop fence: %v", err)
	}
	noExecutionReceipt(t, f, "new-group-after-stop")
}
