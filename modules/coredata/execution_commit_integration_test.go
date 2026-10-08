package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type executionFixture struct {
	store      *engine.Store
	config     engine.Config
	ctx        context.Context
	scope      api.Scope
	databaseID string
}

func newExecutionFixture(t *testing.T, fault string, maxRecords int) *executionFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	spec := coredata.Spec()
	n := api.Namespace{ID: "core", Owner: "core", Schema: model.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []api.Module{spec}}
	for _, id := range []string{"runs", "tasks", "invocations", "definitions"} {
		n.Collections = append(n.Collections, api.Collection{ID: id, MaxRecordBytes: 32 << 10, MaxRecords: maxRecords, MaxBytes: 32 << 20, Retention: "current facts and recovery; explicit cleanup", Recovery: "storage backup", Indexes: []api.Index{{ID: "name", Fields: []string{"name"}, Unique: true}}})
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	initialize := func(ctx context.Context, tx *sql.Tx) error {
		if err := coredata.Initialize(ctx, tx); err != nil {
			return err
		}
		if fault != "" {
			_, err := tx.ExecContext(ctx, fault)
			return err
		}
		return nil
	}
	f := &executionFixture{ctx: ctx, scope: api.Scope{Namespace: "core", User: "operator", Workspace: "station"}, config: engine.Config{Path: filepath.Join(dir, "execution.db"), Create: true, Manifest: api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{n}}, MaxCallTime: time.Minute, Modules: []engine.DataModule{{Spec: spec, Initialize: initialize, Execute: coredata.Execute}}}}
	var err error
	f.store, err = engine.Open(ctx, f.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.store.Close() })
	snapshot, err := f.store.Snapshot(ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"probe"}}}})
	if err != nil {
		t.Fatal(err)
	}
	f.databaseID = snapshot.Token.DatabaseID
	return f
}

func (f *executionFixture) named(id, operation string, data any) (api.NamedResponse, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return api.NamedResponse{}, err
	}
	return f.store.Named(f.ctx, api.NamedRequest{Scope: f.scope, DatabaseID: f.databaseID, Schema: model.Schema, Module: model.Module, Operation: operation, RequestID: id, Payload: payload})
}

func committedExecution(t *testing.T, f *executionFixture, id string, data model.ExecutionCommit) (api.NamedResponse, model.ExecutionCommitted) {
	t.Helper()
	out, err := f.named(id, model.ExecutionCommitOperation, data)
	if err != nil {
		t.Fatal(err)
	}
	if out.Receipt == nil || out.Receipt.RequestID != id || out.Receipt.Token.DatabaseID != f.databaseID || out.Receipt.Token.Schema != model.Schema || out.Receipt.Durability != "sqlite-full" || out.Receipt.CommittedAt == "" {
		t.Fatalf("unconfirmed execution commit: %+v", out)
	}
	var facts model.ExecutionCommitted
	if err = json.Unmarshal(out.Result, &facts); err != nil {
		t.Fatal(err)
	}
	return out, facts
}

func commandFor(id string) *model.CommandRequest {
	return &model.CommandRequest{CommandID: id, RequestID: "business-" + id, IdempotencyKey: "intent-" + id, Actor: "operator", Risk: "low", Target: "run", Action: "run.start", Payload: []byte(`{"at":9007199254740993}`)}
}

func eventFor(entity string) model.ExecutionEventInput {
	return model.ExecutionEventInput{EntityType: "run", EntityID: entity, Type: "run.changed", Payload: []byte(`{"at":9007199254740993}`)}
}

func terminalFor(id string) *model.CommandCompletion {
	return &model.CommandCompletion{CommandID: id, Result: model.CommandResult{Status: "succeeded", ResultRef: "run", Result: []byte(`{"at":9007199254740993,"ok":true}`)}}
}

func recordData(name string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"name":%q,"counter":9007199254740993}`, name))
}

func readCommandFact(t *testing.T, f *executionFixture, id string) model.CommandFound {
	t.Helper()
	out, err := f.named("read-command-"+id, model.ExecutionCommandGetOperation, model.CommandRead{ID: id})
	if err != nil || out.Receipt != nil {
		t.Fatalf("command read %+v %v", out, err)
	}
	var fact model.CommandFound
	if err = json.Unmarshal(out.Result, &fact); err != nil {
		t.Fatal(err)
	}
	return fact
}

func executionCursor(t *testing.T, f *executionFixture) model.EventCursor {
	t.Helper()
	out, err := f.named("cursor", model.ExecutionEventCursorOperation, struct{}{})
	if err != nil || out.Receipt != nil {
		t.Fatalf("cursor %+v %v", out, err)
	}
	var fact model.EventCursor
	if err = json.Unmarshal(out.Result, &fact); err != nil {
		t.Fatal(err)
	}
	return fact
}

func executionErrorCode(err error) string {
	var e *api.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func noExecutionReceipt(t *testing.T, f *executionFixture, id string) {
	t.Helper()
	if _, err := f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: id}); executionErrorCode(err) != "not_found" {
		t.Fatalf("failed action retained receipt: %v", err)
	}
}

func TestEngineExecutionAtomicLargeActionReceiptAndRestart(t *testing.T) {
	f := newExecutionFixture(t, "", 2000)
	initial := executionCursor(t, f)
	intent := model.ExecutionCommit{Command: commandFor("command"), Completion: terminalFor("command")}
	for i := 0; i < 300; i++ {
		data, _ := json.Marshal(map[string]any{"name": fmt.Sprintf("name-%03d", i), "counter": json.Number("9007199254740993"), "padding": strings.Repeat("x", 16<<10)})
		intent.State = append(intent.State, api.Mutation{Collection: "runs", Key: fmt.Sprintf("run-%03d", i), ExpectedVersion: "0", Data: data})
	}
	for i := 0; i < 1000; i++ {
		intent.Events = append(intent.Events, eventFor(fmt.Sprintf("entity-%d", i%2)))
	}
	wire, _ := json.Marshal(intent)
	if len(wire) <= api.MaxRequestBytes || len(intent.State) <= api.MaxOperations {
		t.Fatal("action does not exceed the ordinary profile")
	}
	first, facts := committedExecution(t, f, "large-action", intent)
	if facts.Replayed || !facts.CommandCreated || facts.Command == nil || facts.Command.Status != "succeeded" || len(facts.State) != 300 || len(facts.Events) != 1000 || facts.Events[999].Offset != "1000" || facts.Events[999].Seq != "500" {
		t.Fatalf("incomplete atomic result state=%d events=%d command=%+v", len(facts.State), len(facts.Events), facts.Command)
	}
	for _, r := range facts.State {
		if r.Version != first.Receipt.Token.Revision {
			t.Fatal("state did not use owner commit version")
		}
	}
	q := api.Query{Collection: "runs", Index: "name", Equal: []json.RawMessage{json.RawMessage(`"name-299"`)}, Limit: 1}
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{q}})
	if err != nil || len(snapshot.Results[0].Records) != 1 || snapshot.Results[0].Records[0].Key != "run-299" || !strings.Contains(string(snapshot.Results[0].Records[0].Data), "9007199254740993") {
		t.Fatalf("engine index/precision lost: %v", err)
	}
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.config.Create = false
	f.store, err = engine.Open(f.ctx, f.config)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.named("large-action", model.ExecutionCommitOperation, intent)
	if err != nil || again.Receipt == nil || again.Receipt.Digest != first.Receipt.Digest || string(again.Result) != string(first.Result) {
		t.Fatalf("restart changed committed result: %v", err)
	}
	cursor := executionCursor(t, f)
	if cursor.StreamID != initial.StreamID || cursor.LatestOffset != "1000" {
		t.Fatal("restart changed stream identity or repeated events")
	}
	// The same business intent under a different transport identity skips the
	// stale state CAS and all events, while confirming its original terminal.
	_, replayed := committedExecution(t, f, "business-replay", intent)
	if !replayed.Replayed || replayed.CommandCreated || len(replayed.State) != 0 || len(replayed.Events) != 0 || replayed.Command == nil || !replayed.Command.CompletedAt.Equal(*facts.Command.CompletedAt) {
		t.Fatal("business replay applied another action")
	}
	_, terminalReplay := committedExecution(t, f, "terminal-replay", model.ExecutionCommit{Completion: terminalFor("command"), State: intent.State, Events: intent.Events[:1]})
	if !terminalReplay.Replayed || len(terminalReplay.Events) != 0 || executionCursor(t, f).LatestOffset != "1000" {
		t.Fatal("terminal replay repeated an action")
	}
	changed := *commandFor("command")
	changed.Payload = []byte(`{"at":9007199254740992}`)
	if _, err = f.named("bad-command-replay", model.ExecutionCommitOperation, model.ExecutionCommit{Command: &changed, Events: []model.ExecutionEventInput{eventFor("other")}}); executionErrorCode(err) != "conflict" {
		t.Fatalf("adjacent integer replay accepted: %v", err)
	}
	noExecutionReceipt(t, f, "bad-command-replay")
	read, err := f.named("read-durable-events", model.ExecutionEventReadOperation, model.EventRead{AfterOffset: "998", Through: "1000", Limit: 10})
	if err != nil || read.Receipt != nil {
		t.Fatalf("durable event read %v", err)
	}
	var page model.EventPage
	if err = json.Unmarshal(read.Result, &page); err != nil || len(page.Events) != 2 || page.NextOffset != "1000" {
		t.Fatalf("durable page %+v %v", page, err)
	}
}

func TestEngineExecutionLastEventFailureRollsBackStateCommandAndReceipt(t *testing.T) {
	f := newExecutionFixture(t, `CREATE TRIGGER fail_last BEFORE INSERT ON core_execution_events WHEN NEW.offset=1000 BEGIN SELECT RAISE(ABORT,'last event fault'); END`, 2000)
	intent := model.ExecutionCommit{Command: commandFor("failed"), Completion: terminalFor("failed"), State: []api.Mutation{{Collection: "runs", Key: "run", ExpectedVersion: "0", Data: recordData("new")}}}
	for i := 0; i < 1000; i++ {
		intent.Events = append(intent.Events, eventFor("run"))
	}
	if _, err := f.named("failed-action", model.ExecutionCommitOperation, intent); err == nil {
		t.Fatal("last event fault committed")
	}
	noExecutionReceipt(t, f, "failed-action")
	if readCommandFact(t, f, "failed").Found || executionCursor(t, f).LatestOffset != "0" {
		t.Fatal("failed action published command or counters")
	}
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"run"}}, {Collection: "runs", Index: "name", Equal: []json.RawMessage{json.RawMessage(`"new"`)}, Limit: 1}}})
	if err != nil || snapshot.Token.Revision != "0" || !snapshot.Results[0].Records[0].Missing || len(snapshot.Results[1].Records) != 0 {
		t.Fatalf("failed state/index/scope version persisted: %+v %v", snapshot, err)
	}
	// Inspect the isolated fixture only, through a read-only connection, to
	// prove no quota debit survived the rollback either.
	db, err := sql.Open("sqlite", "file:"+f.config.Path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"core_data_usage", "usage", "core_event_offsets", "core_event_sequences", "core_execution_events", "core_commands", "named_results", "receipts"} {
		var n int
		if err = db.QueryRowContext(f.ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("failed action retained %s: %d %v", table, n, err)
		}
	}
}

func TestEngineExecutionCompletionFailureRestoresAcceptedAction(t *testing.T) {
	f := newExecutionFixture(t, `CREATE TRIGGER fail_completion_event BEFORE INSERT ON core_execution_events WHEN NEW.offset=2 BEGIN SELECT RAISE(ABORT,'completion event fault'); END`, 100)
	accepted, _ := committedExecution(t, f, "accepted", model.ExecutionCommit{Command: commandFor("command"), State: []api.Mutation{{Collection: "runs", Key: "run", ExpectedVersion: "0", Data: recordData("accepted")}}, Events: []model.ExecutionEventInput{eventFor("run")}})
	before := readCommandFact(t, f, "command")
	intent := model.ExecutionCommit{Completion: terminalFor("command"), State: []api.Mutation{{Collection: "runs", Key: "run", ExpectedVersion: accepted.Receipt.Token.Revision, Data: recordData("terminal")}}, Events: []model.ExecutionEventInput{eventFor("run")}}
	if _, err := f.named("bad-completion", model.ExecutionCommitOperation, intent); err == nil {
		t.Fatal("failed completion committed")
	}
	noExecutionReceipt(t, f, "bad-completion")
	after := readCommandFact(t, f, "command")
	if !after.Found || after.Receipt.Status != "accepted" || after.Receipt.CompletedAt != nil || !after.Receipt.CreatedAt.Equal(before.Receipt.CreatedAt) || executionCursor(t, f).LatestOffset != "1" {
		t.Fatal("failed completion changed accepted receipt or events")
	}
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"run"}}}})
	if err != nil || snapshot.Token.Revision != accepted.Receipt.Token.Revision || snapshot.Results[0].Records[0].Version != accepted.Receipt.Token.Revision || !strings.Contains(string(snapshot.Results[0].Records[0].Data), "accepted") {
		t.Fatal("failed completion changed original state")
	}
}

func TestEngineExecutionConcurrentTaskClaimCAS(t *testing.T) {
	f := newExecutionFixture(t, "", 100)
	seed, _ := committedExecution(t, f, "queue-task", model.ExecutionCommit{State: []api.Mutation{{Collection: "tasks", Key: "task", ExpectedVersion: "0", Data: recordData("queued")}}})
	type result struct {
		id  string
		out api.NamedResponse
		err error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, id := range []string{"claim-a", "claim-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			intent := model.ExecutionCommit{Command: commandFor(id), State: []api.Mutation{{Collection: "tasks", Key: "task", ExpectedVersion: seed.Receipt.Token.Revision, Data: recordData(id)}}, Events: []model.ExecutionEventInput{eventFor("task")}}
			out, err := f.named(id, model.ExecutionCommitOperation, intent)
			results <- result{id, out, err}
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result.err == nil {
			successes++
			if result.out.Receipt == nil || !readCommandFact(t, f, result.id).Found {
				t.Fatal("winning claim lacks durable receipt")
			}
		} else {
			if executionErrorCode(result.err) != "conflict" {
				t.Fatalf("unexpected losing claim error %v", result.err)
			}
			noExecutionReceipt(t, f, result.id)
			if readCommandFact(t, f, result.id).Found {
				t.Fatal("losing claim leaked business receipt")
			}
		}
	}
	if successes != 1 || executionCursor(t, f).LatestOffset != "1" {
		t.Fatal("task claimed more than once")
	}
}

func TestEngineExecutionReusesIndexSwapTombstoneAndQuota(t *testing.T) {
	f := newExecutionFixture(t, "", 2)
	seed, _ := committedExecution(t, f, "seed", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "a", ExpectedVersion: "0", Data: recordData("left")}, {Collection: "runs", Key: "b", ExpectedVersion: "0", Data: recordData("right")}}})
	swapped, _ := committedExecution(t, f, "swap", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "a", ExpectedVersion: seed.Receipt.Token.Revision, Data: recordData("right")}, {Collection: "runs", Key: "b", ExpectedVersion: seed.Receipt.Token.Revision, Data: recordData("left")}}})
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Index: "name", Equal: []json.RawMessage{json.RawMessage(`"left"`)}, Limit: 1}}})
	if err != nil || len(snapshot.Results[0].Records) != 1 || snapshot.Results[0].Records[0].Key != "b" {
		t.Fatal("named action did not reuse atomic index swap")
	}
	deleted, _ := committedExecution(t, f, "delete", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "a", ExpectedVersion: swapped.Receipt.Token.Revision, Delete: true}}})
	for id, intent := range map[string]model.ExecutionCommit{
		"stale-recreate": {Command: commandFor("stale"), State: []api.Mutation{{Collection: "runs", Key: "a", ExpectedVersion: "0", Data: recordData("new")}}, Events: []model.ExecutionEventInput{eventFor("run")}},
		"quota":          {Command: commandFor("quota"), State: []api.Mutation{{Collection: "runs", Key: "c", ExpectedVersion: "0", Data: recordData("new")}}, Events: []model.ExecutionEventInput{eventFor("run")}},
	} {
		_, err := f.named(id, model.ExecutionCommitOperation, intent)
		want := "conflict"
		if id == "quota" {
			want = "resource_exhausted"
		}
		if executionErrorCode(err) != want {
			t.Fatalf("%s invalid action accepted: %v", id, err)
		}
		noExecutionReceipt(t, f, id)
		if readCommandFact(t, f, intent.Command.CommandID).Found {
			t.Fatal("invalid state action retained business receipt")
		}
	}
	committedExecution(t, f, "recreate", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "a", ExpectedVersion: deleted.Receipt.Token.Revision, Data: recordData("new")}}})
	if executionCursor(t, f).LatestOffset != "0" {
		t.Fatal("invalid state action appended events")
	}
}

func TestEngineExecutionStopReceiptFencesGroupAtUnchangedRunVersion(t *testing.T) {
	f := newExecutionFixture(t, "", 100)
	parent := model.RunPrepareState{ID: "parent", TargetID: "local", RootRunID: "parent", ExecutionModel: model.OccurrenceExecutionModel, Status: "running"}
	producer := model.ProducerPrepareState{ID: "invocation", RunID: "parent", NodeID: "call", Kind: "automation-call", Status: "running", ChildRunProducer: true}
	parentJSON, _ := json.Marshal(parent)
	producerJSON, _ := json.Marshal(producer)
	seed, _ := committedExecution(t, f, "seed-parent", model.ExecutionCommit{State: []api.Mutation{{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: parentJSON}, {Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: producerJSON}, {Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"pinned":true}`)}}})
	guard := model.RecordGuard{Collection: "runs", Key: "parent", Version: seed.Receipt.Token.Revision}
	plan := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "call-group", ParentGuard: guard, InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: guard.Version}, PinGuard: model.RecordGuard{Collection: "definitions", Key: "pin", Version: guard.Version}, Condition: model.GroupPrepareCondition{Parent: parent, Producer: producer, Ancestors: []model.RecordGuard{guard}}, Body: json.RawMessage(`{}`), Members: []model.GroupMember{{ItemKey: "item", ChildID: "child", EventID: "event", Parameters: []byte(`{}`), Event: json.RawMessage(`{}`), Link: json.RawMessage(`{}`), Body: json.RawMessage(`{}`)}}}
	stop := commandFor("stop")
	stop.Target, stop.Action = model.RunCommandTargetPrefix+parent.ID, model.StopSetAction
	committedExecution(t, f, "stop-command", model.ExecutionCommit{Command: stop})
	if _, err := f.named("blocked-group", model.GroupPrepareOperation, plan); executionErrorCode(err) != "conflict" {
		t.Fatalf("named stop escaped preparation fence: %v", err)
	}
	noExecutionReceipt(t, f, "blocked-group")
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"parent"}}}})
	if err != nil || snapshot.Results[0].Records[0].Version != guard.Version {
		t.Fatal("stop test changed the parent point version")
	}
	if _, err := f.named("read-blocked-group", model.GroupSnapshotOperation, model.GroupRead{ID: "group"}); executionErrorCode(err) != "not_found" {
		t.Fatalf("blocked group published facts: %v", err)
	}
}

func TestEngineExecutionRejectsEmptyLimitsAndMismatchedCommand(t *testing.T) {
	f := newExecutionFixture(t, "", 100)
	for id, intent := range map[string]model.ExecutionCommit{
		"empty":               {},
		"too-many-state":      {State: make([]api.Mutation, model.MaxExecutionStateMutations+1)},
		"too-many-events":     {Events: make([]model.ExecutionEventInput, model.MaxExecutionEvents+1)},
		"different-command":   {Command: commandFor("one"), Completion: terminalFor("two")},
		"invalid-final-event": {Command: commandFor("invalid"), State: []api.Mutation{{Collection: "runs", Key: "invalid", ExpectedVersion: "0", Data: recordData("invalid")}}, Events: []model.ExecutionEventInput{{EntityType: "run", EntityID: "invalid", Type: "invalid", Payload: []byte(`not JSON`)}}},
	} {
		_, err := f.named(id, model.ExecutionCommitOperation, intent)
		want := "invalid_argument"
		if strings.HasPrefix(id, "too-many") {
			want = "resource_exhausted"
		}
		if executionErrorCode(err) != want {
			t.Fatalf("%s accepted invalid action: %v", id, err)
		}
		noExecutionReceipt(t, f, id)
	}
	if readCommandFact(t, f, "one").Found || readCommandFact(t, f, "invalid").Found || executionCursor(t, f).LatestOffset != "0" {
		t.Fatal("invalid action left durable facts")
	}
	snapshot, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "runs", Keys: []string{"invalid"}}}})
	if err != nil || !snapshot.Results[0].Records[0].Missing || snapshot.Token.Revision != "0" {
		t.Fatal("invalid action leaked state")
	}
}
