package coredata_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestExecutionQueueConcurrentStartsCoalesceWakesAndFenceCompletion(t *testing.T) {
	f := newExecutionFixture(t, "", 100, model.WorkflowCollections()...)
	at := time.Date(2026, 10, 10, 6, 0, 0, 0, time.UTC)
	type taskBody struct {
		model.RunReconcileTask
		ClaimToken string `json:"claimToken"`
	}
	read := func(id string) api.Record {
		t.Helper()
		out, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: model.RunTasksCollection, Keys: []string{id}}}})
		if err != nil {
			t.Fatal(err)
		}
		return out.Results[0].Records[0]
	}
	body := func(r api.Record) taskBody {
		t.Helper()
		var v taskBody
		if err := json.Unmarshal(r.Data, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	parallel := func(intent func(int) model.ExecutionCommit) {
		t.Helper()
		start := make(chan struct{})
		var workers sync.WaitGroup
		failures := make(chan error, 2)
		for i := range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				_, err := f.named(fmt.Sprintf("parallel-%d-%d", i, time.Now().UnixNano()), model.ExecutionCommitOperation, intent(i))
				if err != nil {
					failures <- err
				}
			}()
		}
		close(start)
		workers.Wait()
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
	}
	parallel(func(i int) model.ExecutionCommit {
		id := fmt.Sprintf("run-%d", i)
		return model.ExecutionCommit{State: []api.Mutation{{Collection: model.RunsCollection, Key: id, ExpectedVersion: "0", Data: json.RawMessage(`{"status":"running"}`)}}, RunQueue: []model.RunQueueMutation{{RunID: id, At: at}}}
	})
	first, peer := read("run-0"), read("run-1")
	a, b := body(first), body(peer)
	if a.ReadySequence == b.ReadySequence || a.ReadySequence+b.ReadySequence != 3 {
		t.Fatalf("not unique FIFO allocations: %+v %+v", a, b)
	}
	parallel(func(int) model.ExecutionCommit {
		return model.ExecutionCommit{RunQueue: []model.RunQueueMutation{{RunID: "run-0", At: at.Add(time.Second)}}}
	})
	if got := read("run-0"); got.Version != first.Version {
		t.Fatalf("ready wake rewrote task: before=%s after=%s", first.Version, got.Version)
	}
	// The real claim still uses its original point CAS. Wake merging must not
	// weaken that fence or overwrite a claim installed by another writer.
	a.State = model.RunReconcileTaskClaimed
	a.ClaimedGeneration = a.DirtyGeneration
	a.ClaimOwner = "worker"
	a.ClaimToken = "current-token"
	expiry := at.Add(time.Minute)
	a.ClaimExpiresAt = &expiry
	a.Revision++
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	committedExecution(t, f, "claim", model.ExecutionCommit{State: []api.Mutation{{Collection: model.RunTasksCollection, Key: a.RunID, ExpectedVersion: first.Version, Data: raw}}})
	parallel(func(int) model.ExecutionCommit {
		return model.ExecutionCommit{RunQueue: []model.RunQueueMutation{{RunID: a.RunID, At: at.Add(time.Second)}}}
	})
	dirty := body(read(a.RunID))
	if dirty.DirtyGeneration != a.ClaimedGeneration+2 || dirty.ClaimToken != a.ClaimToken {
		t.Fatalf("lost claimed wake/fence: %+v", dirty)
	}
	completion := model.RunQueueMutation{RunID: a.RunID, At: at.Add(2 * time.Second), Complete: true, Owner: a.ClaimOwner, ClaimToken: a.ClaimToken, NextAvailableAt: at.Add(time.Hour)}
	wrong := completion
	wrong.ClaimToken = "obsolete-token"
	_, err = f.named("wrong-claim", model.ExecutionCommitOperation, model.ExecutionCommit{State: []api.Mutation{{Collection: "tasks", Key: "must-not-exist", ExpectedVersion: "0", Data: recordData("wrong")}}, RunQueue: []model.RunQueueMutation{wrong}})
	if executionErrorCode(err) != "conflict" {
		t.Fatalf("wrong claim: %v", err)
	}
	probe, err := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "tasks", Keys: []string{"must-not-exist"}}}})
	if err != nil || !probe.Results[0].Records[0].Missing {
		t.Fatalf("failed completion committed partial state: %+v %v", probe, err)
	}
	_, facts := committedExecution(t, f, "complete", model.ExecutionCommit{RunQueue: []model.RunQueueMutation{completion}})
	var completed taskBody
	for _, r := range facts.State {
		if r.Collection == model.RunTasksCollection && r.Key == a.RunID {
			completed = body(r)
		}
	}
	if completed.State != model.RunReconcileTaskReady || !completed.AvailableAt.Equal(completion.At) || completed.ReadySequence <= b.ReadySequence || completed.ClaimToken != "" {
		t.Fatalf("bad committed completion: %+v", completed)
	}
	_, replayed := committedExecution(t, f, "complete", model.ExecutionCommit{RunQueue: []model.RunQueueMutation{completion}})
	if string(mustMarshalQueueTest(t, replayed)) != string(mustMarshalQueueTest(t, facts)) {
		t.Fatal("receipt replay changed committed task")
	}
	if _, err = f.named("complete-again", model.ExecutionCommitOperation, model.ExecutionCommit{RunQueue: []model.RunQueueMutation{completion}}); executionErrorCode(err) != "conflict" {
		t.Fatalf("stale completion accepted: %v", err)
	}
}

func mustMarshalQueueTest(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
