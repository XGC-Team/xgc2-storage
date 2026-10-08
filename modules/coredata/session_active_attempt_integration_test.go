package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// These are real engine.Named reads of persisted execution.commit facts. The
// pointer is never manufactured by the reader or a prebuilt snapshot fixture.
func TestSessionWorkflowSnapshotActiveAttemptOwnership(t *testing.T) {
	for _, name := range []string{"valid", "absent", "empty", "dangling", "another-invocation", "another-run", "non-string"} {
		t.Run(name, func(t *testing.T) {
			f := openSessionFixture(t, nil)
			rows := sessionBaseRows()
			var invocation map[string]json.RawMessage
			if err := json.Unmarshal(rows[3].Data, &invocation); err != nil {
				t.Fatal(err)
			}
			active := "attempt"
			switch name {
			case "absent":
				active = ""
			case "empty":
				active = ""
			case "dangling":
				active = "missing-attempt"
			case "another-invocation":
				active = "other-attempt"
				rows = append(rows,
					sessionRow(model.InvocationsCollection, "other-invocation", map[string]any{"id": "other-invocation", "runId": "root", "nodeId": "other-node", "kind": "task", "status": "running", "revision": 1}),
					sessionRow(model.AttemptsCollection, active, map[string]any{"id": active, "runId": "root", "invocationId": "other-invocation", "phase": "execution", "number": 1, "status": "running", "revision": 1}),
				)
			case "another-run":
				active = "foreign-attempt"
				rows = append(rows,
					sessionRow(model.RunsCollection, "foreign-run", sessionRun("foreign-run", "")),
					sessionRow(model.InvocationsCollection, "foreign-invocation", map[string]any{"id": "foreign-invocation", "runId": "foreign-run", "nodeId": "other-node", "kind": "task", "status": "running", "revision": 1}),
					sessionRow(model.AttemptsCollection, active, map[string]any{"id": active, "runId": "foreign-run", "invocationId": "invocation", "phase": "execution", "number": 1, "status": "running", "revision": 1}),
				)
			}
			if name != "absent" {
				invocation["activeAttemptId"] = sessionWire(active)
			}
			if name == "non-string" {
				invocation["activeAttemptId"] = json.RawMessage(`7`)
			}
			// Private durable state must remain private even on a valid pointer.
			for _, key := range []string{"checkpoint", "resolvedParameters", "parameters", "leaseToken", "leaseOwner"} {
				invocation[key] = json.RawMessage(`{"secret":"PRIVATE_SENTINEL"}`)
			}
			rows[3].Data = sessionWire(invocation)
			f.save("seed-explicit-pointer", rows)
			r, err := f.snapshot("local", "session")
			if name != "valid" && name != "absent" && name != "empty" {
				requireSessionCode(t, err, "data_loss")
				if len(r.Result) != 0 || r.Receipt != nil {
					t.Fatal("invalid pointer exposed partial snapshot/receipt")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.Receipt != nil || strings.Contains(string(r.Result), "PRIVATE_SENTINEL") {
				t.Fatal("private fields or read receipt escaped")
			}
			var snapshot model.SessionWorkflowLogSnapshot
			if err := json.Unmarshal(r.Result, &snapshot); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Runs) != 1 || len(snapshot.Runs[0].Invocations) != 1 || len(snapshot.Runs[0].Attempts) != 1 {
				t.Fatal("ownership graph changed")
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(snapshot.Runs[0].Invocations[0], &got); err != nil {
				t.Fatal(err)
			}
			if name == "absent" {
				if _, exists := got["activeAttemptId"]; exists {
					t.Fatal("reader synthesized active pointer")
				}
			} else if string(got["activeAttemptId"]) != string(sessionWire(active)) {
				t.Fatal("persisted pointer discarded/changed")
			}
		})
	}
}

func TestSessionWorkflowSnapshotActiveAttemptPointerSharesReadTransaction(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var pause atomic.Bool
	execute := func(ctx context.Context, tx *sql.Tx, scope, op string, payload json.RawMessage) (json.RawMessage, error) {
		if op == model.SessionWorkflowLogSnapshotOperation && pause.CompareAndSwap(true, false) {
			if _, e := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: model.SessionsCollection, Keys: []string{"session"}}); e != nil {
				return nil, e
			}
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return coredata.Execute(ctx, tx, scope, op, payload)
	}
	f := openSessionFixture(t, execute)
	rows := sessionBaseRows()
	var inv map[string]any
	json.Unmarshal(rows[3].Data, &inv)
	inv["activeAttemptId"] = "attempt"
	rows[3].Data = sessionWire(inv)
	f.save("seed-pointer-old", rows)
	pause.Store(true)
	type result struct {
		response api.NamedResponse
		err      error
	}
	done := make(chan result, 1)
	go func() { r, e := f.snapshot("local", "session"); done <- result{r, e} }()
	<-started
	inv["activeAttemptId"] = "attempt-new"
	inv["revision"] = 2
	update := sessionRow(model.InvocationsCollection, "invocation", inv)
	update.ExpectedVersion = "1"
	f.save("atomic-pointer-and-attempt", []api.Mutation{update, sessionRow(model.AttemptsCollection, "attempt-new", map[string]any{"id": "attempt-new", "runId": "root", "invocationId": "invocation", "phase": "execution", "number": 2, "status": "running", "revision": 1})})
	close(release)
	old := <-done
	if old.err != nil {
		t.Fatal(old.err)
	}
	next, e := f.snapshot("local", "session")
	if e != nil {
		t.Fatal(e)
	}
	for i, r := range []api.NamedResponse{old.response, next} {
		var s model.SessionWorkflowLogSnapshot
		if e := json.Unmarshal(r.Result, &s); e != nil {
			t.Fatal(e)
		}
		var inv map[string]json.RawMessage
		json.Unmarshal(s.Runs[0].Invocations[0], &inv)
		want := `"attempt"`
		if i == 1 {
			want = `"attempt-new"`
		}
		if string(inv["activeAttemptId"]) != want || len(s.Runs[0].Attempts) != i+1 || r.Receipt != nil {
			t.Fatal("mixed active pointer/attempt snapshot or read receipt")
		}
	}
}
