package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type sessionFixture struct {
	t     *testing.T
	ctx   context.Context
	store *engine.Store
	scope api.Scope
	token api.Token
}

func openSessionFixture(t *testing.T, execute func(context.Context, *sql.Tx, string, string, json.RawMessage) (json.RawMessage, error)) *sessionFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	if execute == nil {
		execute = coredata.Execute
	}
	n := api.Namespace{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 1000, ReceiptTTLSeconds: 3600, Collections: model.SessionGraphCollections(), Modules: []api.Module{coredata.Spec()}}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "store.db"), Create: true, Manifest: api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{n}}, Modules: []engine.DataModule{{Spec: coredata.Spec(), Initialize: coredata.Initialize, Execute: execute}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	read, err := store.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: model.SessionsCollection, Keys: []string{"session"}}}})
	if err != nil {
		t.Fatal(err)
	}
	return &sessionFixture{t, ctx, store, scope, read.Token}
}

func sessionWire(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func sessionRow(collection, key string, v any) api.Mutation {
	return api.Mutation{Collection: collection, Key: key, ExpectedVersion: "0", Data: sessionWire(v)}
}
func sessionRun(id, parent string) map[string]any {
	return map[string]any{"id": id, "targetId": "local", "rootRunId": "root", "parentRunId": parent, "callNodeId": "call", "definitionId": "definition", "definitionVersion": 1, "configDigest": strings.Repeat("c", 64), "executionPlanDigest": strings.Repeat("e", 64), "registryDigest": strings.Repeat("b", 64), "definitionDigest": strings.Repeat("d", 64), "status": "running", "executionModel": model.OccurrenceExecutionModel, "revision": json.Number("9007199254740993"), "parameters": map[string]any{"secret": "PRIVATE_SENTINEL"}, "bindingContext": "PRIVATE_SENTINEL", "result": "PRIVATE_SENTINEL"}
}
func sessionMember(id, kind, owner string) map[string]any {
	return map[string]any{"id": id, "targetId": "local", "sessionId": "session", "bindingId": id, "kind": kind, "ownerId": owner, "status": "running", "revision": 1}
}
func sessionLink(id, child string, bound, remote bool) map[string]any {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	r := map[string]any{"id": id, "targetId": "local", "rootRunId": "root", "parentRunId": "root", "parentInvocationId": "invocation", "callNodeId": "call", "childRunId": child, "ownerRunId": "root", "childDefinitionId": "definition", "childDefinitionVersion": 1, "childConfigDigest": strings.Repeat("c", 64), "childExecutionPlanDigest": strings.Repeat("e", 64), "childRegistryDigest": strings.Repeat("b", 64), "childDefinitionDigest": strings.Repeat("d", 64), "targetRoot": remote, "relation": "attached", "ordinal": int(h.Sum32()), "revision": 1, "targetRootParameters": "PRIVATE_SENTINEL", "runStatus": "forged", "runRevision": 99}
	if bound {
		r["boundAt"] = "2026-10-09T00:00:00Z"
	}
	return r
}
func sessionRelation(kind string, fact map[string]any) api.Mutation {
	id := fact["id"].(string)
	return sessionRow(model.RelationCollection(kind), id, fact)
}

func sessionBaseRows() []api.Mutation {
	return []api.Mutation{
		sessionRow(model.SessionsCollection, "session", map[string]any{"id": "session", "targetId": "local", "state": "active", "mode": "partial", "runMode": "run", "revision": 1, "activeSlot": "PRIVATE_SENTINEL", "openingRunId": "PRIVATE_SENTINEL"}),
		sessionRow(model.SessionMembersCollection, "member", sessionMember("member", "workflow_run", "root")),
		sessionRow(model.RunsCollection, "root", sessionRun("root", "")),
		sessionRow(model.InvocationsCollection, "invocation", map[string]any{"id": "invocation", "runId": "root", "nodeId": "call", "kind": "automation-call", "status": "running", "revision": 1, "checkpoint": "PRIVATE_SENTINEL", "resolvedParameters": "PRIVATE_SENTINEL", "childRunProducer": true}),
		sessionRow(model.AttemptsCollection, "attempt", map[string]any{"id": "attempt", "runId": "root", "invocationId": "invocation", "phase": "execution", "number": 1, "status": "running", "revision": 1, "owner": "PRIVATE_SENTINEL", "leaseToken": "PRIVATE_SENTINEL", "checkpoint": "PRIVATE_SENTINEL"}),
	}
}

func sessionJobRow() api.Mutation {
	origin := map[string]any{"RunTargetID": "local", "RunID": "root", "RootRunID": "root", "DefinitionID": "definition", "ConfigDigest": strings.Repeat("c", 64), "ExecutionPlanDigest": strings.Repeat("e", 64), "RegistryDigest": strings.Repeat("b", 64), "DefinitionDigest": strings.Repeat("d", 64), "NodeID": "call", "NodeKind": "automation-call", "NodeTypeVersion": 1, "InvocationID": "invocation", "Private": "PRIVATE_SENTINEL"}
	job := map[string]any{"ID": "job", "TargetID": "local", "Kind": "task", "Status": "running", "Revision": 1, "AttemptCount": 1, "UpdatedAt": "2026-10-09T00:00:00Z", "Params": "PRIVATE_SENTINEL", "Checkpoint": "PRIVATE_SENTINEL", "DedupeKey": "PRIVATE_SENTINEL", "Attempts": []any{map[string]any{"ID": "job-attempt", "Number": 1, "Status": "running", "StartedAt": "2026-10-09T00:00:00Z", "Owner": "PRIVATE_SENTINEL", "LeaseToken": "PRIVATE_SENTINEL"}}}
	return sessionRow(model.WorkflowJobsCollection, "job", model.WorkflowJobRecord{RunTargetID: "local", RunID: "root", InvocationID: "invocation", Origin: sessionWire(origin), Job: sessionWire(job)})
}

func (f *sessionFixture) save(requestID string, rows []api.Mutation) {
	f.t.Helper()
	r, err := f.store.Named(f.ctx, api.NamedRequest{Scope: f.scope, DatabaseID: f.token.DatabaseID, Schema: f.token.Schema, Module: coredata.Spec().ID, Operation: model.ExecutionCommitOperation, RequestID: requestID, Payload: sessionWire(model.ExecutionCommit{State: rows})})
	if err != nil {
		f.t.Fatal(err)
	}
	f.token = r.Receipt.Token
}
func (f *sessionFixture) snapshot(target, session string) (api.NamedResponse, error) {
	return f.store.Named(f.ctx, api.NamedRequest{Scope: f.scope, DatabaseID: f.token.DatabaseID, Schema: f.token.Schema, Module: coredata.Spec().ID, Operation: model.SessionWorkflowLogSnapshotOperation, RequestID: "read-session", Payload: sessionWire(model.SessionWorkflowLogRead{TargetID: target, SessionID: session})})
}
func requireSessionCode(t *testing.T, err error, code string) {
	t.Helper()
	var e *api.Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestSessionWorkflowSnapshotExactGraphSafeOriginAndSharedRun(t *testing.T) {
	f := openSessionFixture(t, nil)
	rows := sessionBaseRows()
	rows = append(rows, sessionRow(model.SessionMembersCollection, "command", sessionMember("command", "workflow_command", "root")), sessionRow(model.RunsCollection, "child", sessionRun("child", "root")), sessionRow(model.RunsCollection, "prepared", sessionRun("prepared", "root")), sessionRow(model.RunsCollection, "remote", sessionRun("remote", "root")))
	rows = append(rows, sessionRelation("childRuns", sessionLink("bound", "child", true, false)), sessionRelation("childRuns", sessionLink("prepared-link", "prepared", false, false)), sessionRelation("childRuns", sessionLink("remote-link", "remote", true, true)), sessionJobRow())
	other := sessionMember("unrelated", "workflow_run", "unrelated-run")
	other["sessionId"] = "another-session"
	rows = append(rows, sessionRow(model.SessionMembersCollection, "unrelated", other), sessionRow(model.RunsCollection, "unrelated-run", sessionRun("unrelated-run", "")))
	f.save("seed", rows)
	r, err := f.snapshot("local", "session")
	if err != nil {
		t.Fatal(err)
	}
	if r.Receipt != nil {
		t.Fatal("read minted a receipt")
	}
	if strings.Contains(string(r.Result), "PRIVATE_SENTINEL") || strings.Contains(string(r.Result), "unrelated-run") || !strings.Contains(string(r.Result), "9007199254740993") {
		t.Fatalf("unsafe, unrelated or rounded result: %s", r.Result)
	}
	var snapshot model.SessionWorkflowLogSnapshot
	if err = json.Unmarshal(r.Result, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Runs) != 2 || len(snapshot.Jobs) != 1 {
		t.Fatalf("graph runs=%d jobs=%d", len(snapshot.Runs), len(snapshot.Jobs))
	}
	var origin struct{ InvocationID string }
	if json.Unmarshal(snapshot.Jobs[0].Origin, &origin) != nil || origin.InvocationID != "invocation" {
		t.Fatal("lost explicit Origin")
	}
	for _, run := range snapshot.Runs {
		var id struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(run.Run, &id)
		if id.ID == "root" {
			var rel struct {
				ChildRuns []map[string]json.RawMessage `json:"childRuns"`
			}
			_ = json.Unmarshal(run.Relations, &rel)
			if len(rel.ChildRuns) != 3 {
				t.Fatal("lost prepared/remote relation")
			}
			for _, link := range rel.ChildRuns {
				if string(link["id"]) != `"bound"` && len(link["runRevision"]) != 0 {
					t.Fatal("fabricated child lifecycle")
				}
			}
		}
	}
	_, err = f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: "read-session"})
	requireSessionCode(t, err, "not_found")
	_, err = f.snapshot("foreign", "session")
	requireSessionCode(t, err, "not_found")
	_, err = f.snapshot("local", "missing")
	requireSessionCode(t, err, "not_found")
}

func TestSessionWorkflowSnapshotCorruptDependenciesFailCompleteRead(t *testing.T) {
	for _, kind := range []string{"missing-owner", "missing-bound-child", "cross-target-child", "origin-pin", "ordinary-job", "orphan-attempt", "orphan-relation", "duplicate-invocation-node", "missing-producer", "cycle"} {
		t.Run(kind, func(t *testing.T) {
			f := openSessionFixture(t, nil)
			rows := sessionBaseRows()
			switch kind {
			case "missing-owner":
				rows = append(rows[:2], rows[3:]...)
			case "missing-bound-child":
				rows = append(rows, sessionRelation("childRuns", sessionLink("link", "absent", true, false)))
			case "cross-target-child":
				child := sessionRun("child", "root")
				child["targetId"] = "other"
				rows = append(rows, sessionRow(model.RunsCollection, "child", child), sessionRelation("childRuns", sessionLink("link", "child", true, false)))
			case "origin-pin", "ordinary-job":
				job := sessionJobRow()
				var record model.WorkflowJobRecord
				_ = json.Unmarshal(job.Data, &record)
				if kind == "ordinary-job" {
					record.Origin = nil
				} else {
					var origin map[string]any
					_ = json.Unmarshal(record.Origin, &origin)
					origin["ConfigDigest"] = "wrong"
					record.Origin = sessionWire(origin)
				}
				job.Data = sessionWire(record)
				rows = append(rows, job)
			case "orphan-attempt":
				var a map[string]any
				_ = json.Unmarshal(rows[4].Data, &a)
				a["invocationId"] = "absent"
				rows[4].Data = sessionWire(a)
			case "orphan-relation":
				rows = append(rows, sessionRelation("childRunGroups", map[string]any{"id": "group", "targetId": "local", "rootRunId": "root", "parentRunId": "root", "producerInvocationId": "invocation", "producerNodeId": "call", "memberCount": 1, "revision": 1}), sessionRelation("childRunGroupMembers", map[string]any{"id": "orphan", "groupId": "group", "childRunId": "absent", "revision": 1}))
			case "duplicate-invocation-node":
				rows = append(rows, sessionRow(model.InvocationsCollection, "duplicate", map[string]any{"id": "duplicate", "runId": "root", "nodeId": "call", "kind": "automation-call", "revision": 1}))
			case "missing-producer":
				link := sessionLink("link", "child", false, false)
				link["parentInvocationId"] = "absent"
				rows = append(rows, sessionRelation("childRuns", link))
			case "cycle":
				var root map[string]any
				_ = json.Unmarshal(rows[2].Data, &root)
				root["parentRunId"] = "root"
				rows[2].Data = sessionWire(root)
				rows = append(rows, sessionRelation("childRuns", sessionLink("link", "root", true, false)))
			}
			f.save("seed", rows)
			_, err := f.snapshot("local", "session")
			requireSessionCode(t, err, "data_loss")
		})
	}
}

func TestSessionWorkflowSnapshotMemberAndDependentLimits(t *testing.T) {
	for _, count := range []int{model.MaxSessionWorkflowMembers, model.MaxSessionWorkflowMembers + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := openSessionFixture(t, nil)
			rows := sessionBaseRows()
			for i := 1; i < count; i++ {
				id := fmt.Sprintf("member-%04d", i)
				rows = append(rows, sessionRow(model.SessionMembersCollection, id, sessionMember(id, "robot_operation", id)))
			}
			f.save("seed", rows)
			r, err := f.snapshot("local", "session")
			if count > model.MaxSessionWorkflowMembers {
				requireSessionCode(t, err, "resource_exhausted")
			} else if err != nil || len(r.Result) == 0 {
				t.Fatalf("boundary=%v", err)
			}
		})
	}
}

func TestSessionWorkflowSnapshotOneReadTransactionDuringConcurrentCommit(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var pause atomic.Bool
	execute := func(ctx context.Context, tx *sql.Tx, scope, operation string, payload json.RawMessage) (json.RawMessage, error) {
		if operation == model.SessionWorkflowLogSnapshotOperation && pause.CompareAndSwap(true, false) {
			if _, err := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: model.SessionsCollection, Keys: []string{"session"}}); err != nil {
				return nil, err
			}
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return coredata.Execute(ctx, tx, scope, operation, payload)
	}
	f := openSessionFixture(t, execute)
	f.save("seed", sessionBaseRows())
	pause.Store(true)
	request := api.NamedRequest{Scope: f.scope, DatabaseID: f.token.DatabaseID, Schema: f.token.Schema, Module: coredata.Spec().ID, Operation: model.SessionWorkflowLogSnapshotOperation, RequestID: "paused-read", Payload: sessionWire(model.SessionWorkflowLogRead{TargetID: "local", SessionID: "session"})}
	type result struct {
		r api.NamedResponse
		e error
	}
	done := make(chan result, 1)
	go func() { r, e := f.store.Named(f.ctx, request); done <- result{r, e} }()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	rows := sessionBaseRows()
	for i := range rows {
		rows[i].ExpectedVersion = "1"
		var body map[string]any
		_ = json.Unmarshal(rows[i].Data, &body)
		body["revision"] = 2
		rows[i].Data = sessionWire(body)
	}
	f.save("update-all", rows)
	close(release)
	old := <-done
	if old.e != nil {
		t.Fatal(old.e)
	}
	var before model.SessionWorkflowLogSnapshot
	_ = json.Unmarshal(old.r.Result, &before)
	var view struct {
		Session struct {
			Revision int64 `json:"revision"`
		} `json:"session"`
	}
	_ = json.Unmarshal(before.Session, &view)
	if view.Session.Revision != 1 || !strings.Contains(string(before.Runs[0].Run), "9007199254740993") {
		t.Fatalf("mixed snapshot %s", old.r.Result)
	}
	newRead, err := f.snapshot("local", "session")
	if err != nil {
		t.Fatal(err)
	}
	var after model.SessionWorkflowLogSnapshot
	_ = json.Unmarshal(newRead.Result, &after)
	_ = json.Unmarshal(after.Session, &view)
	if view.Session.Revision != 2 || !strings.Contains(string(after.Runs[0].Run), `"revision":2`) {
		t.Fatalf("new commit not visible %s", newRead.Result)
	}
}

func (f *sessionFixture) saveChunks(prefix string, rows []api.Mutation) {
	f.t.Helper()
	// Keep fixture seeding beneath the owner's per-request deadline with race
	// instrumentation. The single read still exercises the complete limit.
	const seedRows = 512
	for i := 0; i < len(rows); i += seedRows {
		end := min(i+seedRows, len(rows))
		f.save(fmt.Sprintf("%s-%d", prefix, i), rows[i:end])
	}
}

func TestSessionWorkflowSnapshot4096RunsAndOverflow(t *testing.T) {
	for _, count := range []int{model.MaxSessionWorkflowRuns, model.MaxSessionWorkflowRuns + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := openSessionFixture(t, nil)
			rows := sessionBaseRows()[:4]
			for i := 1; i < count; i++ {
				id := fmt.Sprintf("child-%04d", i)
				link := sessionLink("link-"+id, id, true, false)
				link["ordinal"] = i
				rows = append(rows, sessionRow(model.RunsCollection, id, sessionRun(id, "root")), sessionRelation("childRuns", link))
			}
			f.saveChunks("seed", rows)
			r, err := f.snapshot("local", "session")
			if count > model.MaxSessionWorkflowRuns {
				requireSessionCode(t, err, "resource_exhausted")
				if len(r.Result) != 0 || r.Receipt != nil {
					t.Fatal("Run overflow returned a partial graph or receipt")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r.Receipt != nil || len(sessionWire(r)) > api.MaxNamedResponseBytes {
				t.Fatal("read minted a receipt or exceeded the complete wire limit")
			}
			var snapshot model.SessionWorkflowLogSnapshot
			if err = json.Unmarshal(r.Result, &snapshot); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Runs) != count {
				t.Fatalf("truncated Runs: %d", len(snapshot.Runs))
			}
		})
	}
}

func TestSessionWorkflowSnapshot16384DependentFactsAndOverflow(t *testing.T) {
	for _, count := range []int{model.MaxSessionWorkflowSources - 3, model.MaxSessionWorkflowSources - 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := openSessionFixture(t, nil)
			rows := sessionBaseRows()[:4]
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("attempt-%05d", i)
				rows = append(rows, sessionRow(model.AttemptsCollection, id, map[string]any{"id": id, "runId": "root", "invocationId": "invocation", "phase": "execution", "number": i + 1, "status": "succeeded", "revision": 1}))
			}
			f.saveChunks("seed", rows)
			r, err := f.snapshot("local", "session")
			if count > model.MaxSessionWorkflowSources-3 {
				requireSessionCode(t, err, "resource_exhausted")
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var snapshot model.SessionWorkflowLogSnapshot
			_ = json.Unmarshal(r.Result, &snapshot)
			if len(snapshot.Runs) != 1 || len(snapshot.Runs[0].Attempts) != count {
				t.Fatal("truncated attempt history")
			}
		})
	}
}

func TestSessionWorkflowSnapshotConsumesFlatGroupCommitAuthority(t *testing.T) {
	f := openSessionFixture(t, nil)
	f.save("seed", sessionBaseRows())
	link := sessionLink("link", "prepared", false, false)
	link["ordinal"] = 0
	group := map[string]any{"id": "group", "targetId": "local", "rootRunId": "root", "parentRunId": "root", "producerInvocationId": "invocation", "producerNodeId": "call", "groupKey": "fanout", "expectedMembers": 1, "memberCount": 1, "state": "sealed", "revision": 1, "resolution": "PRIVATE_SENTINEL"}
	member := map[string]any{"id": "member", "groupId": "group", "childRunId": "prepared", "ordinal": 0, "itemKey": "item", "state": "queued", "revision": 1, "leaseToken": "PRIVATE_SENTINEL"}
	f.save("group-commit", []api.Mutation{sessionRelation("childRuns", link), sessionRelation("childRunGroups", group), sessionRelation("childRunGroupMembers", member)})
	read, err := f.snapshot("local", "session")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(read.Result), "PRIVATE_SENTINEL") {
		t.Fatal("sealed group private bytes leaked")
	}
	var snapshot model.SessionWorkflowLogSnapshot
	_ = json.Unmarshal(read.Result, &snapshot)
	if len(snapshot.Runs) != 1 {
		t.Fatal("prepared group fabricated local Run")
	}
	var rel struct {
		Groups  []json.RawMessage `json:"childRunGroups"`
		Members []json.RawMessage `json:"childRunGroupMembers"`
		Links   []json.RawMessage `json:"childRuns"`
	}
	_ = json.Unmarshal(snapshot.Runs[0].Relations, &rel)
	if len(rel.Groups) != 1 || len(rel.Members) != 1 || len(rel.Links) != 1 {
		t.Fatalf("flat authority unavailable %s", snapshot.Runs[0].Relations)
	}
	// The record key is the physical identity. A forged body cannot redefine it.
	group["id"] = "wrong-group"
	group["groupKey"] = "other-fanout"
	f.save("corrupt-group", []api.Mutation{sessionRow(model.ChildGroupsCollection, "corrupt-group", group)})
	_, err = f.snapshot("local", "session")
	requireSessionCode(t, err, "data_loss")
}

func TestSessionWorkflowSnapshotMaterializationBytesFailWithoutPartialGraph(t *testing.T) {
	f := openSessionFixture(t, nil)
	f.save("seed", sessionBaseRows())
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("large-child-%d", i)
		run := sessionRun(id, "root")
		run["reason"] = strings.Repeat("x", 2<<20)
		f.save(fmt.Sprintf("add-%d", i), []api.Mutation{sessionRow(model.RunsCollection, id, run), sessionRelation("childRuns", sessionLink("link-"+id, id, true, false))})
	}
	r, err := f.snapshot("local", "session")
	requireSessionCode(t, err, "resource_exhausted")
	if len(r.Result) != 0 || r.Receipt != nil {
		t.Fatal("overflow returned a partial graph")
	}
}

func TestSessionWorkflowSnapshotOriginUniquenessUsesWriterIndex(t *testing.T) {
	f := openSessionFixture(t, nil)
	f.save("seed", sessionBaseRows())
	a, b := sessionJobRow(), sessionJobRow()
	b.Key = "another-job"
	var r model.WorkflowJobRecord
	_ = json.Unmarshal(b.Data, &r)
	var job map[string]any
	_ = json.Unmarshal(r.Job, &job)
	job["ID"] = b.Key
	r.Job = sessionWire(job)
	b.Data = sessionWire(r)
	request := api.NamedRequest{Scope: f.scope, DatabaseID: f.token.DatabaseID, Schema: f.token.Schema, Module: coredata.Spec().ID, Operation: model.ExecutionCommitOperation, RequestID: "duplicate-origin", Payload: sessionWire(model.ExecutionCommit{State: []api.Mutation{a, b}})}
	_, err := f.store.Named(f.ctx, request)
	requireSessionCode(t, err, "conflict")
	_, err = f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: request.RequestID})
	requireSessionCode(t, err, "not_found")
	read, err := f.snapshot("local", "session")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot model.SessionWorkflowLogSnapshot
	_ = json.Unmarshal(read.Result, &snapshot)
	if len(snapshot.Jobs) != 0 {
		t.Fatal("duplicate-origin write left a partial Job")
	}
}
