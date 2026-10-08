//go:build linux

package faults_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func equalJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	decode := func(raw []byte) any {
		if !json.Valid(raw) {
			t.Fatalf("invalid recovered JSON: %s", raw)
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var value any
		if err := d.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	return reflect.DeepEqual(decode(a), decode(b))
}

func faultPreparationStates() (model.RunPrepareState, model.ProducerPrepareState) {
	return model.RunPrepareState{ID: "parent", TargetID: "fault-target", RootRunID: "parent",
			ExecutionModel: model.OccurrenceExecutionModel, Status: "running"},
		model.ProducerPrepareState{ID: "invocation", RunID: "parent", NodeID: "producer", Kind: "call",
			Status: "running", ChildRunProducer: true}
}

func faultPreparationBody(producer bool) json.RawMessage {
	parentState, producerState := faultPreparationStates()
	var state any = parentState
	if producer {
		state = producerState
	}
	raw, _ := json.Marshal(state)
	return raw
}

func faultGroupCondition(group *model.GroupPrepare) {
	parent, producer := faultPreparationStates()
	group.Condition = model.GroupPrepareCondition{Parent: parent, Producer: producer,
		Ancestors: []model.RecordGuard{group.ParentGuard}}
}

func coreConfig(path string, create bool) engine.Config {
	m := manifest()
	for _, id := range []string{"runs", "invocations", "definitions"} {
		m.Namespaces[0].Collections = append(m.Namespaces[0].Collections, api.Collection{ID: id,
			MaxRecordBytes: 1 << 20, MaxRecords: 2048, MaxBytes: 32 << 20, Retention: "fixture-owner", Recovery: "consistent-backup"})
	}
	spec := coredata.Spec()
	m.Namespaces[0].Modules = []api.Module{spec}
	return engine.Config{Path: path, Create: create, Manifest: m, Readers: 2, WriterQueue: 4,
		MaxCallTime: 10 * time.Second, MaxDBBytes: 32 << 20, MaxWALBytes: 16 << 20,
		Modules: []engine.DataModule{{Spec: spec, Initialize: coredata.Initialize, Execute: coredata.Execute}}}
}

func openCore(t *testing.T, path string, create bool) *engine.Store {
	t.Helper()
	s, err := engine.Open(deadline(t), coreConfig(path, create))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestFaultNamedGroupBusinessBackupRestore(t *testing.T) {
	s := openCore(t, filepath.Join(privateDir(t), "fixture.db"), true)
	initial, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "runs", Keys: []string{"parent"}}}})
	if err != nil {
		t.Fatal(err)
	}
	seed, err := s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: initial.Token, RequestID: "group-guards",
		Mutations: []api.Mutation{
			{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: faultPreparationBody(false)},
			{Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: faultPreparationBody(true)},
			{Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"immutable":true}`)},
		}})
	if err != nil {
		t.Fatal(err)
	}
	group := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "members",
		Body:            json.RawMessage(`{"policy":"all","counter":9223372036854775806}`),
		ParentGuard:     model.RecordGuard{Collection: "runs", Key: "parent", Version: seed.Token.Revision},
		InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: seed.Token.Revision},
		PinGuard:        model.RecordGuard{Collection: "definitions", Key: "pin", Version: seed.Token.Revision}}
	faultGroupCondition(&group)
	parameterBytes := 0
	for i := 0; i < 1000; i++ {
		parameters, _ := json.Marshal(map[string]any{"padding": strings.Repeat("x", 8192), "number": i})
		event, _ := json.Marshal(map[string]any{"sequence": i, "type": "child-prepared"})
		link, _ := json.Marshal(map[string]any{"ordinal": i, "parent": "parent"})
		body, _ := json.Marshal(map[string]any{"child": fmt.Sprintf("child-%04d", i), "phase": "prepared"})
		group.Members = append(group.Members, model.GroupMember{ItemKey: fmt.Sprintf("item-%04d", i),
			ChildID: fmt.Sprintf("child-%04d", i), EventID: fmt.Sprintf("event-%04d", i),
			Parameters: parameters, Event: event, Link: link, Body: body})
		parameterBytes += len(parameters)
	}
	if parameterBytes > coredata.MaxParameterBytes || parameterBytes < 8_000_000 {
		t.Fatalf("large named business fixture is outside intended bounds: %d", parameterBytes)
	}
	payload, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	req := api.NamedRequest{Scope: scope, DatabaseID: initial.Token.DatabaseID, Schema: initial.Token.Schema,
		Module: coredata.Spec().ID, Operation: "group.prepare", RequestID: "prepare-group", Payload: payload}
	prepared, err := s.Named(deadline(t), req)
	if err != nil || prepared.Receipt == nil || prepared.Receipt.Token.Revision != "2" {
		t.Fatalf("named prepare did not commit: %+v %v", prepared, err)
	}
	var preparedMeta model.GroupPrepared
	if err = json.Unmarshal(prepared.Result, &preparedMeta); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(privateDir(t), "named.db")
	if _, err = s.Backup(deadline(t), backup); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	restored := openCore(t, backup, false)
	if err = restored.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	replayed, err := restored.Named(deadline(t), req)
	if err != nil || !reflect.DeepEqual(replayed, prepared) {
		t.Fatalf("named receipt/result changed after restore: %+v %v", replayed, err)
	}
	serviceReceipt, err := restored.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(serviceReceipt, *prepared.Receipt) {
		t.Fatalf("named durable receipt missing: %+v %v", serviceReceipt, err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
	// Audit the private, closed restore candidate using fixed read-only SQL.
	// Replaying named_results alone would not prove member bodies survived.
	dsn := (&url.URL{Scheme: "file", Path: backup, RawQuery: "mode=ro&_pragma=query_only(1)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	scopeRaw, _ := json.Marshal(scope)
	var members, sealed int
	var parent, invocation, groupKey, membershipDigest string
	var groupBody []byte
	if err = db.QueryRowContext(deadline(t), "SELECT member_count,sealed,body,parent_id,invocation_id,group_key,membership_digest FROM core_groups WHERE scope=? AND id=?", string(scopeRaw), group.ID).Scan(&members, &sealed, &groupBody, &parent, &invocation, &groupKey, &membershipDigest); err != nil || members != 1000 || sealed != 1 || !equalJSON(t, groupBody, group.Body) || parent != group.ParentID || invocation != group.InvocationID || groupKey != group.GroupKey || membershipDigest != preparedMeta.MembershipDigest {
		t.Fatalf("restored group header mismatch: members=%d sealed=%d body=%s err=%v", members, sealed, groupBody, err)
	}
	rows, err := db.QueryContext(deadline(t), "SELECT ordinal,item_key,child_id,event_id,parameters,event_body,link_body,member_body FROM core_group_members WHERE scope=? AND group_id=? ORDER BY ordinal", string(scopeRaw), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ordinal int
		var item, child, eventID string
		var parameters, event, link, body []byte
		if err = rows.Scan(&ordinal, &item, &child, &eventID, &parameters, &event, &link, &body); err != nil {
			t.Fatal(err)
		}
		if count >= len(group.Members) {
			t.Fatal("restore duplicated named members")
		}
		want := group.Members[count]
		if ordinal != count || item != want.ItemKey || child != want.ChildID || eventID != want.EventID ||
			string(parameters) != string(want.Parameters) || string(event) != string(want.Event) ||
			string(link) != string(want.Link) || string(body) != string(want.Body) {
			t.Fatalf("restored member %d lost fields", count)
		}
		count++
	}
	if err = rows.Err(); err != nil || count != 1000 {
		t.Fatalf("restored group membership incomplete: %d %v", count, err)
	}
	evidence(t, map[string]any{"operation": "group.prepare", "members": count, "parameter_bytes": parameterBytes,
		"receipt_result_and_all_member_bodies_restored": true, "schema_digest": coredata.Spec().Digest,
		"restore_activation_cli_tested": false, "native_daemon_named_wiring_tested": false})
}

func TestFaultKillUncommittedNamedGroupAtomicity(t *testing.T) {
	dir := privateDir(t)
	path := filepath.Join(dir, "fixture.db")
	s := openCore(t, path, true)
	seed, err := s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: read(t, s).Token, RequestID: "uncommitted-guards",
		Mutations: []api.Mutation{
			{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: faultPreparationBody(false)},
			{Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: faultPreparationBody(true)},
			{Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"immutable":true}`)},
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(deadline(t), os.Args[0], "-test.run=^TestFaultChild$", "-test.v")
	cmd.Env = append(os.Environ(), "FAULT_CHILD=group-uncommitted", "FAULT_CHILD_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	reached := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "UNCOMMITTED_GROUP" {
				reached <- true
				return
			}
		}
		reached <- false
	}()
	select {
	case ok := <-reached:
		if !ok {
			_ = cmd.Wait()
			waited = true
			t.Fatalf("named child did not reach its real SQL transaction: %s", stderr.String())
		}
	case <-time.After(8 * time.Second):
		t.Fatal("named child did not reach held uncommitted transaction")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waited = true
	recovered := openCore(t, path, false)
	if read(t, recovered).Token != seed.Token {
		t.Fatal("uncommitted named operation changed the durable scope revision")
	}
	if _, err = recovered.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: "uncommitted-group"}); code(err) != "not_found" {
		t.Fatalf("crashed uncommitted group published receipt: %v", err)
	}
	if err = recovered.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = recovered.Close(); err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&_pragma=query_only(1)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{"SELECT COUNT(*) FROM core_groups", "SELECT COUNT(*) FROM core_group_members", "SELECT COUNT(*) FROM named_results", "SELECT COUNT(*) FROM core_data_usage"} {
		var count int
		if err = db.QueryRowContext(deadline(t), query).Scan(&count); err != nil || count != 0 {
			t.Fatalf("SIGKILL left a partial named transaction: %s count=%d err=%v", query, count, err)
		}
	}
	evidence(t, map[string]any{"boundary": "after real coredata SQL before engine COMMIT", "signal": "SIGKILL",
		"group_members_receipt_and_usage_rolled_back": true, "revision": seed.Token.Revision,
		"test_only_module_wrapper": true})
}

func uncommittedGroupChild(t *testing.T, dir string) {
	config := coreConfig(filepath.Join(dir, "fixture.db"), false)
	realExecute := config.Modules[0].Execute
	config.Modules[0].Execute = func(ctx context.Context, tx *sql.Tx, scopeID, operation string, payload json.RawMessage) (json.RawMessage, error) {
		result, err := realExecute(ctx, tx, scopeID, operation, payload)
		if err != nil {
			return result, err
		}
		fmt.Println("UNCOMMITTED_GROUP")
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s, err := engine.Open(deadline(t), config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	token := read(t, s).Token
	group := model.GroupPrepare{ID: "group", ParentID: "parent", InvocationID: "invocation", GroupKey: "members", Body: json.RawMessage(`{"policy":"all"}`),
		ParentGuard:     model.RecordGuard{Collection: "runs", Key: "parent", Version: token.Revision},
		InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: token.Revision},
		PinGuard:        model.RecordGuard{Collection: "definitions", Key: "pin", Version: token.Revision}}
	faultGroupCondition(&group)
	for i := 0; i < 32; i++ {
		parameters, _ := json.Marshal(map[string]string{"payload": strings.Repeat("x", 8192)})
		group.Members = append(group.Members, model.GroupMember{ItemKey: fmt.Sprint("item-", i), ChildID: fmt.Sprint("child-", i), EventID: fmt.Sprint("event-", i),
			Parameters: parameters, Event: json.RawMessage(`{"type":"prepared"}`), Link: json.RawMessage(`{"parent":"parent"}`), Body: json.RawMessage(`{"phase":"prepared"}`)})
	}
	payload, _ := json.Marshal(group)
	_, err = s.Named(deadline(t), api.NamedRequest{Scope: scope, DatabaseID: token.DatabaseID, Schema: token.Schema,
		Module: coredata.Spec().ID, Operation: "group.prepare", RequestID: "uncommitted-group", Payload: payload})
	t.Fatalf("private held named operation should be killed by parent: %v", err)
}

func TestFaultNativeNamedGroupRestartAndBackupRestore(t *testing.T) {
	dir := privateDir(t)
	m := coreConfig("", false).Manifest
	d := startWithManifest(t, dir, true, "", m)
	initial := nativeRead(t, d)
	seed, err := d.client.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: initial.Token, RequestID: "native-group-guards",
		Mutations: []api.Mutation{
			{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: faultPreparationBody(false)},
			{Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: faultPreparationBody(true)},
			{Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"immutable":true}`)},
		}})
	if err != nil {
		t.Fatal(err)
	}
	group := model.GroupPrepare{ID: "native-group", ParentID: "parent", InvocationID: "invocation", GroupKey: "native-members",
		Body:            json.RawMessage(`{"counter":9223372036854775806,"policy":"all"}`),
		ParentGuard:     model.RecordGuard{Collection: "runs", Key: "parent", Version: seed.Token.Revision},
		InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: seed.Token.Revision},
		PinGuard:        model.RecordGuard{Collection: "definitions", Key: "pin", Version: seed.Token.Revision}}
	faultGroupCondition(&group)
	for i := 0; i < 1000; i++ {
		parameters, _ := json.Marshal(map[string]any{"ordinal": i, "padding": strings.Repeat("x", 8192)})
		event, _ := json.Marshal(map[string]any{"sequence": i, "type": "prepared"})
		body, _ := json.Marshal(map[string]any{"phase": "prepared", "title": fmt.Sprintf("成员%d", i)})
		group.Members = append(group.Members, model.GroupMember{ItemKey: fmt.Sprint("item-", i), ChildID: fmt.Sprint("child-", i), EventID: fmt.Sprint("event-", i),
			Parameters: parameters, Event: event, Link: json.RawMessage(`{"parent":"parent"}`), Body: body})
	}
	payload, _ := json.Marshal(group)
	req := api.NamedRequest{Scope: scope, DatabaseID: initial.Token.DatabaseID, Schema: initial.Token.Schema,
		Module: coredata.Spec().ID, Operation: "group.prepare", RequestID: "native-group-prepare", Payload: payload}
	prepared, err := d.client.Named(deadline(t), req)
	if err != nil || prepared.Receipt == nil || prepared.Receipt.Token.Revision != "2" {
		t.Fatalf("production named 1000-member commit failed: receipt=%+v error=%v", prepared.Receipt, err)
	}
	var preparedMeta model.GroupPrepared
	if err = json.Unmarshal(prepared.Result, &preparedMeta); err != nil {
		t.Fatal(err)
	}
	d.kill(t)
	d = startWithManifest(t, dir, false, "", m)
	verify := func(d *daemon) {
		result, err := d.client.NamedResult(deadline(t), "resolve-native-group", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
		if err != nil || !reflect.DeepEqual(result, prepared) {
			t.Fatalf("native named outcome resolution lost receipt/result: %v", err)
		}
		readPayload, _ := json.Marshal(model.GroupRead{ID: group.ID})
		readReq := req
		readReq.Operation, readReq.RequestID, readReq.Payload = "group.snapshot", "read-native-group", readPayload
		out, err := d.client.Named(deadline(t), readReq)
		if err != nil {
			t.Fatalf("native group materialization failed: %v", err)
		}
		var snapshot model.GroupSnapshot
		if err = json.Unmarshal(out.Result, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.ID != group.ID || snapshot.MemberCount != len(group.Members) || snapshot.MembershipDigest != preparedMeta.MembershipDigest || len(snapshot.Members) != len(group.Members) || !equalJSON(t, snapshot.Body, group.Body) {
			t.Fatal("native group snapshot lost membership/header")
		}
		for i, want := range group.Members {
			got := snapshot.Members[i]
			if got.ItemKey != want.ItemKey || got.ChildID != want.ChildID || got.EventID != want.EventID ||
				!equalJSON(t, got.Parameters, want.Parameters) || !equalJSON(t, got.Event, want.Event) ||
				!equalJSON(t, got.Link, want.Link) || !equalJSON(t, got.Body, want.Body) {
				t.Fatalf("native restored member %d lost business fields", i)
			}
		}
		if nativeRead(t, d).Token != prepared.Receipt.Token {
			t.Fatal("read-only group snapshot changed the durable revision")
		}
	}
	verify(d)
	process := processResources(t, d.cmd.Process.Pid)
	d.kill(t)
	// Administrative backup runs only after the private daemon has exited.
	s := openCore(t, filepath.Join(dir, "fixture.db"), false)
	restoredDir := privateDir(t)
	if _, err = s.Backup(deadline(t), filepath.Join(restoredDir, "fixture.db")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	d = startWithManifest(t, restoredDir, false, "", m)
	verify(d)
	evidence(t, map[string]any{"profile": "http-unix", "operation": "group.prepare", "members": len(group.Members), "payload_bytes": len(payload),
		"production_cli_registry_and_large_materialized_snapshot_tested": true,
		"sigkill_restart_and_backup_candidate_restore":                   true, "all_member_business_fields_verified": true,
		"process_after_materialization": process, "restore_activation_cli_tested": false})
}
