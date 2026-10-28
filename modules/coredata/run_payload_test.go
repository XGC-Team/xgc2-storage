package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunPayloadRoundTripAndStoredQuota(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	inputs := json.RawMessage(`{ "integer":9007199254740993, "configuration":"` + strings.Repeat("shared robot configuration ", 10000) + `" }`)
	nodes := json.RawMessage(`[{"id":"mapped","output":"` + strings.Repeat("shared robot configuration ", 15000) + `"}]`)
	q := model.NewRun{ID: "packed", TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: strings.Repeat("a", 64), ActionID: "run", Inputs: inputs, Trigger: json.RawMessage(`{ "manual":true }`)}
	if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	want := model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Termination: "completed", Nodes: nodes, Result: json.RawMessage(`9007199254740993`)}
	got, err := finishRun(ctx, tx, testScope, want)
	if err != nil || !bytes.Equal(got.Inputs, inputs) || !bytes.Equal(got.Nodes, nodes) || !bytes.Equal(got.Trigger, q.Trigger) || !bytes.Equal(got.Result, want.Result) {
		t.Fatalf("lossless Run round trip failed: %v", err)
	}
	var rowBytes, quotaBytes int64
	var encodedInputs, encodedNodes []byte
	if err = tx.QueryRowContext(ctx, "SELECT bytes,inputs_json,nodes_json FROM runs WHERE id=?", q.ID).Scan(&rowBytes, &encodedInputs, &encodedNodes); err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&quotaBytes); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(encodedInputs, inputs) || bytes.Equal(encodedNodes, nodes) || rowBytes != quotaBytes || rowBytes > int64(len(inputs)+len(nodes))/5 {
		t.Fatalf("stored representation/quota: row=%d scope=%d", rowBytes, quotaBytes)
	}
	pruned, err := pruneRuns(ctx, tx, testScope, model.RunRetention{OlderThan: got.FinishedAt.Add(1)})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("prune: %+v %v", pruned, err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&quotaBytes); err != nil || quotaBytes != 0 {
		t.Fatalf("prune did not release stored bytes: %d %v", quotaBytes, err)
	}
}

func TestRunObservationDoesNotReadOrDecompressExecutionPayloads(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	trigger := json.RawMessage(`{"kind":"session-action","instanceId":"native","presetId":"start","n":9007199254740993}`)
	request := model.NewRun{ID: "observed", TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: strings.Repeat("d", 64), ActionID: "run", Inputs: json.RawMessage(`{"large":"` + strings.Repeat("input", 100000) + `"}`), Trigger: trigger}
	if _, _, err = createRun(ctx, tx, testScope, request); err != nil {
		t.Fatal(err)
	}
	finish := model.RunFinish{ID: request.ID, Status: model.RunSucceeded, Result: json.RawMessage(`{"large":"` + strings.Repeat("result", 100000) + `"}`), Nodes: json.RawMessage(`[{"output":"` + strings.Repeat("node", 100000) + `"}]`), Error: json.RawMessage(`{"code":"observed"}`), CleanupErrors: json.RawMessage(`[{"code":"cleanup"}]`)}
	full, err := finishRun(ctx, tx, testScope, finish)
	if err != nil || !full.HasResult {
		t.Fatalf("finish result: %+v %v", full, err)
	}
	// Corrupt all three execution payloads. An observation must still read the
	// exact metadata, proving these blobs never enter the decompression path.
	if _, err = tx.ExecContext(ctx, "UPDATE runs SET inputs_json=x'01',result_json=x'02',nodes_json=x'03' WHERE id=?", request.ID); err != nil {
		t.Fatal(err)
	}
	page, err := listRuns(ctx, tx, testScope, model.RunFilter{Projection: model.RunObservation})
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("observation: %+v %v", page, err)
	}
	got := page.Runs[0]
	if len(got.Inputs) != 0 || len(got.Result) != 0 || len(got.Nodes) != 0 || !got.HasResult || !bytes.Equal(got.Trigger, trigger) || !bytes.Equal(got.Error, finish.Error) || !bytes.Equal(got.CleanupErrors, finish.CleanupErrors) {
		t.Fatalf("observation lost metadata or read payloads: %+v", got)
	}
	identity, err := listRuns(ctx, tx, testScope, model.RunFilter{Projection: model.RunIdentity})
	if err != nil || len(identity.Runs) != 1 || len(identity.Runs[0].Trigger) != 0 || len(identity.Runs[0].Error) != 0 || len(identity.Runs[0].CleanupErrors) != 0 || !identity.Runs[0].HasResult {
		t.Fatalf("identity: %+v %v", identity, err)
	}
	if _, err = selectRun(ctx, tx, testScope, "id=?", request.ID); err == nil {
		t.Fatal("full detail concealed corrupt payloads")
	}
	if _, err = listRuns(ctx, tx, testScope, model.RunFilter{Projection: "unknown"}); err == nil {
		t.Fatal("unknown projection accepted")
	}
}

func TestRunPayloadRejectsCorruptionAndDecodedOverflow(t *testing.T) {
	raw := []byte(`{"large":"` + strings.Repeat("x", 1000) + `"}`)
	packed, err := encodeRunPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string][]byte{
		"plaintext is not schema 3": raw,
		"missing checksum":          packed[:len(packed)-1],
		"trailing data":             append(append([]byte{}, packed...), 0),
		"decoded overflow":          packed,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeRunPayload(value, 100); err == nil {
				t.Fatal("invalid payload accepted")
			}
		})
	}
	if _, err = decodeRunPayload(packed[:len(packed)-1], len(raw)); err == nil {
		t.Fatal("checksum truncation accepted")
	}
	if _, err = decodeRunPayload(append(append([]byte{}, packed...), 0), len(raw)); err == nil {
		t.Fatal("trailing bytes accepted")
	}
}

func assertRunUsage(t *testing.T, ctx context.Context, tx *sql.Tx, scope string) {
	t.Helper()
	var stored, quota int64
	if err := tx.QueryRowContext(ctx, "SELECT sum(bytes) FROM runs WHERE scope=?", scope).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", scope).Scan(&quota); err != nil || quota != stored || quota >= MaxScopeBytes {
		t.Fatalf("migration quota stored=%d charged=%d error=%v", stored, quota, err)
	}
}
