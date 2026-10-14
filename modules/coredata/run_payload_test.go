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
	if err != nil || pruned != 1 {
		t.Fatalf("prune: %d %v", pruned, err)
	}
	if err = tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&quotaBytes); err != nil || quotaBytes != 0 {
		t.Fatalf("prune did not release stored bytes: %d %v", quotaBytes, err)
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

func TestMigrateRunPayloadsPreservesFactsAndRecountsUsage(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	raw := []byte(` {"configuration":"` + strings.Repeat("robot ", 1000) + `","integer":9007199254740993} `)
	for _, scope := range []string{testScope, "second-scope"} {
		q := model.NewRun{ID: "historical", TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: strings.Repeat("a", 64), ActionID: "run", Inputs: raw, Trigger: raw}
		r, _, err := createRun(ctx, tx, scope, q)
		if err != nil {
			t.Fatal(err)
		}
		// Install the exact schema-2 representation, including all JSON columns.
		r.Error, r.Result, r.Nodes, r.CleanupErrors = raw, raw, raw, raw
		if _, err = tx.ExecContext(ctx, "UPDATE runs SET inputs_json=?,trigger_json=?,error_json=?,result_json=?,nodes_json=?,cleanup_errors_json=?,bytes=?,status='failed',finished_at=1,revision=4 WHERE scope=?", raw, raw, raw, raw, raw, raw, runBytes(r), scope); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE core_data_usage SET bytes=?", MaxScopeBytes+1); err != nil {
		t.Fatal(err)
	}
	if err = migrate2(ctx, tx, nil); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{testScope, "second-scope"} {
		r, err := getRun(ctx, tx, scope, "historical")
		if err != nil || r.Revision != 4 || r.Status != model.RunFailed {
			t.Fatalf("history identity changed: %v", err)
		}
		for _, field := range []json.RawMessage{r.Inputs, r.Trigger, r.Error, r.Result, r.Nodes, r.CleanupErrors} {
			if !bytes.Equal(field, raw) {
				t.Fatal("historical JSON bytes changed")
			}
		}
		assertRunUsage(t, ctx, tx, scope)
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
