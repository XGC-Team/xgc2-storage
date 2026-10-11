//go:build linux

package faults_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
)

func TestFaultWALPressureCheckpointWriteRecovery(t *testing.T) {
	s := open(t, filepath.Join(privateDir(t), "fixture.db"), true, func(c *engine.Config) {
		c.MaxWALBytes = 1 << 20
	})
	initial := read(t, s)
	payload, _ := json.Marshal(map[string]string{"operation": "wal-pressure", "padding": strings.Repeat("x", 2<<20)})
	req := api.BatchRequest{Scope: scope, Expected: initial.Token, RequestID: "wal-large-commit",
		Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: "0", Data: payload}}}
	commit, err := s.Batch(deadline(t), req)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.Stats()
	if err != nil || before.WALBytes < 1<<20 {
		t.Fatalf("fixture did not create actual WAL pressure: %+v %v", before, err)
	}
	checkpoint, err := s.Checkpoint(deadline(t))
	if err != nil || checkpoint.Busy != 0 || checkpoint.LogPages != checkpoint.CheckpointedPages {
		t.Fatalf("checkpoint did not complete: %+v %v", checkpoint, err)
	}
	after, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Batch(deadline(t), req)
	if err != nil || !reflect.DeepEqual(replay, commit) {
		t.Fatalf("WAL pressure lost retained outcome: %+v %v", replay, err)
	}
	small := api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "wal-recovered-write",
		Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: commit.Token.Revision,
			Data: json.RawMessage(`{"operation":"wal-pressure","phase":"resumed"}`)}}}
	recovered, err := s.Batch(deadline(t), small)
	evidence(t, map[string]any{"wal_limit_bytes": 1 << 20, "before_checkpoint": before, "checkpoint": checkpoint,
		"after_checkpoint": after, "retained_replay_unchanged": true, "small_write_error": code(err), "small_write_receipt": recovered})
	if err != nil || recovered.Token.Revision != "2" {
		t.Fatalf("completed checkpoint leaves new small write locked out: WAL=%d receipt=%+v err=%v", after.WALBytes, recovered, err)
	}
	if err = s.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestFaultWALReaderPinnedCheckpointRecovery(t *testing.T) {
	path := filepath.Join(privateDir(t), "fixture.db")
	s := open(t, path, true, func(c *engine.Config) { c.MaxWALBytes = 1 << 20 })
	seed := api.BatchRequest{Scope: scope, Expected: read(t, s).Token, RequestID: "wal-reader-seed",
		Mutations: []api.Mutation{{Collection: "state", Key: "seed", ExpectedVersion: "0", Data: capacityDocument("reader-seed", 32)}}}
	first, err := s.Batch(deadline(t), seed)
	if err != nil {
		t.Fatal(err)
	}
	// This second connection is readonly and points only at this test's newly
	// created private database. A real SELECT pins the old SQLite WAL end mark.
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(0)"}).String()
	reader, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	tx, err := reader.BeginTx(deadline(t), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRowContext(deadline(t), "SELECT count(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("private readonly WAL pin was not established: count=%d err=%v", count, err)
	}
	large := api.BatchRequest{Scope: scope, Expected: first.Token, RequestID: "wal-reader-large",
		Mutations: []api.Mutation{{Collection: "state", Key: "large", ExpectedVersion: "0", Data: capacityDocument("reader-large", 2<<20)}}}
	commit, err := s.Batch(deadline(t), large)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.Stats()
	if err != nil || before.WALBytes < 1<<20 {
		t.Fatalf("reader fixture did not create WAL pressure: %+v %v", before, err)
	}
	budget, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	started := time.Now()
	pinned, err := s.Checkpoint(budget)
	elapsed := time.Since(started)
	cancel()
	if err != nil || pinned.Busy != 1 || elapsed >= 500*time.Millisecond {
		t.Fatalf("reader-pinned checkpoint waited or lost Busy result: %+v elapsed=%s err=%v", pinned, elapsed, err)
	}
	underPressure, err := idleStats(t, s)
	if err != nil || underPressure.WALBytes < 1<<20 || underPressure.WritersQueued != 0 || underPressure.ReadersActive != 0 {
		t.Fatalf("reader-pinned checkpoint changed pressure or leaked admission: %+v %v", underPressure, err)
	}
	replay, err := s.Batch(deadline(t), large)
	if err != nil || !reflect.DeepEqual(replay, commit) {
		t.Fatalf("reader-pinned WAL pressure lost retained outcome: %v", err)
	}
	small := api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "wal-reader-recovered-write",
		Mutations: []api.Mutation{{Collection: "state", Key: "small", ExpectedVersion: "0", Data: capacityDocument("reader-small", 32)}}}
	if _, err = s.Batch(deadline(t), small); code(err) != "resource_exhausted" {
		t.Fatalf("pinned physical WAL did not retain finite write admission: %v", err)
	}
	if _, err = s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: small.RequestID}); code(err) != "not_found" {
		t.Fatalf("pinned rejected write published a receipt: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	released, err := s.Checkpoint(deadline(t))
	if err != nil || released.Busy != 0 {
		t.Fatalf("released reader did not permit checkpoint: %+v %v", released, err)
	}
	after, err := s.Stats()
	if err != nil || after.WALBytes >= 1<<20 {
		t.Fatalf("released reader did not reclaim physical WAL: %+v %v", after, err)
	}
	recovered, err := s.Batch(deadline(t), small)
	if err != nil || recovered.Token.Revision != "3" {
		t.Fatalf("released reader did not recover the unchanged write request: %+v %v", recovered, err)
	}
	final, err := idleStats(t, s)
	if err != nil || final.WritersQueued != 0 || final.ReadersActive != 0 {
		t.Fatalf("reader recovery leaked engine admission: %+v %v", final, err)
	}
	if err = s.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	values, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "state", Keys: []string{"seed", "large", "small"}}}})
	if err != nil || values.Token != recovered.Token || len(values.Results) != 1 || len(values.Results[0].Records) != 3 {
		t.Fatalf("WAL reclamation lost business snapshot: %v", err)
	}
	for i, mutation := range []api.Mutation{seed.Mutations[0], large.Mutations[0], small.Mutations[0]} {
		record := values.Results[0].Records[i]
		if record.Key != mutation.Key || record.Missing || record.Deleted || !bytes.Equal(record.Data, mutation.Data) {
			t.Fatalf("WAL reclamation changed business body %s", mutation.Key)
		}
	}
	evidence(t, map[string]any{"before_checkpoint": before, "pinned_checkpoint": pinned,
		"pinned_elapsed_ns": elapsed.Nanoseconds(), "pinned_pressure": underPressure,
		"retained_replay_unchanged": true, "rejected_write_receipt_absent": true,
		"released_checkpoint": released, "released_pressure": after,
		"same_small_write_recovered": true, "seed_large_and_small_bodies_preserved_exactly": true, "final_admission": final})
}
