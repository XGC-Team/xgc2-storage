package engine

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func synchronous(t *testing.T, s *Store, d Durability) int {
	t.Helper()
	var level int
	if err := s.Write(budget(t), d, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&level)
	}); err != nil {
		t.Fatal(err)
	}
	return level
}

func TestWriterSwitchesSynchronousPerTransaction(t *testing.T) {
	s, _, _ := setup(t)
	// 2 is FULL, 1 is NORMAL: the class of each transaction decides, in any order.
	for i, want := range []struct {
		class Durability
		level int
	}{{Durable, 2}, {Relaxed, 1}, {Relaxed, 1}, {Durable, 2}, {Relaxed, 1}, {Durable, 2}} {
		if got := synchronous(t, s, want.class); got != want.level {
			t.Fatalf("transaction %d class %v ran with synchronous=%d, want %d", i, want.class, got, want.level)
		}
	}
	stats, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.CommitsDurable != 3 || stats.CommitsRelaxed != 3 {
		t.Fatalf("commits by class: durable=%d relaxed=%d", stats.CommitsDurable, stats.CommitsRelaxed)
	}
	// Opening already ran a durable create; every durable commit asks for one sync.
	if stats.Fsyncs < 3 {
		t.Fatalf("durable commits did not account their durability barrier: %d", stats.Fsyncs)
	}
}

func TestFailedTransactionRollsBackAndCountsNoCommit(t *testing.T) {
	s, _, ctx := setup(t)
	before, _ := s.Stats()
	boom := errors.New("injected")
	err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(ctx, "INSERT INTO scopes VALUES('x','test',0,0)"); e != nil {
			return e
		}
		return boom
	})
	if code(err) != "internal" {
		t.Fatalf("unexpected error class %v", err)
	}
	var n int
	if e := s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope='x'").Scan(&n)
	}); e != nil || n != 0 {
		t.Fatalf("failed transaction left a row: %d %v", n, e)
	}
	after, _ := s.Stats()
	if after.CommitsDurable != before.CommitsDurable || after.CommitsRelaxed != before.CommitsRelaxed {
		t.Fatal("a rolled back transaction was counted as a commit")
	}
	// The writer is still usable and still at the last requested level.
	if got := synchronous(t, s, Durable); got != 2 {
		t.Fatalf("writer left at synchronous=%d", got)
	}
}

func TestBatchClassNamesItsReceiptDurability(t *testing.T) {
	s, _, ctx := setup(t)
	read := snapshot(t, s, ctx, "a")
	durable, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "durable", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a"}`)}})
	if err != nil || durable.Durability != "sqlite-full" {
		t.Fatalf("default batch must be durable: %+v %v", durable, err)
	}
	relaxed, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: durable.Token, RequestID: "relaxed", Relaxed: true, Mutations: []api.Mutation{mutation("state", "b", "0", `{"name":"b"}`)}})
	if err != nil || relaxed.Durability != "sqlite-normal" {
		t.Fatalf("relaxed batch: %+v %v", relaxed, err)
	}
	replay, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "relaxed"})
	if err != nil || replay.Durability != "sqlite-normal" {
		t.Fatalf("stored receipt lost the class: %+v %v", replay, err)
	}
	stats, _ := s.Stats()
	if stats.CommitsRelaxed != 1 {
		t.Fatalf("relaxed commits: %d", stats.CommitsRelaxed)
	}
}
