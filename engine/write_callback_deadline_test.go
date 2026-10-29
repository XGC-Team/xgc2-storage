package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

type heldCallbackConnection struct {
	*settlementConnection
	prefix, point   string
	entered, resume chan struct{}
	once            sync.Once
}

func (c *heldCallbackConnection) hold() { c.once.Do(func() { close(c.entered); <-c.resume }) }
func (c *heldCallbackConnection) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.settlementConnection.ExecContext(ctx, q, args)
	if c.point == "exec" && strings.HasPrefix(q, c.prefix) {
		c.hold()
	}
	return result, err
}
func (c *heldCallbackConnection) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.settlementConnection.QueryContext(ctx, q, args)
	if err == nil && c.point == "row" && strings.HasPrefix(q, c.prefix) {
		return &heldCallbackRows{Rows: rows, owner: c}, nil
	}
	return rows, err
}

type heldCallbackRows struct {
	driver.Rows
	owner *heldCallbackConnection
}

func (r *heldCallbackRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	r.owner.hold()
	return err
}

// GateCallbackSQLForModuleTest holds one actual SQLite callback statement or
// row return; its exact physical owner remains the existing Store writer.
func GateCallbackSQLForModuleTest(t *testing.T, s *Store, prefix, point string) (<-chan struct{}, func()) {
	t.Helper()
	return gateCallbackSQL(t, s, prefix, point, &settlementPlan{})
}

func GateCallbackSQLWithFailedCloseForModuleTest(t *testing.T, s *Store, prefix, point string) (<-chan struct{}, func()) {
	t.Helper()
	plan := &settlementPlan{}
	plan.rollback.Store(true)
	plan.closeFailures.Store(1)
	return gateCallbackSQL(t, s, prefix, point, plan)
}

func gateCallbackSQL(t *testing.T, s *Store, prefix, point string, plan *settlementPlan) (<-chan struct{}, func()) {
	t.Helper()
	installSettlementDriver(t, s, plan)
	_, release, err := s.admit(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	base, ok := s.physicalWriter.Conn.(*settlementConnection)
	if !ok {
		release()
		t.Fatal("unexpected writer driver")
	}
	held := &heldCallbackConnection{settlementConnection: base, prefix: prefix, point: point, entered: make(chan struct{}), resume: make(chan struct{})}
	s.physicalWriter.Conn = held
	release()
	var once sync.Once
	allow := func() { once.Do(func() { close(held.resume) }) }
	t.Cleanup(allow)
	return held.entered, allow
}

func TestHeldBatchCallbackDeadlineRetainsPhysicalAttempt(t *testing.T) {
	for _, point := range []string{"exec", "row"} {
		t.Run(point, func(t *testing.T) {
			s, cfg, base := setup(t)
			before := snapshot(t, s, base, "a")
			prefix := "INSERT INTO scopes VALUES("
			if point == "row" {
				prefix = "SELECT revision,receipts FROM scopes"
			}
			entered, release := GateCallbackSQLForModuleTest(t, s, prefix, point)
			request := api.BatchRequest{Scope: testScope, Expected: before.Token, RequestID: "held-batch-" + point, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"original"}`)}}
			ctx, cancel := context.WithTimeout(base, 100*time.Millisecond)
			defer cancel()
			type answer struct {
				receipt api.Receipt
				err     error
			}
			done := make(chan answer, 1)
			go func() { receipt, err := s.Batch(ctx, request); done <- answer{receipt, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("actual callback SQL did not enter")
			}
			var got answer
			select {
			case got = <-done:
			case <-time.After(time.Second):
				t.Fatal("Batch blocked beyond caller deadline")
			}
			var outcome *WriteError
			if !errors.As(got.err, &outcome) || outcome.Disposition != WriteUnknown || outcome.Settled || outcome.MayHaveCommitted || !outcome.CallbackPending || got.receipt.RequestID != "" || len(got.receipt.Versions) != 0 {
				t.Fatalf("unjoined callback exposed a result: %+v error=%v", got.receipt, got.err)
			}
			// The prepared Batch owns the accepted bytes, even after the caller returns.
			for i := range request.Mutations[0].Data {
				request.Mutations[0].Data[i] = 'x'
			}
			request.Mutations[0].Key = "caller-changed"
			if err := s.Write(base, Durable, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
				t.Fatalf("new writer entered: %v", err)
			}
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			readCtx, readCancel := context.WithTimeout(base, 50*time.Millisecond)
			defer readCancel()
			readDone := make(chan error, 1)
			go func() { readDone <- s.Read(readCtx, func(context.Context, *sql.Tx) error { return nil }) }()
			select {
			case err := <-readDone:
				if code(err) != "unavailable" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("read=%v", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("read blocked behind closing writer")
			}
			cfg.Create = false
			if other, err := Open(base, cfg); other != nil || code(err) != "conflict" {
				if other != nil {
					other.Close()
				}
				t.Fatalf("physical owner released early: %v", err)
			}
			release()
			if err := outcome.WaitSettlement(base); err != nil {
				t.Fatalf("exact callback rollback: %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatalf("Close after join: %v", err)
			}
			reopened, err := Open(base, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, err := reopened.Receipt(base, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); code(err) != "not_found" {
				t.Fatalf("canceled receipt exists: %v", err)
			}
			after := snapshot(t, reopened, base, "a")
			if len(after.Results) != 1 || len(after.Results[0].Records) != 1 || !after.Results[0].Records[0].Missing {
				t.Fatalf("canceled data committed: %+v", after)
			}
			stats, err := reopened.Stats()
			if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 {
				t.Fatalf("joined owner not idle: %+v %v", stats, err)
			}
			t.Logf("%s: deadline returned one owned callback, rollback joined; reopened readers/writers=%d/%d", point, stats.ReadersActive, stats.WritersQueued)
		})
	}
}

func TestHeldCallbackFailedRollbackRetainsOwnerForLaterClose(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	plan.rollback.Store(true)
	plan.closeFailures.Store(2)
	plan.closeEntered, plan.continueClose = make(chan struct{}), make(chan struct{})
	close(plan.continueClose)
	entered, release := gateCallbackSQL(t, s, "INSERT INTO scopes VALUES(", "exec", plan)
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			return insertFaultRow(ctx, tx, "held-rollback-failure")
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback SQL did not enter")
	}
	err := <-done
	var outcome *WriteError
	if !errors.As(err, &outcome) || !outcome.CallbackPending || outcome.Settled || outcome.MayHaveCommitted {
		t.Fatalf("unjoined callback disposition: %v", err)
	}
	release()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("failed rollback did not enter physical close")
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := outcome.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed rollback/close signaled completion: %v", err)
	}
	waitCancel()
	if err := s.Close(); !errors.Is(err, errSettlementClose) {
		t.Fatalf("first explicit close lost physical failure: %v", err)
	}
	cfg.Create = false
	if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed close released exact writer: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("later explicit join: %v", err)
	}
	if err := outcome.WaitSettlement(ctx); !errors.Is(err, errSettlementRollback) || !errors.Is(err, errSettlementClose) {
		t.Fatalf("later join lost the original physical failures: %v", err)
	}
	if !outcome.NonCommittedAfterJoin() || outcome.Disposition != WriteUnknown || outcome.Settled {
		t.Fatalf("later join did not retain distinct original and joined facts: %+v", outcome)
	}
	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if count := countFaultRow(t, reopened, "held-rollback-failure"); count != 0 {
		t.Fatalf("abandoned callback committed %d rows", count)
	}
}
