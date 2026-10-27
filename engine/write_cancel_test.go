package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
)

// The wrapper delegates all SQL to real SQLite. Only BEGIN/ROLLBACK completion
// can be paused, so the test can reproduce ownership handoff deterministically.
type transactionControl struct {
	began                             chan context.Context
	beforeBegin, continueBegin        chan struct{}
	rollbackEntered, continueRollback chan struct{}
	rollbackOnce                      sync.Once
}
type controlledConnector struct{ control *transactionControl }

func (c controlledConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c controlledConnector) Connect(context.Context) (driver.Conn, error) {
	inner, err := c.Driver().Open("file::memory:?_txlock=immediate&_pragma=busy_timeout(0)")
	if err != nil {
		return nil, err
	}
	return &controlledConnection{Conn: inner, control: c.control}, nil
}

type controlledConnection struct {
	driver.Conn
	control *transactionControl
}

func (c *controlledConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	select {
	case c.control.began <- ctx:
	default:
	}
	if c.control.beforeBegin != nil {
		close(c.control.beforeBegin)
		<-c.control.continueBegin
	}
	inner, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return controlledTransaction{Tx: inner, control: c.control}, nil
}
func (c *controlledConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *controlledConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type controlledTransaction struct {
	driver.Tx
	control *transactionControl
}

func (tx controlledTransaction) Rollback() error {
	if tx.control.rollbackEntered != nil {
		tx.control.rollbackOnce.Do(func() { close(tx.control.rollbackEntered) })
		<-tx.control.continueRollback
	}
	return tx.Tx.Rollback()
}

func controlledWriter(t *testing.T, control *transactionControl) *Store {
	t.Helper()
	db := sql.OpenDB(controlledConnector{control: control})
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); db.Close() })
	if _, err := conn.ExecContext(t.Context(), "CREATE TABLE work(id TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	return &Store{writer: conn}
}

func TestCanceledWriterWaitsForRealRollbackBeforeReturning(t *testing.T) {
	control := &transactionControl{began: make(chan context.Context, 1), rollbackEntered: make(chan struct{}), continueRollback: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(control.continueRollback) }) }
	s := controlledWriter(t, control)
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	callback, returnCallback := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.transact(ctx, Relaxed, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO work VALUES('rolled-back')"); err != nil {
				return err
			}
			close(callback)
			<-returnCallback
			return ctx.Err()
		})
	}()
	<-callback
	beginContext := <-control.began
	cancel()
	if beginContext.Done() != nil {
		// A transaction tied to caller cancellation lets database/sql claim
		// rollback first. Hold its real driver call before the callback returns.
		<-control.rollbackEntered
	}
	close(returnCallback)
	<-control.rollbackEntered
	select {
	case err := <-done:
		release()
		t.Fatalf("writer returned before driver rollback completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel result=%v", err)
	}
	if err := s.transact(t.Context(), Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO work VALUES('kept')")
		return err
	}); err != nil {
		t.Fatalf("next durability-class write=%v", err)
	}
	var count int
	if err := s.writer.QueryRowContext(t.Context(), "SELECT count(*) FROM work").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rolled-back data leaked: rows=%d %v", count, err)
	}
	if s.relaxed.Load() != 0 || s.durable.Load() != 1 {
		t.Fatalf("rollback counted as commit: relaxed=%d durable=%d", s.relaxed.Load(), s.durable.Load())
	}
}

func TestCancelBeforeAndDuringBeginNeverEntersBusinessCallback(t *testing.T) {
	control := &transactionControl{began: make(chan context.Context, 1), beforeBegin: make(chan struct{}), continueBegin: make(chan struct{})}
	s := controlledWriter(t, control)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	callback := func(context.Context, *sql.Tx) error { t.Error("canceled BEGIN entered business callback"); return nil }
	if err := s.transact(ctx, Durable, callback); !errors.Is(err, context.Canceled) {
		t.Fatalf("before BEGIN=%v", err)
	}
	select {
	case <-control.began:
		t.Fatal("already canceled request began a transaction")
	default:
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.transact(ctx, Durable, callback) }()
	<-control.beforeBegin
	cancel()
	close(control.continueBegin)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("during BEGIN=%v", err)
	}
	control.beforeBegin = nil
	if err := s.transact(t.Context(), Relaxed, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO work VALUES('after-cancel')")
		return err
	}); err != nil {
		t.Fatalf("BEGIN cancellation left an owned transaction: %v", err)
	}
}

func TestImmediateBeginDoesNotWaitOnACompetingSQLiteWriter(t *testing.T) {
	s, _, ctx := setup(t)
	var timeout int
	if err := s.writer.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 0 {
		t.Fatalf("writer lock wait must be disabled: timeout=%d %v", timeout, err)
	}
	other, err := sql.Open("sqlite", s.writerDSN(s.owner))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	competing, err := other.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer competing.Rollback()
	called := false
	if err := s.Write(ctx, Relaxed, func(context.Context, *sql.Tx) error { called = true; return nil }); code(err) != "busy" || called {
		t.Fatalf("BEGIN waited/reached callback behind external write lock: called=%v err=%v", called, err)
	}
}

func TestCanceledSQLAndPreCommitCancelKeepTheNextDurableWriterUsable(t *testing.T) {
	s, _, ctx := setup(t)
	before, _ := s.Stats()
	for i := 0; i < 200; i++ {
		attempt, cancel := context.WithCancel(ctx)
		err := s.Write(attempt, Relaxed, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,'test',0,0)", fmt.Sprintf("canceled-%d", i)); err != nil {
				return err
			}
			cancel()
			return nil // owner must check cancellation before attempting COMMIT
		})
		cancel()
		if code(err) != "cancelled" {
			t.Fatalf("pre-commit cancel %d: %v", i, err)
		}
		if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,'test',0,0)", fmt.Sprintf("kept-%d", i))
			return err
		}); err != nil {
			t.Fatalf("durable reentry %d: %v", i, err)
		}
	}
	after, _ := s.Stats()
	if after.CommitsDurable-before.CommitsDurable != 200 || after.CommitsRelaxed != before.CommitsRelaxed {
		t.Fatalf("wrong committed classes: before=%+v after=%+v", before, after)
	}
	var canceled, kept int
	if err := s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope LIKE 'canceled-%'").Scan(&canceled); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope LIKE 'kept-%'").Scan(&kept)
	}); err != nil || canceled != 0 || kept != 200 {
		t.Fatalf("canceled SQL leaked: canceled=%d kept=%d %v", canceled, kept, err)
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	err := s.Write(deadline, Relaxed, func(ctx context.Context, tx *sql.Tx) error {
		var total int64
		return tx.QueryRowContext(ctx, "WITH RECURSIVE n(v) AS (VALUES(1) UNION ALL SELECT v+1 FROM n WHERE v<1000000000) SELECT sum(v) FROM n").Scan(&total)
	})
	if code(err) != "deadline_exceeded" {
		t.Fatalf("running SQL ignored deadline: %v", err)
	}
	if got := synchronous(t, s, Durable); got != 2 {
		t.Fatalf("SQL deadline left next writer unusable: %d", got)
	}
}
