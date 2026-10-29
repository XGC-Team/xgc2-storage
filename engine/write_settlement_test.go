package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"modernc.org/sqlite"
)

var (
	errSettlementCommit   = errors.New("injected commit response")
	errSettlementBegin    = errors.New("injected begin response")
	errSettlementRollback = errors.New("injected rollback failure")
	errSettlementClose    = errors.New("injected physical close failure")
	errSettlementConnect  = errors.New("injected replacement connection failure")
)

type settlementPlan struct {
	// 1 returns before COMMIT; 2 returns an error after the real COMMIT.
	commit          atomic.Int32
	commitEntered   chan struct{}
	continueCommit  chan struct{}
	commitEnterOnce sync.Once
	beginAfter      atomic.Bool
	rollbackBad     atomic.Bool
	pragmaBad       atomic.Bool
	checkpointBad   atomic.Bool
	backupBad       atomic.Bool
	beginEntered    chan struct{}
	continueBegin   chan struct{}
	beginEnterOnce  sync.Once
	rollback        atomic.Bool
	closeFailures   atomic.Int32
	connectFailures atomic.Int32
	closeCalls      atomic.Int32
	closeEntered    chan struct{}
	continueClose   chan struct{}
	closeEnterOnce  sync.Once
	connMu          sync.Mutex
	lastConn        *settlementConnection
}

type settlementConnector struct {
	dsn  string
	plan *settlementPlan
}

func (c settlementConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c settlementConnector) Connect(context.Context) (driver.Conn, error) {
	for {
		remaining := c.plan.connectFailures.Load()
		if remaining == 0 {
			break
		}
		if c.plan.connectFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, errSettlementConnect
		}
	}
	inner, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	conn := &settlementConnection{Conn: inner, plan: c.plan}
	c.plan.connMu.Lock()
	c.plan.lastConn = conn
	c.plan.connMu.Unlock()
	return conn, nil
}

type settlementConnection struct {
	driver.Conn
	plan *settlementPlan
}

func (c *settlementConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if c.plan.beginAfter.Swap(false) {
		return nil, errSettlementBegin
	}
	if c.plan.beginEntered != nil {
		c.plan.beginEnterOnce.Do(func() {
			close(c.plan.beginEntered)
			<-c.plan.continueBegin
		})
	}
	return settlementTransaction{Tx: tx, plan: c.plan}, nil
}
func (c *settlementConnection) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if q == "PRAGMA synchronous=NORMAL" && c.plan.pragmaBad.Swap(false) {
		return nil, driver.ErrBadConn
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c *settlementConnection) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if q == "PRAGMA wal_checkpoint(PASSIVE)" && c.plan.checkpointBad.Swap(false) {
		return nil, driver.ErrBadConn
	}
	if q == "PRAGMA page_count" && c.plan.backupBad.Swap(false) {
		return nil, driver.ErrBadConn
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}
func (c *settlementConnection) Close() error {
	c.plan.closeCalls.Add(1)
	if c.plan.closeEntered != nil {
		c.plan.closeEnterOnce.Do(func() {
			close(c.plan.closeEntered)
			<-c.plan.continueClose
		})
	}
	for {
		remaining := c.plan.closeFailures.Load()
		if remaining == 0 {
			break
		}
		if c.plan.closeFailures.CompareAndSwap(remaining, remaining-1) {
			return errSettlementClose
		}
	}
	return c.Conn.Close()
}

type settlementTransaction struct {
	driver.Tx
	plan *settlementPlan
}

func (tx settlementTransaction) Commit() error {
	if tx.plan.commitEntered != nil {
		tx.plan.commitEnterOnce.Do(func() {
			close(tx.plan.commitEntered)
			<-tx.plan.continueCommit
		})
	}
	switch tx.plan.commit.Swap(0) {
	case 1:
		return errSettlementCommit
	case 2:
		if err := tx.Tx.Commit(); err != nil {
			return err
		}
		return errSettlementCommit
	default:
		return tx.Tx.Commit()
	}
}
func (tx settlementTransaction) Rollback() error {
	if tx.plan.rollbackBad.Swap(false) {
		return driver.ErrBadConn
	}
	if tx.plan.rollback.Swap(false) {
		return errSettlementRollback
	}
	return tx.Tx.Rollback()
}

// installSettlementDriver replaces only an isolated fixture's actual writer
// with a wrapper delegating all SQL and successful boundaries to SQLite.
func installSettlementDriver(t *testing.T, s *Store, plan *settlementPlan) {
	t.Helper()
	_, release, err := s.admit(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = s.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.wdb.Close(); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(writerConnector{owner: s, base: settlementConnector{dsn: s.writerDSN(s.owner), plan: plan}})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(t.Context())
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	s.wdb, s.writer, s.synchronous = db, conn, Durable
	if err = s.configureWriterCapacity(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// InjectCommitAfterForModuleTest exposes this same real-driver fault to the
// external Coredata integration test, without a production injection hook.
func InjectCommitAfterForModuleTest(t *testing.T, s *Store) error {
	t.Helper()
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	return errSettlementCommit
}

// InjectCommitAfterFailedCloseForModuleTest leaves the exact real driver
// unjoined after a committed reply is lost; explicit Close can join it later.
func InjectCommitAfterFailedCloseForModuleTest(t *testing.T, s *Store) error {
	t.Helper()
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	plan.closeFailures.Store(1)
	return errSettlementCommit
}

func InjectCommitBeforeForModuleTest(t *testing.T, s *Store) error {
	t.Helper()
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(1)
	return errSettlementCommit
}

func GateConfirmedRollbackForModuleTest(t *testing.T, s *Store) (<-chan struct{}, func()) {
	t.Helper()
	plan := &settlementPlan{commitEntered: make(chan struct{}), continueCommit: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(1)
	var once sync.Once
	return plan.commitEntered, func() { once.Do(func() { close(plan.continueCommit) }) }
}

// InjectRollbackFailureForModuleTest exercises a module refusal whose actual
// transaction rollback fails before the writer is physically closed.
func InjectRollbackFailureForModuleTest(t *testing.T, s *Store) error {
	t.Helper()
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	return errSettlementRollback
}

// InjectUnjoinedRollbackForModuleTest leaves the first physical close failed;
// explicit Store.Close can later join the same retained connection.
func InjectUnjoinedRollbackForModuleTest(t *testing.T, s *Store) error {
	t.Helper()
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	plan.closeFailures.Store(1)
	return errSettlementRollback
}

func requireWriteDisposition(t *testing.T, err error, want WriteDisposition, settled bool) {
	t.Helper()
	var writeErr *WriteError
	if !errors.As(err, &writeErr) || writeErr.Disposition != want || writeErr.Settled != settled {
		t.Fatalf("write disposition=%+v error=%v want=%v settled=%v", writeErr, err, want, settled)
	}
}

func insertFaultRow(ctx context.Context, tx *sql.Tx, id string) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,'test',0,0)", id)
	return err
}

func countFaultRow(t *testing.T, s *Store, id string) int {
	t.Helper()
	var count int
	if err := s.Read(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope=?", id).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestWriteCommitFaultReportsRealDispositionAndJoins(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       int32
		want       WriteDisposition
		wantRows   int
		wantClosed bool
	}{{"before-commit", 1, WriteNotCommitted, 0, false}, {"after-real-commit", 2, WriteUnknown, 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, ctx := setup(t)
			plan := &settlementPlan{}
			installSettlementDriver(t, s, plan)
			plan.commit.Store(tc.mode)
			err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
				return insertFaultRow(ctx, tx, tc.name)
			})
			requireWriteDisposition(t, err, tc.want, true)
			var attempt *WriteError
			if !errors.As(err, &attempt) || (tc.want == WriteUnknown && attempt.NonCommittedAfterJoin()) {
				t.Fatalf("possible COMMIT was reported as noncommit: %v", err)
			}
			if !errors.Is(err, errSettlementCommit) || (plan.closeCalls.Load() > 0) != tc.wantClosed {
				t.Fatalf("cause/physical close err=%v closes=%d", err, plan.closeCalls.Load())
			}
			if got := countFaultRow(t, s, tc.name); got != tc.wantRows {
				t.Fatalf("real durable row count=%d want=%d", got, tc.wantRows)
			}
			before, _ := s.Stats()
			if err = s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
				var level int
				var pageSize, pageLimit int64
				if err := tx.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&level); err != nil {
					return err
				}
				if err := tx.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
					return err
				}
				if err := tx.QueryRowContext(ctx, "PRAGMA max_page_count").Scan(&pageLimit); err != nil {
					return err
				}
				if level != 2 || pageSize <= 0 || pageLimit > s.config.MaxDBBytes/pageSize {
					return fmt.Errorf("replacement writer settings: synchronous=%d page-size=%d limit=%d", level, pageSize, pageLimit)
				}
				return insertFaultRow(ctx, tx, tc.name+"-next")
			}); err != nil {
				t.Fatalf("next owner write: %v", err)
			}
			after, _ := s.Stats()
			if after.Calls-before.Calls != 1 || after.CommitsDurable-before.CommitsDurable != 1 || after.WritersQueued != 0 || after.ReadersActive != 0 {
				t.Fatalf("after-join owner work before=%+v after=%+v", before, after)
			}
			t.Logf("outcome=%v settled=true real-rows=%d after-join calls=+1 durable=+1 writers/readers=0/0", tc.want, tc.wantRows)
		})
	}
}

func TestWriteLostBeginResponseClosesTheRealTransaction(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.beginAfter.Store(true)
	called := false
	err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error {
		called = true
		return nil
	})
	requireWriteDisposition(t, err, WriteNotCommitted, true)
	if called || !errors.Is(err, errSettlementBegin) || plan.closeCalls.Load() == 0 {
		t.Fatalf("lost BEGIN response: called=%t closes=%d error=%v", called, plan.closeCalls.Load(), err)
	}
	if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "after-begin")
	}); err != nil {
		t.Fatalf("replacement writer after lost BEGIN: %v", err)
	}
	if got := countFaultRow(t, s, "after-begin"); got != 1 {
		t.Fatalf("replacement did not commit: %d", got)
	}
}

func TestWriteCanceledBeforeCallbackRemainsNotCommittedAfterFailedRollback(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{beginEntered: make(chan struct{}), continueBegin: make(chan struct{})}
	var once sync.Once
	allow := func() { once.Do(func() { close(plan.continueBegin) }) }
	t.Cleanup(allow)
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	called := false
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Durable, func(context.Context, *sql.Tx) error { called = true; return nil })
	}()
	select {
	case <-plan.beginEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("BEGIN did not enter the real driver")
	}
	cancel()
	err := <-done
	requireWriteDisposition(t, err, WriteNotCommitted, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-callback cancellation called=%t error=%v", called, err)
	}
	allow()
	if err := outcome.WaitSettlement(ctx); err != nil || called || plan.closeCalls.Load() == 0 {
		t.Fatalf("pre-callback cleanup called=%t closes=%d err=%v", called, plan.closeCalls.Load(), err)
	}
}

func TestWriteRollbackFailureWaitsForPhysicalClose(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	var once sync.Once
	allow := func() { once.Do(func() { close(plan.continueClose) }) }
	t.Cleanup(allow)
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	business := &api.Error{Code: "failed_precondition", Message: "business callback failed"}
	done := make(chan error, 1)
	go func() {
		done <- s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			if err := insertFaultRow(ctx, tx, "rolled-back"); err != nil {
				return err
			}
			return business
		})
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not start")
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("read entered an unsettled owner: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("write returned before physical close joined: %v", err)
	default:
	}
	allow()
	err := <-done
	requireWriteDisposition(t, err, WriteUnknown, true)
	if !errors.Is(err, business) || !errors.Is(err, errSettlementRollback) || countFaultRow(t, s, "rolled-back") != 0 {
		t.Fatalf("rollback/close result lost its cause or leaked a row: %v", err)
	}
	var disposition *WriteError
	var original *api.Error
	if !errors.As(err, &disposition) || !errors.As(disposition.Cause(), &original) || original != business || code(err) != "internal" {
		t.Fatalf("original refusal or physical classification lost: original=%v error=%v", original, err)
	}
	if err = s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "after-join")
	}); err != nil {
		t.Fatalf("replacement writer: %v", err)
	}
	stats, _ := s.Stats()
	if stats.WritersQueued != 0 || stats.ReadersActive != 0 {
		t.Fatalf("owner did not return to baseline: %+v", stats)
	}
}

func TestWriteFailedPhysicalCloseFencesAndRetainsOwnership(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	plan.closeFailures.Store(2)
	business := errors.New("callback failure")
	err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		if err := insertFaultRow(ctx, tx, "never-committed"); err != nil {
			return err
		}
		return business
	})
	requireWriteDisposition(t, err, WriteUnknown, false)
	var original *WriteError
	if !errors.As(err, &original) {
		t.Fatal(err)
	}
	if !errors.Is(err, business) || !errors.Is(err, errSettlementRollback) || !errors.Is(err, errSettlementClose) {
		t.Fatalf("lost failure causes: %v", err)
	}
	if err = s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("unsettled read admitted: %v", err)
	}
	if err = s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("unsettled write admitted: %v", err)
	}
	cfg.Create = false
	if other, e := Open(ctx, cfg); code(e) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("owner lock was released before close: %v", e)
	}
	if err = s.Close(); !errors.Is(err, errSettlementClose) {
		t.Fatalf("unjoined close did not report its failure: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if err := original.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed close signaled join: %v", err)
	}
	cancel()
	if other, e := Open(ctx, cfg); code(e) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed Close released owner lock: %v", e)
	}
	if err = s.Close(); err != nil {
		t.Fatalf("explicit close after physical join: %v", err)
	}
	if err := original.WaitSettlement(ctx); err != nil {
		t.Fatalf("original attempt missed exact later join: %v", err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after joined close: %v", err)
	}
	defer other.Close()
	if got := countFaultRow(t, other, "never-committed"); got != 0 {
		t.Fatalf("unjoined transaction later committed: %d", got)
	}
}

func TestWriteDiscardedDriverWithFailedCloseKeepsOwnerFenced(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.rollbackBad.Store(true)
	plan.closeFailures.Store(2)
	err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		if err := insertFaultRow(ctx, tx, "unjoined-driver"); err != nil {
			return err
		}
		return errors.New("callback refused")
	})
	requireWriteDisposition(t, err, WriteUnknown, false)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("driver failure cause lost: %v", err)
	}
	if err = s.Close(); !errors.Is(err, errSettlementClose) {
		t.Fatalf("discarded driver claimed a joined close: %v", err)
	}
	if err = s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("unjoined driver allowed a read: %v", err)
	}
	cfg.Create = false
	if other, e := Open(ctx, cfg); code(e) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("unjoined driver released its owner lock: %v", e)
	}
	if err = s.Close(); err != nil {
		t.Fatalf("explicit joined close: %v", err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after exact driver joined: %v", err)
	}
	defer other.Close()
	if got := countFaultRow(t, other, "unjoined-driver"); got != 0 {
		t.Fatalf("discarded transaction later committed: %d", got)
	}
}

func TestWriteRollbackFailureDoesNotTurnReplayIntoSuccess(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return errReplayed })
	requireWriteDisposition(t, err, WriteUnknown, true)
	var attempt *WriteError
	if !errors.As(err, &attempt) || attempt.NonCommittedAfterJoin() || !errors.Is(err, errReplayed) || DefinitelyNotCommitted(err) || code(err) == "replayed" {
		t.Fatalf("replay sentinel concealed rollback failure: %v", err)
	}
	if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, fmt.Sprint("after-replay"))
	}); err != nil {
		t.Fatal(err)
	}
}
