package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"modernc.org/sqlite"
)

type heldReaderGate struct {
	prefix, point    string
	entered, release chan struct{}
	trigger          atomic.Bool
	unblock          sync.Once
}

func (g *heldReaderGate) hold() {
	if g.trigger.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
}
func (g *heldReaderGate) allow() { g.unblock.Do(func() { close(g.release) }) }

type heldReaderConnector struct {
	dsn  string
	gate *heldReaderGate
}

func (c heldReaderConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c heldReaderConnector) Connect(context.Context) (driver.Conn, error) {
	inner, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &heldReaderConnection{Conn: inner, gate: c.gate}, nil
}

type heldReaderConnection struct {
	driver.Conn
	gate *heldReaderGate
}

func (c *heldReaderConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (c *heldReaderConnection) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if strings.HasPrefix(q, c.gate.prefix) {
		if c.gate.point == "query" {
			c.gate.hold()
		}
		if c.gate.point == "row" && err == nil {
			return &heldReaderRows{Rows: rows, gate: c.gate}, nil
		}
	}
	return rows, err
}

type heldReaderRows struct {
	driver.Rows
	gate *heldReaderGate
}

var errHeldReaderClose = errors.New("injected reader close failure")
var errReaderRollback = errors.New("injected reader rollback failure")

type readerRollbackFault struct {
	driver.Conn
	closeFailures atomic.Int32
	closeCalls    atomic.Int32
}

type readerBeginFault struct {
	driver.Conn
	closes atomic.Int32
}

func (c *readerBeginFault) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, driver.ErrBadConn
}

func (c *readerBeginFault) Close() error {
	if c.closes.Add(1) == 1 {
		return errHeldReaderClose
	}
	return c.Conn.Close()
}

func TestReaderBeginFailureDoesNotRetryPastUnjoinedDriver(t *testing.T) {
	s, _, ctx := setup(t)
	s.readerMu.Lock()
	var physical *readerDriver
	for reader := range s.physicalReaders {
		physical = reader
		break
	}
	if physical == nil {
		s.readerMu.Unlock()
		t.Fatal("no actual reader driver")
	}
	fault := &readerBeginFault{Conn: physical.Conn}
	physical.Conn = fault
	s.readerMu.Unlock()
	called := false
	err := s.Read(ctx, func(context.Context, *sql.Tx) error { called = true; return nil })
	var outcome *ReadError
	if !errors.As(err, &outcome) || called || fault.closes.Load() != 1 {
		t.Fatalf("reader retried after failed physical begin/close: called=%t closes=%d err=%v", called, fault.closes.Load(), err)
	}
	wait, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := outcome.WaitSettlement(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined reader reported settlement: %v", err)
	}
	cancel()
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("unjoined driver lost fence: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("explicit original reader join: %v", err)
	}
	if err := outcome.WaitSettlement(ctx); err != nil || fault.closes.Load() != 2 {
		t.Fatalf("original reader did not join: calls=%d err=%v", fault.closes.Load(), err)
	}
}

func (c *readerRollbackFault) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return failedReaderRollbackTx{Tx: tx}, nil
}

func (c *readerRollbackFault) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}

func (c *readerRollbackFault) Close() error {
	c.closeCalls.Add(1)
	if c.closeFailures.CompareAndSwap(1, 0) {
		return errHeldReaderClose
	}
	return c.Conn.Close()
}

type failedReaderRollbackTx struct{ driver.Tx }

func (t failedReaderRollbackTx) Rollback() error { return errReaderRollback }

func TestReaderRollbackFailureJoinsExactPhysicalConnection(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		name := "close-joins"
		if closeFails {
			name = "close-fails-then-retries"
		}
		t.Run(name, func(t *testing.T) {
			s, cfg, ctx := setup(t)
			s.readerMu.Lock()
			var physical *readerDriver
			for reader := range s.physicalReaders {
				physical = reader
				break
			}
			if physical == nil {
				s.readerMu.Unlock()
				t.Fatal("no actual reader driver")
			}
			fault := &readerRollbackFault{Conn: physical.Conn}
			if closeFails {
				fault.closeFailures.Store(1)
			}
			physical.Conn = fault
			s.readerMu.Unlock()
			err := s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
				var one int
				return tx.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			})
			var outcome *ReadError
			if !errors.As(err, &outcome) || !errors.Is(err, errReaderRollback) {
				t.Fatalf("rollback failure lost its exact reader owner: %v", err)
			}
			if closeFails {
				wait, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
				if err := outcome.WaitSettlement(wait); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("failed physical close reported join: %v", err)
				}
				cancel()
				if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
					t.Fatalf("unjoined reader reused: %v", err)
				}
				cfg.Create = false
				if other, err := Open(ctx, cfg); other != nil || code(err) != "conflict" {
					if other != nil {
						other.Close()
					}
					t.Fatalf("unjoined reader lost lock: %v", err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("explicit reader join: %v", err)
				}
			}
			if err := outcome.WaitSettlement(ctx); err != nil {
				t.Fatalf("exact reader did not settle: %v", err)
			}
			if closeFails && fault.closeCalls.Load() != 2 || !closeFails && fault.closeCalls.Load() != 1 {
				t.Fatalf("physical close attempts=%d", fault.closeCalls.Load())
			}
			if !closeFails {
				if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
					t.Fatalf("joined replacement reader: %v", err)
				}
			}
		})
	}
}

type failedReaderClose struct {
	driver.Conn
	calls atomic.Int32
}

func (c *failedReaderClose) Close() error {
	if c.calls.Add(1) == 1 {
		return errHeldReaderClose
	}
	return c.Conn.Close()
}

func TestFailedReaderCloseRetainsExactOwnerUntilExplicitRetry(t *testing.T) {
	s, cfg, ctx := setup(t)
	s.readerMu.Lock()
	var physical *readerDriver
	for reader := range s.physicalReaders {
		physical = reader
		break
	}
	if physical == nil {
		s.readerMu.Unlock()
		t.Fatal("no actual reader driver")
	}
	fault := &failedReaderClose{Conn: physical.Conn}
	physical.Conn = fault
	s.readerMu.Unlock()
	cfg.Create = false
	if err := s.Close(); !errors.Is(err, errHeldReaderClose) || fault.calls.Load() != 1 {
		t.Fatalf("failed reader close lost its physical owner: calls=%d err=%v", fault.calls.Load(), err)
	}
	if other, err := Open(ctx, cfg); other != nil || code(err) != "conflict" {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed reader close released database owner: %v", err)
	}
	if err := s.Close(); err != nil || fault.calls.Load() != 2 {
		t.Fatalf("later close did not join the same reader: calls=%d err=%v", fault.calls.Load(), err)
	}
	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestFailedReaderCloseStaysFencedWhenConcurrentWriterCommits(t *testing.T) {
	s, cfg, ctx := setup(t)
	entered, release := make(chan struct{}), make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- s.Write(ctx, Durable, func(context.Context, *sql.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter its actual transaction")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	reader, err := s.rdb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var physical *readerDriver
	var fault *failedReaderClose
	err = reader.Raw(func(raw any) error {
		var ok bool
		physical, ok = raw.(*readerDriver)
		if !ok {
			t.Fatalf("unexpected reader driver %T", raw)
		}
		fault = &failedReaderClose{Conn: physical.Conn}
		physical.Conn = fault
		return driver.ErrBadConn
	})
	if !errors.Is(err, driver.ErrBadConn) || fault.calls.Load() != 1 {
		t.Fatalf("failed reader was not discarded: calls=%d err=%v", fault.calls.Load(), err)
	}
	close(release)
	if err := <-writerDone; err != nil {
		t.Fatalf("concurrent writer: %v", err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("writer commit unfenced an unjoined reader: %v", err)
	}
	cfg.Create = false
	if other, err := Open(ctx, cfg); other != nil || code(err) != "conflict" {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed reader lost the database owner: %v", err)
	}
	if err := s.Close(); err != nil || fault.calls.Load() != 2 {
		t.Fatalf("explicit close did not join the original reader: calls=%d err=%v", fault.calls.Load(), err)
	}
}

func TestPendingWriteProbeJoinsFailedReaderBeforeRevalidation(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	plan.closeFailures.Store(1)
	entered, release := make(chan struct{}), make(chan struct{})
	written := make(chan error, 1)
	var found int
	go func() {
		written <- s.WriteWithFaultProbe(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			if err := insertFaultRow(ctx, tx, "pending-reader-probe"); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		}, func(ctx context.Context, tx *sql.Tx, possible bool) error {
			if !possible || tx == nil {
				return errors.New("missing exact fault snapshot")
			}
			return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope=?", "pending-reader-probe").Scan(&found)
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	conn, err := s.rdb.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var reader *readerDriver
	var readerFault *failedReaderClose
	err = conn.Raw(func(raw any) error {
		reader = raw.(*readerDriver)
		readerFault = &failedReaderClose{Conn: reader.Conn}
		reader.Conn = readerFault
		return driver.ErrBadConn
	})
	if !errors.Is(err, driver.ErrBadConn) || readerFault.calls.Load() != 1 {
		t.Fatalf("reader close injection: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = s.Close() })
	close(release)
	err = <-written
	var outcome *WriteError
	if !errors.As(err, &outcome) || outcome.Disposition != WriteUnknown || outcome.Settled {
		t.Fatalf("writer did not retain its unknown physical attempt: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("explicit close must join failed reader before exact probe: %v", err)
	}
	if err := outcome.WaitSettlement(ctx); err != nil || found != 1 || readerFault.calls.Load() != 2 {
		t.Fatalf("retained writer/probe after reader join: found=%d reader closes=%d err=%v", found, readerFault.calls.Load(), err)
	}
}

func (r *heldReaderRows) Next(values []driver.Value) error {
	err := r.Rows.Next(values)
	r.gate.hold()
	return err
}

// GateReaderForModuleTest holds the actual SQLite query or row return while
// leaving the ordinary reader pool, transaction and owner unchanged.
func GateReaderForModuleTest(t *testing.T, s *Store, prefix, point string) (<-chan struct{}, func()) {
	t.Helper()
	if err := s.rdb.Close(); err != nil {
		t.Fatal(err)
	}
	gate := &heldReaderGate{prefix: prefix, point: point, entered: make(chan struct{}), release: make(chan struct{})}
	s.rdb = sql.OpenDB(readerConnector{owner: s, base: heldReaderConnector{dsn: s.readerDSN(s.owner), gate: gate}})
	s.rdb.SetMaxOpenConns(s.config.Readers)
	s.rdb.SetMaxIdleConns(s.config.Readers)
	t.Cleanup(gate.allow)
	return gate.entered, gate.allow
}

func TestReadPreparationPanicReleasesAdmission(t *testing.T) {
	s, _, ctx := setup(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected preparation panic")
			}
		}()
		_ = s.ReadPrepared(ctx, func() { panic("preparation") }, func(context.Context, *sql.Tx) error { return nil })
	}()
	stats, err := s.Stats()
	if err != nil || stats.ReadersActive != 0 {
		t.Fatalf("panicking preparation retained reader admission: %+v %v", stats, err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatalf("reader after preparation panic: %v", err)
	}
}

func TestReceiptDeadlineRetainsReaderUntilRealJoinAndClose(t *testing.T) {
	s, cfg, base := setup(t)
	before := snapshot(t, s, base, "a")
	q := api.BatchRequest{Scope: testScope, Expected: before.Token, RequestID: "reader-owner", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"value"}`)}}
	if _, err := s.Batch(base, q); err != nil {
		t.Fatal(err)
	}
	entered, release := GateReaderForModuleTest(t, s, "SELECT body FROM receipts", "query")
	ctx, cancel := context.WithTimeout(base, 100*time.Millisecond)
	defer cancel()
	type answer struct {
		receipt api.Receipt
		err     error
	}
	done := make(chan answer, 1)
	go func() {
		receipt, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: q.RequestID})
		done <- answer{receipt, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real receipt query did not enter")
	}
	var got answer
	select {
	case got = <-done:
	case <-time.After(time.Second):
		t.Fatal("receipt missed deadline")
	}
	var outcome *ReadError
	if !errors.As(got.err, &outcome) || !errors.Is(got.err, context.DeadlineExceeded) || got.receipt.RequestID != "" {
		t.Fatalf("late read exposed a receipt or lost its owner: %+v %v", got.receipt, got.err)
	}
	if stats, err := s.Stats(); err != nil || stats.ReadersActive != 1 {
		t.Fatalf("physical reader was dropped: %+v %v", stats, err)
	}
	// A second admitted reader uses its own connection while the first remains held.
	if _, err := s.Receipt(base, api.ReceiptRequest{Scope: testScope, RequestID: q.RequestID}); err != nil {
		t.Fatalf("independent reader could not proceed: %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	cfg.Create = false
	if other, err := Open(base, cfg); other != nil || code(err) != "conflict" {
		if other != nil {
			other.Close()
		}
		t.Fatalf("Close released the held reader owner: %v", err)
	}
	wait, waitCancel := context.WithTimeout(base, 30*time.Millisecond)
	if err := outcome.WaitSettlement(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined reader reported completion: %v", err)
	}
	waitCancel()
	release()
	if err := outcome.WaitSettlement(base); err != nil {
		t.Fatalf("reader join: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close after reader join: %v", err)
	}
	reopened, err := Open(base, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	receipt, err := reopened.Receipt(base, api.ReceiptRequest{Scope: testScope, RequestID: q.RequestID})
	if err != nil || receipt.RequestID != q.RequestID {
		t.Fatalf("reopened receipt: %+v %v", receipt, err)
	}
	if stats, err := reopened.Stats(); err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("after-join owner baseline: %+v %v", stats, err)
	}
}

func TestSnapshotDeadlineRetainsActualRowAndOwnedRequest(t *testing.T) {
	s, _, base := setup(t)
	before := snapshot(t, s, base, "a")
	if _, err := s.Batch(base, api.BatchRequest{Scope: testScope, Expected: before.Token, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"value"}`)}}); err != nil {
		t.Fatal(err)
	}
	entered, release := GateReaderForModuleTest(t, s, "SELECT r.key,r.version,r.deleted,r.data", "row")
	request := api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Limit: 1}}}
	ctx, cancel := context.WithTimeout(base, 100*time.Millisecond)
	defer cancel()
	type answer struct {
		response api.SnapshotResponse
		err      error
	}
	done := make(chan answer, 1)
	go func() { response, err := s.Snapshot(ctx, request); done <- answer{response, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real row iteration did not enter")
	}
	got := <-done
	var outcome *ReadError
	if !errors.As(got.err, &outcome) || !errors.Is(got.err, context.DeadlineExceeded) || len(got.response.Results) != 0 {
		t.Fatalf("late row exposed a partial result: %+v %v", got.response, got.err)
	}
	request.Queries[0].Limit = 99
	request.Queries[0].Collection = "caller-changed"
	release()
	if err := outcome.WaitSettlement(base); err != nil {
		t.Fatalf("row join: %v", err)
	}
	stats, err := s.Stats()
	if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("reader did not return to baseline: %+v %v", stats, err)
	}
}
