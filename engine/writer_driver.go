package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"

	"modernc.org/sqlite"
)

func writerConnectionLost(err error) bool {
	return errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone)
}

// The dedicated writer keeps the actual driver handle even if database/sql
// discards its *sql.Conn after ErrBadConn. This is the same writer owner, not
// a second transaction authority.
type writerConnector struct {
	owner *Store
	base  driver.Connector
}

func (c writerConnector) Driver() driver.Driver { return c.base.Driver() }
func (c writerConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	tracked := &writerDriver{Conn: conn, joined: make(chan struct{})}
	c.owner.writerMu.Lock()
	c.owner.physicalWriter = tracked
	c.owner.writerMu.Unlock()
	return tracked, nil
}

type sqliteWriterConnector struct{ dsn string }

func (c sqliteWriterConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c sqliteWriterConnector) Connect(context.Context) (driver.Conn, error) {
	return c.Driver().Open(c.dsn)
}

// Readers use the same bounded pool, but retain an actual driver whose Close
// failed so Store.Close can join that exact connection before unlocking.
type readerConnector struct {
	owner *Store
	base  driver.Connector
}

func (c readerConnector) Driver() driver.Driver { return c.base.Driver() }
func (c readerConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.owner.readerUnsettled.Load() != 0 {
		return nil, fail("unavailable", "reader physical settlement is unavailable")
	}
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	tracked := &readerDriver{writerDriver: &writerDriver{Conn: conn, joined: make(chan struct{})}, owner: c.owner}
	c.owner.readerMu.Lock()
	if c.owner.physicalReaders == nil {
		c.owner.physicalReaders = make(map[*readerDriver]struct{})
	}
	c.owner.physicalReaders[tracked] = struct{}{}
	c.owner.readerMu.Unlock()
	return tracked, nil
}

type readerDriver struct {
	*writerDriver
	owner     *Store
	unsettled atomic.Bool
}

func (c *readerDriver) markUnsettled() {
	if c.unsettled.CompareAndSwap(false, true) {
		c.owner.readerUnsettled.Add(1)
	}
}

func (c *readerDriver) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if completion, ok := ctx.Value(readAttemptKey{}).(*readCompletion); ok {
		completion.physical = c
	}
	tx, err := c.writerDriver.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &readerTransaction{Tx: tx, owner: c}, nil
}

type readerTransaction struct {
	driver.Tx
	owner *readerDriver
}

func (tx *readerTransaction) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		// A failed rollback must not return this connection to the pool. Mark
		// it before database/sql releases it, and force physical disposal.
		tx.owner.markUnsettled()
		return errors.Join(err, driver.ErrBadConn)
	}
	return nil
}

func (c *readerDriver) Close() error {
	err := c.writerDriver.Close()
	if err != nil {
		c.markUnsettled()
		return err
	}
	if c.unsettled.Swap(false) {
		c.owner.readerUnsettled.Add(-1)
	}
	c.owner.readerMu.Lock()
	delete(c.owner.physicalReaders, c)
	c.owner.readerMu.Unlock()
	return nil
}

// A pending writer fault still needs the reader pool for its exact selected
// snapshot. Join only failed, discarded reader drivers before that probe;
// healthy readers and the pool remain available to it.
func (s *Store) closeFailedReaders() error {
	s.readerMu.Lock()
	var failed []*readerDriver
	for reader := range s.physicalReaders {
		if reader.unsettled.Load() {
			failed = append(failed, reader)
		}
	}
	s.readerMu.Unlock()
	for _, reader := range failed {
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Called under lifecycle.Lock. Pool close is attempted once; a reported close
// failure retains the exact drivers and only a later explicit Close retries.
func (s *Store) closeReaders() error {
	if s.rdb != nil {
		err := s.rdb.Close()
		s.rdb = nil
		if err != nil {
			return err
		}
	}
	s.readerMu.Lock()
	readers := make([]*readerDriver, 0, len(s.physicalReaders))
	for reader := range s.physicalReaders {
		readers = append(readers, reader)
	}
	s.readerMu.Unlock()
	for _, reader := range readers {
		if err := reader.Close(); err != nil {
			return err
		}
	}
	return nil
}

type writerDriver struct {
	driver.Conn
	mu        sync.Mutex
	attempted bool
	closed    bool
	closeErr  error
	joined    chan struct{}
}

func (c *writerDriver) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.attempted = true
	c.closeErr = c.Conn.Close()
	if c.closeErr == nil {
		c.closed = true
		close(c.joined)
	}
	return c.closeErr
}

func (c *writerDriver) closeState() (closed, attempted bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed, c.attempted, c.closeErr
}

func (c *writerDriver) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if conn, ok := c.Conn.(driver.ConnBeginTx); ok {
		return conn.BeginTx(ctx, options)
	}
	if options.Isolation != 0 || options.ReadOnly {
		return nil, errors.New("writer does not support requested transaction options")
	}
	return c.Conn.Begin()
}
func (c *writerDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return conn.PrepareContext(ctx, query)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Conn.Prepare(query)
}
func (c *writerDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if conn, ok := c.Conn.(driver.ExecerContext); ok {
		return conn.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}
func (c *writerDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if conn, ok := c.Conn.(driver.QueryerContext); ok {
		return conn.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}
func (c *writerDriver) Ping(ctx context.Context) error {
	if conn, ok := c.Conn.(driver.Pinger); ok {
		return conn.Ping(ctx)
	}
	return nil
}
func (c *writerDriver) ResetSession(ctx context.Context) error {
	if conn, ok := c.Conn.(driver.SessionResetter); ok {
		return conn.ResetSession(ctx)
	}
	return nil
}
func (c *writerDriver) IsValid() bool {
	if conn, ok := c.Conn.(driver.Validator); ok {
		return conn.IsValid()
	}
	return true
}
