package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"golang.org/x/sys/unix"
	"modernc.org/sqlite"
)

// Config selects one database and its finite resources. Zero values select the
// documented defaults.
type Config struct {
	Path     string
	Create   bool
	Manifest api.Manifest
	// Modules are the compiled data modules that the manifest's namespaces may
	// name. A module a namespace names is installed or migrated at Open.
	Modules []Module
	// Readers is the number of concurrent read connections (default 4, at most 16).
	Readers int
	// WriterQueue bounds the writers admitted at once (default 64, at most
	// 1024). A caller beyond it waits for room until its own deadline.
	WriterQueue int
	// CallBudget caps every call (default 30 s, at most 5 min). A shorter
	// caller deadline wins; a caller without one gets the budget.
	CallBudget time.Duration
	// MaxDBBytes is the finite database capacity (default 1 GiB).
	MaxDBBytes int64
	// MaxWALBytes is the WAL size above which new writes are refused until a
	// checkpoint reclaims it (default 64 MiB).
	MaxWALBytes int64
	// MinFreeBytes is the free disk space below which new writes are refused
	// (default 16 MiB).
	MinFreeBytes int64
	// CheckpointBytes is the WAL size that requests a checkpoint at once
	// (default 4 MiB, or a quarter of MaxWALBytes when that is smaller).
	CheckpointBytes int64
	// CheckpointDelay is how long committed frames may wait for a checkpoint
	// after a commit, which also bounds how long a relaxed commit stays
	// exposed to power loss (default 1 s). Nothing runs while the database is idle.
	CheckpointDelay time.Duration
	// NoMigrate refuses an older schema instead of migrating it.
	NoMigrate bool
	// OnMaintenanceError receives failures of background maintenance.
	OnMaintenanceError func(error)
}

func (c *Config) defaults() error {
	if c.Readers == 0 {
		c.Readers = 4
	}
	if c.WriterQueue == 0 {
		c.WriterQueue = 64
	}
	if c.CallBudget == 0 {
		c.CallBudget = 30 * time.Second
	}
	if c.MaxDBBytes == 0 {
		c.MaxDBBytes = 1 << 30
	}
	if c.MaxWALBytes == 0 {
		c.MaxWALBytes = 64 << 20
	}
	if c.MinFreeBytes == 0 {
		c.MinFreeBytes = 16 << 20
	}
	if c.CheckpointBytes == 0 {
		c.CheckpointBytes = min(4<<20, max(c.MaxWALBytes/4, 64<<10))
	}
	if c.CheckpointDelay == 0 {
		c.CheckpointDelay = time.Second
	}
	if c.Readers < 1 || c.Readers > 16 || c.WriterQueue < 1 || c.WriterQueue > 1024 || c.CallBudget <= 0 || c.CallBudget > 5*time.Minute ||
		c.MaxDBBytes < 1<<20 || c.MaxWALBytes < 1<<20 || c.MinFreeBytes < 1<<20 || c.CheckpointBytes < 64<<10 || c.CheckpointBytes > c.MaxWALBytes || c.CheckpointDelay < 10*time.Millisecond {
		return fail("invalid_argument", "invalid execution/disk limits")
	}
	return nil
}

type Store struct {
	config     Config
	owner      *owner
	wdb, rdb   *sql.DB
	writer     *sql.Conn // dedicated writer connection; used only under gate
	dbid       string
	engine     string
	namespaces map[string]api.Namespace
	modules    map[string]Module
	// queue admits writers, gate runs one of them, reads admits readers.
	queue, gate, reads chan struct{}
	closed             atomic.Bool
	lifecycle          sync.RWMutex
	// synchronous is the writer connection's current safety level; gate guards it.
	synchronous                                  Durability
	calls, conflicts, timeouts                   atomic.Uint64
	waitNS, sqlNS                                atomic.Uint64
	durable, relaxed, fsyncs                     atomic.Uint64
	checkpoints, receiptsPruned, maintenanceErrs atomic.Uint64
	maintenance                                  *maintenance
}

// Durability is the per-transaction commit class of the writer.
type Durability uint8

const (
	// Durable commits with synchronous=FULL: the commit is on stable storage
	// when the call returns.
	Durable Durability = iota
	// Relaxed commits with synchronous=NORMAL: it survives a process crash but
	// may be lost with the last relaxed commits on power loss; it becomes
	// durable at the next checkpoint or durable commit.
	Relaxed
)

func (d Durability) String() string {
	if d == Relaxed {
		return "sqlite-normal"
	}
	return "sqlite-full"
}

func (s *Store) writerDSN(o *owner) string {
	return (&url.URL{Scheme: "file", Path: o.path(), RawQuery: "mode=rw&_txlock=immediate&_pragma=busy_timeout(0)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)&_pragma=temp_store(2)&_pragma=cache_size(-4096)&_pragma=wal_autocheckpoint(0)&_pragma=journal_size_limit(1048576)"}).String()
}

func (s *Store) readerDSN(o *owner) string {
	return (&url.URL{Scheme: "file", Path: o.path(), RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(0)&_pragma=cache_size(-2048)&_pragma=temp_store(2)"}).String()
}

func Open(ctx context.Context, c Config) (s *Store, err error) {
	if err = c.defaults(); err != nil {
		return nil, err
	}
	if err = ValidateManifest(c.Manifest); err != nil {
		return nil, err
	}
	// Freeze deployment schemas; caller-owned slices cannot alter a running owner.
	manifestRaw, err := json.Marshal(c.Manifest)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(manifestRaw, &c.Manifest); err != nil {
		return nil, err
	}
	modules := map[string]Module{}
	for _, m := range c.Modules {
		if err = m.validate(); err != nil {
			return nil, err
		}
		if _, dup := modules[m.ID]; dup {
			return nil, fail("invalid_argument", "unique compiled module required")
		}
		modules[m.ID] = m
	}
	namespaces := map[string]api.Namespace{}
	for _, n := range c.Manifest.Namespaces {
		namespaces[n.ID] = n
		for _, id := range n.Modules {
			if _, ok := modules[id]; !ok {
				return nil, fail("failed_precondition", "namespace names data module "+id+" that is not compiled into this owner")
			}
		}
	}
	o, e := acquire(c.Path, c.Create)
	if e != nil {
		return nil, e
	}
	s = &Store{config: c, owner: o, namespaces: namespaces, modules: modules, queue: make(chan struct{}, c.WriterQueue), gate: make(chan struct{}, 1), reads: make(chan struct{}, c.Readers)}
	opened := s
	defer func() {
		if err != nil {
			opened.Close()
		}
	}()
	s.wdb, err = sql.Open("sqlite", s.writerDSN(o))
	if err != nil {
		return nil, err
	}
	s.wdb.SetMaxOpenConns(1)
	s.wdb.SetMaxIdleConns(1)
	if s.writer, err = s.wdb.Conn(ctx); err != nil {
		return nil, err
	}
	if err = s.writer.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&s.engine); err != nil {
		return nil, err
	}
	if !versionAtLeast(s.engine, 3, 51, 3) {
		return nil, fail("failed_precondition", "SQLite 3.51.3 or newer required (earlier versions can corrupt a WAL database on reset)")
	}
	var journal string
	if err = s.writer.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return nil, err
	}
	if journal != "wal" {
		return nil, fail("failed_precondition", "WAL unavailable")
	}
	if c.Create {
		err = s.create(ctx)
	} else {
		err = s.upgrade(ctx)
	}
	if err != nil {
		return nil, err
	}
	// Leave the schema, and any migration, in the main file rather than the WAL.
	var busy, logPages, checkpointed int
	if err = s.writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed); err != nil {
		return nil, err
	}
	var pageSize, pageCount, pageLimit int64
	if err = s.writer.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return nil, err
	}
	if err = s.writer.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount); err != nil {
		return nil, err
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 || pageCount > c.MaxDBBytes/pageSize {
		return nil, fail("resource_exhausted", "existing database exceeds configured page byte budget")
	}
	requestedPageLimit := c.MaxDBBytes / pageSize
	if err = s.writer.QueryRowContext(ctx, fmt.Sprintf("PRAGMA max_page_count=%d", requestedPageLimit)).Scan(&pageLimit); err != nil {
		return nil, err
	}
	if pageLimit > requestedPageLimit || pageLimit < pageCount {
		return nil, fail("failed_precondition", "SQLite did not enforce configured page byte budget")
	}
	s.rdb, err = sql.Open("sqlite", s.readerDSN(o))
	if err != nil {
		return nil, err
	}
	s.rdb.SetMaxOpenConns(c.Readers)
	s.rdb.SetMaxIdleConns(c.Readers)
	if err = s.rdb.PingContext(ctx); err != nil {
		return nil, err
	}
	s.startMaintenance(ctx)
	return s, nil
}

func versionAtLeast(version string, want ...int) bool {
	parts := strings.SplitN(version, ".", 4)
	for i, w := range want {
		if i >= len(parts) {
			return false
		}
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return false
		}
		if n != w {
			return n > w
		}
	}
	return true
}

// Close waits for admitted calls, stops maintenance and releases the owner.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	// Maintenance admits its calls like any other, so it stops before Close
	// waits for the admitted ones.
	s.stopMaintenance()
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	var e error
	if s.rdb != nil {
		e = errors.Join(e, s.rdb.Close())
	}
	if s.writer != nil {
		e = errors.Join(e, s.writer.Close())
	}
	if s.wdb != nil {
		e = errors.Join(e, s.wdb.Close())
	}
	s.owner.close()
	return e
}

func (s *Store) scope(scope api.Scope) (api.Namespace, error) {
	n, ok := s.namespaces[scope.Namespace]
	if !ok || !key(scope.User) || !key(scope.Workspace) {
		return n, fail("invalid_argument", "registered namespace and exact user/workspace required")
	}
	return n, nil
}

// CheckScope reports whether scope names a registered namespace and a valid
// user and workspace.
func (s *Store) CheckScope(scope api.Scope) error {
	_, err := s.scope(scope)
	return err
}

// HasModule reports whether the scope's namespace uses the named data module.
func (s *Store) HasModule(scope api.Scope, module string) bool {
	n, err := s.scope(scope)
	if err != nil {
		return false
	}
	for _, id := range n.Modules {
		if id == module {
			return true
		}
	}
	return false
}

func collection(n api.Namespace, id string) (api.Collection, error) {
	for _, c := range n.Collections {
		if c.ID == id {
			return c, nil
		}
	}
	return api.Collection{}, fail("invalid_argument", "collection is not registered")
}

// admit bounds one call by the call budget and admits it as a reader or a
// writer. A caller that finds the writer queue full waits for room until its
// own deadline; it is never rejected only because others are ahead of it.
func (s *Store) admit(ctx context.Context, write bool) (context.Context, func(), error) {
	s.lifecycle.RLock()
	if s.closed.Load() {
		s.lifecycle.RUnlock()
		return nil, nil, fail("unavailable", "store closed")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.CallBudget)
	refuse := func(err error) (context.Context, func(), error) {
		if errors.Is(err, context.DeadlineExceeded) {
			s.timeouts.Add(1)
		}
		cancel()
		s.lifecycle.RUnlock()
		return nil, nil, err
	}
	start := time.Now()
	slot := s.reads
	if write {
		slot = s.queue
	}
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return refuse(ctx.Err())
	}
	if write {
		select {
		case s.gate <- struct{}{}:
		case <-ctx.Done():
			<-slot
			return refuse(ctx.Err())
		}
	}
	s.waitNS.Add(uint64(time.Since(start)))
	s.calls.Add(1)
	work := time.Now()
	return ctx, func() {
		s.sqlNS.Add(uint64(time.Since(work)))
		if write {
			<-s.gate
		}
		<-slot
		cancel()
		s.lifecycle.RUnlock()
	}, nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	var domain *api.Error
	if errors.As(err, &domain) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fail("deadline_exceeded", err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return fail("cancelled", err.Error())
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		switch se.Code() & 255 {
		case 5, 6:
			return fail("busy", err.Error())
		case 13:
			return fail("disk_full", err.Error())
		case 10:
			return fail("io_error", err.Error())
		case 11, 26:
			return fail("corrupt", err.Error())
		case 8:
			return fail("permission_denied", err.Error())
		case 19:
			return fail("conflict", "registered unique constraint rejected batch")
		}
	}
	return fail("internal", err.Error())
}

func (s *Store) token(n api.Namespace, rev int64) api.Token {
	return api.Token{DatabaseID: s.dbid, Schema: n.Schema, Revision: strconv.FormatInt(rev, 10)}
}

func (s *Store) checkToken(n api.Namespace, t api.Token, rev int64) error {
	if _, e := revision(t.Revision); e != nil {
		return e
	}
	if t != s.token(n, rev) {
		s.conflicts.Add(1)
		return fail("conflict", "database/schema/scope revision changed")
	}
	return nil
}

func (s *Store) walSize() int64 {
	var st unix.Stat_t
	if unix.Fstatat(s.owner.parent, s.owner.name+"-wal", &st, unix.AT_SYMLINK_NOFOLLOW) != nil {
		return 0
	}
	return st.Size
}

func (s *Store) diskCheck() error {
	var fs unix.Statfs_t
	if e := unix.Fstatfs(s.owner.parent, &fs); e != nil {
		return e
	}
	if fs.Bavail*uint64(fs.Bsize) < uint64(s.config.MinFreeBytes) {
		return fail("resource_exhausted", "free disk watermark reached")
	}
	if s.walSize() >= s.config.MaxWALBytes {
		return fail("resource_exhausted", "WAL admission watermark reached")
	}
	return nil
}
