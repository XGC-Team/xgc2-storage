package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"golang.org/x/sys/unix"
	"modernc.org/sqlite"
)

type Config struct {
	Path         string
	Create       bool
	Manifest     api.Manifest
	Readers      int
	WriterQueue  int
	MaxCallTime  time.Duration
	MaxDBBytes   int64
	MaxWALBytes  int64
	MinFreeBytes int64
	Modules      []DataModule
}

// DataModule is trusted storage-owned compiled code, never received over RPC.
type DataModule struct {
	Spec       api.Module
	Initialize func(context.Context, *sql.Tx) error
	// Deploy applies explicit owner-supplied deployment facts before RPC starts.
	// It is compiled code, never an operation supplied by a data caller.
	Deploy  func(context.Context, *sql.Tx) error
	Execute func(context.Context, *sql.Tx, string, string, json.RawMessage) (json.RawMessage, error)
}
type Store struct {
	config                      Config
	owner                       *owner
	writer, reader              *sql.DB
	dbid, engine                string
	namespaces                  map[string]api.Namespace
	modules                     map[string]DataModule
	writers, gate, reads        chan struct{}
	closed                      atomic.Bool
	lifecycle                   sync.RWMutex
	calls, conflicts, overloads atomic.Uint64
	waitNS, sqlNS               atomic.Uint64
}

const ddl = `
CREATE TABLE storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),format TEXT NOT NULL,database_id TEXT NOT NULL,manifest_hash TEXT NOT NULL);
CREATE TABLE scopes(scope TEXT PRIMARY KEY,namespace TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0));
CREATE INDEX scopes_namespace ON scopes(namespace);
CREATE TABLE usage(scope TEXT NOT NULL,collection TEXT NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(scope,collection));
CREATE TABLE records(scope TEXT NOT NULL,collection TEXT NOT NULL,key TEXT NOT NULL,version INTEGER NOT NULL,deleted INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(scope,collection,key));
CREATE TABLE lookups(scope TEXT NOT NULL,collection TEXT NOT NULL,index_name TEXT NOT NULL,index_value TEXT NOT NULL,key TEXT NOT NULL,unique_value TEXT,PRIMARY KEY(scope,collection,index_name,index_value,key),UNIQUE(scope,collection,index_name,unique_value));
CREATE TABLE receipts(scope TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,expires INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,request_id));
CREATE INDEX receipts_expiry ON receipts(expires);
CREATE TABLE named_results(scope TEXT NOT NULL,request_id TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,request_id),FOREIGN KEY(scope,request_id) REFERENCES receipts(scope,request_id) ON DELETE CASCADE);
`

func Open(ctx context.Context, c Config) (s *Store, err error) {
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
	if c.Readers == 0 {
		c.Readers = 4
	}
	if c.WriterQueue == 0 {
		c.WriterQueue = 32
	}
	if c.MaxCallTime == 0 {
		c.MaxCallTime = 30 * time.Second
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
	if c.Readers < 1 || c.Readers > 16 || c.WriterQueue < 1 || c.WriterQueue > 256 || c.MaxCallTime <= 0 || c.MaxCallTime > time.Minute || c.MaxDBBytes < 1<<20 || c.MaxWALBytes < 1<<20 || c.MinFreeBytes < 1<<20 {
		return nil, fail("invalid_argument", "invalid execution/disk limits")
	}
	o, e := acquire(c.Path, c.Create)
	if e != nil {
		return nil, e
	}
	s = &Store{config: c, owner: o, namespaces: map[string]api.Namespace{}, modules: map[string]DataModule{}, writers: make(chan struct{}, c.WriterQueue+1), gate: make(chan struct{}, 1), reads: make(chan struct{}, c.Readers)}
	opened := s
	defer func() {
		if err != nil {
			opened.Close()
		}
	}()
	for _, n := range c.Manifest.Namespaces {
		s.namespaces[n.ID] = n
	}
	for _, m := range c.Modules {
		if m.Initialize == nil || m.Execute == nil || s.modules[m.Spec.ID].Execute != nil {
			return nil, fail("invalid_argument", "unique compiled module initializer/executor required")
		}
		s.modules[m.Spec.ID] = m
	}
	for _, n := range c.Manifest.Namespaces {
		for _, registered := range n.Modules {
			compiled, ok := s.modules[registered.ID]
			if !ok || hash(compiled.Spec) != hash(registered) {
				return nil, fail("failed_precondition", "named module differs from compiled schema/operations")
			}
		}
	}
	dsn := (&url.URL{Scheme: "file", Path: o.path(), RawQuery: "mode=rw&_pragma=busy_timeout(0)&_pragma=foreign_keys(1)"}).String()
	s.writer, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	s.writer.SetMaxOpenConns(1)
	s.writer.SetMaxIdleConns(1)
	if err = s.writer.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&s.engine); err != nil {
		return nil, err
	}
	if s.engine != "3.51.3" {
		return nil, fail("failed_precondition", "pinned SQLite 3.51.3 engine required")
	}
	if c.Create {
		tx, e := s.writer.BeginTx(ctx, nil)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, ddl); err != nil {
			return nil, err
		}
		initialized := map[string]bool{}
		for _, n := range c.Manifest.Namespaces {
			for _, m := range n.Modules {
				if !initialized[m.ID] {
					if err = s.modules[m.ID].Initialize(ctx, tx); err != nil {
						return nil, err
					}
					initialized[m.ID] = true
				}
			}
		}
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return nil, err
		}
		s.dbid = hex.EncodeToString(id[:])
		if _, err = tx.ExecContext(ctx, "INSERT INTO storage_meta VALUES(1,?,?,?)", "storage-v1", s.dbid, hash(c.Manifest)); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	} else {
		var format, digest string
		if err = s.writer.QueryRowContext(ctx, "SELECT format,database_id,manifest_hash FROM storage_meta WHERE id=1").Scan(&format, &s.dbid, &digest); err != nil {
			return nil, fail("failed_precondition", "not a storage-v1 database; explicit new-data rebuild required")
		}
		if format != "storage-v1" || digest != hash(c.Manifest) {
			return nil, fail("failed_precondition", "manifest mismatch; exact deployed schema or explicit new-data rebuild required")
		}
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
	var journal string
	if err = s.writer.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return nil, err
	}
	if journal != "wal" {
		return nil, fail("failed_precondition", "WAL unavailable")
	}
	for _, statement := range []string{"PRAGMA synchronous=FULL", "PRAGMA temp_store=MEMORY", "PRAGMA cache_size=-4096", "PRAGMA wal_autocheckpoint=256", "PRAGMA journal_size_limit=1048576"} {
		if _, err = s.writer.ExecContext(ctx, statement); err != nil {
			return nil, err
		}
	}
	deployed := map[string]bool{}
	for _, namespace := range c.Manifest.Namespaces {
		for _, registered := range namespace.Modules {
			module := s.modules[registered.ID]
			if deployed[registered.ID] || module.Deploy == nil {
				continue
			}
			deployed[registered.ID] = true
			tx, e := s.writer.BeginTx(ctx, nil)
			if e != nil {
				return nil, e
			}
			if e = module.Deploy(ctx, tx); e != nil {
				tx.Rollback()
				return nil, e
			}
			if e = tx.Commit(); e != nil {
				return nil, e
			}
		}
	}
	readDSN := (&url.URL{Scheme: "file", Path: o.path(), RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(0)&_pragma=cache_size(-2048)&_pragma=temp_store(2)"}).String()
	s.reader, err = sql.Open("sqlite", readDSN)
	if err != nil {
		return nil, err
	}
	s.reader.SetMaxOpenConns(c.Readers)
	s.reader.SetMaxIdleConns(c.Readers)
	if err = s.reader.PingContext(ctx); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.closed.Swap(true) {
		return nil
	}
	var e error
	if s.reader != nil {
		e = errors.Join(e, s.reader.Close())
	}
	if s.writer != nil {
		e = errors.Join(e, s.writer.Close())
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
func collection(n api.Namespace, id string) (api.Collection, error) {
	for _, c := range n.Collections {
		if c.ID == id {
			return c, nil
		}
	}
	return api.Collection{}, fail("invalid_argument", "collection is not registered")
}
func (s *Store) beginCall(ctx context.Context, write bool) (context.Context, func(), error) {
	s.lifecycle.RLock()
	if s.closed.Load() {
		s.lifecycle.RUnlock()
		return nil, nil, fail("unavailable", "store closed")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 {
		s.lifecycle.RUnlock()
		return nil, nil, fail("invalid_argument", "finite caller deadline required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.MaxCallTime)
	slot := s.reads
	if write {
		slot = s.writers
	}
	select {
	case slot <- struct{}{}:
	default:
		s.overloads.Add(1)
		cancel()
		s.lifecycle.RUnlock()
		return nil, nil, fail("resource_exhausted", "storage execution capacity exhausted")
	}
	start := time.Now()
	if write {
		select {
		case s.gate <- struct{}{}:
		case <-ctx.Done():
			<-slot
			cancel()
			s.lifecycle.RUnlock()
			return nil, nil, ctx.Err()
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
func (s *Store) diskCheck() error {
	var fs unix.Statfs_t
	if e := unix.Fstatfs(s.owner.parent, &fs); e != nil {
		return e
	}
	if fs.Bavail*uint64(fs.Bsize) < uint64(s.config.MinFreeBytes) {
		return fail("resource_exhausted", "free disk watermark reached")
	}
	var st unix.Stat_t
	if e := unix.Fstatat(s.owner.parent, s.owner.name+"-wal", &st, unix.AT_SYMLINK_NOFOLLOW); e == nil && st.Size >= s.config.MaxWALBytes {
		return fail("resource_exhausted", "WAL admission watermark reached")
	}
	return nil
}
