package engine

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

type readSnapshotKey struct{}
type readSnapshot struct {
	store   *Store
	scope   api.Scope
	ctx     context.Context
	conn    *sql.Conn
	tx      *sql.Tx
	release func()
	mu      sync.Mutex
	closed  bool
}

type rowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) readRows(ctx context.Context, scope api.Scope) (context.Context, rowReader, func(), error) {
	if view := snapshotContext(ctx); view != nil {
		unlock, err := view.borrow(ctx, s, scope)
		if err != nil {
			return ctx, nil, nil, err
		}
		return ctx, view.tx, unlock, nil
	}
	ctx, release, err := s.beginCall(ctx, false)
	return ctx, s.reader, release, err
}

func snapshotContext(ctx context.Context) *readSnapshot {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(readSnapshotKey{}).(*readSnapshot)
	return v
}

// WithReadSnapshot lends only a context for typed reads. The transaction and
// database stay inside Storage. WAL readers do not acquire the writer gate;
// plans commit separately, with their existing record and ownership guards.
func (s *Store) WithReadSnapshot(ctx context.Context, scope api.Scope, fn func(context.Context) error) error {
	if fn == nil {
		return fail("invalid_argument", "read snapshot callback required")
	}
	if _, err := s.scope(scope); err != nil {
		return err
	}
	if prior := snapshotContext(ctx); prior != nil {
		prior.mu.Lock()
		err := prior.validate(ctx, s, scope)
		prior.mu.Unlock()
		if err != nil {
			return err
		}
		return fn(ctx)
	}
	if s.closed.Load() {
		return fail("unavailable", "store closed")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 {
		return fail("invalid_argument", "finite caller deadline required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.MaxCallTime)
	defer cancel()
	v := &readSnapshot{store: s, scope: scope, ctx: ctx}
	defer func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.closed = true
		if v.tx != nil {
			_ = v.tx.Rollback()
			_ = v.conn.Close()
			v.release()
		}
	}()
	return fn(context.WithValue(ctx, readSnapshotKey{}, v))
}

// One borrowed view may serialize its own reads, never other workflows.
func (v *readSnapshot) borrow(ctx context.Context, s *Store, scope api.Scope) (func(), error) {
	v.mu.Lock()
	if err := v.validate(ctx, s, scope); err != nil {
		v.mu.Unlock()
		return nil, err
	}
	if v.tx == nil {
		_, release, err := s.beginCall(ctx, false)
		if err != nil {
			v.mu.Unlock()
			return nil, classify(err)
		}
		conn, err := s.reader.Conn(ctx)
		if err != nil {
			release()
			v.mu.Unlock()
			return nil, classify(err)
		}
		// The view owns the transaction lifetime, not the first typed call's
		// shorter context (which its client may cancel between reads).
		tx, err := conn.BeginTx(v.ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			_ = conn.Close()
			release()
			v.mu.Unlock()
			return nil, classify(err)
		}
		v.conn, v.tx, v.release = conn, tx, release
	}
	return v.mu.Unlock, nil
}

// validate requires v.mu but does not acquire a reader for an empty/nested view.
func (v *readSnapshot) validate(ctx context.Context, s *Store, scope api.Scope) error {
	if v.closed || v.store != s || v.scope != scope {
		return fail("failed_precondition", "read snapshot is closed or belongs to another owner/scope")
	}
	if err := ctx.Err(); err != nil {
		return classify(err)
	}
	if err := v.ctx.Err(); err != nil {
		return classify(err)
	}
	return nil
}
