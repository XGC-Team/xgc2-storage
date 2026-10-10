package engine

import (
	"context"
	"database/sql"
	"sync"

	"github.com/XGC-Team/xgc2-storage/api"
)

type readSnapshotKey struct{}
type readSnapshot struct {
	store  *Store
	scope  api.Scope
	tx     *sql.Tx
	mu     sync.Mutex
	closed bool
}

type rowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) readRows(ctx context.Context, scope api.Scope) (context.Context, rowReader, func(), error) {
	if view := snapshotContext(ctx); view != nil {
		unlock, err := view.borrow(ctx, s, scope)
		return ctx, view.tx, unlock, err
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
		unlock, err := prior.borrow(ctx, s, scope)
		if err != nil {
			return err
		}
		unlock()
		return fn(ctx)
	}
	ctx, release, err := s.beginCall(ctx, false)
	if err != nil {
		return classify(err)
	}
	defer release()
	tx, err := s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return classify(err)
	}
	v := &readSnapshot{store: s, scope: scope, tx: tx}
	defer func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.closed = true
		_ = tx.Rollback()
	}()
	return fn(context.WithValue(ctx, readSnapshotKey{}, v))
}

// One borrowed view may serialize its own reads, never other workflows.
func (v *readSnapshot) borrow(ctx context.Context, s *Store, scope api.Scope) (func(), error) {
	v.mu.Lock()
	if v.closed || v.store != s || v.scope != scope {
		v.mu.Unlock()
		return nil, fail("failed_precondition", "read snapshot is closed or belongs to another owner/scope")
	}
	if err := ctx.Err(); err != nil {
		v.mu.Unlock()
		return nil, classify(err)
	}
	return v.mu.Unlock, nil
}
