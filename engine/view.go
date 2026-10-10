package engine

import (
	"context"
	"sync"

	"github.com/XGC-Team/xgc2-storage/api"
)

type readViewKey struct{}

// readView fences a series of reads to one scope revision. It holds no
// connection: every read inside it takes a reader only for its own query, so
// the consumer may compute between reads without occupying a reader slot. A
// write that lands in between makes the next read fail with a conflict and the
// consumer restarts the view.
type readView struct {
	mu     sync.Mutex
	scope  api.Scope
	token  *api.Token
	closed bool
}

func viewOf(ctx context.Context) *readView {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(readViewKey{}).(*readView)
	return v
}

// WithReadSnapshot lends a context whose Snapshot calls all observe the same
// revision of one scope, or fail with a conflict. Mutations through it are
// refused. The view is unusable once fn returns.
func (s *Store) WithReadSnapshot(ctx context.Context, scope api.Scope, fn func(context.Context) error) error {
	if fn == nil {
		return fail("invalid_argument", "read snapshot callback required")
	}
	if _, err := s.scope(scope); err != nil {
		return err
	}
	if prior := viewOf(ctx); prior != nil {
		prior.mu.Lock()
		err := prior.validate(scope)
		prior.mu.Unlock()
		if err != nil {
			return err
		}
		return fn(ctx)
	}
	if s.closed.Load() {
		return fail("unavailable", "store closed")
	}
	v := &readView{scope: scope}
	defer func() {
		v.mu.Lock()
		v.closed = true
		v.mu.Unlock()
	}()
	return fn(context.WithValue(ctx, readViewKey{}, v))
}

func (v *readView) validate(scope api.Scope) error {
	if v.closed || v.scope != scope {
		return fail("failed_precondition", "read snapshot is closed or belongs to another scope")
	}
	return nil
}

// begin serializes the view's own reads and fences the request to the pinned
// revision. The returned function releases the view.
func (v *readView) begin(scope api.Scope, r *api.SnapshotRequest) (func(), error) {
	v.mu.Lock()
	if err := v.validate(scope); err != nil {
		v.mu.Unlock()
		return nil, err
	}
	if v.token != nil {
		if r.At != nil && *r.At != *v.token {
			v.mu.Unlock()
			return nil, fail("conflict", "read snapshot request pins another revision")
		}
		r.At = v.token
	}
	return v.mu.Unlock, nil
}

// pin records the revision of the first successful read.
func (v *readView) pin(out api.SnapshotResponse, err error) {
	if err == nil && v.token == nil {
		token := out.Token
		v.token = &token
	}
}
