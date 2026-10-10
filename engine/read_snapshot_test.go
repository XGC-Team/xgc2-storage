package engine

import (
	"context"
	"github.com/XGC-Team/xgc2-storage/api"
	"sync"
	"testing"
	"time"
)

func TestLocalReadSnapshotDoesNotBlockWriterOrMixRevisions(t *testing.T) {
	s, _, ctx := setup(t)
	var escaped context.Context
	err := s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
		escaped = view
		before := snapshot(t, s, view, "a")
		request := api.BatchRequest{Scope: testScope, Expected: before.Token, RequestID: "concurrent-write",
			Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a"}`)}}
		if _, err := s.Batch(view, request); code(err) != "failed_precondition" {
			t.Fatalf("write borrowed a read view: %v", err)
		}
		// A separate writer must finish while the old read view remains open.
		if _, err := s.Batch(ctx, request); err != nil {
			t.Fatalf("reader blocked writer: %v", err)
		}
		after, err := s.Snapshot(view, api.SnapshotRequest{Scope: testScope, At: &before.Token,
			Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
		if err != nil || after.Token != before.Token || !after.Results[0].Records[0].Missing {
			t.Fatalf("view mixed revisions: %+v %v", after, err)
		}
		if _, err := s.Receipt(view, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); code(err) != "not_found" {
			t.Fatalf("receipt escaped snapshot: %v", err)
		}
		other := testScope
		other.Workspace = "other"
		_, err = s.Snapshot(view, api.SnapshotRequest{Scope: other, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
		if code(err) != "failed_precondition" {
			t.Fatalf("cross-scope view: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, ctx, "a").Results[0].Records[0].Missing {
		t.Fatal("write was lost")
	}
	_, err = s.Snapshot(escaped, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
	if code(err) != "failed_precondition" {
		t.Fatalf("escaped closed view accepted: %v", err)
	}
}

func TestReadCapacityWaitsWithinCallerDeadline(t *testing.T) {
	s, _, ctx := setup(t)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	entered := make(chan struct{}, s.config.Readers)
	holders := make(chan error, s.config.Readers)
	for range s.config.Readers {
		go func() {
			holders <- s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
				// Nested and ordinary typed reads borrow this same already-held lease.
				if err := s.WithReadSnapshot(view, testScope, func(nested context.Context) error {
					_, err := s.Snapshot(nested, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
					return err
				}); err != nil {
					return err
				}
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
	for range s.config.Readers {
		select {
		case <-entered:
		case err := <-holders:
			t.Fatalf("read holder failed: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Queue-only preparation must not acquire a reader, even when nested.
	var emptyView context.Context
	if err := s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
		emptyView = view
		return s.WithReadSnapshot(view, testScope, func(context.Context) error { return nil })
	}); err != nil {
		t.Fatalf("zero-read view occupied reader capacity: %v", err)
	}
	if _, err := s.Snapshot(emptyView, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}}); code(err) != "failed_precondition" {
		t.Fatalf("escaped zero-read view accepted: %v", err)
	}
	timed, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	_, err := s.Snapshot(timed, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
	cancel()
	if code(err) != "deadline_exceeded" {
		t.Fatalf("saturated ordinary read must wait for its deadline: %v", err)
	}
	waited := make(chan error, 2)
	go func() {
		waited <- s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
			_, err := s.Snapshot(view, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
			return err
		})
	}()
	go func() {
		_, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
		waited <- err
	}()
	select {
	case err := <-waited:
		t.Fatalf("reader capacity was bypassed or rejected instead of waiting: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	for range s.config.Readers {
		if err := <-holders; err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := <-waited; err != nil {
			t.Fatal(err)
		}
	}
	if s.reader.Stats().InUse != 0 {
		t.Fatal("read admission leaked a reader after cancellation/completion")
	}
}
