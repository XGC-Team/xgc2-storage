package engine

import (
	"context"
	"github.com/XGC-Team/xgc2-storage/api"
	"testing"
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
