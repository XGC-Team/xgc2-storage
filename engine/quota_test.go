package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func usageOf(t *testing.T, s *Store, collection string) (records, bytes, tombstones int64) {
	t.Helper()
	err := s.Read(budget(t), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT records,bytes,tombstones FROM usage WHERE scope=? AND collection=?", ScopeID(testScope), collection).Scan(&records, &bytes, &tombstones)
	})
	if err != nil && code(err) != "internal" {
		t.Fatal(err)
	}
	return
}

func countRows(t *testing.T, s *Store, query string, args ...any) (n int64) {
	t.Helper()
	if err := s.Read(budget(t), func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, query, args...).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func smallStore(t *testing.T, maxRecords int, mutate func(*Config)) (*Store, context.Context) {
	t.Helper()
	ctx := budget(t)
	m := manifest()
	m.Namespaces[0].Collections[1].MaxRecords = maxRecords
	m.Namespaces[0].Collections[1].MaxBytes = 1024
	c := Config{Path: filepath.Join(t.TempDir(), "quota.db"), Create: true, Manifest: m}
	os.Chmod(filepath.Dir(c.Path), 0700)
	if mutate != nil {
		mutate(&c)
	}
	s, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, ctx
}

func put(t *testing.T, s *Store, ctx context.Context, id string, mutations ...api.Mutation) (api.Receipt, error) {
	t.Helper()
	read, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Limit: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	return s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: id, Mutations: mutations})
}

func TestQuotaCountsLiveRecordsAndADeleteFreesIt(t *testing.T) {
	s, ctx := smallStore(t, 3, nil)
	var last api.Receipt
	var err error
	for i := 0; i < 3; i++ {
		last, err = put(t, s, ctx, fmt.Sprintf("fill-%d", i), mutation("events", fmt.Sprint("k", i), "0", `{"n":1}`))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = put(t, s, ctx, "overflow", mutation("events", "k3", "0", `{"n":1}`)); code(err) != "resource_exhausted" {
		t.Fatalf("fourth live record admitted: %v", err)
	}
	records, bytes, tombstones := usageOf(t, s, "events")
	if records != 3 || tombstones != 0 || bytes != 3*int64(len(`{"n":1}`)+2) {
		t.Fatalf("live usage: records=%d bytes=%d tombstones=%d", records, bytes, tombstones)
	}
	// Deleting really frees the quota: the same key set can turn over forever.
	if _, err = put(t, s, ctx, "delete", api.Mutation{Collection: "events", Key: "k0", ExpectedVersion: "1", Delete: true}); err != nil {
		t.Fatal(err)
	}
	records, bytes, tombstones = usageOf(t, s, "events")
	if records != 2 || tombstones != 1 || bytes != 2*int64(len(`{"n":1}`)+2) {
		t.Fatalf("delete did not free quota: records=%d bytes=%d tombstones=%d", records, bytes, tombstones)
	}
	if last, err = put(t, s, ctx, "reuse", mutation("events", "k3", "0", `{"n":1}`)); err != nil {
		t.Fatalf("freed quota not reusable: %v", err)
	}
	// Bytes are bounded on live data too: a large update fails, a shrink frees.
	big := fmt.Sprintf(`{"padding":"%0*d"}`, 1100, 0)
	if _, err = put(t, s, ctx, "too-big", mutation("events", "k1", "2", big)); code(err) != "resource_exhausted" {
		t.Fatalf("live byte quota not enforced: %v", err)
	}
	_ = last
}

func TestTombstonesKeepCASVersionsButAreBounded(t *testing.T) {
	s, ctx := smallStore(t, 3, nil)
	for i := 0; i < 6; i++ {
		key := fmt.Sprint("k", i)
		created, err := put(t, s, ctx, "create-"+key, mutation("events", key, "0", `{"n":1}`))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = put(t, s, ctx, "drop-"+key, api.Mutation{Collection: "events", Key: key, ExpectedVersion: created.Token.Revision, Delete: true}); err != nil {
			t.Fatal(err)
		}
	}
	records, _, tombstones := usageOf(t, s, "events")
	rows := countRows(t, s, "SELECT count(*) FROM records WHERE scope=? AND collection='events' AND deleted=1", ScopeID(testScope))
	if records != 0 || tombstones != 3 || rows != 3 {
		t.Fatalf("tombstones are not bounded: records=%d counter=%d rows=%d", records, tombstones, rows)
	}
	// The oldest were dropped: their key is plainly absent again. The newest
	// still demands its tombstone version, so a stale create cannot slip in.
	read, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Keys: []string{"k0", "k5"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !read.Results[0].Records[0].Missing || read.Results[0].Records[1].Missing || !read.Results[0].Records[1].Deleted {
		t.Fatalf("unexpected tombstone retention: %+v", read.Results[0].Records)
	}
	if _, err = put(t, s, ctx, "aba", mutation("events", "k5", "0", `{"n":2}`)); code(err) != "conflict" {
		t.Fatalf("recreating a retained tombstone with version 0: %v", err)
	}
	if _, err = put(t, s, ctx, "recreate-old", mutation("events", "k0", "0", `{"n":2}`)); err != nil {
		t.Fatalf("a dropped tombstone must leave the key creatable: %v", err)
	}
}

func receiptCount(t *testing.T, s *Store) int64 {
	t.Helper()
	return countRows(t, s, "SELECT receipts FROM scopes WHERE scope=?", ScopeID(testScope))
}

func TestReceiptsExistOnlyForRequestsWithAnIDAndAreCounted(t *testing.T) {
	s, ctx := smallStore(t, 100, func(c *Config) { c.Manifest.Namespaces[0].MaxReceipts = 2 })
	// An in-process caller that needs no replay protection leaves nothing behind.
	for i := 0; i < 5; i++ {
		if _, err := put(t, s, ctx, "", mutation("events", fmt.Sprint("quiet", i), "0", `{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if n := countRows(t, s, "SELECT count(*) FROM receipts"); n != 0 || receiptCount(t, s) != 0 {
		t.Fatalf("request-less batches stored %d receipts", n)
	}
	first, err := put(t, s, ctx, "with-id-1", mutation("events", "loud1", "0", `{"n":1}`))
	if err != nil || first.RequestID != "with-id-1" || first.Digest == "" {
		t.Fatalf("identified batch: %+v %v", first, err)
	}
	// A replay is the identical request, token included.
	again, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: api.Token{DatabaseID: first.Token.DatabaseID, Schema: first.Token.Schema, Revision: "5"}, RequestID: "with-id-1", Mutations: []api.Mutation{mutation("events", "loud1", "0", `{"n":1}`)}})
	if err != nil || again.Digest != first.Digest || receiptCount(t, s) != 1 {
		t.Fatalf("replay changed the counter or the outcome: %+v %v counter=%d", again, err, receiptCount(t, s))
	}
	if _, err = put(t, s, ctx, "with-id-2", mutation("events", "loud2", "0", `{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = put(t, s, ctx, "with-id-3", mutation("events", "loud3", "0", `{"n":1}`)); code(err) != "resource_exhausted" {
		t.Fatalf("receipt quota admitted a third identified request: %v", err)
	}
	// The quota is on retained receipts only: the same write without an id passes.
	if _, err = put(t, s, ctx, "", mutation("events", "loud3", "0", `{"n":1}`)); err != nil {
		t.Fatalf("a request without an id must not pay the receipt quota: %v", err)
	}
	if got, want := receiptCount(t, s), countRows(t, s, "SELECT count(*) FROM receipts"); got != want || got != 2 {
		t.Fatalf("counter %d disagrees with the table %d", got, want)
	}
}

func TestExpiredReceiptsReleaseTheirCounter(t *testing.T) {
	s, ctx := smallStore(t, 100, func(c *Config) {
		c.Manifest.Namespaces[0].MaxReceipts = 2
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
	})
	for i := 0; i < 2; i++ {
		if _, err := put(t, s, ctx, fmt.Sprint("r", i), mutation("events", fmt.Sprint("k", i), "0", `{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	// The expiry timer fires by itself; nothing in this test prunes.
	deadline := time.Now().Add(6 * time.Second)
	for receiptCount(t, s) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("expired receipts were not pruned by the scheduled maintenance")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := put(t, s, ctx, "after", mutation("events", "k9", "0", `{"n":1}`)); err != nil {
		t.Fatalf("released quota not reusable: %v", err)
	}
	stats, _ := s.Stats()
	if stats.ReceiptsPruned != 2 {
		t.Fatalf("pruned %d receipts", stats.ReceiptsPruned)
	}
}
