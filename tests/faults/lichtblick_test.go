//go:build linux

package faults_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

// The Lichtblick launcher keeps its persistence documents in the daemon. This
// test calls the daemon with the requests that managed-storage.cjs sends, on the
// launcher's own deployed manifest: exact-key snapshots, family pages pinned by
// a token, compare-and-set batches with deletes, and receipt lookups.

func lichtblickManifest(t *testing.T) api.Manifest {
	t.Helper()
	raw, err := os.ReadFile("testdata/lichtblick-storage-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m api.Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func document(family, key string, value any) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{"family": family, "key": key, "value": value})
	return raw
}

func TestDaemonRoundTripForTheLichtblickPersistenceSurface(t *testing.T) {
	lichtblick := api.Scope{Namespace: "lichtblick", User: "operator", Workspace: "station"}
	daemonScope = lichtblick
	t.Cleanup(func() { daemonScope = scope })
	dir := privateDir(t)
	manifest := lichtblickManifest(t)
	d := startWithManifest(t, dir, true, "", manifest)

	read := func(queries ...api.Query) api.SnapshotResponse {
		t.Helper()
		out, err := d.client.Snapshot(deadline(t), "lb-read", api.SnapshotRequest{Scope: lichtblick, Queries: queries})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	// Exact keys that do not exist yet come back missing at version 0.
	empty := read(api.Query{Collection: "documents", Keys: []string{"layouts:local/main", "profile:default"}})
	for _, record := range empty.Results[0].Records {
		if !record.Missing || record.Version != "0" {
			t.Fatalf("absent document: %+v", record)
		}
	}
	// One batch commits the layouts and the selected pointer together.
	var mutations []api.Mutation
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("layouts:local/layout-%d", i)
		mutations = append(mutations, api.Mutation{Collection: "documents", Key: key, ExpectedVersion: "0", Data: document("layouts", fmt.Sprintf("local/layout-%d", i), map[string]any{"panels": i})})
	}
	mutations = append(mutations, api.Mutation{Collection: "documents", Key: "profile:default", ExpectedVersion: "0", Data: document("profile", "default", map[string]any{"theme": "dark"})})
	receipt, err := d.client.Batch(deadline(t), api.BatchRequest{Scope: lichtblick, Expected: empty.Token, RequestID: "lb-save-1", Mutations: mutations})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Durability != "sqlite-full" || receipt.RequestID != "lb-save-1" || len(receipt.Versions) != 6 || receipt.Token.Schema != "lichtblick.persistence.v1" {
		t.Fatalf("the launcher requires a durable receipt for its request: %+v", receipt)
	}
	for _, version := range receipt.Versions {
		if version.Version != receipt.Token.Revision || version.Collection != "documents" {
			t.Fatalf("receipt version: %+v", version)
		}
	}
	// A family page is pinned by the token of its first page.
	first := read(api.Query{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 2})
	if len(first.Results[0].Records) != 2 || first.Results[0].NextAfter == "" {
		t.Fatalf("first family page: %+v", first.Results[0])
	}
	second, err := d.client.Snapshot(deadline(t), "lb-page-2", api.SnapshotRequest{Scope: lichtblick, At: &first.Token,
		Queries: []api.Query{{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 2, After: first.Results[0].NextAfter}}})
	if err != nil || len(second.Results[0].Records) != 2 || second.Token != first.Token {
		t.Fatalf("second family page: %+v %v", second, err)
	}
	// Another writer changes the family: the pinned continuation must restart.
	current := read(api.Query{Collection: "documents", Keys: []string{"layouts:local/layout-0"}})
	if _, err = d.client.Batch(deadline(t), api.BatchRequest{Scope: lichtblick, Expected: current.Token, RequestID: "lb-save-2", Mutations: []api.Mutation{
		{Collection: "documents", Key: "layouts:local/layout-0", ExpectedVersion: current.Results[0].Records[0].Version, Data: document("layouts", "local/layout-0", map[string]any{"panels": 99})}}}); err != nil {
		t.Fatal(err)
	}
	_, err = d.client.Snapshot(deadline(t), "lb-page-3", api.SnapshotRequest{Scope: lichtblick, At: &first.Token,
		Queries: []api.Query{{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 2, After: second.Results[0].NextAfter}}})
	if code(err) != "conflict" {
		t.Fatalf("a pinned page survived a concurrent write: %v", err)
	}
	// Deleting keeps the version, so compare-and-set stays meaningful.
	before := read(api.Query{Collection: "documents", Keys: []string{"layouts:local/layout-4"}})
	deleted, err := d.client.Batch(deadline(t), api.BatchRequest{Scope: lichtblick, Expected: before.Token, RequestID: "lb-delete", Mutations: []api.Mutation{
		{Collection: "documents", Key: "layouts:local/layout-4", ExpectedVersion: before.Results[0].Records[0].Version, Delete: true}}})
	if err != nil || !deleted.Versions[0].Deleted {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	tomb := read(api.Query{Collection: "documents", Keys: []string{"layouts:local/layout-4"}})
	if !tomb.Results[0].Records[0].Deleted || tomb.Results[0].Records[0].Version != deleted.Token.Revision {
		t.Fatalf("tombstone must keep its version: %+v", tomb.Results[0].Records[0])
	}
	if live := read(api.Query{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 10}); len(live.Results[0].Records) != 4 {
		t.Fatalf("a deleted document stayed in its family: %d", len(live.Results[0].Records))
	}
	if _, err = d.client.Batch(deadline(t), api.BatchRequest{Scope: lichtblick, Expected: tomb.Token, RequestID: "lb-recreate", Mutations: []api.Mutation{
		{Collection: "documents", Key: "layouts:local/layout-4", ExpectedVersion: "0", Data: document("layouts", "local/layout-4", 1)}}}); code(err) != "conflict" {
		t.Fatalf("recreating over a tombstone with version 0: %v", err)
	}
	// Bounds of the launcher's manifest.
	big := document("layouts", "local/big", strings.Repeat("x", 2<<20))
	if _, err = d.client.Batch(deadline(t), api.BatchRequest{Scope: lichtblick, Expected: tomb.Token, RequestID: "lb-big", Mutations: []api.Mutation{{Collection: "documents", Key: "layouts:local/big", ExpectedVersion: "0", Data: big}}}); code(err) != "resource_exhausted" {
		t.Fatalf("a document above the record bound: %v", err)
	}
	if _, err = d.client.Snapshot(deadline(t), "lb-undeclared", api.SnapshotRequest{Scope: lichtblick, Queries: []api.Query{{Collection: "not-declared", Keys: []string{"x"}}}}); code(err) != "invalid_argument" {
		t.Fatalf("undeclared collection: %v", err)
	}
	forbidden := lichtblick
	forbidden.User = "someone-else"
	if _, err = d.client.Snapshot(deadline(t), "lb-forbidden", api.SnapshotRequest{Scope: forbidden, Queries: []api.Query{{Collection: "documents", Keys: []string{"x"}}}}); err == nil {
		t.Fatal("a scope outside the owner grant was served")
	}
	// A killed daemon restarts on the same database with every receipt.
	pre := read(api.Query{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 10})
	d.kill(t)
	d = startWithManifest(t, dir, false, "", manifest)
	again, err := d.client.Receipt(deadline(t), "lb-receipt", api.ReceiptRequest{Scope: lichtblick, RequestID: "lb-save-1"})
	if err != nil || again.Digest != receipt.Digest || again.Durability != "sqlite-full" {
		t.Fatalf("receipt after restart: %+v %v", again, err)
	}
	if _, err = d.client.Receipt(deadline(t), "lb-receipt-missing", api.ReceiptRequest{Scope: lichtblick, RequestID: "never-sent"}); code(err) != "not_found" {
		t.Fatalf("unknown request: %v", err)
	}
	post := read(api.Query{Collection: "documents", Index: "family", Equal: []json.RawMessage{json.RawMessage(`"layouts"`)}, Limit: 10})
	if fmt.Sprint(pre.Results[0].Records) != fmt.Sprint(post.Results[0].Records) || pre.Token != post.Token {
		t.Fatalf("documents changed across the restart")
	}
	if _, err = os.Stat(filepath.Join(dir, "fixture.db")); err != nil {
		t.Fatal(err)
	}
}
