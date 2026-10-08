package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestFinalBatchQuotaSwapAndExponentBound(t *testing.T) {
	ctx := budget(t)
	m := manifest()
	m.Namespaces[0].Collections[0].MaxBytes = 20
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	s, e := Open(ctx, Config{Path: filepath.Join(dir, "quota.db"), Create: true, Manifest: m})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	read := snapshot(t, s, ctx, "a", "b")
	_, e = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "initial", Mutations: []api.Mutation{mutation("state", "a", "0", `{"x":""}`), mutation("state", "b", "0", `{"x":"xx"}`)}})
	if e != nil {
		t.Fatal(e)
	}
	read = snapshot(t, s, ctx, "a", "b")
	_, e = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "swap", Mutations: []api.Mutation{mutation("state", "a", "1", `{"x":"xx"}`), mutation("state", "b", "1", `{"x":""}`)}})
	if e != nil {
		t.Fatalf("final valid quota swap rejected: %v", e)
	}
	read = snapshot(t, s, ctx, "a")
	_, e = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "exponent", Mutations: []api.Mutation{mutation("state", "a", "2", `{"name":1e1000000000}`)}})
	if code(e) != "resource_exhausted" {
		t.Fatalf("unbounded exponent %v", e)
	}
}
func TestEqualityQueryPlanUsesBoundedOrderedIndex(t *testing.T) {
	s, _, ctx := setup(t)
	read := snapshot(t, s, ctx, "a")
	_, e := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "index-record", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":1.0}`)}})
	if e != nil {
		t.Fatal(e)
	}
	result, e := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{json.RawMessage(`1e0`)}, Limit: 1}}})
	if e != nil || len(result.Results[0].Records) != 1 {
		t.Fatalf("numeric indexed query %v %v", result, e)
	}
	rows, e := s.reader.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT r.key,r.version,r.deleted,r.data FROM lookups l JOIN records r ON r.scope=l.scope AND r.collection=l.collection AND r.key=l.key WHERE l.scope=? AND l.collection=? AND l.index_name=? AND l.index_value=? AND l.key>? ORDER BY l.key LIMIT ?", scopeID(testScope), "state", "unique", `[{"number":"1"}]`, "", 2)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if e = rows.Scan(&id, &parent, &unused, &detail); e != nil {
			t.Fatal(e)
		}
		if strings.Contains(detail, "TEMP B-TREE") || strings.Contains(detail, "LIST SUBQUERY") {
			t.Fatalf("unbounded index page plan: %s", detail)
		}
	}
}
func BenchmarkAtomicDocumentCommit(b *testing.B) {
	dir, e := os.MkdirTemp("", "storage-bench-")
	if e != nil {
		b.Fatal(e)
	}
	defer os.RemoveAll(dir)
	m := manifest()
	m.Namespaces[0].MaxReceipts = 1000000
	finite, c := context.WithTimeout(context.Background(), 10*time.Minute)
	defer c()
	s, e := Open(finite, Config{Path: filepath.Join(dir, "bench.db"), Create: true, Manifest: m})
	if e != nil {
		b.Fatal(e)
	}
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		read, e := s.Snapshot(finite, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
		if e != nil {
			b.Fatal(e)
		}
		_, e = s.Batch(finite, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: fmt.Sprintf("commit-%d", i), Mutations: []api.Mutation{mutation("state", "a", read.Results[0].Records[0].Version, `{"name":"state","value":1}`)}})
		if e != nil {
			b.Fatal(e)
		}
	}
}
