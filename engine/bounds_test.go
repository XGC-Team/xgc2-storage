package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	rows, e := s.rdb.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT r.key,r.version,r.deleted,r.data FROM lookups l JOIN records r ON r.scope=l.scope AND r.collection=l.collection AND r.key=l.key WHERE l.scope=? AND l.collection=? AND l.index_name=? AND l.index_value=? AND l.key>? ORDER BY l.key LIMIT ?", ScopeID(testScope), "state", "unique", `[{"number":"1"}]`, "", 2)
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
func TestRestoredDatabaseActualPageByteBudget(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(fmt.Sprintf("oversized-%t", oversized), func(t *testing.T) {
			ctx := budget(t)
			m := manifest()
			m.Namespaces[0].Collections[0].MaxRecordBytes = 2 << 20
			m.Namespaces[0].Collections[0].MaxBytes = 8 << 20
			dir := t.TempDir()
			os.Chmod(dir, 0700)
			path := filepath.Join(dir, "restore.db")
			config := Config{Path: path, Create: true, Manifest: m, MaxDBBytes: 16 << 20}
			s, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			if oversized {
				before := snapshot(t, s, ctx, "a")
				raw, _ := json.Marshal(map[string]string{"value": strings.Repeat("x", 1280<<10)})
				_, err = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: before.Token, RequestID: "seed-large", Mutations: []api.Mutation{{Collection: "state", Key: "a", ExpectedVersion: "0", Data: raw}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			// A restored SQLite file can legitimately use a different page size.
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"PRAGMA journal_mode=DELETE", "PRAGMA page_size=65536", "VACUUM"} {
				if _, err = db.ExecContext(ctx, query); err != nil {
					db.Close()
					t.Fatal(err)
				}
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			config.Create, config.MaxDBBytes = false, 1<<20
			if !oversized {
				config.MaxDBBytes = 2 << 20
			}
			s, err = Open(ctx, config)
			if oversized {
				if s != nil {
					s.Close()
				}
				if code(err) != "resource_exhausted" {
					t.Fatalf("oversized restore admitted: %v", err)
				}
				config.MaxDBBytes = 16 << 20
				retry, retryError := Open(ctx, config)
				if retryError != nil {
					t.Fatalf("rejected startup leaked owner or database handles: %v", retryError)
				}
				retry.Close()
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, s, ctx, "a")
			raw, _ := json.Marshal(map[string]string{"value": strings.Repeat("x", 1280<<10)})
			_, err = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: before.Token, RequestID: "grow", Mutations: []api.Mutation{{Collection: "state", Key: "a", ExpectedVersion: "0", Data: raw}}})
			if code(err) != "disk_full" {
				s.Close()
				t.Fatalf("large pages escaped byte budget: %v", err)
			}
			if snapshot(t, s, ctx, "a").Token != before.Token {
				t.Fatal("failed growth changed durable revision")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			st, err := os.Stat(path)
			if err != nil || st.Size() > config.MaxDBBytes {
				t.Fatalf("physical page budget escaped: %v %v", st, err)
			}
		})
	}
}
