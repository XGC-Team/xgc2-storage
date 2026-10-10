package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func manifest() api.Manifest {
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "test", Owner: "tests", Schema: "test.v1", MaxScopes: 8, MaxReceipts: 1000, ReceiptTTLSeconds: 3600, Collections: []api.Collection{
		{ID: "state", MaxRecordBytes: 4096, MaxRecords: 2000, MaxBytes: 1 << 20, Retention: "test owner", Recovery: "consistent backup", Indexes: []api.Index{{ID: "unique", Fields: []string{"name"}, Unique: true}}},
		{ID: "events", MaxRecordBytes: 4096, MaxRecords: 2000, MaxBytes: 1 << 20, Retention: "test owner", Recovery: "consistent backup"},
	}}}}
}
func budget(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func setup(t *testing.T) (*Store, Config, context.Context) {
	t.Helper()
	ctx := budget(t)
	c := Config{Path: filepath.Join(t.TempDir(), "fixture.db"), Create: true, Manifest: manifest()}
	if e := os.Chmod(filepath.Dir(c.Path), 0700); e != nil {
		t.Fatal(e)
	}
	s, e := Open(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, c, ctx
}

var testScope = api.Scope{Namespace: "test", User: "operator", Workspace: "station"}

func snapshot(t *testing.T, s *Store, ctx context.Context, keys ...string) api.SnapshotResponse {
	t.Helper()
	r, e := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: keys}}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func mutation(collection, key, version, body string) api.Mutation {
	return api.Mutation{Collection: collection, Key: key, ExpectedVersion: version, Data: json.RawMessage(body)}
}
func code(err error) string {
	var e *api.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
func TestAtomicRollbackReceiptRestartAndTombstone(t *testing.T) {
	s, c, ctx := setup(t)
	read := snapshot(t, s, ctx, "a")
	r := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "first", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a","value":1}`), mutation("events", "1", "0", `{"seq":1}`)}}
	committed, e := s.Batch(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Batch(ctx, r)
	if e != nil || again.Digest != committed.Digest {
		t.Fatalf("replay: %+v %v", again, e)
	}
	changed := r
	changed.Mutations = append([]api.Mutation(nil), r.Mutations...)
	changed.Mutations[0].Data = json.RawMessage(`{"name":"different"}`)
	if _, e = s.Batch(ctx, changed); code(e) != "conflict" {
		t.Fatalf("identity mismatch %v", e)
	}
	read = snapshot(t, s, ctx, "a")
	bad := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "bad-unique", Mutations: []api.Mutation{mutation("state", "a", "1", `{"name":"a","value":2}`), mutation("state", "b", "0", `{"name":"a"}`)}}
	if _, e = s.Batch(ctx, bad); code(e) != "conflict" {
		t.Fatalf("unique error %v", e)
	}
	read = snapshot(t, s, ctx, "a", "b")
	if string(read.Results[0].Records[0].Data) != `{"name":"a","value":1}` || !read.Results[0].Records[1].Missing || read.Token.Revision != "1" {
		t.Fatalf("partial batch applied: %+v", read)
	}
	if _, e = s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "bad-unique"}); code(e) != "not_found" {
		t.Fatalf("failed receipt %v", e)
	}
	del := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "delete", Mutations: []api.Mutation{{Collection: "state", Key: "a", ExpectedVersion: "1", Delete: true}}}
	if _, e = s.Batch(ctx, del); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Batch(ctx, del); e != nil {
		t.Fatalf("delete replay: %v", e)
	}
	read = snapshot(t, s, ctx, "a")
	if !read.Results[0].Records[0].Deleted || read.Results[0].Records[0].Version != "2" {
		t.Fatalf("missing tombstone %+v", read)
	}
	wrong := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "wrong-create", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a"}`)}}
	if _, e = s.Batch(ctx, wrong); code(e) != "conflict" {
		t.Fatalf("ABA create %v", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	c.Create = false
	s, e = Open(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	receipt, e := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "first"})
	if e != nil || receipt.Digest != committed.Digest {
		t.Fatalf("restart receipt %+v %v", receipt, e)
	}
}
func TestConsistentSnapshotAndPageFence(t *testing.T) {
	s, _, ctx := setup(t)
	r := snapshot(t, s, ctx, "a")
	_, e := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: r.Token, RequestID: "initial", Mutations: []api.Mutation{mutation("state", "a", "0", `{"counter":0}`), mutation("events", "a", "0", `{"counter":0}`)}})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= 60; i++ {
			read, e := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
			if e != nil {
				errs <- e
				return
			}
			v := read.Results[0].Records[0].Version
			_, e = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: fmt.Sprintf("counter-%d", i), Mutations: []api.Mutation{mutation("state", "a", v, fmt.Sprintf(`{"counter":%d}`, i)), mutation("events", "a", v, fmt.Sprintf(`{"counter":%d}`, i))}})
			if e != nil {
				errs <- e
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		read, e := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}, {Collection: "events", Keys: []string{"a"}}}})
		if e != nil {
			t.Fatal(e)
		}
		if string(read.Results[0].Records[0].Data) != string(read.Results[1].Records[0].Data) {
			t.Fatalf("torn projection %+v", read)
		}
	}
	wg.Wait()
	select {
	case e := <-errs:
		t.Fatal(e)
	default:
	}
	if _, e = s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, At: &r.Token, Queries: []api.Query{{Collection: "state", Limit: 1}}}); code(e) != "conflict" {
		t.Fatalf("unfenced page %v", e)
	}
}
func TestQuotaOwnershipSchemaAndNumbers(t *testing.T) {
	s, c, ctx := setup(t)
	if _, e := Open(ctx, Config{Path: c.Path, Manifest: c.Manifest}); code(e) != "conflict" {
		t.Fatalf("second owner %v", e)
	}
	read := snapshot(t, s, ctx, "a")
	_, e := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "number", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":1.0}`), mutation("state", "b", "0", `{"name":1}`)}})
	if code(e) != "conflict" {
		t.Fatalf("numeric unique %v", e)
	}
	if _, e = s.Batch(context.Background(), api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "no-budget", Mutations: []api.Mutation{mutation("state", "a", "0", `{}`)}}); code(e) != "invalid_argument" {
		t.Fatalf("missing deadline %v", e)
	}
	s.Close()
	c.Create = false
	c.Manifest.Namespaces[0].Schema = "changed"
	if _, e = Open(ctx, c); code(e) != "failed_precondition" {
		t.Fatalf("silent schema change %v", e)
	}
	link := filepath.Join(t.TempDir(), "linked.db")
	if e = os.Symlink(c.Path, link); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(ctx, Config{Path: link, Manifest: manifest()}); e == nil {
		t.Fatal("symlink accepted")
	}
}
func TestQueueBoundAndCancellation(t *testing.T) {
	s, _, ctx := setup(t)
	s.gate <- struct{}{}
	defer func() { <-s.gate }()
	read := snapshot(t, s, ctx, "a")
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, e := s.Batch(short, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "blocked", Mutations: []api.Mutation{mutation("state", "a", "0", `{}`)}})
	if code(e) != "deadline_exceeded" || time.Since(start) > time.Second {
		t.Fatalf("queue deadline %v %v", e, time.Since(start))
	}
	for i := 0; i < cap(s.writers); i++ {
		s.writers <- struct{}{}
	}
	_, e = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "overflow", Mutations: []api.Mutation{mutation("state", "a", "0", `{}`)}})
	if code(e) != "unavailable" {
		t.Fatalf("overflow %v", e)
	}
	for i := 0; i < cap(s.writers); i++ {
		<-s.writers
	}
}
