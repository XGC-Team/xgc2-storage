package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestReadViewHoldsNoReaderBetweenReads(t *testing.T) {
	ctx := budget(t)
	c := Config{Path: filepath.Join(t.TempDir(), "view.db"), Create: true, Manifest: manifest(), Readers: 1}
	os.Chmod(filepath.Dir(c.Path), 0700)
	s, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	err = s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
		if _, err := s.Snapshot(view, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}}); err != nil {
			return err
		}
		// The consumer now computes for a while. With one reader in total, an
		// independent reader must still get through, and none is occupied.
		stats, _ := s.Stats()
		if stats.ReadersActive != 0 {
			t.Fatalf("the view holds %d readers while the consumer computes", stats.ReadersActive)
		}
		other, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if _, err := s.Snapshot(other, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}}); err != nil {
			t.Fatalf("an unrelated read starved behind a view: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadViewFencesEveryReadToOneRevision(t *testing.T) {
	s, _, ctx := setup(t)
	first := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: first.Token, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a"}`)}}); err != nil {
		t.Fatal(err)
	}
	q := api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}}
	var outside *Store = s
	err := s.WithReadSnapshot(ctx, testScope, func(view context.Context) error {
		one, err := s.Snapshot(view, q)
		if err != nil {
			return err
		}
		two, err := s.Snapshot(view, q)
		if err != nil || one.Token != two.Token {
			t.Fatalf("two reads without a write differ: %+v %+v %v", one.Token, two.Token, err)
		}
		// A caller that pins another revision cannot mix it in.
		stale := q
		stale.At = &first.Token
		if _, err = s.Snapshot(view, stale); code(err) != "conflict" {
			t.Fatalf("view accepted a request pinning another revision: %v", err)
		}
		// Mutations are not part of a view.
		if _, err = s.Batch(view, api.BatchRequest{Scope: testScope, Expected: one.Token, Mutations: []api.Mutation{mutation("state", "b", "0", `{"name":"b"}`)}}); code(err) != "failed_precondition" {
			t.Fatalf("view admitted a mutation: %v", err)
		}
		// A write from elsewhere ends the view's consistency: restart it.
		if _, err = outside.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: one.Token, Mutations: []api.Mutation{mutation("state", "c", "0", `{"name":"c"}`)}}); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Snapshot(view, q); code(err) != "conflict" {
			t.Fatalf("view mixed revisions: %v", err)
		}
		// Another scope's reads do not belong to this view.
		otherScope := testScope
		otherScope.User = "someone"
		if err = s.WithReadSnapshot(view, otherScope, func(context.Context) error { return nil }); code(err) != "failed_precondition" {
			t.Fatalf("nested view for another scope: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadViewIsUnusableAfterItsCallback(t *testing.T) {
	s, _, ctx := setup(t)
	var borrowed context.Context
	if err := s.WithReadSnapshot(ctx, testScope, func(view context.Context) error { borrowed = view; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(borrowed, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}}); code(err) != "failed_precondition" {
		t.Fatalf("view stayed usable after its callback: %v", err)
	}
}

func TestReadersAndWritersMakeProgressTogetherWithoutFailing(t *testing.T) {
	ctx := budget(t)
	c := Config{Path: filepath.Join(t.TempDir(), "load.db"), Create: true, Manifest: toyManifest(), Modules: []Module{toy(1)}, Readers: 2, WriterQueue: 2}
	os.Chmod(filepath.Dir(c.Path), 0700)
	s, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const writers, each = 16, 25
	var wg sync.WaitGroup
	failures := make(chan error, writers*each+8)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				class := Durable
				if i%2 == 1 {
					class = Relaxed
				}
				if err := s.Write(ctx, class, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "INSERT INTO toy_items(name) VALUES('w')")
					return err
				}); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	// Readers see a monotonically growing count while 14 of 16 writers wait
	// outside the two-slot writer queue instead of being rejected.
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			last := int64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := countRows(t, s, "SELECT count(*) FROM toy_items")
				if n < last {
					failures <- fmt.Errorf("reader saw the count shrink %d -> %d", last, n)
					return
				}
				last = n
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	if n := countRows(t, s, "SELECT count(*) FROM toy_items"); n != writers*each {
		t.Fatalf("lost writes: %d of %d", n, writers*each)
	}
	stats, _ := s.Stats()
	if stats.Timeouts != 0 || stats.CommitsDurable < writers*each/2 || stats.CommitsRelaxed < writers*each/2-writers {
		t.Fatalf("unexpected load stats: %+v", stats)
	}
}
