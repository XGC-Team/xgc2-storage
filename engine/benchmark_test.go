package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

// The benchmarks isolate what the commit class costs. STORAGE_BENCH_DIR
// chooses the filesystem (default: the temporary directory); fsync dominates a
// durable commit, so the numbers only mean something for the disk they ran on.

func benchStore(b *testing.B) *Store {
	b.Helper()
	root := os.Getenv("STORAGE_BENCH_DIR")
	if root == "" {
		root = os.TempDir()
	}
	dir, err := os.MkdirTemp(root, "storage-bench-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	m := toyManifest()
	m.Namespaces[0].MaxReceipts = 1000000
	c := Config{Path: filepath.Join(dir, "bench.db"), Create: true, Manifest: m, Modules: []Module{toy(1)}, MaxDBBytes: 4 << 30, CheckpointBytes: 4 << 20}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	b.Cleanup(cancel)
	s, err := Open(ctx, c)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

func reportSyncs(b *testing.B, s *Store, before Stats) {
	b.Helper()
	after, _ := s.Stats()
	b.ReportMetric(float64(after.Fsyncs-before.Fsyncs)/float64(b.N), "syncs/op")
}

func benchCommit(b *testing.B, class Durability, rows int) {
	s := benchStore(b)
	ctx := context.Background()
	before, _ := s.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Write(ctx, class, func(ctx context.Context, tx *sql.Tx) error {
			for r := 0; r < rows; r++ {
				if _, err := tx.ExecContext(ctx, "INSERT INTO toy_items(name) VALUES('commit')"); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportSyncs(b, s, before)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
	if rows > 1 {
		b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "rows/s")
	}
}

func BenchmarkCommitDurable(b *testing.B)         { benchCommit(b, Durable, 1) }
func BenchmarkCommitRelaxed(b *testing.B)         { benchCommit(b, Relaxed, 1) }
func BenchmarkCommitDurableBatch100(b *testing.B) { benchCommit(b, Durable, 100) }
func BenchmarkCommitRelaxedBatch100(b *testing.B) { benchCommit(b, Relaxed, 100) }

func benchParallel(b *testing.B, class Durability) {
	s := benchStore(b)
	ctx := context.Background()
	before, _ := s.Stats()
	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.Write(ctx, class, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "INSERT INTO toy_items(name) VALUES('parallel')")
				return err
			}); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	reportSyncs(b, s, before)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

// With many callers the single writer still commits one transaction at a time.
func BenchmarkParallelCommitDurable(b *testing.B) { benchParallel(b, Durable) }
func BenchmarkParallelCommitRelaxed(b *testing.B) { benchParallel(b, Relaxed) }

func benchDocument(b *testing.B, relaxed bool) {
	s := benchStore(b)
	ctx := context.Background()
	version := "0"
	read, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a"}}}})
	if err != nil {
		b.Fatal(err)
	}
	token := read.Token
	before, _ := s.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		receipt, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: token, Relaxed: relaxed, Mutations: []api.Mutation{
			{Collection: "state", Key: "a", ExpectedVersion: version, Data: []byte(fmt.Sprintf(`{"name":"state","value":%d}`, i))}}})
		if err != nil {
			b.Fatal(err)
		}
		token, version = receipt.Token, receipt.Token.Revision
	}
	b.StopTimer()
	reportSyncs(b, s, before)
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

func BenchmarkDocumentBatchDurable(b *testing.B) { benchDocument(b, false) }
func BenchmarkDocumentBatchRelaxed(b *testing.B) { benchDocument(b, true) }

// percentile reports the p-th percentile of sorted durations in nanoseconds.
func percentile(sorted []time.Duration, p float64) float64 {
	return float64(sorted[int(float64(len(sorted)-1)*p)].Nanoseconds())
}

func benchRead(b *testing.B, readers int, writer bool) {
	s := benchStore(b)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		if err := s.Write(ctx, Relaxed, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO toy_items(name) VALUES('row')")
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var background sync.WaitGroup
	if writer {
		background.Add(1)
		go func() {
			defer background.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, "INSERT INTO toy_items(name) VALUES('background')")
					return err
				})
			}
		}()
	}
	latencies := make([][]time.Duration, readers)
	var wg sync.WaitGroup
	per := b.N/readers + 1
	b.ResetTimer()
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			out := make([]time.Duration, 0, per)
			for i := 0; i < per; i++ {
				start := time.Now()
				var name string
				if err := s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, "SELECT name FROM toy_items WHERE id=?", 1+i%1000).Scan(&name)
				}); err != nil {
					b.Error(err)
					return
				}
				out = append(out, time.Since(start))
			}
			latencies[r] = out
		}(r)
	}
	wg.Wait()
	b.StopTimer()
	close(stop)
	background.Wait()
	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	b.ReportMetric(percentile(all, 0.5), "p50-ns")
	b.ReportMetric(percentile(all, 0.99), "p99-ns")
}

func BenchmarkReadPoint(b *testing.B)                    { benchRead(b, 1, false) }
func BenchmarkReadPointFourReaders(b *testing.B)         { benchRead(b, 4, false) }
func BenchmarkReadPointDuringDurableWrites(b *testing.B) { benchRead(b, 4, true) }
