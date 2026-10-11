package coredata_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The benchmarks time the Run lifecycle the way Core drives it. STORAGE_BENCH_DIR
// chooses the filesystem; a durable commit costs one fsync of it.

func benchCore(b *testing.B) (*coredata.Store, *engine.Store) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	b.Cleanup(cancel)
	db, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "core.db"), Create: true, Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}, MaxDBBytes: 4 << 30})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	core, err := coredata.New(db, api.Scope{Namespace: "core", User: "operator", Workspace: "station"})
	if err != nil {
		b.Fatal(err)
	}
	return core, db
}

func benchRun(id string) model.NewRun {
	return model.NewRun{ID: id, TargetID: "local", WorkflowResourceID: "wf", WorkflowCommitID: "v1", DefinitionDigest: strings.Repeat("d", 64), ActionID: "run",
		Inputs: []byte(`{"session":{"id":"s1","robots":["r1","r2","r3"],"scene":"` + strings.Repeat("s", 2000) + `"},"parameters":{"speed":1.5}}`), Trigger: []byte(`{"kind":"manual"}`)}
}

func report(b *testing.B, db *engine.Store, before engine.Stats, unit string) {
	b.Helper()
	after, _ := db.Stats()
	b.ReportMetric(float64(after.Fsyncs-before.Fsyncs)/float64(b.N), "syncs/op")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), unit)
}

func BenchmarkCreateRunDurable(b *testing.B) {
	core, db := benchCore(b)
	ctx := context.Background()
	before, _ := db.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := core.CreateRun(ctx, benchRun(fmt.Sprint("run-", i))); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	report(b, db, before, "runs/s")
}

func BenchmarkUpdateRunStatusRelaxed(b *testing.B) {
	core, db := benchCore(b)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if _, _, err := core.CreateRun(ctx, benchRun(fmt.Sprint("run-", i))); err != nil {
			b.Fatal(err)
		}
	}
	before, _ := db.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := core.UpdateRunStatus(ctx, model.RunStatusUpdate{ID: fmt.Sprint("run-", i), Status: model.RunRunning}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	report(b, db, before, "updates/s")
}

// Fifty status transitions commit together, as when a parent Run starts its children.
func BenchmarkUpdateRunStatusRelaxedBatch50(b *testing.B) {
	core, db := benchCore(b)
	ctx := context.Background()
	for i := 0; i < b.N*50; i++ {
		if _, _, err := core.CreateRun(ctx, benchRun(fmt.Sprint("run-", i))); err != nil {
			b.Fatal(err)
		}
	}
	before, _ := db.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		updates := make([]model.RunStatusUpdate, 50)
		for j := range updates {
			updates[j] = model.RunStatusUpdate{ID: fmt.Sprint("run-", i*50+j), Status: model.RunRunning}
		}
		if err := core.UpdateRunStatus(ctx, updates...); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	report(b, db, before, "batches/s")
	b.ReportMetric(float64(b.N*50)/b.Elapsed().Seconds(), "updates/s")
}

func BenchmarkFinishRunDurable(b *testing.B) {
	core, db := benchCore(b)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		if _, _, err := core.CreateRun(ctx, benchRun(fmt.Sprint("run-", i))); err != nil {
			b.Fatal(err)
		}
	}
	before, _ := db.Stats()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := core.FinishRun(ctx, model.RunFinish{ID: fmt.Sprint("run-", i), Status: model.RunSucceeded, Termination: "completed",
			Result: []byte(`{"ok":true}`), Nodes: []byte(`[{"id":"a","status":"succeeded"},{"id":"b","status":"succeeded"},{"id":"c","status":"succeeded"}]`)}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	report(b, db, before, "runs/s")
}

func benchSeeded(b *testing.B, runs int) (*coredata.Store, *engine.Store) {
	core, db := benchCore(b)
	ctx := context.Background()
	for i := 0; i < runs; i++ {
		q := benchRun(fmt.Sprintf("run-%05d", i))
		q.TargetID = []string{"local", "agent-1"}[i%2]
		q.At = time.Unix(1_700_000_000+int64(i), 0)
		if _, _, err := core.CreateRun(ctx, q); err != nil {
			b.Fatal(err)
		}
	}
	return core, db
}

func latencies(b *testing.B, fn func(i int) error) {
	b.Helper()
	all := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := fn(i); err != nil {
			b.Fatal(err)
		}
		all = append(all, time.Since(start))
	}
	b.StopTimer()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	b.ReportMetric(float64(all[len(all)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(all[(len(all)-1)*99/100].Nanoseconds()), "p99-ns")
}

func BenchmarkGetRun(b *testing.B) {
	core, _ := benchSeeded(b, 5000)
	ctx := context.Background()
	latencies(b, func(i int) error { _, err := core.GetRun(ctx, fmt.Sprintf("run-%05d", i%5000)); return err })
}

func BenchmarkListRunsPage50WithoutPayloads(b *testing.B) {
	core, _ := benchSeeded(b, 5000)
	ctx := context.Background()
	latencies(b, func(i int) error {
		_, err := core.ListRuns(ctx, model.RunFilter{TargetID: "local", Limit: 50, OmitPayloads: true})
		return err
	})
}

func BenchmarkListRunsPage50WithPayloads(b *testing.B) {
	core, _ := benchSeeded(b, 5000)
	ctx := context.Background()
	latencies(b, func(i int) error {
		_, err := core.ListRuns(ctx, model.RunFilter{TargetID: "local", Limit: 50})
		return err
	})
}
