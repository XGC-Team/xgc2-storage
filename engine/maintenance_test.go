package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func maintained(t *testing.T, mutate func(*Config)) (*Store, Config) {
	t.Helper()
	c := Config{Path: filepath.Join(t.TempDir(), "maintenance.db"), Create: true, Manifest: manifest()}
	os.Chmod(filepath.Dir(c.Path), 0700)
	mutate(&c)
	s, err := Open(budget(t), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, c
}

func commitOne(t *testing.T, s *Store, id string, relaxed bool) {
	t.Helper()
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, Relaxed: relaxed, Mutations: []api.Mutation{mutation("events", id, "0", `{"n":1}`)}}); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, what string, limit time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestACommitSchedulesOneCheckpointAfterTheDelay(t *testing.T) {
	s, _ := maintained(t, func(c *Config) { c.CheckpointDelay = 100 * time.Millisecond })
	before, _ := s.Stats()
	for i := 0; i < 5; i++ {
		commitOne(t, s, fmt.Sprint("k", i), true)
	}
	// A burst of commits arms a single checkpoint, which also makes the
	// relaxed commits durable.
	waitFor(t, "the scheduled checkpoint", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints > before.Checkpoints
	})
	time.Sleep(300 * time.Millisecond)
	after, _ := s.Stats()
	if after.Checkpoints-before.Checkpoints != 1 {
		t.Fatalf("a burst should cost one checkpoint, got %d", after.Checkpoints-before.Checkpoints)
	}
	if after.DurabilityEstimates <= before.DurabilityEstimates {
		t.Fatal("the checkpoint did not account its durability estimates")
	}
}

func TestAnIdleDatabaseRunsNothing(t *testing.T) {
	s, _ := maintained(t, func(c *Config) { c.CheckpointDelay = 50 * time.Millisecond })
	commitOne(t, s, "only", false)
	waitFor(t, "the checkpoint after the commit", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints >= 1
	})
	quiet, _ := s.Stats()
	time.Sleep(700 * time.Millisecond)
	idle, _ := s.Stats()
	if idle.Checkpoints != quiet.Checkpoints || idle.Calls != quiet.Calls || idle.CommitsDurable != quiet.CommitsDurable || idle.CommitsRelaxed != quiet.CommitsRelaxed || idle.ReceiptsPruned != quiet.ReceiptsPruned {
		t.Fatalf("an idle database did work: %+v -> %+v", quiet, idle)
	}
}

func TestAnOversizedWALRequestsACheckpointAtOnce(t *testing.T) {
	s, _ := maintained(t, func(c *Config) {
		c.CheckpointBytes = 64 << 10
		c.CheckpointDelay = time.Hour // only the size may trigger it
		c.MaxDBBytes = 64 << 20
	})
	big := fmt.Sprintf(`{"padding":"%0*d"}`, 3000, 0)
	for i := 0; i < 40; i++ {
		ctx := budget(t)
		read := snapshot(t, s, ctx, "a")
		if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, Mutations: []api.Mutation{mutation("events", fmt.Sprint("big", i), "0", big)}}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "a size-triggered checkpoint", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints >= 1
	})
}

func TestReceiptExpiryIsArmedForItsOwnTimeAndSurvivesRestart(t *testing.T) {
	s, c := maintained(t, func(c *Config) { c.Manifest.Namespaces[0].ReceiptTTLSeconds = 2 })
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "first", Mutations: []api.Mutation{mutation("events", "x", "0", `{}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The new owner finds the stored receipt and schedules its expiry by itself.
	c.Create = false
	s, err := Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	waitFor(t, "the receipt to expire after restart", 6*time.Second, func() bool {
		return countRows(t, s, "SELECT count(*) FROM receipts") == 0
	})
	if receiptCount(t, s) != 0 {
		t.Fatal("receipt counter not released")
	}
}

func TestMaintenanceFailuresAreReportedNotLogged(t *testing.T) {
	reported := make(chan error, 4)
	s, _ := maintained(t, func(c *Config) {
		c.CheckpointDelay = 20 * time.Millisecond
		c.OnMaintenanceError = func(err error) { reported <- err }
	})
	commitOne(t, s, "x", false)
	// Close the writer connection underneath maintenance: its next checkpoint fails.
	s.writer.Close()
	select {
	case err := <-reported:
		if err == nil {
			t.Fatal("nil failure reported")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance failure never reported")
	}
	if stats, _ := s.Stats(); stats.MaintenanceErrors == 0 {
		t.Fatal("failure not counted")
	}
}

func retryAfter(t *testing.T, d time.Duration) {
	t.Helper()
	old := maintenanceRetry
	maintenanceRetry = d
	t.Cleanup(func() { maintenanceRetry = old })
}

// Holding the gate stands for a writer that never finishes: maintenance cannot
// be admitted within the call budget and fails.
func TestAFailedCheckpointIsRetried(t *testing.T) {
	retryAfter(t, 100*time.Millisecond)
	var failures atomic.Int32
	s, _ := maintained(t, func(c *Config) {
		c.CheckpointDelay = 300 * time.Millisecond
		c.CallBudget = 100 * time.Millisecond
		c.OnMaintenanceError = func(error) { failures.Add(1) }
	})
	commitOne(t, s, "a", true)
	before, _ := s.Stats()
	s.gate <- struct{}{}
	waitFor(t, "the failed checkpoint", 3*time.Second, func() bool { return failures.Load() > 0 })
	<-s.gate
	waitFor(t, "the retried checkpoint", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints > before.Checkpoints
	})
}

func TestAFailedReceiptExpiryIsRetried(t *testing.T) {
	retryAfter(t, 100*time.Millisecond)
	var failures atomic.Int32
	s, _ := maintained(t, func(c *Config) {
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
		c.CheckpointDelay = time.Hour
		c.CallBudget = 100 * time.Millisecond
		c.OnMaintenanceError = func(error) { failures.Add(1) }
	})
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "first", Mutations: []api.Mutation{mutation("events", "x", "0", `{}`)}}); err != nil {
		t.Fatal(err)
	}
	s.gate <- struct{}{}
	waitFor(t, "the failed expiry", 6*time.Second, func() bool { return failures.Load() > 0 })
	<-s.gate
	waitFor(t, "the retried expiry", 6*time.Second, func() bool {
		return countRows(t, s, "SELECT count(*) FROM receipts") == 0
	})
	if receiptCount(t, s) != 0 {
		t.Fatal("receipt counter not released")
	}
}

func TestCheckpointGaugeCoversArmedQueuedAndRunningWork(t *testing.T) {
	s, _ := maintained(t, func(c *Config) {
		c.CheckpointDelay = 150 * time.Millisecond
		c.CallBudget = 3 * time.Second
	})
	before, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	commitOne(t, s, "gauge", true)
	armed, err := s.Stats()
	if err != nil || !armed.CheckpointPending || armed.CheckpointRunning {
		t.Fatalf("armed=%+v err=%v", armed, err)
	}
	// The timer can fire, but the real checkpoint cannot complete until the
	// single writer gate is released.
	s.gate <- struct{}{}
	waitFor(t, "the running checkpoint", 2*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.CheckpointRunning
	})
	running, _ := s.Stats()
	if running.CheckpointPending || !running.CheckpointRunning {
		t.Fatalf("running=%+v", running)
	}
	// A second request while the first is running must remain pending; neither
	// the first completion nor the queued-to-running handoff can appear idle.
	s.maintenance.request(workCheckpoint)
	queued, _ := s.Stats()
	if !queued.CheckpointPending || !queued.CheckpointRunning {
		t.Fatalf("queued=%+v", queued)
	}
	stop := make(chan struct{})
	violation := make(chan Stats, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			stats, err := s.Stats()
			if err == nil && !stats.CheckpointPending && !stats.CheckpointRunning && stats.Checkpoints < before.Checkpoints+2 {
				select {
				case violation <- stats:
				default:
				}
			}
			select {
			case <-stop:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()
	<-s.gate
	waitFor(t, "both requested checkpoints", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints >= before.Checkpoints+2 && !stats.CheckpointPending && !stats.CheckpointRunning
	})
	close(stop)
	<-done
	select {
	case bad := <-violation:
		t.Fatalf("false quiet gap: %+v", bad)
	default:
	}
	quiet, _ := s.Stats()
	for i := 0; i < 20; i++ {
		observed, err := s.Stats()
		if err != nil || observed.CheckpointPending || observed.CheckpointRunning || observed.Calls != quiet.Calls {
			t.Fatalf("passive settled observation=%+v err=%v", observed, err)
		}
	}
}

func TestCheckpointGaugeCoversFailureAndScheduledRetry(t *testing.T) {
	retryAfter(t, 250*time.Millisecond)
	failed := make(chan struct{})
	unblockReport := make(chan struct{})
	s, _ := maintained(t, func(c *Config) {
		c.CheckpointDelay = 80 * time.Millisecond
		c.CallBudget = 80 * time.Millisecond
		c.OnMaintenanceError = func(error) { close(failed); <-unblockReport }
	})
	commitOne(t, s, "retry-gauge", true)
	before, _ := s.Stats()
	s.gate <- struct{}{}
	select {
	case <-failed:
	case <-time.After(3 * time.Second):
		<-s.gate
		close(unblockReport)
		t.Fatal("checkpoint did not fail")
	}
	reporting, _ := s.Stats()
	if !reporting.CheckpointRunning {
		t.Fatalf("failure report appeared quiet: %+v", reporting)
	}
	<-s.gate
	close(unblockReport)
	waitFor(t, "the armed retry", time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.CheckpointPending && !stats.CheckpointRunning
	})
	waitFor(t, "the completed retry", 3*time.Second, func() bool {
		stats, _ := s.Stats()
		return stats.Checkpoints > before.Checkpoints && !stats.CheckpointPending && !stats.CheckpointRunning
	})
}
