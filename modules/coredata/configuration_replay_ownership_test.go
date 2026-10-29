package coredata_test

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestConfigurationReplayDoesNotCloneUnusedPayload(t *testing.T) {
	f := openStore(t)
	q := f.create(t, "replay-input-owner")
	q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"value":"` + strings.Repeat("x", 2<<20) + `"}}`)
	first, err := f.core.CreateResource(f.ctx, q)
	if err != nil || !first.Found || first.Replayed {
		t.Fatalf("initial create: %+v %v", first, err)
	}
	beforeStats, err := f.db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 5; i++ {
		result, err := f.core.CreateResource(f.ctx, q)
		if err != nil || !result.Replayed || result.PlanDigest != first.PlanDigest {
			t.Fatalf("replay %d: %+v %v", i, result, err)
		}
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(q)
	allocated := after.TotalAlloc - before.TotalAlloc
	if allocated >= uint64(len(q.Snapshot.Payload)) {
		t.Fatalf("five receipt-first replays allocated %d bytes for an unused %d-byte payload", allocated, len(q.Snapshot.Payload))
	}
	afterStats, err := f.db.Stats()
	if err != nil || afterStats.Calls-beforeStats.Calls != 5 || afterStats.CommitsDurable != beforeStats.CommitsDurable || afterStats.ReadersActive != 0 || afterStats.WritersQueued != 0 {
		t.Fatalf("replay owner work: before=%+v after=%+v err=%v", beforeStats, afterStats, err)
	}
	t.Logf("five ordinary replays: %d allocated bytes for %d-byte payload, 5 owner calls and 0 commits, joined readers/writers=0/0", allocated, len(q.Snapshot.Payload))
}

func TestConfigurationReceiptReplayDoesNotScheduleCheckpoint(t *testing.T) {
	f := openStore(t, func(c *engine.Config) { c.CheckpointDelay = 100 * time.Millisecond; c.CheckpointBytes = 32 << 20 })
	q := f.create(t, "replay-checkpoint")
	if _, err := f.core.CreateResource(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var before engine.Stats
	for {
		var err error
		before, err = f.db.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if before.Checkpoints > 0 && !before.CheckpointPending && !before.CheckpointRunning && before.WritersQueued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial changed create checkpoint did not join")
		}
		time.Sleep(2 * time.Millisecond)
	}
	result, err := f.core.CreateResource(f.ctx, q)
	if err != nil || !result.Replayed {
		t.Fatalf("same receipt replay: %+v %v", result, err)
	}
	time.Sleep(300 * time.Millisecond)
	after, err := f.db.Stats()
	if err != nil || after.Calls-before.Calls != 1 || after.CommitsDurable != before.CommitsDurable || after.Checkpoints != before.Checkpoints || after.CheckpointPending || after.CheckpointRunning || after.ReadersActive != 0 || after.WritersQueued != 0 {
		t.Fatalf("no-change replay did extra owner work: before=%+v after=%+v err=%v", before, after, err)
	}
	// A real later change still requests its ordinary checkpoint.
	if _, err := f.core.CreateResource(f.ctx, f.create(t, "new-checkpoint")); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		changed, err := f.db.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if changed.Checkpoints == before.Checkpoints+1 && !changed.CheckpointPending && !changed.CheckpointRunning && changed.WritersQueued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual changed create did not checkpoint: %+v", changed)
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Log("one replay: +1 owner call, +0 commits/checkpoints, joined readers/writers=0/0; a later changed create checkpoints once")
}

func TestConcurrentConfigurationReceiptsCommitOnlyTheChangedIntent(t *testing.T) {
	f := openStore(t, func(c *engine.Config) { c.CheckpointDelay = time.Hour })
	q := f.create(t, "concurrent-receipt")
	before, err := f.db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		result model.ConfigurationMutationResult
		err    error
	}
	ready := make(chan struct{})
	answers := make(chan answer, 2)
	var workers sync.WaitGroup
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-ready
			result, err := f.core.CreateResource(f.ctx, q)
			answers <- answer{result, err}
		}()
	}
	close(ready)
	workers.Wait()
	close(answers)
	var created, replayed int
	var digest string
	for got := range answers {
		if got.err != nil || !got.result.Found {
			t.Fatalf("concurrent create: %+v %v", got.result, got.err)
		}
		if digest != "" && digest != got.result.PlanDigest {
			t.Fatal("concurrent receipt changed plan")
		}
		digest = got.result.PlanDigest
		if got.result.Replayed {
			replayed++
		} else {
			created++
		}
	}
	after, err := f.db.Stats()
	if err != nil || created != 1 || replayed != 1 || after.Calls-before.Calls != 2 || after.CommitsDurable-before.CommitsDurable != 1 || after.WritersQueued != 0 || after.ReadersActive != 0 {
		t.Fatalf("concurrent receipt cut: created=%d replayed=%d before=%+v after=%+v err=%v", created, replayed, before, after, err)
	}
}
