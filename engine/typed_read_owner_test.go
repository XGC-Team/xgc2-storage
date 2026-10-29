package engine_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestTypedFindRunDeadlineDoesNotPublishLateResult(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	q := faultRun("reader-typed", time.Now())
	if _, created, err := store.CreateRun(t.Context(), q); err != nil || !created {
		t.Fatalf("create: %t %v", created, err)
	}
	entered, release := engine.GateReaderForModuleTest(t, db, "SELECT ", "row")
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	type answer struct {
		run   model.Run
		found bool
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		run, found, err := store.FindRunByIdempotencyKey(ctx, q.IdempotencyKey)
		done <- answer{run, found, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual Run row did not enter")
	}
	got := <-done
	var outcome *engine.ReadError
	if !errors.As(got.err, &outcome) || !errors.Is(got.err, context.DeadlineExceeded) || got.found || got.run.ID != "" {
		t.Fatalf("typed read leaked late result: %+v %t %v", got.run, got.found, got.err)
	}
	release()
	if err := outcome.WaitSettlement(t.Context()); err != nil {
		t.Fatal(err)
	}
	run, found, err := store.FindRunByIdempotencyKey(t.Context(), q.IdempotencyKey)
	if err != nil || !found || run.ID != q.ID {
		t.Fatalf("reused reader: %+v %t %v", run, found, err)
	}
	stats, err := db.Stats()
	if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("reader not idle: %+v %v", stats, err)
	}
}

func TestQueuedTypedAndBatchInputsRemainBorrowedUntilOwner(t *testing.T) {
	db, store, _, scope, _ := remoteSettlementFixture(t)
	ctx := t.Context()
	entered, release := make(chan struct{}), make(chan struct{})
	holder := make(chan error, 1)
	go func() {
		holder <- db.Write(ctx, engine.Durable, func(context.Context, *sql.Tx) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not enter")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	payload := []byte(`{"body":"` + strings.Repeat("x", 1<<20) + `"}`)
	type answer struct{ err error }
	answers := make(chan answer, 8)
	cancels := make([]context.CancelFunc, 0, 8)
	start := func(i int) {
		callCtx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		go func() {
			var err error
			switch i % 4 {
			case 0:
				q := faultRun(fmt.Sprintf("queued-%d", i), time.Now())
				q.Inputs = payload
				_, _, err = store.CreateRun(callCtx, q)
			case 1:
				_, err = store.FinishRun(callCtx, model.RunFinish{ID: "root", Status: model.RunSucceeded, Result: payload}, model.RunRetentionPolicy{})
			case 2:
				_, err = store.CreateResource(callCtx, model.ConfigurationResourceCreate{Snapshot: model.PreparedConfigurationSnapshot{Payload: payload}})
			case 3:
				requestID := ""
				if i == 7 {
					requestID = "queued-receipt-batch"
				}
				_, err = db.Batch(callCtx, api.BatchRequest{Scope: scope, RequestID: requestID, Mutations: []api.Mutation{{Collection: "missing", Key: "x", ExpectedVersion: "0", Data: payload}}})
			}
			answers <- answer{err: err}
		}()
	}
	waitQueued := func(count int) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			stats, err := db.Stats()
			if err == nil && stats.WritersQueued == count && stats.WriterCapacity == 64 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("calls did not reach default writer admission: %+v %v", stats, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	start(0)
	waitQueued(2)
	runtime.GC()
	var one, many runtime.MemStats
	runtime.ReadMemStats(&one)
	for i := 1; i < 8; i++ {
		start(i)
	}
	waitQueued(9)
	runtime.GC()
	runtime.ReadMemStats(&many)
	growth := int64(many.HeapAlloc) - int64(one.HeapAlloc)
	if growth >= int64(len(payload)) {
		t.Fatalf("waiting calls retained a payload-sized allocation before the writer: growth=%d payload=%d", growth, len(payload))
	}
	for _, cancel := range cancels {
		cancel()
	}
	for i := 0; i < 8; i++ {
		if got := <-answers; !engine.DefinitelyNotCommitted(got.err) || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("queued operation was not refused before callback: %v", got.err)
		}
	}
	for i := range payload {
		payload[i] = 'z'
	}
	close(release)
	if err := <-holder; err != nil {
		t.Fatalf("holder join: %v", err)
	}
	stats, err := db.Stats()
	if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 {
		t.Fatalf("after-join baseline: %+v %v", stats, err)
	}
	runtime.KeepAlive(payload)
	t.Logf("held-writer fault: 1→8 waiting calls changed heap by %+d bytes for shared %d-byte payload; after cancellation/join default queue=0/64", growth, len(payload))
}
