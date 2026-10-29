package engine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestAbandonedWriterPanicRollsBackAndReleasesItsOwner(t *testing.T) {
	s, _, ctx := setup(t)
	entered, release := make(chan struct{}), make(chan struct{})
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		returned <- s.Write(caller, Durable, func(context.Context, *sql.Tx) error {
			close(entered)
			<-release
			panic("abandoned business callback")
		})
	}()
	<-entered
	cancel()
	err := <-returned
	var attempt *WriteError
	if !errors.As(err, &attempt) || attempt.Disposition != WriteUnknown || !attempt.CallbackPending || attempt.Settled {
		close(release)
		t.Fatalf("held callback lost its exact unknown owner: %v", err)
	}
	if attempt.NonCommittedAfterJoin() {
		close(release)
		t.Fatal("held callback was reported as physically joined")
	}
	close(release)
	if err := attempt.WaitSettlement(ctx); err == nil || !strings.Contains(err.Error(), "abandoned business callback") {
		t.Fatalf("joined callback panic was lost: %v", err)
	}
	if !attempt.NonCommittedAfterJoin() || DefinitelyNotCommitted(err) || attempt.Disposition != WriteUnknown || attempt.Settled {
		t.Fatalf("post-join fact rewrote the original unknown disposition: %+v", attempt)
	}
	if err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatalf("joined callback panic fenced later writes: %v", err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatalf("joined callback panic fenced later reads: %v", err)
	}
}
