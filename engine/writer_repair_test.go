package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestHealthyCloseFailureRetainsPhysicalOwnerUntilJoined(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.closeFailures.Store(2)
	cfg.Create = false
	for attempt := 0; attempt < 2; attempt++ {
		if err := s.Close(); !errors.Is(err, errSettlementClose) {
			t.Fatalf("close %d lost physical failure: %v", attempt, err)
		}
		if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
			if other != nil {
				other.Close()
			}
			t.Fatalf("close %d released live owner: %v", attempt, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("joined close: %v", err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after physical close: %v", err)
	}
	defer other.Close()
}

func TestPreBeginBadConnectionRetainsAndSettlesExactDriver(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "close-joined", true: "close-failed"}[failClose], func(t *testing.T) {
			s, cfg, ctx := setup(t)
			plan := &settlementPlan{}
			installSettlementDriver(t, s, plan)
			plan.pragmaBad.Store(true)
			if failClose {
				plan.closeFailures.Store(1)
			}
			called := false
			err := s.Write(ctx, Relaxed, func(context.Context, *sql.Tx) error { called = true; return nil })
			requireWriteDisposition(t, err, WriteNotCommitted, !failClose)
			if called || !errors.Is(err, driver.ErrBadConn) {
				t.Fatalf("pre-BEGIN fault called=%v err=%v", called, err)
			}
			if failClose {
				if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
					t.Fatalf("unjoined reader: %v", err)
				}
				cfg.Create = false
				if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
					if other != nil {
						other.Close()
					}
					t.Fatalf("unjoined owner unlocked: %v", err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("explicit physical join: %v", err)
				}
				other, err := Open(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				other.Close()
			} else if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error { return insertFaultRow(ctx, tx, "after-pragma") }); err != nil {
				t.Fatalf("restored writer: %v", err)
			}
		})
	}
}

func TestCheckpointBadConnectionFencesUntilPhysicalJoin(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.checkpointBad.Store(true)
	plan.closeFailures.Store(1)
	_, err := s.Checkpoint(ctx)
	requireWriteDisposition(t, err, WriteNotCommitted, false)
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("checkpoint cause: %v", err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("checkpoint allowed stale revalidation: %v", err)
	}
	cfg.Create = false
	if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("checkpoint released owner: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
}

func TestCheckpointHeldDriverCloseReturnsAndJoins(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.checkpointBad.Store(true)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Checkpoint(callCtx); done <- err }()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint physical close did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("checkpoint blocked beyond deadline")
	}
	requireWriteDisposition(t, err, WriteNotCommitted, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || outcome.MayHaveCommitted {
		t.Fatalf("checkpoint outcome: %v", err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("reader entered before physical join: %v", err)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("checkpoint settlement: %v", err)
	}
	if _, err := s.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint after join: %v", err)
	}
}

func TestBackupHeldDriverCloseRetainsOwnerAndOutput(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.backupBad.Store(true)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "backup.db")
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Backup(callCtx, destination); done <- err }()
	select {
	case <-plan.closeEntered:
	case err := <-done:
		t.Fatalf("backup returned before the injected physical boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("backup physical close did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("backup blocked beyond deadline")
	}
	requireWriteDisposition(t, err, WriteUnknown, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || outcome.MayHaveCommitted {
		t.Fatalf("backup outcome: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup output before join: %v", err)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("backup settlement: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup left output: %v", err)
	}
	if _, err := s.Backup(ctx, destination); err != nil {
		t.Fatalf("backup after join: %v", err)
	}
}

func TestPreBeginBadConnectionHeldCloseRespectsDeadline(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.pragmaBad.Store(true)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	called := false
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Relaxed, func(context.Context, *sql.Tx) error { called = true; return nil })
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("pre-BEGIN deadline blocked on physical close")
	}
	requireWriteDisposition(t, err, WriteNotCommitted, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || outcome.MayHaveCommitted {
		t.Fatalf("pre-BEGIN outcome: %v", err)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil || called {
		t.Fatalf("pre-BEGIN cleanup called=%t err=%v", called, err)
	}
	if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error { return insertFaultRow(ctx, tx, "after-held-pragma") }); err != nil {
		t.Fatalf("joined replacement: %v", err)
	}
}

func TestJoinedOldDriverWithFailedReplacementRemainsFenced(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.pragmaBad.Store(true)
	plan.connectFailures.Store(1)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Relaxed, func(context.Context, *sql.Tx) error {
			t.Error("pre-BEGIN failure entered callback")
			return nil
		})
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not enter")
	}
	err := <-done
	requireWriteDisposition(t, err, WriteNotCommitted, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) {
		t.Fatal(err)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("old physical connection did not join: %v", err)
	}
	if err := s.Read(ctx, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("failed replacement cleared owner fence: %v", err)
	}
	cfg.Create = false
	if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed replacement released physical owner: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close after old connection joined: %v", err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after owner close: %v", err)
	}
	other.Close()
}

func TestHeldPhysicalCloseRespectsDeadlineAndKeepsOwner(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.rollback.Store(true)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	callCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			if err := insertFaultRow(ctx, tx, "held-close"); err != nil {
				return err
			}
			return errors.New("callback failed")
		})
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not enter")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("deadline did not return while close held")
	}
	requireWriteDisposition(t, err, WriteUnknown, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) {
		t.Fatal(err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := outcome.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined completion=%v", err)
	}
	waitCancel()
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	readCtx, readCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer readCancel()
	readDone := make(chan error, 1)
	go func() { readDone <- s.Read(readCtx, func(context.Context, *sql.Tx) error { return nil }) }()
	select {
	case err := <-readDone:
		if code(err) != "unavailable" && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("reader=%v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("reader blocked behind Close")
	}
	cfg.Create = false
	if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("physical owner released while held: %v", err)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("actual physical join: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("joined close: %v", err)
	}
	other, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if count := countFaultRow(t, other, "held-close"); count != 0 {
		t.Fatalf("failed transaction committed: %d", count)
	}
}

func TestHeldFailedCloseLaterJoinsOriginalAttempt(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	plan.closeFailures.Store(1)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			return insertFaultRow(ctx, tx, "later-physical-join")
		})
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not enter")
	}
	err := <-done
	requireWriteDisposition(t, err, WriteUnknown, false)
	var original *WriteError
	if !errors.As(err, &original) || !original.MayHaveCommitted {
		t.Fatalf("original attempt: %v", err)
	}
	close(plan.continueClose)
	released = true
	waitCtx, waitCancel := context.WithTimeout(ctx, 40*time.Millisecond)
	if err := original.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed close signaled physical join: %v", err)
	}
	waitCancel()
	if err := s.Close(); err != nil {
		t.Fatalf("later explicit close: %v", err)
	}
	if err := original.WaitSettlement(ctx); err != nil {
		t.Fatalf("original attempt missed later join: %v", err)
	}
	cfg.Create = false
	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := countFaultRow(t, reopened, "later-physical-join"); got != 1 {
		t.Fatalf("real COMMIT lost after join: %d", got)
	}
}

func TestFaultProbeHoldsOriginalWriterUntilExactRead(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	entered, proceed := make(chan struct{}), make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(proceed)
		}
	})
	var found int
	err := s.WriteWithFaultProbe(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "fault-probe")
	}, func(ctx context.Context, tx *sql.Tx, possible bool) error {
		if !possible || tx == nil {
			return errors.New("possible COMMIT lacked a snapshot")
		}
		close(entered)
		<-proceed
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope=?", "fault-probe").Scan(&found)
	})
	requireWriteDisposition(t, err, WriteUnknown, true)
	var outcome *WriteError
	if !errors.As(err, &outcome) {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-join probe did not start")
	}
	if err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("intervening writer entered the fault cut: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := outcome.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe reported completion before its snapshot joined: %v", err)
	}
	cancel()
	close(proceed)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil || found != 1 {
		t.Fatalf("post-join exact fact found=%d err=%v", found, err)
	}
	if err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return nil }); err != nil {
		t.Fatalf("writer after joined probe: %v", err)
	}
}

func TestCanceledCallbackRollbackFactHoldsOriginalAdmission(t *testing.T) {
	s, _, ctx := setup(t)
	callbackEntered, callbackRelease := make(chan struct{}), make(chan struct{})
	probeEntered, probeRelease := make(chan struct{}), make(chan struct{})
	callbackOpen, probeOpen := true, true
	t.Cleanup(func() {
		if callbackOpen {
			close(callbackRelease)
		}
		if probeOpen {
			close(probeRelease)
		}
	})
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.WriteWithFaultProbe(callCtx, Durable, func(ctx context.Context, tx *sql.Tx) error {
			if err := insertFaultRow(ctx, tx, "rollback-owned-fact"); err != nil {
				return err
			}
			close(callbackEntered)
			<-callbackRelease
			return nil
		}, func(_ context.Context, tx *sql.Tx, possible bool) error {
			if tx != nil || possible {
				return errors.New("confirmed rollback requested a database snapshot")
			}
			close(probeEntered)
			<-probeRelease
			return nil
		})
	}()
	select {
	case <-callbackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not reach its actual write")
	}
	cancel()
	var outcome *WriteError
	select {
	case err := <-done:
		if !errors.As(err, &outcome) || outcome.Disposition != WriteUnknown || !outcome.CallbackPending || outcome.MayHaveCommitted {
			t.Fatalf("unjoined callback disposition: %+v %v", outcome, err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller remained blocked by the physical callback")
	}
	close(callbackRelease)
	callbackOpen = false
	select {
	case <-probeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-rollback owned fact did not start")
	}
	if err := s.Write(ctx, Durable, func(context.Context, *sql.Tx) error { return nil }); code(err) != "unavailable" {
		t.Fatalf("intervening writer entered the rollback fact cut: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := outcome.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished rollback fact reported settlement: %v", err)
	}
	waitCancel()
	close(probeRelease)
	probeOpen = false
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("original rollback did not join its fact: %v", err)
	}
	if got := countFaultRow(t, s, "rollback-owned-fact"); got != 0 {
		t.Fatalf("abandoned transaction committed %d rows", got)
	}
	stats, err := s.Stats()
	if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 {
		t.Fatalf("joined owner did not return to idle: %+v %v", stats, err)
	}
}

func TestFaultProbeAddsNoNormalOrConfirmedRollbackRead(t *testing.T) {
	s, _, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	before, _ := s.Stats()
	probes := 0
	probe := func(context.Context, *sql.Tx, bool) error { probes++; return nil }
	if err := s.WriteWithFaultProbe(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "normal-probe")
	}, probe); err != nil {
		t.Fatal(err)
	}
	plan.commit.Store(1)
	err := s.WriteWithFaultProbe(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "rollback-probe")
	}, probe)
	if !DefinitelyNotCommitted(err) || probes != 0 {
		t.Fatalf("normal or confirmed rollback ran a fault probe: calls=%d err=%v", probes, err)
	}
	after, _ := s.Stats()
	if after.Calls-before.Calls != 2 || after.CommitsDurable-before.CommitsDurable != 1 || after.ReadersActive != 0 || after.WritersQueued != 0 {
		t.Fatalf("normal owner work did not return to baseline: before=%+v after=%+v", before, after)
	}
	t.Logf("one normal change and one confirmed rollback: owner calls +2, durable commits +1, fault probes %d, joined readers/writers=%d/%d", probes, after.ReadersActive, after.WritersQueued)
}

func TestFailedCloseAndProbeRetainOneOwnerUntilExplicitJoin(t *testing.T) {
	s, cfg, ctx := setup(t)
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	plan.closeFailures.Store(2)
	probeFailure := errors.New("injected scoped revalidation failure")
	attempts, found := 0, 0
	err := s.WriteWithFaultProbe(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return insertFaultRow(ctx, tx, "pending-probe")
	}, func(ctx context.Context, tx *sql.Tx, possible bool) error {
		attempts++
		if attempts == 1 {
			return probeFailure
		}
		if !possible || tx == nil {
			return errors.New("missing physical snapshot")
		}
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE scope=?", "pending-probe").Scan(&found)
	})
	requireWriteDisposition(t, err, WriteUnknown, false)
	var original *WriteError
	if !errors.As(err, &original) {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, errSettlementClose) || attempts != 0 {
		t.Fatalf("failed physical join ran probe: attempts=%d err=%v", attempts, err)
	}
	if err := s.Close(); !errors.Is(err, probeFailure) || attempts != 1 {
		t.Fatalf("failed exact probe released owner: attempts=%d err=%v", attempts, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	if err := original.WaitSettlement(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed probe signaled completion: %v", err)
	}
	cancel()
	cfg.Create = false
	if other, err := Open(ctx, cfg); code(err) != "conflict" || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatalf("failed probe unlocked owner: %v", err)
	}
	if err := s.Close(); err != nil || attempts != 2 || found != 1 {
		t.Fatalf("explicit exact join attempts=%d found=%d err=%v", attempts, found, err)
	}
	if err := original.WaitSettlement(ctx); err != nil {
		t.Fatalf("original handle missed completion: %v", err)
	}
	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestUnknownRelaxedCommitSchedulesOnlyNecessaryCheckpoint(t *testing.T) {
	for _, mode := range []int32{1, 2} {
		t.Run(map[int32]string{1: "confirmed-rollback", 2: "lost-commit-reply"}[mode], func(t *testing.T) {
			s, _ := maintained(t, func(c *Config) { c.CheckpointDelay = 30 * time.Millisecond })
			plan := &settlementPlan{}
			installSettlementDriver(t, s, plan)
			before, _ := s.Stats()
			plan.commit.Store(mode)
			err := s.Write(budget(t), Relaxed, func(ctx context.Context, tx *sql.Tx) error { return insertFaultRow(ctx, tx, "relaxed") })
			if mode == 1 {
				requireWriteDisposition(t, err, WriteNotCommitted, true)
			} else {
				requireWriteDisposition(t, err, WriteUnknown, true)
			}
			if !errors.Is(err, errSettlementCommit) {
				t.Fatal(err)
			}
			if mode == 2 {
				waitFor(t, "possible-commit checkpoint", 3*time.Second, func() bool {
					v, _ := s.Stats()
					return v.Checkpoints > before.Checkpoints && !v.CheckpointPending && !v.CheckpointRunning
				})
				if count := countFaultRow(t, s, "relaxed"); count != 1 {
					t.Fatalf("committed row=%d", count)
				}
			} else {
				time.Sleep(150 * time.Millisecond)
				if count := countFaultRow(t, s, "relaxed"); count != 0 {
					t.Fatalf("rolled-back row=%d", count)
				}
			}
			after, _ := s.Stats()
			want := uint64(0)
			if mode == 2 {
				want = 1
			}
			if after.Checkpoints-before.Checkpoints != want || after.CommitsRelaxed != before.CommitsRelaxed || after.WritersQueued != 0 || after.ReadersActive != 0 {
				t.Fatalf("checkpoint/known counters before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestUnknownCommittedBatchOwnsReceiptExpiry(t *testing.T) {
	s, _ := maintained(t, func(c *Config) {
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
		c.CheckpointDelay = 30 * time.Millisecond
	})
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	request := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "unknown-expiry", Mutations: []api.Mutation{mutation("events", "unknown-event", "0", `{"n":1}`)}}
	_, err := s.Batch(ctx, request)
	requireWriteDisposition(t, err, WriteUnknown, true)
	if _, err = s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); err != nil {
		t.Fatalf("actual receipt missing: %v", err)
	}
	if replay, err := s.Batch(ctx, request); err != nil || replay.RequestID != request.RequestID {
		t.Fatalf("receipt replay: %+v %v", replay, err)
	}
	waitFor(t, "unknown committed receipt expiry", 5*time.Second, func() bool {
		_, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID})
		return code(err) == "not_found"
	})
	waitFor(t, "owner idle", 3*time.Second, func() bool {
		v, _ := s.Stats()
		return !v.CheckpointPending && !v.CheckpointRunning && v.WritersQueued == 0 && v.ReadersActive == 0
	})
	stats, _ := s.Stats()
	if stats.ReceiptsPruned != 1 {
		t.Fatalf("expiry count=%d", stats.ReceiptsPruned)
	}
}

func TestCanceledInFlightBatchRetainsReceiptExpiryAfterJoin(t *testing.T) {
	s, _ := maintained(t, func(c *Config) {
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
		c.CheckpointDelay = 30 * time.Millisecond
	})
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	entered, release := GateNextCommitForModuleTest(t, s)
	defer release()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	request := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "held-expiry", Mutations: []api.Mutation{mutation("events", "held-event", "0", `{"n":1}`)}}
	done := make(chan error, 1)
	go func() { _, err := s.Batch(callCtx, request); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("COMMIT did not start")
	}
	cancel()
	err := <-done
	requireWriteDisposition(t, err, WriteUnknown, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || !outcome.MayHaveCommitted {
		t.Fatalf("lost COMMIT phase: %v", err)
	}
	release()
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("held COMMIT did not join: %v", err)
	}
	if _, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); err != nil {
		t.Fatalf("actual receipt missing: %v", err)
	}
	waitFor(t, "in-flight receipt expiry", 5*time.Second, func() bool {
		_, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID})
		return code(err) == "not_found"
	})
}

func TestHeldUnknownCommitRetainsCheckpointUntilPhysicalJoin(t *testing.T) {
	s, _ := maintained(t, func(c *Config) { c.CheckpointDelay = 30 * time.Millisecond })
	ctx := budget(t)
	plan := &settlementPlan{closeEntered: make(chan struct{}), continueClose: make(chan struct{})}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	released := false
	t.Cleanup(func() {
		if !released {
			close(plan.continueClose)
		}
	})
	before, _ := s.Stats()
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Write(callCtx, Relaxed, func(ctx context.Context, tx *sql.Tx) error { return insertFaultRow(ctx, tx, "held-unknown") })
	}()
	select {
	case <-plan.closeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical close did not start")
	}
	err := <-done
	requireWriteDisposition(t, err, WriteUnknown, false)
	var outcome *WriteError
	if !errors.As(err, &outcome) || !outcome.MayHaveCommitted {
		t.Fatalf("lost possible COMMIT: %v", err)
	}
	// Exercise the deadline interleaving where the maintenance owner has
	// already retained this possible-COMMIT edge while the writer is fenced.
	s.maintenance.possibleCommit(false, 0)
	whileHeld, _ := s.Stats()
	if whileHeld.Checkpoints != before.Checkpoints {
		t.Fatalf("checkpoint ran before physical join: %+v", whileHeld)
	}
	close(plan.continueClose)
	released = true
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("physical join: %v", err)
	}
	s.maintenance.mu.Lock()
	retained := s.maintenance.faultCheckpoint
	s.maintenance.mu.Unlock()
	if retained {
		t.Fatal("joined usable writer did not activate its retained checkpoint")
	}
	waitFor(t, "joined possible-commit checkpoint", 3*time.Second, func() bool {
		v, _ := s.Stats()
		return v.Checkpoints > before.Checkpoints && !v.CheckpointPending && !v.CheckpointRunning
	})
	after, _ := s.Stats()
	if after.Checkpoints-before.Checkpoints != 1 || after.CommitsRelaxed != before.CommitsRelaxed || after.WritersQueued != 0 || after.ReadersActive != 0 {
		t.Fatalf("joined checkpoint work: before=%+v after=%+v", before, after)
	}
}

func TestRolledBackBatchDoesNotScheduleReceiptExpiry(t *testing.T) {
	s, _ := maintained(t, func(c *Config) {
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
		c.CheckpointDelay = 30 * time.Millisecond
	})
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(1)
	request := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "rolled-back-expiry", Mutations: []api.Mutation{mutation("events", "rolled-back-event", "0", `{"n":1}`)}}
	_, err := s.Batch(ctx, request)
	requireWriteDisposition(t, err, WriteNotCommitted, true)
	s.maintenance.mu.Lock()
	expiry, faultExpiry := s.maintenance.expiryAt, s.maintenance.faultExpiry
	s.maintenance.mu.Unlock()
	if expiry != 0 || faultExpiry != 0 {
		t.Fatalf("confirmed rollback scheduled expiry: %d/%d", expiry, faultExpiry)
	}
	if _, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); code(err) != "not_found" {
		t.Fatalf("rolled-back receipt exists: %v", err)
	}
}

func TestUnknownUnjoinedBatchRetainsMaintenanceUntilReopen(t *testing.T) {
	s, cfg := maintained(t, func(c *Config) {
		c.Manifest.Namespaces[0].ReceiptTTLSeconds = 2
		c.CheckpointDelay = 30 * time.Millisecond
	})
	ctx := budget(t)
	read := snapshot(t, s, ctx, "a")
	plan := &settlementPlan{}
	installSettlementDriver(t, s, plan)
	plan.commit.Store(2)
	plan.closeFailures.Store(1)
	request := api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "unjoined-expiry", Mutations: []api.Mutation{mutation("events", "unjoined-event", "0", `{"n":1}`)}}
	_, err := s.Batch(ctx, request)
	requireWriteDisposition(t, err, WriteUnknown, false)
	s.maintenance.mu.Lock()
	checkpoint, expiry := s.maintenance.faultCheckpoint, s.maintenance.faultExpiry
	s.maintenance.mu.Unlock()
	if !checkpoint || expiry == 0 {
		t.Fatalf("unjoined owner lost possible-commit work: checkpoint=%t expiry=%d", checkpoint, expiry)
	}
	if _, err := s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID}); code(err) != "unavailable" {
		t.Fatalf("unjoined owner allowed receipt read: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("physical close: %v", err)
	}
	cfg.Create = false
	reopened, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after join: %v", err)
	}
	defer reopened.Close()
	waitFor(t, "reopened receipt expiry", 6*time.Second, func() bool {
		_, err := reopened.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: request.RequestID})
		return code(err) == "not_found"
	})
}
