package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Write runs fn in one writer transaction. The commit class is chosen per
// transaction: the writer connection switches synchronous=FULL (Durable) or
// synchronous=NORMAL (Relaxed) before it begins. An error from fn requires
// rollback; a physical rollback or COMMIT failure can leave its outcome
// unknown. WriteError reports the disposition and physical settlement. No
// savepoint or receipt is involved. The callback must not issue transaction
// control statements; this owner alone begins, commits and rolls back. If a
// deadline returns while the callback is still running, callback-captured
// state remains owned by this attempt until WaitSettlement. Data modules call
// it with typed operations and detach mutable caller inputs.
func (s *Store) Write(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error) error {
	return s.write(ctx, d, true, fn)
}

// WriteWithFaultProbe attaches one exact revalidation to an ambiguous attempt.
// The probe runs only on that exceptional path, after the physical attempt
// joins and while this writer admission still excludes other mutations. A
// confirmed rollback supplies no transaction and needs no database read. It
// cannot turn the original error into a COMMIT receipt.
func (s *Store) WriteWithFaultProbe(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error, probe FaultProbe) error {
	return s.writeWithFaultProbe(ctx, d, true, fn, probe, nil)
}

// WriteWithInput lets the admitted callback freeze mutable caller input only
// when it is actually needed. A canceled caller cannot return while that
// bounded copy is in progress, and a callback cannot start one after return.
func (s *Store) WriteWithInput(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx, *WriteInput) error) error {
	return s.WriteWithFaultProbeInput(ctx, d, fn, nil)
}

func (s *Store) WriteWithFaultProbeInput(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx, *WriteInput) error, probe FaultProbe) error {
	input := &WriteInput{}
	return s.writeWithFaultProbe(ctx, d, true, func(ctx context.Context, tx *sql.Tx) error {
		return fn(ctx, tx, input)
	}, probe, input)
}

// write admits one writer. Maintenance that reclaims space skips the disk
// watermark check that refuses ordinary writes.
func (s *Store) write(ctx context.Context, d Durability, checked bool, fn func(context.Context, *sql.Tx) error, receiptExpiry ...*int64) (err error) {
	return s.writeWithFaultProbe(ctx, d, checked, fn, nil, nil, receiptExpiry...)
}

func (s *Store) writeWithFaultProbe(ctx context.Context, d Durability, checked bool, fn func(context.Context, *sql.Tx) error, probe FaultProbe, input *WriteInput, receiptExpiry ...*int64) (err error) {
	defer func() {
		if err != nil {
			var disposition *WriteError
			if !errors.As(err, &disposition) {
				err = newWriteError(err, WriteNotCommitted, true)
			}
		}
	}()
	ctx, release, err := s.admit(ctx, true)
	if err != nil {
		return err
	}
	defer func() { releaseWriterAfter(err, release) }()
	if checked {
		if err = s.diskCheck(); err != nil {
			return err
		}
	}
	var expiry *int64
	if len(receiptExpiry) > 0 {
		expiry = receiptExpiry[0]
	}
	return s.transactWithExpiry(ctx, d, fn, expiry, probe, input)
}

// transact must run under writer admission.
func (s *Store) transact(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error) (err error) {
	return s.transactWithExpiry(ctx, d, fn, nil, nil, nil)
}

func (s *Store) transactWithExpiry(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error, expiry *int64, probe FaultProbe, input *WriteInput) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d != s.synchronous {
		level := "FULL"
		if d == Relaxed {
			level = "NORMAL"
		}
		// The level cannot change inside a transaction, so it is set before BEGIN.
		err := s.waitPhysical(ctx, func() error {
			if _, err := s.writer.ExecContext(ctx, "PRAGMA synchronous="+level); err != nil {
				// An error does not prove the PRAGMA was not applied.
				s.synchronous = Durability(255)
				if writerConnectionLost(err) {
					settled, settleErr := s.settleFailedWriter(ctx)
					return newWriteError(errors.Join(err, settleErr), WriteNotCommitted, settled)
				}
				return newWriteError(err, WriteNotCommitted, true)
			}
			s.synchronous = d
			return nil
		}, nil, WriteNotCommitted, false)
		if err != nil {
			return err
		}
	}
	// This owner, rather than database/sql.awaitDone, must finish rollback
	// before releasing writer admission. SQL operations still receive ctx and
	// remain cancellable; only the transaction lifetime is detached. BEGIN is
	// IMMEDIATE with busy_timeout=0 on this dedicated connection, so it never
	// waits for another SQLite writer lock after detaching caller cancellation.
	tx, entered, callbackErr, panicValue := s.beginWriterCallback(ctx, fn, probe, input)
	if tx == nil {
		return callbackErr
	}
	// The writer connection is shared by every later call: whatever happens in
	// fn, including a panic that a caller recovers, must not leave its
	// transaction open.
	finished := false
	defer func() {
		if !finished {
			if err == nil {
				// A panic must not release a live transaction; preserve the panic.
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					_, _ = s.settleFailedWriter(ctx)
				}
				return
			}
			cause := err
			disposition := WriteUnknown
			if !entered {
				disposition = WriteNotCommitted
			}
			err = s.waitPhysical(ctx, func() error {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					settled, settleErr := s.settleFailedWriter(ctx)
					physical := fmt.Errorf("rollback: %w", rollbackErr)
					return newSettlementError(errors.Join(cause, physical, settleErr), physical, disposition, settled)
				}
				return newWriteError(cause, WriteNotCommitted, true)
			}, cause, disposition, false, probe)
		}
	}()
	if panicValue != nil {
		panic(panicValue)
	}
	if callbackErr != nil {
		err = callbackErr
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	finished = true
	return s.waitPhysical(ctx, func() error { return s.finishCommit(ctx, tx, d, expiry) }, nil, WriteUnknown, true, probe)
}

func (s *Store) finishCommit(ctx context.Context, tx *sql.Tx, d Durability, expiry *int64) error {
	err := tx.Commit()
	if err == nil {
		if !s.closing.Load() && s.readerUnsettled.Load() == 0 {
			s.fenced.Store(false)
		}
		if s.maintenance != nil {
			s.maintenance.mu.Lock()
		}
		s.committed(d)
		if s.maintenance != nil {
			if expiry != nil && *expiry != 0 {
				s.maintenance.armExpiryLocked(*expiry)
			}
			s.maintenance.mu.Unlock()
		}
		return nil
	}
	_, rollbackErr := s.writer.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	if rollbackErr == nil {
		return newWriteError(err, WriteNotCommitted, true)
	}
	settled, settleErr := s.settleFailedWriter(ctx)
	if s.maintenance != nil {
		var at int64
		if expiry != nil {
			at = *expiry
		}
		// Physical join alone is not enough when the replacement writer is
		// unavailable or Close has already fenced this owner.
		s.maintenance.possibleCommit(settled && !s.fenced.Load() && s.readerUnsettled.Load() == 0, at)
	}
	out := newWriteError(errors.Join(err, fmt.Errorf("rollback after commit: %w", rollbackErr), settleErr), WriteUnknown, settled).(*WriteError)
	out.MayHaveCommitted = true
	return out
}
