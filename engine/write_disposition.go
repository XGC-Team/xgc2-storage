package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/XGC-Team/xgc2-storage/api"
)

// WriteDisposition describes only the outcome of this write attempt. Unknown
// never asserts that the operation committed or that a later read is a receipt.
type WriteDisposition uint8

const (
	// WriteNotCommitted means no business effect committed in this attempt.
	WriteNotCommitted WriteDisposition = iota + 1
	// WriteUnknown means the owner cannot establish whether the attempt committed.
	WriteUnknown
)

// WriteError preserves both the classified API error and the original cause.
// Settled says that this attempt's physical writer transaction has joined; it
// does not imply the Store is available or that the outcome is known.
type WriteError struct {
	Disposition WriteDisposition
	Settled     bool
	// MayHaveCommitted is set only when this attempt has handed COMMIT to the
	// physical owner. The physical call may start after the caller returns.
	MayHaveCommitted bool
	// CallbackPending means the admitted callback may not have joined at
	// return; its results and validation facts stay owned until settlement.
	CallbackPending bool
	classified      error
	cause           error
	completion      *writeCompletion
}

func (e *WriteError) Error() string   { return e.classified.Error() }
func (e *WriteError) Unwrap() []error { return []error{e.classified, e.cause} }

// Cause retains the original operation and any physical-settlement failures.
// The outer API classification may instead name the physical failure.
func (e *WriteError) Cause() error { return e.cause }

type writeCompletion struct {
	done          <-chan struct{}
	release       <-chan struct{}
	ownerReleased <-chan struct{}
	err           error // published before done closes
	nonCommitted  bool  // same owner proved no business COMMIT before done closes
}

func trackNonCommit(probe FaultProbe, completion *writeCompletion) FaultProbe {
	if probe == nil {
		return nil
	}
	return func(ctx context.Context, tx *sql.Tx, possibleCommit bool) error {
		if err := probe(ctx, tx, possibleCommit); err != nil {
			return err
		}
		if !possibleCommit {
			completion.nonCommitted = true
		}
		return nil
	}
}

// WriteInput protects only the caller's mutable input during this admission.
// Own copies it inside the real callback; it must precede any later use of the
// borrowed bytes. It is not a queue or a transaction result.
type WriteInput struct {
	mu      sync.Mutex
	revoked bool
}

func (i *WriteInput) Own(ctx context.Context, copyInput func()) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.revoked {
		return context.Canceled
	}
	copyInput()
	return ctx.Err()
}

func (i *WriteInput) revoke() {
	if i == nil {
		return
	}
	i.mu.Lock()
	i.revoked = true
	i.mu.Unlock()
}

// FaultProbe inspects only identities captured by its original transaction.
// possibleCommit=false supplies a nil transaction and means the physical owner
// confirmed non-commit; it must not manufacture a policy event. Otherwise tx
// is a read-only snapshot while the same writer admission remains fenced.
type FaultProbe func(ctx context.Context, tx *sql.Tx, possibleCommit bool) error

type pendingFault struct {
	probe          FaultProbe
	possibleCommit bool
	done           chan struct{}
}

func (s *Store) runFaultProbe(probe FaultProbe, possibleCommit bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.CallBudget)
	defer cancel()
	if !possibleCommit {
		return probe(ctx, nil, false)
	}
	if s.rdb == nil {
		return fail("unavailable", "fault revalidation reader is unavailable")
	}
	tx, err := s.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	return errors.Join(probe(ctx, tx, true), tx.Rollback())
}

// A failed physical close or failed probe keeps one exact pending attempt in
// the existing fenced owner. Close can join/revalidate it while holding the
// exclusive database owner, without admitting another writer.
func (s *Store) retainFault(probe FaultProbe, possibleCommit bool, done chan struct{}, released chan struct{}) {
	s.fenced.Store(true)
	s.pendingFault = &pendingFault{probe: probe, possibleCommit: possibleCommit, done: done}
	close(released)
}

func (s *Store) finishPendingFault() error {
	pending := s.pendingFault
	if pending == nil {
		return nil
	}
	if err := s.runFaultProbe(pending.probe, pending.possibleCommit); err != nil {
		return err
	}
	s.pendingFault = nil
	close(pending.done)
	return nil
}

func (s *Store) completeFault(probe FaultProbe, possibleCommit bool, done chan struct{}, released chan struct{}) {
	if err := s.runFaultProbe(probe, possibleCommit); err != nil {
		s.retainFault(probe, possibleCommit, done, released)
		return
	}
	if !s.closing.Load() && s.writerUsable() {
		s.fenced.Store(false)
		if s.maintenance != nil {
			s.maintenance.activatePendingAfterJoin()
		}
	}
	close(done)
	close(released)
}

// An already-failed Close is no longer running, so release the admission to
// let explicit Store.Close retry the same retained driver. Its joined channel
// stays open until that exact physical Close is acknowledged.
func (s *Store) attachFailedClose(outcome *WriteError) {
	if outcome == nil || outcome.Settled || outcome.completion != nil {
		return
	}
	s.writerMu.Lock()
	physical := s.physicalWriter
	s.writerMu.Unlock()
	if physical == nil {
		return
	}
	released := make(chan struct{})
	close(released)
	outcome.completion = &writeCompletion{done: physical.joined, release: released}
}

// waitPhysical detaches only the actual physical ending operation. The
// admitted writer and its lock remain owned until that same operation ends.
func (s *Store) waitPhysical(ctx context.Context, operation func() error, cause error, disposition WriteDisposition, mayCommit bool, probes ...FaultProbe) error {
	var probe FaultProbe
	if len(probes) != 0 {
		probe = probes[0]
	}
	result := make(chan error, 1)
	go func() { result <- operation() }()
	select {
	case err := <-result:
		var outcome *WriteError
		if errors.As(err, &outcome) {
			if probe != nil && outcome.Disposition == WriteUnknown {
				done, released := make(chan struct{}), make(chan struct{})
				outcome.completion = &writeCompletion{done: done, release: released}
				ownedProbe := trackNonCommit(probe, outcome.completion)
				s.fenced.Store(true)
				if !outcome.Settled {
					s.retainFault(ownedProbe, outcome.MayHaveCommitted, done, released)
				} else {
					go s.completeFault(ownedProbe, outcome.MayHaveCommitted, done, released)
				}
				return err
			}
			s.attachFailedClose(outcome)
		}
		return err
	case <-ctx.Done():
		s.fenced.Store(true)
		done, released := make(chan struct{}), make(chan struct{})
		completion := &writeCompletion{done: done, release: released}
		ownedProbe := trackNonCommit(probe, completion)
		go func() {
			err := <-result
			var outcome *WriteError
			if err != nil && errors.As(err, &outcome) && !outcome.Settled {
				if probe != nil && disposition == WriteUnknown {
					s.retainFault(ownedProbe, mayCommit, done, released)
					return
				}
				s.attachFailedClose(outcome)
				close(released)
				if outcome.completion == nil {
					completion.err = err
				} else {
					<-outcome.completion.done
					completion.nonCommitted = !mayCommit
				}
				close(done)
				return
			}
			if probe != nil && disposition == WriteUnknown {
				knownRollback := err != nil && errors.As(err, &outcome) && outcome.Disposition == WriteNotCommitted && outcome.Settled
				s.completeFault(ownedProbe, mayCommit && !knownRollback, done, released)
				return
			}
			completion.nonCommitted = !mayCommit && (err == nil || DefinitelyNotCommitted(err))
			if !s.closing.Load() && s.writerUsable() {
				s.fenced.Store(false)
				if s.maintenance != nil {
					s.maintenance.activatePendingAfterJoin()
				}
			}
			close(done)
			close(released)
		}()
		out := newWriteError(errors.Join(cause, ctx.Err()), disposition, false).(*WriteError)
		out.MayHaveCommitted = mayCommit
		out.completion = completion
		return out
	}
}

func releaseWriterAfter(err error, release func()) {
	var outcome *WriteError
	if errors.As(err, &outcome) && outcome.completion != nil {
		ownerReleased := make(chan struct{})
		outcome.completion.ownerReleased = ownerReleased
		finish := func() {
			release()
			close(ownerReleased)
		}
		select {
		case <-outcome.completion.release:
			finish()
		default:
			go func() { <-outcome.completion.release; finish() }()
		}
		return
	}
	release()
}

// beginWriterCallback keeps BEGIN and the real callback in one physical
// attempt. If the caller's deadline wins, that same owner waits for the
// callback and rolls its transaction back before releasing admission.
func (s *Store) beginWriterCallback(ctx context.Context, fn func(context.Context, *sql.Tx) error, probe FaultProbe, input *WriteInput) (*sql.Tx, bool, error, any) {
	type answer struct {
		tx         *sql.Tx
		err        error
		entered    bool
		panicValue any
	}
	reply := make(chan answer)
	abandoned := make(chan struct{})
	var done chan error // allocated only if the caller actually abandons the attempt
	var started atomic.Bool
	go func() {
		var result answer
		result.tx, result.err = s.writer.BeginTx(context.WithoutCancel(ctx), nil)
		if result.err != nil {
			settled, settleErr := s.settleFailedWriter(ctx)
			result.err = newWriteError(errors.Join(result.err, settleErr), WriteNotCommitted, settled)
		} else if ctx.Err() != nil {
			result.err = ctx.Err()
		} else {
			// A second check closes the race with a deadline that wins before
			// the callback starts: after that deadline no callback may begin.
			started.Store(true)
			if result.err = ctx.Err(); result.err == nil {
				result.entered = true
				func() {
					defer func() { result.panicValue = recover() }()
					result.err = fn(ctx, result.tx)
				}()
			}
		}
		select {
		case reply <- result:
		case <-abandoned:
			var panicErr error
			if result.panicValue != nil {
				panicErr = fmt.Errorf("writer callback panicked after caller returned: %v", result.panicValue)
			}
			if result.tx != nil {
				if rollbackErr := result.tx.Rollback(); rollbackErr != nil {
					settled, settleErr := s.settleFailedWriter(ctx)
					if !settled {
						done <- newWriteError(errors.Join(rollbackErr, settleErr, panicErr), WriteUnknown, false)
						return
					}
				}
			}
			var outcome *WriteError
			if result.err != nil && errors.As(result.err, &outcome) && !outcome.Settled {
				done <- result.err
			} else if panicErr != nil {
				if probe != nil && started.Load() {
					if err := s.runFaultProbe(probe, false); err != nil {
						done <- newWriteError(errors.Join(panicErr, err), WriteUnknown, false)
						return
					}
				}
				done <- panicErr
			} else if probe != nil && started.Load() {
				if err := s.runFaultProbe(probe, false); err != nil {
					done <- newWriteError(err, WriteUnknown, false)
				} else {
					done <- nil
				}
			} else {
				done <- nil
			}
		}
	}()
	select {
	case result := <-reply:
		var outcome *WriteError
		if errors.As(result.err, &outcome) {
			s.attachFailedClose(outcome)
		}
		return result.tx, result.entered, result.err, result.panicValue
	case <-ctx.Done():
		input.revoke()
		s.fenced.Store(true)
		done = make(chan error, 1)
		close(abandoned)
		joined, released := make(chan struct{}), make(chan struct{})
		completion := &writeCompletion{done: joined, release: released}
		ownedProbe := trackNonCommit(probe, completion)
		go func() {
			err := <-done
			var outcome *WriteError
			if err != nil && errors.As(err, &outcome) && !outcome.Settled {
				completion.err = err
				if probe != nil && started.Load() {
					s.retainFault(ownedProbe, false, joined, released)
					return
				}
				s.attachFailedClose(outcome)
				close(released)
				if outcome.completion == nil {
					completion.err = err
				} else {
					<-outcome.completion.done
					completion.nonCommitted = true
				}
				close(joined)
				return
			}
			completion.err = err
			completion.nonCommitted = true
			// A reported callback panic does not keep a rolled-back, joined
			// writer fenced. Its error still belongs to this completion; only
			// unresolved physical ownership makes the Store unavailable.
			if !s.closing.Load() && s.writerUsable() {
				s.fenced.Store(false)
			}
			close(joined)
			close(released)
		}()
		pending := started.Load()
		disposition := WriteNotCommitted
		if pending {
			disposition = WriteUnknown
		}
		out := newWriteError(ctx.Err(), disposition, false).(*WriteError)
		out.CallbackPending = pending
		out.completion = completion
		return nil, pending, out, nil
	}
}

// Called only while the physical attempt still holds writer admission. A
// joined old connection is not proof that its replacement opened and passed
// the existing capacity checks; an unavailable replacement retains the fence.
func (s *Store) writerUsable() bool {
	return s.writer != nil && !s.writerUnsettled && s.readerUnsettled.Load() == 0
}

// WaitSettlement joins this exact physical attempt. It does not report a
// successful COMMIT or turn the original unknown outcome into a receipt.
// A retained attempt failure remains observable after a later successful
// physical Close. NonCommittedAfterJoin reports the separate post-join fact.
func (e *WriteError) WaitSettlement(ctx context.Context) error {
	if e.completion == nil {
		if e.Settled {
			return nil
		}
		return fail("unavailable", "physical writer settlement is unavailable")
	}
	select {
	case <-e.completion.done:
		if e.completion.ownerReleased == nil {
			return e.completion.err
		}
		select {
		case <-e.completion.ownerReleased:
			return e.completion.err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NonCommittedAfterJoin is a later fact of this exact physical attempt. It
// never changes the original error's disposition or converts a failed call
// into a replay success. A synchronous unknown rollback/close failure without
// a retained completion has no such proof.
func (e *WriteError) NonCommittedAfterJoin() bool {
	if e.Disposition == WriteNotCommitted && e.Settled {
		return true
	}
	if e.completion == nil || e.MayHaveCommitted {
		return false
	}
	select {
	case <-e.completion.done:
	default:
		return false
	}
	if e.completion.ownerReleased != nil {
		select {
		case <-e.completion.ownerReleased:
		default:
			return false
		}
	}
	return e.completion.nonCommitted
}

func newWriteError(cause error, disposition WriteDisposition, settled bool) error {
	return &WriteError{Disposition: disposition, Settled: settled, classified: classify(cause), cause: cause}
}

func newSettlementError(cause, physical error, disposition WriteDisposition, settled bool) error {
	// A replay/no-change sentinel can be the callback's cause. If rollback
	// failed, classify the actual physical failure rather than exposing that
	// private sentinel as though it were a successful replay.
	classification := classify(physical)
	// SQLite may automatically end a transaction on SQLITE_FULL, making the
	// later explicit rollback report no active transaction. Keep the real disk
	// failure visible while retaining the unknown physical disposition and the
	// rollback cause; neither is a committed-result assertion.
	var original *api.Error
	if errors.As(classify(cause), &original) && original.Code == "disk_full" {
		classification = original
	}
	return &WriteError{Disposition: disposition, Settled: settled, classified: classification, cause: cause}
}

// WriteRefused marks an error detected before a typed operation enters its
// owner. It cannot describe an attempted transaction or an ambiguous result.
func WriteRefused(cause error) error {
	if cause == nil {
		return nil
	}
	return newWriteError(cause, WriteNotCommitted, true)
}

// DefinitelyNotCommitted is true only when this attempt cannot have committed
// and its physical transaction is no longer outstanding.
func DefinitelyNotCommitted(err error) bool {
	var outcome *WriteError
	return errors.As(err, &outcome) && outcome.Disposition == WriteNotCommitted && outcome.Settled
}

// closePhysicalWriter joins the driver close while retaining the same owner
// lock. If close fails, Raw deliberately keeps the connection instead of
// discarding an unjoined driver whose outcome would no longer be observable.
// On success the bad-connection return also removes it from database/sql's
// pool; the current SQLite driver's Close is safe when called again by the pool.
func (s *Store) closePhysicalWriter(retry bool) error {
	s.writerMu.Lock()
	physical := s.physicalWriter
	s.writerMu.Unlock()
	if physical == nil {
		if s.writer == nil {
			return nil // Open failed before acquiring a writer.
		}
		return errors.New("writer driver cannot be joined")
	}
	if s.writer == nil {
		closed, attempted, prior := physical.closeState()
		if closed {
			return nil
		}
		if !retry {
			if attempted && prior != nil {
				return prior
			}
			return errors.New("writer connection cannot be joined")
		}
		return physical.Close()
	}
	var closeErr error
	invoked := false
	err := s.writer.Raw(func(raw any) error {
		conn, ok := raw.(*writerDriver)
		if !ok || conn != physical {
			return errors.New("writer driver identity disagrees")
		}
		invoked = true
		closeErr = conn.Close()
		if closeErr != nil {
			// Do not unwrap ErrBadConn here: Raw would discard the very
			// connection whose failed close must remain owned and fenced.
			return fmt.Errorf("physical writer close failed: %v", closeErr)
		}
		return driver.ErrBadConn
	})
	if closeErr != nil {
		return closeErr
	}
	if !invoked && errors.Is(err, sql.ErrConnDone) {
		// database/sql may already have called Close and discarded its Conn.
		// The existing owner still holds this exact driver and its close result.
		closed, attempted, prior := physical.closeState()
		if closed {
			s.writer = nil
			return nil
		}
		if !retry {
			if attempted && prior != nil {
				return prior
			}
			return err
		}
		if closeErr = physical.Close(); closeErr != nil {
			return closeErr
		}
		s.writer = nil
		return nil
	}
	if !errors.Is(err, driver.ErrBadConn) {
		if err == nil {
			return errors.New("physical writer close was not acknowledged")
		}
		return err
	}
	s.writer = nil
	return nil
}

// settleFailedWriter runs under the existing writer gate. New calls are fenced
// until the old physical connection has closed and its replacement is ready.
// A failed close stays owned and fenced; explicit Store.Close can try to join
// it without releasing the database's exclusive owner lock prematurely.
func (s *Store) settleFailedWriter(ctx context.Context) (bool, error) {
	s.fenced.Store(true)
	if err := s.closePhysicalWriter(false); err != nil {
		s.writerUnsettled = true
		return false, fmt.Errorf("writer physical settlement: %w", err)
	}
	s.writerUnsettled = false
	if s.wdb == nil {
		return true, errors.New("writer pool is unavailable")
	}
	conn, err := s.wdb.Conn(context.WithoutCancel(ctx))
	if err != nil {
		return true, fmt.Errorf("reopen writer after settlement: %w", err)
	}
	s.writer = conn
	// writerDSN sets FULL on each new physical connection.
	s.synchronous = Durable
	if err = s.configureWriterCapacity(context.WithoutCancel(ctx)); err != nil {
		if closeErr := s.closePhysicalWriter(false); closeErr != nil {
			s.writerUnsettled = true
			return true, errors.Join(fmt.Errorf("configure replacement writer: %w", err), fmt.Errorf("close replacement writer: %w", closeErr))
		}
		return true, fmt.Errorf("configure replacement writer: %w", err)
	}
	if !s.closing.Load() && s.readerUnsettled.Load() == 0 {
		s.fenced.Store(false)
	}
	return true, nil
}
