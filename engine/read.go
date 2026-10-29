package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// ReadError means the caller stopped waiting for a read. The same reader and
// its callback remain owned until WaitSettlement returns; it makes no claim
// about a write or COMMIT. A raw Read callback's captured state must not be
// accessed by its caller before settlement when this error is returned.
type ReadError struct {
	classified error
	cause      error
	completion *readCompletion
}

func (e *ReadError) Error() string   { return e.classified.Error() }
func (e *ReadError) Unwrap() []error { return []error{e.classified, e.cause} }

type readCompletion struct {
	done        chan struct{}
	err         error // published before done closes
	beginErr    error
	rollbackErr error
	physical    *readerDriver
	panicValue  any
}

type readAttemptKey struct{}

// WaitSettlement joins this exact reader callback, transaction and admission.
func (e *ReadError) WaitSettlement(ctx context.Context) error {
	select {
	case <-e.completion.done:
		if e.completion.err != nil {
			return e.completion.err
		}
		if e.completion.beginErr != nil || e.completion.rollbackErr != nil {
			if e.completion.physical == nil {
				return errors.Join(e.completion.beginErr, e.completion.rollbackErr)
			}
			select {
			case <-e.completion.physical.joined:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Read runs fn in a read-only transaction. A deadline may return while its
// actual callback is still in a driver call; ReadError retains that owner.
func (s *Store) Read(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	return s.ReadPrepared(ctx, nil, fn)
}

// ReadPrepared freezes the input needed by fn after reader admission and
// before a deadline can return while that callback is still running.
func (s *Store) ReadPrepared(ctx context.Context, prepare func(), fn func(context.Context, *sql.Tx) error) error {
	var freeze func(context.Context) error
	if prepare != nil {
		freeze = func(context.Context) error { prepare(); return nil }
	}
	return s.readPrepared(ctx, freeze, fn, nil)
}

// readWithInput holds an existing reader admission while an exact lookup
// decides whether mutable caller input must be owned for that read.
func (s *Store) readWithInput(ctx context.Context, fn func(context.Context, *sql.Tx, *WriteInput) error) error {
	input := &WriteInput{}
	return s.readPrepared(ctx, nil, func(ctx context.Context, tx *sql.Tx) error { return fn(ctx, tx, input) }, input)
}

func (s *Store) readPrepared(ctx context.Context, prepare func(context.Context) error, fn func(context.Context, *sql.Tx) error, input *WriteInput) error {
	callerCtx := ctx
	ctx, release, err := s.admit(ctx, false)
	if err != nil {
		return classify(err)
	}
	callerOwnsAdmission := true
	defer func() {
		if callerOwnsAdmission {
			release()
		}
	}()
	if prepare != nil {
		if err = ctx.Err(); err != nil {
			return classify(err)
		}
		if err = prepare(ctx); err != nil {
			return classify(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return classify(err)
	}
	completion := &readCompletion{done: make(chan struct{})}
	finished := make(chan struct{})
	var result error
	var completedContextErr error
	callerOwnsAdmission = false
	go func() {
		defer func() {
			if value := recover(); value != nil {
				completion.panicValue = value
				completion.err = fmt.Errorf("reader callback panicked: %v", value)
			}
			completedContextErr = ctx.Err()
			close(finished)
			release()
			close(completion.done)
		}()
		// Hold one exact connection rather than database/sql's automatic BEGIN
		// retry. Its physical owner remains accountable if BEGIN or rollback
		// fails; SQL statements still receive the caller's budget.
		attemptCtx := context.WithValue(context.WithoutCancel(ctx), readAttemptKey{}, completion)
		conn, e := s.rdb.Conn(attemptCtx)
		if e != nil {
			result = e
			return
		}
		defer conn.Close()
		if e = ctx.Err(); e != nil {
			result = e
			return
		}
		if s.readerUnsettled.Load() != 0 {
			result = fail("unavailable", "reader physical settlement is unavailable")
			return
		}
		tx, e := conn.BeginTx(attemptCtx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			result = e
			completion.beginErr = e
			if !errors.Is(e, driver.ErrBadConn) {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			return
		}
		defer func() {
			if e := tx.Rollback(); e != nil {
				completion.rollbackErr = e
				result = errors.Join(result, e)
			}
		}()
		if e = ctx.Err(); e != nil {
			result = e
			return
		}
		result = fn(ctx, tx)
	}()
	finish := func() error {
		<-completion.done
		if completedContextErr == nil {
			completedContextErr = callerCtx.Err()
		}
		if err := completedContextErr; err != nil {
			return &ReadError{classified: classify(err), cause: err, completion: completion}
		}
		if completion.panicValue != nil {
			panic(completion.panicValue)
		}
		if completion.beginErr != nil || completion.rollbackErr != nil {
			return &ReadError{classified: classify(result), cause: result, completion: completion}
		}
		return classify(result)
	}
	select {
	case <-finished:
		return finish()
	case <-ctx.Done():
		select {
		case <-finished:
			return finish()
		default:
		}
		input.revoke()
		err := ctx.Err()
		return &ReadError{classified: classify(err), cause: err, completion: completion}
	}
}
