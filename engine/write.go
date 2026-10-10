package engine

import (
	"context"
	"database/sql"
)

// Write runs fn in one writer transaction. The commit class is chosen per
// transaction: the writer connection switches synchronous=FULL (Durable) or
// synchronous=NORMAL (Relaxed) before it begins. An error from fn rolls the
// transaction back; no savepoint or receipt is involved. Data modules call it
// with their typed operations.
func (s *Store) Write(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error) error {
	return s.write(ctx, d, true, fn)
}

// write admits one writer. Maintenance that reclaims space skips the disk
// watermark check that refuses ordinary writes.
func (s *Store) write(ctx context.Context, d Durability, checked bool, fn func(context.Context, *sql.Tx) error) (err error) {
	defer func() { err = classify(err) }()
	ctx, release, err := s.admit(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	if checked {
		if err = s.diskCheck(); err != nil {
			return err
		}
	}
	return s.transact(ctx, d, fn)
}

// transact must run under writer admission.
func (s *Store) transact(ctx context.Context, d Durability, fn func(context.Context, *sql.Tx) error) error {
	if d != s.synchronous {
		level := "FULL"
		if d == Relaxed {
			level = "NORMAL"
		}
		// The level cannot change inside a transaction, so it is set before BEGIN.
		if _, err := s.writer.ExecContext(ctx, "PRAGMA synchronous="+level); err != nil {
			return err
		}
		s.synchronous = d
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err = fn(ctx, tx); err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.committed(d)
	return nil
}

// Read runs fn in one read-only transaction on a reader connection. The
// connection is released when fn returns, so a consumer never keeps a reader
// across its own computation.
func (s *Store) Read(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	defer func() { err = classify(err) }()
	ctx, release, err := s.admit(ctx, false)
	if err != nil {
		return err
	}
	defer release()
	tx, err := s.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(ctx, tx)
}
