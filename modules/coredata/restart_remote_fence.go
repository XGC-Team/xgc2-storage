package coredata

import (
	"context"
	"database/sql"
)

// HasPendingRemoteRoot reports whether open durable control relations still
// own any remote Run in this exact root. A settled or released relation no
// longer fences restart: its terminal outcome was already proven and its
// release is bookkeeping that resumes independently. It reads neither Run
// payloads nor remote link blobs.
func (s *Store) HasPendingRemoteRoot(ctx context.Context, root string) (bool, error) {
	if !textKey(root) {
		return false, failure("invalid_argument", "root run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (bool, error) {
		var pending bool
		err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM run_remote_pending INDEXED BY run_remote_pending_root_open WHERE scope=? AND root_run_id=? AND phase='open')", scope, root).Scan(&pending)
		return pending, err
	})
}

// HasRootStopIntent reads the existing Session's durable accepted Stop for
// this exact root. A Core restart can interrupt the Run without erasing the
// separately accepted Session Stop; an interrupted Run alone is not a Stop.
func (s *Store) HasRootStopIntent(ctx context.Context, root string) (bool, error) {
	if !textKey(root) {
		return false, failure("invalid_argument", "root run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (bool, error) {
		var requested bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN sessions s ON s.scope=r.scope AND s.id=r.session_id
			WHERE r.scope=? AND r.id=? AND r.root_run_id=r.id AND length(s.stop_intent_json)>0)`, scope, root).Scan(&requested)
		return requested, err
	})
}
