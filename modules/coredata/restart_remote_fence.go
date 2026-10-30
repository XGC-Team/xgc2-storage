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
