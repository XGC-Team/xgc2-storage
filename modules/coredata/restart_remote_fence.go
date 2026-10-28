package coredata

import (
	"context"
	"database/sql"
)

// HasPendingRemoteRoot reports whether the existing durable control relation
// still owns any remote Run in this exact root. It reads neither Run payloads
// nor remote link blobs, and uses the same root index as history retention.
func (s *Store) HasPendingRemoteRoot(ctx context.Context, root string) (bool, error) {
	if !textKey(root) {
		return false, failure("invalid_argument", "root run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (bool, error) {
		var pending bool
		err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM runs WHERE scope=? AND root_run_id=? AND pending_remote_count>0)", scope, root).Scan(&pending)
		return pending, err
	})
}
