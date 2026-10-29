package coredata

import (
	"context"
	"database/sql"
	"errors"
)

// HasPendingRemoteOwner checks the exact relations owned by an accepted Run.
// The Run payload and its derived count are not read. A missing Run is an
// error: absence cannot establish that a previously accepted owner joined.
func (s *Store) HasPendingRemoteOwner(ctx context.Context, owner string) (bool, error) {
	if !textKey(owner) {
		return false, failure("invalid_argument", "owner run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (bool, error) {
		var pending bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_remote_pending WHERE scope=r.scope AND owner_run_id=r.id)
			FROM runs AS r WHERE r.scope=? AND r.id=?`, scope, owner).Scan(&pending)
		if errors.Is(err, sql.ErrNoRows) {
			return false, failure("not_found", "remote control owner is unavailable")
		}
		return pending, err
	})
}
