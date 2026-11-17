package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Select the existing cleanup facts only. Keep corrupt oversized encoded
// payloads out of the driver; decoding retains the existing cleanup byte limit.
const runCleanupQuery = `SELECT id,cleanup_failed,
CASE WHEN length(cleanup_errors_json)<=? THEN cleanup_errors_json ELSE x'00' END
FROM runs WHERE scope=? AND id=?`

func getRunCleanup(ctx context.Context, tx *sql.Tx, scope, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	var out model.Run
	var encoded []byte
	err := tx.QueryRowContext(ctx, runCleanupQuery, 2*model.MaxRunCleanupErrorsBytes, scope, id).Scan(&out.ID, &out.CleanupFailed, &encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	if err != nil {
		return model.Run{}, err
	}
	raw, err := decodeRunPayload(encoded, model.MaxRunCleanupErrorsBytes)
	if err != nil {
		return model.Run{}, err
	}
	out.CleanupErrors = rawOrNil(raw)
	return out, nil
}

// GetRunCleanup returns only ID, CleanupFailed and exact CleanupErrors from
// one accepted row. It does not read execution or observation payloads and is
// not a physical cleanup proof.
func (s *Store) GetRunCleanup(ctx context.Context, id string) (model.Run, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return getRunCleanup(ctx, tx, scope, id)
	})
}
