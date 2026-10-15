package coredata

import (
	"context"
	"database/sql"
	"strings"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// GetSessions reads at most 500 session identities in one indexed owner
// transaction. Duplicates appear once; missing identities are absent from the
// result, so callers requiring complete associations must check membership.
func (s *Store) GetSessions(ctx context.Context, ids []string) ([]model.Session, error) {
	if len(ids) > 500 {
		return nil, failure("invalid_argument", "at most 500 session identities are allowed")
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, s.scope)
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !textKey(id) {
			return nil, failure("invalid_argument", "session identity is required")
		}
		if !seen[id] {
			args = append(args, id)
			seen[id] = true
		}
	}
	if len(seen) == 0 {
		return []model.Session{}, nil
	}
	query := sessionBatchQuery(len(seen))
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, _ string) ([]model.Session, error) {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		result := make([]model.Session, 0, len(seen))
		for rows.Next() {
			record, err := scanSession(rows)
			if err != nil {
				return nil, err
			}
			result = append(result, record)
		}
		return result, rows.Err()
	})
}

func sessionBatchQuery(count int) string {
	return "SELECT " + sessionColumns + " FROM sessions WHERE scope=? AND id IN (" + strings.TrimSuffix(strings.Repeat("?,", count), ",") + ")"
}
