package engine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/api"
)

// ImportRecords is an offline owner seam for an empty record scope. It shares
// the sole mutation/index/quota algorithm, creates no receipts or HTTP operation,
// and commits the complete finite import once. Normal startup never calls it.
func (s *Store) ImportRecords(ctx context.Context, scope api.Scope, records []api.Mutation) (err error) {
	defer func() { err = classify(err) }()
	n, err := s.scope(scope)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return err
	}
	defer release()
	if err = s.diskCheck(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET id=id WHERE id=1"); err != nil {
		return err
	}
	id := scopeID(scope)
	var existing int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE scope=?", id).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return fail("failed_precondition", "offline import requires empty record scope")
	}
	var revision int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM scopes WHERE scope=?", id).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM scopes WHERE namespace=?", n.ID).Scan(&count); err != nil {
			return err
		}
		if count >= n.MaxScopes {
			return fail("resource_exhausted", "namespace scope quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,?,0)", id, n.ID); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if revision != 0 {
		return fail("failed_precondition", "offline import requires fresh scope revision")
	}
	for len(records) > 0 {
		count, bytes := 0, 0
		for count < len(records) && count < api.MaxOperations {
			m := records[count]
			if m.Delete || m.ExpectedVersion != "0" {
				return fail("invalid_argument", "offline import only creates original records")
			}
			size := len(m.Collection) + len(m.Key) + len(m.Data) + 128
			if size > api.MaxRequestBytes {
				return fail("resource_exhausted", "offline record exceeds existing mutation budget")
			}
			if count > 0 && bytes+size > api.MaxRequestBytes {
				break
			}
			bytes += size
			count++
		}
		prepared, e := prepareMutations(ctx, n, records[:count], api.MaxOperations, api.MaxRequestBytes)
		if e != nil {
			return e
		}
		if _, err = s.applyMutations(ctx, tx, id, 1, prepared); err != nil {
			return err
		}
		records = records[count:]
	}
	if _, err = tx.ExecContext(ctx, "UPDATE scopes SET revision=1 WHERE scope=?", id); err != nil {
		return err
	}
	return tx.Commit()
}
