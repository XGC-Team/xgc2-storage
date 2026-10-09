package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/XGC-Team/xgc2-storage/api"
)

type preparedMutations struct {
	mutations []api.Mutation
	documents []map[string]any
	columns   []api.Collection
}

func prepareMutations(ctx context.Context, n api.Namespace, mutations []api.Mutation, maximumOperations, maximumBytes int) (prepared preparedMutations, err error) {
	if len(mutations) == 0 || len(mutations) > maximumOperations {
		return prepared, fail("invalid_argument", "bounded nonempty state mutations required")
	}
	mutations = append([]api.Mutation(nil), mutations...)
	seen := map[string]bool{}
	requestBytes := 0
	documents := make([]map[string]any, len(mutations))
	columns := make([]api.Collection, len(mutations))
	for i := range mutations {
		if err = ctx.Err(); err != nil {
			return prepared, err
		}
		m := &mutations[i]
		c, e := collection(n, m.Collection)
		if e != nil {
			return prepared, e
		}
		columns[i] = c
		requestBytes += len(m.Data) + len(m.Key) + len(m.Collection) + 128
		if len(m.Data) > maximumBytes || requestBytes > maximumBytes {
			return prepared, fail("resource_exhausted", "batch raw payload byte limit exceeded")
		}
		compound := m.Collection + "\x00" + m.Key
		if !key(m.Key) || seen[compound] {
			return prepared, fail("invalid_argument", "invalid/duplicate mutation key")
		}
		seen[compound] = true
		if _, e = revision(m.ExpectedVersion); e != nil {
			return prepared, e
		}
		if m.Delete {
			if len(m.Data) > 0 {
				return prepared, fail("invalid_argument", "delete must not contain data")
			}
			m.Data = json.RawMessage(`{}`)
		} else {
			canonical, obj, e := canonicalObject(m.Data)
			if e != nil {
				return prepared, e
			}
			if len(canonical) > c.MaxRecordBytes {
				return prepared, fail("resource_exhausted", "record exceeds declared byte limit")
			}
			m.Data = canonical
			documents[i] = obj
			for _, idx := range c.Indexes {
				if _, _, e = indexTuple(obj, idx); e != nil {
					return prepared, e
				}
			}
		}
	}

	return preparedMutations{mutations: mutations, documents: documents, columns: columns}, nil
}

// applyMutations is the one record mutation algorithm used by Batch and trusted
// named modules. Admission, receipt and COMMIT remain with the outer owner.
func (s *Store) applyMutations(ctx context.Context, tx *sql.Tx, id string, next int64, prepared preparedMutations) ([]api.Record, error) {
	var err error
	versions := make([]api.Record, 0, len(prepared.mutations))
	// Remove all old indexes first, permitting valid swaps in an atomic batch.
	for _, m := range prepared.mutations {
		var v int64
		err = tx.QueryRowContext(ctx, "SELECT version FROM records WHERE scope=? AND collection=? AND key=?", id, m.Collection, m.Key).Scan(&v)
		if errors.Is(err, sql.ErrNoRows) {
			v = 0
		} else if err != nil {
			return nil, err
		}
		if strconv.FormatInt(v, 10) != m.ExpectedVersion {
			s.conflicts.Add(1)
			return nil, fail("conflict", "record version changed")
		}
		if v > 0 {
			if _, err = tx.ExecContext(ctx, "DELETE FROM lookups WHERE scope=? AND collection=? AND key=?", id, m.Collection, m.Key); err != nil {
				return nil, err
			}
		}
	}
	type totals struct {
		count, bytes int64
		collection   api.Collection
	}
	usage := map[string]*totals{}
	for i, m := range prepared.mutations {
		c := prepared.columns[i]
		values := usage[c.ID]
		if values == nil {
			values = &totals{collection: c}
			usage[c.ID] = values
			err = tx.QueryRowContext(ctx, "SELECT records,bytes FROM usage WHERE scope=? AND collection=?", id, c.ID).Scan(&values.count, &values.bytes)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		var oldBytes int64
		err = tx.QueryRowContext(ctx, "SELECT length(data)+length(CAST(key AS BLOB)) FROM records WHERE scope=? AND collection=? AND key=?", id, c.ID, m.Key).Scan(&oldBytes)
		if errors.Is(err, sql.ErrNoRows) {
			values.count++
		} else if err != nil {
			return nil, err
		}
		values.bytes += int64(len(m.Data)+len(m.Key)) - oldBytes
	}
	for idCollection, values := range usage {
		if values.count > int64(values.collection.MaxRecords) || values.bytes > values.collection.MaxBytes {
			return nil, fail("resource_exhausted", "collection record/byte quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO usage VALUES(?,?,?,?) ON CONFLICT(scope,collection) DO UPDATE SET records=excluded.records,bytes=excluded.bytes", id, idCollection, values.count, values.bytes); err != nil {
			return nil, err
		}
	}
	for i, m := range prepared.mutations {
		c := prepared.columns[i]
		deleted := 0
		if m.Delete {
			deleted = 1
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?,?,?) ON CONFLICT(scope,collection,key) DO UPDATE SET version=excluded.version,deleted=excluded.deleted,data=excluded.data", id, c.ID, m.Key, next, deleted, []byte(m.Data)); err != nil {
			return nil, err
		}
		if !m.Delete {
			for _, idx := range c.Indexes {
				tuple, nullable, e := indexTuple(prepared.documents[i], idx)
				if e != nil {
					return nil, e
				}
				var unique any
				if idx.Unique && !nullable {
					unique = tuple
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO lookups VALUES(?,?,?,?,?,?)", id, c.ID, idx.ID, tuple, m.Key, unique); err != nil {
					return nil, err
				}
			}
		}
		versions = append(versions, api.Record{Collection: m.Collection, Key: m.Key, Version: strconv.FormatInt(next, 10), Deleted: m.Delete})
	}

	return versions, nil
}
