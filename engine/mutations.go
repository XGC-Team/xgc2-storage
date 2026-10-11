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

// applyMutations is the one record mutation algorithm used by Batch. Admission,
// receipt and COMMIT remain with the caller. Quotas count live records and
// bytes, so a delete frees them at once; the tombstone it leaves keeps the CAS
// version and is itself bounded, oldest dropped first.
func (s *Store) applyMutations(ctx context.Context, tx *sql.Tx, id string, next int64, prepared preparedMutations) ([]api.Record, error) {
	var err error
	versions := make([]api.Record, 0, len(prepared.mutations))
	type previousRecord struct {
		version, bytes  int64
		live, tombstone bool
	}
	type lookupChange struct {
		index, tuple string
		unique       any
	}
	previous := make([]previousRecord, len(prepared.mutations))
	insertions := make([][]lookupChange, len(prepared.mutations))
	// Remove changed old indexes before any insert, permitting atomic swaps.
	// Unchanged tuples need no index writes or history-wide lookup scans.
	for i, m := range prepared.mutations {
		c := prepared.columns[i]
		var v int64
		var deleted bool
		var oldData []byte
		err = tx.QueryRowContext(ctx, "SELECT version,deleted,data FROM records WHERE scope=? AND collection=? AND key=?", id, m.Collection, m.Key).Scan(&v, &deleted, &oldData)
		if errors.Is(err, sql.ErrNoRows) {
			v = 0
		} else if err != nil {
			return nil, err
		}
		if strconv.FormatInt(v, 10) != m.ExpectedVersion {
			s.conflicts.Add(1)
			return nil, fail("conflict", "record version changed")
		}
		previous[i] = previousRecord{version: v, live: v > 0 && !deleted, tombstone: v > 0 && deleted}
		if previous[i].live {
			previous[i].bytes = int64(len(oldData) + len(m.Key))
		}
		hadIndexes := previous[i].live
		var oldDocument map[string]any
		if hadIndexes && len(c.Indexes) > 0 {
			_, oldDocument, err = canonicalObject(oldData)
			if err != nil {
				return nil, err
			}
		}
		for _, idx := range c.Indexes {
			var oldTuple string
			if hadIndexes {
				oldTuple, _, err = indexTuple(oldDocument, idx)
				if err != nil {
					return nil, err
				}
			}
			change := lookupChange{index: idx.ID}
			if !m.Delete {
				var nullable bool
				change.tuple, nullable, err = indexTuple(prepared.documents[i], idx)
				if err != nil {
					return nil, err
				}
				if hadIndexes && oldTuple == change.tuple {
					continue
				}
				if idx.Unique && !nullable {
					change.unique = change.tuple
				}
				insertions[i] = append(insertions[i], change)
			}
			if hadIndexes {
				if _, err = tx.ExecContext(ctx, "DELETE FROM lookups WHERE scope=? AND collection=? AND index_name=? AND index_value=? AND key=?", id, c.ID, idx.ID, oldTuple, m.Key); err != nil {
					return nil, err
				}
			}
		}
	}
	type totals struct {
		records, bytes, tombstones int64
		collection                 api.Collection
	}
	usage := map[string]*totals{}
	for i, m := range prepared.mutations {
		c := prepared.columns[i]
		values := usage[c.ID]
		if values == nil {
			values = &totals{collection: c}
			usage[c.ID] = values
			err = tx.QueryRowContext(ctx, "SELECT records,bytes,tombstones FROM usage WHERE scope=? AND collection=?", id, c.ID).Scan(&values.records, &values.bytes, &values.tombstones)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		p := previous[i]
		if p.live {
			values.records--
			values.bytes -= p.bytes
		}
		if p.tombstone {
			values.tombstones--
		}
		if m.Delete {
			values.tombstones++
		} else {
			values.records++
			values.bytes += int64(len(m.Data) + len(m.Key))
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
		for _, change := range insertions[i] {
			if _, err = tx.ExecContext(ctx, "INSERT INTO lookups VALUES(?,?,?,?,?,?)", id, c.ID, change.index, change.tuple, m.Key, change.unique); err != nil {
				return nil, err
			}
		}
		versions = append(versions, api.Record{Collection: m.Collection, Key: m.Key, Version: strconv.FormatInt(next, 10), Deleted: m.Delete})
	}
	for collectionID, values := range usage {
		if values.records > int64(values.collection.MaxRecords) || values.bytes > values.collection.MaxBytes {
			return nil, fail("resource_exhausted", "collection record/byte quota reached")
		}
		if values.tombstones > int64(values.collection.MaxRecords) {
			// Older tombstones go first; the ones written by this batch stay.
			result, e := tx.ExecContext(ctx, "DELETE FROM records WHERE rowid IN (SELECT rowid FROM records WHERE scope=? AND collection=? AND deleted=1 AND version<? ORDER BY version LIMIT ?)", id, collectionID, next, values.tombstones-int64(values.collection.MaxRecords))
			if e != nil {
				return nil, e
			}
			dropped, e := result.RowsAffected()
			if e != nil {
				return nil, e
			}
			values.tombstones -= dropped
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO usage VALUES(?,?,?,?,?) ON CONFLICT(scope,collection) DO UPDATE SET records=excluded.records,bytes=excluded.bytes,tombstones=excluded.tombstones", id, collectionID, values.records, values.bytes, values.tombstones); err != nil {
			return nil, err
		}
	}
	return versions, nil
}
