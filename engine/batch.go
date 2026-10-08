package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func (s *Store) Batch(ctx context.Context, r api.BatchRequest) (out api.Receipt, err error) {
	r.Mutations = append([]api.Mutation(nil), r.Mutations...)
	defer func() { err = classify(err) }()
	n, err := s.scope(r.Scope)
	if err != nil {
		return out, err
	}
	if !identifier(r.RequestID) || len(r.Mutations) == 0 || len(r.Mutations) > api.MaxOperations {
		return out, fail("invalid_argument", "request identity and 1..256 mutations required")
	}
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return out, err
	}
	defer release()
	seen := map[string]bool{}
	requestBytes := 0
	documents := make([]map[string]any, len(r.Mutations))
	columns := make([]api.Collection, len(r.Mutations))
	for i := range r.Mutations {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		m := &r.Mutations[i]
		c, e := collection(n, m.Collection)
		if e != nil {
			return out, e
		}
		columns[i] = c
		requestBytes += len(m.Data) + len(m.Key) + len(m.Collection) + 128
		if len(m.Data) > api.MaxRequestBytes || requestBytes > api.MaxRequestBytes {
			return out, fail("resource_exhausted", "batch raw payload byte limit exceeded")
		}
		compound := m.Collection + "\x00" + m.Key
		if !key(m.Key) || seen[compound] {
			return out, fail("invalid_argument", "invalid/duplicate mutation key")
		}
		seen[compound] = true
		if _, e = revision(m.ExpectedVersion); e != nil {
			return out, e
		}
		if m.Delete {
			if len(m.Data) > 0 {
				return out, fail("invalid_argument", "delete must not contain data")
			}
			m.Data = json.RawMessage(`{}`)
		} else {
			canonical, obj, e := canonicalObject(m.Data)
			if e != nil {
				return out, e
			}
			if len(canonical) > c.MaxRecordBytes {
				return out, fail("resource_exhausted", "record exceeds declared byte limit")
			}
			m.Data = canonical
			documents[i] = obj
			for _, idx := range c.Indexes {
				if _, _, e = indexTuple(obj, idx); e != nil {
					return out, e
				}
			}
		}
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return out, e
	}
	if len(raw) > api.MaxRequestBytes {
		return out, fail("resource_exhausted", "batch byte limit exceeded")
	}
	digest := hash(r)
	if err = s.diskCheck(); err != nil {
		return out, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	// Acquire the WAL writer before any read; no snapshot-upgrade busy loop.
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET id=id WHERE id=1"); err != nil {
		return out, err
	}
	id := scopeID(r.Scope)
	var previousDigest string
	var receiptRaw []byte
	var expires int64
	err = tx.QueryRowContext(ctx, "SELECT digest,body,expires FROM receipts WHERE scope=? AND request_id=?", id, r.RequestID).Scan(&previousDigest, &receiptRaw, &expires)
	if err == nil {
		if previousDigest != digest {
			return out, fail("conflict", "request identity reused with different plan")
		}
		if err = json.Unmarshal(receiptRaw, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var rev int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM scopes WHERE scope=?", id).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		rev = 0
		var scopes int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE namespace=?", n.ID).Scan(&scopes); err != nil {
			return out, err
		}
		if scopes >= n.MaxScopes {
			return out, fail("resource_exhausted", "namespace scope quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,?,0)", id, n.ID); err != nil {
			return out, err
		}
	} else if err != nil {
		return out, err
	}
	if err = s.checkToken(n, r.Expected, rev); err != nil {
		return out, err
	}
	if rev == math.MaxInt64 {
		return out, fail("resource_exhausted", "revision space exhausted")
	}
	next := rev + 1
	var receiptCount int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM receipts WHERE scope=?", id).Scan(&receiptCount); err != nil {
		return out, err
	}
	if receiptCount >= n.MaxReceipts {
		return out, fail("resource_exhausted", "receipt quota reached; owner maintenance required")
	}
	out = api.Receipt{RequestID: r.RequestID, Digest: digest, Token: s.token(n, next), Durability: "sqlite-full", Versions: []api.Record{}}
	// Remove all old indexes first, permitting valid swaps in an atomic batch.
	for _, m := range r.Mutations {
		var v int64
		err = tx.QueryRowContext(ctx, "SELECT version FROM records WHERE scope=? AND collection=? AND key=?", id, m.Collection, m.Key).Scan(&v)
		if errors.Is(err, sql.ErrNoRows) {
			v = 0
		} else if err != nil {
			return out, err
		}
		if strconv.FormatInt(v, 10) != m.ExpectedVersion {
			s.conflicts.Add(1)
			return out, fail("conflict", "record version changed")
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM lookups WHERE scope=? AND collection=? AND key=?", id, m.Collection, m.Key); err != nil {
			return out, err
		}
	}
	type totals struct {
		count, bytes int64
		collection   api.Collection
	}
	usage := map[string]*totals{}
	for i, m := range r.Mutations {
		c := columns[i]
		values := usage[c.ID]
		if values == nil {
			values = &totals{collection: c}
			usage[c.ID] = values
			err = tx.QueryRowContext(ctx, "SELECT records,bytes FROM usage WHERE scope=? AND collection=?", id, c.ID).Scan(&values.count, &values.bytes)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return out, err
			}
		}
		var oldBytes int64
		err = tx.QueryRowContext(ctx, "SELECT length(data)+length(CAST(key AS BLOB)) FROM records WHERE scope=? AND collection=? AND key=?", id, c.ID, m.Key).Scan(&oldBytes)
		if errors.Is(err, sql.ErrNoRows) {
			values.count++
		} else if err != nil {
			return out, err
		}
		values.bytes += int64(len(m.Data)+len(m.Key)) - oldBytes
	}
	for idCollection, values := range usage {
		if values.count > int64(values.collection.MaxRecords) || values.bytes > values.collection.MaxBytes {
			return out, fail("resource_exhausted", "collection record/byte quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO usage VALUES(?,?,?,?) ON CONFLICT(scope,collection) DO UPDATE SET records=excluded.records,bytes=excluded.bytes", id, idCollection, values.count, values.bytes); err != nil {
			return out, err
		}
	}
	for i, m := range r.Mutations {
		c := columns[i]
		deleted := 0
		if m.Delete {
			deleted = 1
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(?,?,?,?,?,?) ON CONFLICT(scope,collection,key) DO UPDATE SET version=excluded.version,deleted=excluded.deleted,data=excluded.data", id, c.ID, m.Key, next, deleted, []byte(m.Data)); err != nil {
			return out, err
		}
		if !m.Delete {
			for _, idx := range c.Indexes {
				tuple, nullable, e := indexTuple(documents[i], idx)
				if e != nil {
					return out, e
				}
				var unique any
				if idx.Unique && !nullable {
					unique = tuple
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO lookups VALUES(?,?,?,?,?,?)", id, c.ID, idx.ID, tuple, m.Key, unique); err != nil {
					return out, err
				}
			}
		}
		out.Versions = append(out.Versions, api.Record{Collection: m.Collection, Key: m.Key, Version: strconv.FormatInt(next, 10), Deleted: m.Delete})
	}
	now := time.Now().UTC()
	out.CommittedAt = now.Format(time.RFC3339Nano)
	out.ExpiresAt = now.Add(time.Duration(n.ReceiptTTLSeconds) * time.Second).Format(time.RFC3339Nano)
	receiptRaw, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE scopes SET revision=? WHERE scope=?", next, id); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO receipts VALUES(?,?,?,?,?)", id, r.RequestID, digest, now.Unix()+int64(n.ReceiptTTLSeconds), receiptRaw); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return api.Receipt{}, err
	}
	return out, nil
}
func (s *Store) Receipt(ctx context.Context, r api.ReceiptRequest) (out api.Receipt, err error) {
	defer func() { err = classify(err) }()
	if _, err = s.scope(r.Scope); err != nil {
		return out, err
	}
	if !identifier(r.RequestID) {
		return out, fail("invalid_argument", "invalid receipt identity")
	}
	ctx, release, err := s.beginCall(ctx, false)
	if err != nil {
		return out, err
	}
	defer release()
	var raw []byte
	err = s.reader.QueryRowContext(ctx, "SELECT body FROM receipts WHERE scope=? AND request_id=?", scopeID(r.Scope), r.RequestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fail("not_found", "receipt not retained; previous outcome may still be unknown")
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
