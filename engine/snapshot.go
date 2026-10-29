package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
)

// Snapshot reads a consistent set of queries at one scope revision. Inside a
// read view the first read pins the revision and later reads must see it.
func (s *Store) Snapshot(ctx context.Context, r api.SnapshotRequest) (out api.SnapshotResponse, err error) {
	defer func() { err = classify(err) }()
	n, err := s.scope(r.Scope)
	if err != nil {
		return out, err
	}
	if len(r.Queries) == 0 || len(r.Queries) > api.MaxQueries {
		return out, fail("invalid_argument", "bounded nonempty query plan required")
	}
	if view := viewOf(ctx); view != nil {
		unlock, err := view.begin(r.Scope, &r)
		if err != nil {
			return out, err
		}
		defer unlock()
		defer func() { view.pin(out, err) }()
	}
	var response api.SnapshotResponse
	err = s.readWithInput(ctx, func(ctx context.Context, tx *sql.Tx, input *WriteInput) error {
		if err := input.Own(ctx, func() { r = ownedSnapshotRequest(r) }); err != nil {
			return err
		}
		var e error
		response, e = s.snapshot(ctx, tx, n, r, input)
		return e
	})
	if err == nil {
		out = response
	}
	return out, err
}

// The outer plan must survive caller return; each nested key/equality slice is
// owned only when its validated query reaches the point that consumes it.
func ownedSnapshotRequest(r api.SnapshotRequest) api.SnapshotRequest {
	if r.At != nil {
		at := *r.At
		r.At = &at
	}
	r.Queries = append([]api.Query(nil), r.Queries...)
	return r
}

// An accepted JSON string can use at most six source bytes per output byte
// (for example, \u0061 for a). Numbers already have a tighter 128-byte
// index bound. Strip only JSON whitespace before applying this bound, so
// escaped scalars and arbitrarily padded valid values remain accepted.
const maxIndexScalarSourceBytes = 6*maxIndexTupleBytes + 2

type ownedEquality struct {
	value any
	kind  byte // oversized valid string, number, array or object
}

func ownSnapshotEquality(ctx context.Context, equal []json.RawMessage) ([]ownedEquality, error) {
	owned := make([]ownedEquality, len(equal))
	for i, raw := range equal {
		start, end := 0, len(raw)
		for start < end && jsonSpace(raw[start]) {
			start++
			if start&4095 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		for start < end && jsonSpace(raw[end-1]) {
			end--
			if (len(raw)-end)&4095 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		if end-start > maxIndexScalarSourceBytes {
			if err := validateLargeEqualityJSON(ctx, raw[start:end]); err != nil {
				return nil, err
			}
			owned[i].kind = raw[start]
		} else {
			// Decode while the input owner still protects the borrowed bytes.
			// An earlier syntax error must stop work before later large values.
			d := json.NewDecoder(bytes.NewReader(raw[start:end]))
			d.UseNumber()
			if err := d.Decode(&owned[i].value); err != nil {
				return nil, fail("invalid_argument", "invalid equality scalar")
			}
			var tail any
			if err := d.Decode(&tail); err != io.EOF {
				return nil, fail("invalid_argument", "one equality scalar required")
			}
		}
	}
	return owned, nil
}

func (s *Store) snapshot(ctx context.Context, tx *sql.Tx, n api.Namespace, r api.SnapshotRequest, input *WriteInput) (out api.SnapshotResponse, err error) {
	id := ScopeID(r.Scope)
	var rev int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM scopes WHERE scope=?", id).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		rev = 0
		err = nil
	}
	if err != nil {
		return out, err
	}
	if r.At != nil {
		if err = s.checkToken(n, *r.At, rev); err != nil {
			return out, err
		}
	}
	out = api.SnapshotResponse{Scope: r.Scope, Token: s.token(n, rev), Results: make([]api.QueryResult, 0, len(r.Queries))}
	total, bytes := 0, 0
	appendRecord := func(result *api.QueryResult, record api.Record) error {
		total++
		bytes += len(record.Key) + len(record.Data) + 128
		if record.Header != nil {
			bytes += len(record.Header.Tuple) + len(record.Header.Index) + 128
		}
		if total > api.MaxRows || bytes > api.MaxResponseBytes-8192 {
			return fail("resource_exhausted", "whole snapshot exceeds row/byte budget")
		}
		result.Records = append(result.Records, record)
		return nil
	}
	for _, q := range r.Queries {
		result, e := s.queryRecords(ctx, tx, id, n, q, input, appendRecord)
		if e != nil {
			return out, e
		}
		out.Results = append(out.Results, result)
	}
	return out, nil
}

// queryRecords materializes one bounded data query in a caller-owned
// transaction. appendRecord charges every record to the snapshot's budget.
func (s *Store) queryRecords(ctx context.Context, tx *sql.Tx, id string, n api.Namespace, q api.Query, input *WriteInput, appendRecord func(*api.QueryResult, api.Record) error) (result api.QueryResult, err error) {
	result = api.QueryResult{Collection: q.Collection, Records: []api.Record{}}
	c, e := collection(n, q.Collection)
	if e != nil {
		return result, e
	}
	if q.HeaderIndex != "" {
		return s.queryRecordHeaders(ctx, tx, id, c, q, input, appendRecord)
	}
	if q.After != "" && !key(q.After) {
		return result, fail("invalid_argument", "invalid page key")
	}
	if len(q.Keys) > 0 {
		if len(q.Keys) > api.MaxRows || q.Index != "" || len(q.Equal) > 0 || q.After != "" || q.Limit != 0 {
			return result, fail("invalid_argument", "exact-key query cannot contain page/index fields")
		}
		if err := input.Own(ctx, func() { q.Keys = append([]string(nil), q.Keys...) }); err != nil {
			return result, err
		}
		for _, k := range q.Keys {
			if !key(k) {
				return result, fail("invalid_argument", "invalid record key")
			}
			record := api.Record{Key: k}
			var v int64
			var deleted int
			e := tx.QueryRowContext(ctx, "SELECT version,deleted,data FROM records WHERE scope=? AND collection=? AND key=?", id, c.ID, k).Scan(&v, &deleted, &record.Data)
			s.snapshotPayloadReads.Add(1)
			s.snapshotPayloadBytes.Add(uint64(len(record.Data)))
			if errors.Is(e, sql.ErrNoRows) {
				record.Missing = true
				v = 0
			} else if e != nil {
				return result, e
			}
			record.Version = strconv.FormatInt(v, 10)
			record.Deleted = deleted != 0
			if err = appendRecord(&result, record); err != nil {
				return result, err
			}
		}
	} else {
		limit := q.Limit
		if limit == 0 {
			limit = 200
		}
		if limit < 1 || limit > api.MaxRows {
			return result, fail("invalid_argument", "page limit outside 1..2048")
		}
		statement := "SELECT r.key,r.version,r.deleted,r.data FROM records r WHERE r.scope=? AND r.collection=? AND r.key>?"
		args := []any{id, c.ID, q.After}
		if !q.IncludeDeleted {
			statement += " AND r.deleted=0"
		}
		if q.Index != "" {
			var idx *api.Index
			for i := range c.Indexes {
				if c.Indexes[i].ID == q.Index {
					idx = &c.Indexes[i]
					break
				}
			}
			if idx == nil || len(q.Equal) != len(idx.Fields) || q.IncludeDeleted {
				return result, fail("invalid_argument", "registered scalar equality index required")
			}
			var equal []ownedEquality
			var ownErr error
			if err := input.Own(ctx, func() { equal, ownErr = ownSnapshotEquality(ctx, q.Equal) }); err != nil {
				return result, err
			}
			if ownErr != nil {
				return result, ownErr
			}
			data := map[string]any{}
			for i, v := range equal {
				var decoded any
				// A large value's exact bytes cannot fit a stored tuple. These
				// stand-ins preserve indexTuple's type/error ordering, and a
				// later occurrence of the same field may still overwrite them.
				switch v.kind {
				case 0:
					decoded = v.value
				case '"':
					decoded = strings.Repeat("x", maxIndexTupleBytes+1)
				case '[', '{':
					decoded = []any{}
				default:
					decoded = json.Number(strings.Repeat("1", maxIndexNumberBytes+1))
				}
				// Preserve the existing last-value semantics for repeated fields.
				data[idx.Fields[i]] = decoded
			}
			value, _, e := indexTuple(data, *idx)
			if e != nil {
				return result, e
			}
			statement = "SELECT r.key,r.version,r.deleted,r.data FROM lookups l JOIN records r ON r.scope=l.scope AND r.collection=l.collection AND r.key=l.key WHERE l.scope=? AND l.collection=? AND l.index_name=? AND l.index_value=? AND l.key>?"
			args = []any{id, c.ID, q.Index, value, q.After}
		} else if len(q.Equal) > 0 {
			return result, fail("invalid_argument", "equal requires registered index")
		}
		if q.Index != "" {
			statement += " ORDER BY l.key LIMIT ?"
		} else {
			statement += " ORDER BY r.key LIMIT ?"
		}
		args = append(args, limit+1)
		rows, e := tx.QueryContext(ctx, statement, args...)
		if e != nil {
			return result, e
		}
		count := 0
		for rows.Next() {
			record := api.Record{}
			var v int64
			var deleted int
			if e = rows.Scan(&record.Key, &v, &deleted, &record.Data); e != nil {
				rows.Close()
				return result, e
			}
			s.snapshotPayloadReads.Add(1)
			s.snapshotPayloadBytes.Add(uint64(len(record.Data)))
			record.Version = strconv.FormatInt(v, 10)
			record.Deleted = deleted != 0
			count++
			if count > limit {
				result.NextAfter = result.Records[len(result.Records)-1].Key
				break
			}
			if e = appendRecord(&result, record); e != nil {
				rows.Close()
				return result, e
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return result, e
		}
	}

	return result, nil
}
