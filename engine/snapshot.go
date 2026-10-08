package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
)

func (s *Store) Snapshot(ctx context.Context, r api.SnapshotRequest) (out api.SnapshotResponse, err error) {
	defer func() { err = classify(err) }()
	n, err := s.scope(r.Scope)
	if err != nil {
		return out, err
	}
	if len(r.Queries) == 0 || len(r.Queries) > api.MaxQueries {
		return out, fail("invalid_argument", "bounded nonempty query plan required")
	}
	ctx, release, err := s.beginCall(ctx, false)
	if err != nil {
		return out, err
	}
	defer release()
	tx, err := s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	id := scopeID(r.Scope)
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
		if total > api.MaxRows || bytes > api.MaxResponseBytes-8192 {
			return fail("resource_exhausted", "whole snapshot exceeds row/byte budget")
		}
		result.Records = append(result.Records, record)
		return nil
	}
	for _, q := range r.Queries {
		c, e := collection(n, q.Collection)
		if e != nil {
			return out, e
		}
		if q.After != "" && !key(q.After) {
			return out, fail("invalid_argument", "invalid page key")
		}
		result := api.QueryResult{Collection: q.Collection, Records: []api.Record{}}
		if len(q.Keys) > 0 {
			if len(q.Keys) > api.MaxRows || q.Index != "" || len(q.Equal) > 0 || q.After != "" || q.Limit != 0 {
				return out, fail("invalid_argument", "exact-key query cannot contain page/index fields")
			}
			for _, k := range q.Keys {
				if !key(k) {
					return out, fail("invalid_argument", "invalid record key")
				}
				record := api.Record{Key: k}
				var v int64
				var deleted int
				e := tx.QueryRowContext(ctx, "SELECT version,deleted,data FROM records WHERE scope=? AND collection=? AND key=?", id, c.ID, k).Scan(&v, &deleted, &record.Data)
				if errors.Is(e, sql.ErrNoRows) {
					record.Missing = true
					v = 0
				} else if e != nil {
					return out, e
				}
				record.Version = strconv.FormatInt(v, 10)
				record.Deleted = deleted != 0
				if err = appendRecord(&result, record); err != nil {
					return out, err
				}
			}
		} else {
			limit := q.Limit
			if limit == 0 {
				limit = 200
			}
			if limit < 1 || limit > api.MaxRows {
				return out, fail("invalid_argument", "page limit outside 1..2048")
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
					return out, fail("invalid_argument", "registered scalar equality index required")
				}
				data := map[string]any{}
				for i, v := range q.Equal {
					d := json.NewDecoder(strings.NewReader(string(v)))
					d.UseNumber()
					var decoded any
					if e = d.Decode(&decoded); e != nil {
						return out, fail("invalid_argument", "invalid equality scalar")
					}
					var tail any
					if e = d.Decode(&tail); e != io.EOF {
						return out, fail("invalid_argument", "one equality scalar required")
					}
					data[idx.Fields[i]] = decoded
				}
				value, _, e := indexTuple(data, *idx)
				if e != nil {
					return out, e
				}
				statement = "SELECT r.key,r.version,r.deleted,r.data FROM lookups l JOIN records r ON r.scope=l.scope AND r.collection=l.collection AND r.key=l.key WHERE l.scope=? AND l.collection=? AND l.index_name=? AND l.index_value=? AND l.key>?"
				args = []any{id, c.ID, q.Index, value, q.After}
			} else if len(q.Equal) > 0 {
				return out, fail("invalid_argument", "equal requires registered index")
			}
			if q.Index != "" {
				statement += " ORDER BY l.key LIMIT ?"
			} else {
				statement += " ORDER BY r.key LIMIT ?"
			}
			args = append(args, limit+1)
			rows, e := tx.QueryContext(ctx, statement, args...)
			if e != nil {
				return out, e
			}
			count := 0
			for rows.Next() {
				record := api.Record{}
				var v int64
				var deleted int
				if e = rows.Scan(&record.Key, &v, &deleted, &record.Data); e != nil {
					rows.Close()
					return out, e
				}
				count++
				if count > limit {
					result.NextAfter = result.Records[len(result.Records)-1].Key
					break
				}
				record.Version = strconv.FormatInt(v, 10)
				record.Deleted = deleted != 0
				if e = appendRecord(&result, record); e != nil {
					rows.Close()
					return out, e
				}
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return out, e
			}
		}
		out.Results = append(out.Results, result)
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	raw, e := json.Marshal(out)
	if e != nil {
		return out, e
	}
	if len(raw) > api.MaxResponseBytes {
		return api.SnapshotResponse{}, fail("resource_exhausted", fmt.Sprintf("snapshot response %d exceeds byte limit", len(raw)))
	}
	return out, nil
}
