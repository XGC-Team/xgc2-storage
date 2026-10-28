package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
)

func recordHeadersQuery(count int) string {
	var query strings.Builder
	query.WriteString("WITH requested(ordinal,key) AS (VALUES ")
	for i := range count {
		if i != 0 {
			query.WriteByte(',')
		}
		fmt.Fprintf(&query, "(%d,?)", i)
	}
	query.WriteString(`) SELECT q.ordinal,q.key,r.version,r.deleted,length(r.data),l.index_value
FROM requested q LEFT JOIN records r ON r.scope=? AND r.collection=? AND r.key=q.key
LEFT JOIN lookups l INDEXED BY lookups_by_key
ON l.scope=r.scope AND l.collection=r.collection AND l.key=r.key AND l.index_name=?
ORDER BY q.ordinal`)
	return query.String()
}

func (s *Store) queryRecordHeaders(ctx context.Context, tx *sql.Tx, scope string, collection api.Collection, query api.Query, appendRecord func(*api.QueryResult, api.Record) error) (api.QueryResult, error) {
	result := api.QueryResult{Collection: collection.ID, Records: []api.Record{}}
	if len(query.Keys) == 0 || len(query.Keys) > api.MaxRows || query.Index != "" || len(query.Equal) != 0 || query.After != "" || query.Limit != 0 {
		return result, fail("invalid_argument", "record header projection requires bounded exact keys only")
	}
	var index *api.Index
	for i := range collection.Indexes {
		if collection.Indexes[i].ID == query.HeaderIndex {
			index = &collection.Indexes[i]
			break
		}
	}
	if index == nil {
		return result, fail("invalid_argument", "record header projection requires a declared index")
	}
	args := make([]any, 0, len(query.Keys)+3)
	for _, recordKey := range query.Keys {
		if !key(recordKey) {
			return result, fail("invalid_argument", "invalid record header key")
		}
		args = append(args, recordKey)
	}
	args = append(args, scope, collection.ID, index.ID)
	rows, err := tx.QueryContext(ctx, recordHeadersQuery(len(query.Keys)), args...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ordinal int
		var recordKey string
		var version, deleted, bytes sql.NullInt64
		var tuple sql.NullString
		if err := rows.Scan(&ordinal, &recordKey, &version, &deleted, &bytes, &tuple); err != nil {
			return result, err
		}
		if ordinal != count || ordinal >= len(query.Keys) || recordKey != query.Keys[ordinal] {
			return result, fail("data_loss", "record header index entries do not match the exact key plan")
		}
		count++
		record := api.Record{Key: recordKey, Version: "0", Missing: !version.Valid, Header: &api.RecordHeader{Index: index.ID}}
		if version.Valid {
			s.snapshotHeaderReads.Add(1)
			s.snapshotHeaderBytes.Add(uint64(len(tuple.String)))
			record.Version, record.Deleted, record.Header.Bytes = strconv.FormatInt(version.Int64, 10), deleted.Int64 != 0, bytes.Int64
			maximum := int64(collection.MaxRecordBytes)
			if record.Deleted {
				maximum = max(maximum, 2) // A canonical tombstone stores {}.
			}
			if !deleted.Valid || !bytes.Valid || record.Header.Bytes < 0 || record.Header.Bytes > maximum {
				return result, fail("data_loss", "record header length exceeds its declaration")
			}
			if record.Deleted {
				if tuple.Valid {
					return result, fail("data_loss", "deleted record retains an index tuple")
				}
			} else {
				if !tuple.Valid || !validStoredTuple(tuple.String, len(index.Fields)) {
					return result, fail("data_loss", "live record is missing its declared scalar tuple")
				}
				record.Header.Tuple = json.RawMessage(tuple.String)
			}
		}
		if err := appendRecord(&result, record); err != nil {
			return result, err
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return result, err
	}
	if count != len(query.Keys) {
		return result, fail("data_loss", "record header result omitted an exact key")
	}
	return result, nil
}

func validStoredTuple(value string, fields int) bool {
	if len(value) > 4096 {
		return false
	}
	var tuple []json.RawMessage
	if json.Unmarshal([]byte(value), &tuple) != nil || len(tuple) != fields {
		return false
	}
	canonical := make([]any, len(tuple))
	for i, raw := range tuple {
		var scalar any
		if json.Unmarshal(raw, &scalar) != nil {
			return false
		}
		canonical[i] = scalar
		switch value := scalar.(type) {
		case nil, bool, string:
		case map[string]any:
			number, ok := value["number"].(string)
			if !ok || len(value) != 1 || len(number) > 512 {
				return false
			}
			rational, ok := new(big.Rat).SetString(number)
			if !ok || rational.RatString() != number {
				return false
			}
		default:
			return false
		}
	}
	encoded, err := json.Marshal(canonical)
	return err == nil && string(encoded) == value
}
