package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationCatalog(ctx context.Context, tx *sql.Tx, scope, operation string, q model.ConfigurationCatalogRead) ([]json.RawMessage, error) {
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return nil, err
	}
	if q.Offset < 0 || q.Limit < 0 || q.Limit > 4096 || len(q.ID) > 255 || q.ParentID != nil && len(*q.ParentID) > 255 {
		return nil, failure("invalid_argument", "invalid catalog selector")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 4096
	}
	args := []any{scope, q.Domain.Key}
	// Every query is fixed, scoped and bounded at its indexed owner table.
	// The operation is declared in Spec; callers never supply predicates/columns.
	var query string
	switch operation {
	case model.ConfigurationNamespacesOperation:
		query = "SELECT body FROM core_namespaces WHERE scope=? AND domain=?"
		if q.ID != "" {
			query += " AND id=?"
			args = append(args, q.ID)
		}
		if q.ParentID != nil {
			query += " AND coalesce(parent_id,'')=?"
			args = append(args, *q.ParentID)
		}
		if !q.IncludeArchived {
			query += " AND archived=0"
		}
		query += " ORDER BY coalesce(parent_id,''),name_key,id"
	case model.ConfigurationResourcesOperation:
		query = "SELECT body FROM core_resources WHERE scope=? AND domain=?"
		if q.ID != "" {
			query += " AND id=?"
			args = append(args, q.ID)
		}
		if q.ParentID != nil {
			query += " AND coalesce(namespace_id,'')=?"
			args = append(args, *q.ParentID)
		}
		if !q.IncludeArchived {
			query += " AND archived=0"
		}
		query += " ORDER BY name_key,id"
	case model.ConfigurationBranchesOperation:
		if !textKey(q.ID) {
			return nil, failure("invalid_argument", "resource identity required")
		}
		query = "SELECT body FROM core_branches WHERE scope=? AND domain=? AND resource_id=?"
		args = append(args, q.ID)
		if !q.IncludeArchived {
			query += " AND coalesce(json_extract(CAST(body AS TEXT),'$.archived_at'),'')=''"
		}
		query += " ORDER BY name_key,id"
	case model.ConfigurationCommitsOperation:
		if !textKey(q.ID) {
			return nil, failure("invalid_argument", "resource identity required")
		}
		query = "SELECT body FROM core_snapshots WHERE scope=? AND domain=? AND resource_id=? ORDER BY version DESC"
		args = append(args, q.ID)
	case model.ConfigurationChangesOperation:
		if !textKey(q.ID) {
			return nil, failure("invalid_argument", "resource identity required")
		}
		query = "SELECT body FROM core_changes WHERE scope=? AND domain=? AND entity_id=? ORDER BY id DESC"
		args = append(args, q.ID)
	default:
		return nil, failure("invalid_argument", "unregistered configuration catalog")
	}
	readLimit := limit
	if q.Limit == 0 {
		readLimit++
	}
	query += " LIMIT ? OFFSET ?"
	args = append(args, readLimit, q.Offset)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]json.RawMessage, 0)
	size := 2
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			return nil, err
		}
		size += len(body) + 1
		if len(result) == limit {
			return nil, failure("resource_exhausted", "catalog read exceeds requested bound")
		}
		if size > MaxResponseBytes {
			return nil, failure("resource_exhausted", "catalog metadata exceeds response bound")
		}
		if !object(body) {
			return nil, failure("data_loss", "invalid catalog metadata")
		}
		result = append(result, json.RawMessage(body))
	}
	return result, rows.Err()
}
