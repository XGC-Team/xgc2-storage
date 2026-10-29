package coredata

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const maxCommitIdentityBatch = 8192

// ReadCommitIdentities resolves an exact finite set in one reader admission.
// Missing or corrupt individual identities are explicitly marked invalid; the
// owner never reads payloads, manifests or references. Invalid domain/cursor,
// cancellation, SQL and aggregate overflow fail without any partial result.
func (s *Store) ReadCommitIdentities(ctx context.Context, domain model.ConfigurationDomainGuard, ids []string) ([]model.ConfigurationCommitIdentityState, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationCommitIdentityState, error) {
		return configurationCommitIdentities(ctx, tx, scope, domain, ids)
	}, func() {
		if len(ids) <= maxCommitIdentityBatch {
			ids = append([]string(nil), ids...)
		}
	})
}

const configurationCommitIdentitiesSQL = `SELECT x.id,x.resource_id,x.branch_id,x.version,x.source_commit_id,x.content_digest,CASE WHEN length(x.body)<=262144 THEN x.body ELSE X'' END,CASE WHEN length(x.identity_json)<=16384 THEN x.identity_json ELSE X'' END,CASE WHEN length(x.catalog_summary_json)<=16384 THEN x.catalog_summary_json ELSE X'' END,coalesce(xb.resource_id,''),` + configurationResourceHeaderColumns + `
FROM json_each(?) wanted JOIN core_snapshots x ON x.scope=? AND x.domain=? AND x.id=wanted.value
LEFT JOIN core_branches xb ON xb.scope=x.scope AND xb.domain=x.domain AND xb.id=x.branch_id
JOIN core_resources r ON r.scope=x.scope AND r.domain=x.domain AND r.id=x.resource_id
LEFT JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
LEFT JOIN core_snapshots s ON s.scope=r.scope AND s.domain=r.domain AND s.resource_id=r.id AND s.id=r.main_commit_id`

func configurationCommitIdentities(ctx context.Context, tx *sql.Tx, scope string, domain model.ConfigurationDomainGuard, ids []string) ([]model.ConfigurationCommitIdentityState, error) {
	if _, err := configurationDomain(ctx, tx, domain); err != nil {
		return nil, err
	}
	if len(ids) > maxCommitIdentityBatch {
		return nil, failure("resource_exhausted", "commit identity batch exceeds count bound")
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !textKey(id) || seen[id] {
			return nil, failure("invalid_argument", "distinct exact commit identities required")
		}
		seen[id] = true
	}
	out := make([]model.ConfigurationCommitIdentityState, 0, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, configurationCommitIdentitiesSQL, string(raw), scope, domain.Key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := make(map[string]model.ConfigurationCommitIdentityState, len(ids))
	bytes, responseBytes := len(raw), 2
	for rows.Next() {
		var selected model.ConfigurationCommit
		var body, identity, summary []byte
		var branchResource string
		var main configurationHeaderScan
		fields := append([]any{&selected.ID, &selected.ResourceID, &selected.BranchID, &selected.Version, &selected.SourceCommitID, &selected.ContentDigest, &body, &identity, &summary, &branchResource}, main.fields()...)
		if err = rows.Scan(fields...); err != nil {
			return nil, err
		}
		bytes += len(body) + len(identity) + len(summary) + len(main.resourceBody) + len(main.branchBody) + len(main.commitBody) + len(main.header.Identity) + len(main.header.CatalogSummary)
		if bytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "commit identity batch exceeds byte bound")
		}
		item := model.ConfigurationCommitIdentityState{ID: selected.ID}
		commit, e := configurationDecodeCommitRow(selected, body)
		if e == nil && branchResource == selected.ResourceID {
			e = configurationProjection(commit, identity, summary)
		} else if e == nil {
			e = failure("data_loss", "selected commit branch disagrees")
		}
		if e == nil {
			_, e = main.decoded()
		}
		if e == nil {
			item.Identity = identity
			item.Valid = true
		}
		encoded, e := encode(item)
		if e != nil {
			return nil, e
		}
		responseBytes += len(encoded) + 1
		if responseBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "commit identity batch exceeds response-byte bound")
		}
		found[item.ID] = item
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		item, ok := found[id]
		if !ok {
			item = model.ConfigurationCommitIdentityState{ID: id}
			encoded, e := encode(item)
			if e != nil {
				return nil, e
			}
			responseBytes += len(encoded) + 1
			if responseBytes > MaxResponseBytes {
				return nil, failure("resource_exhausted", "commit identity batch exceeds response-byte bound")
			}
		}
		out = append(out, item)
	}
	return out, nil
}
