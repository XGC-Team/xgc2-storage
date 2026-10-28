package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadCommitCatalog returns a bounded history page without reading its frozen
// payloads, manifests or references. Domains admit each identity themselves.
func (s *Store) ReadCommitCatalog(ctx context.Context, q model.ConfigurationCatalogRead) (model.ConfigurationCommitCatalog, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationCommitCatalog, error) {
		return configurationCommitCatalog(ctx, tx, scope, q)
	})
}

const configurationCommitCatalogSQL = `SELECT s.id,s.resource_id,s.branch_id,s.version,s.source_commit_id,s.content_digest,CASE WHEN length(s.body)<=262144 THEN s.body ELSE X'' END,CASE WHEN length(s.identity_json)<=16384 THEN s.identity_json ELSE X'' END,CASE WHEN length(s.catalog_summary_json)<=16384 THEN s.catalog_summary_json ELSE X'' END,
coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),coalesce(b.revision,0),CASE WHEN length(b.body)<=262144 THEN b.body ELSE X'' END
FROM core_snapshots s LEFT JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.id=s.branch_id
WHERE s.scope=? AND s.domain=? AND s.resource_id=? ORDER BY s.version DESC LIMIT ? OFFSET ?`

func configurationCommitCatalog(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead) (out model.ConfigurationCommitCatalog, err error) {
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return out, err
	}
	if !textKey(q.ID) || q.ParentID != nil || q.Offset < 0 || q.Limit < 0 || q.Limit > 4096 {
		return out, failure("invalid_argument", "exact resource and bounded history page required")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 4096
	}
	readLimit := limit
	if q.Limit == 0 {
		readLimit++
	}
	out.Resource, err = configurationResource(ctx, tx, scope, q.Domain.Key, q.ID, q.IncludeArchived)
	if err != nil {
		return out, err
	}
	out.CurrentMain, err = configurationCurrentMain(ctx, tx, scope, q.Domain.Key, out.Resource, nil)
	if err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, configurationCommitCatalogSQL, scope, q.Domain.Key, q.ID, readLimit, q.Offset)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	out.Items = []model.ConfigurationCommitHeader{}
	metadata, err := encode(out)
	if err != nil {
		return model.ConfigurationCommitCatalog{}, err
	}
	responseBytes := len(metadata)
	selectedBytes := len(out.CurrentMain.Identity)
	for rows.Next() {
		var item model.ConfigurationCommitHeader
		var commit model.ConfigurationCommit
		var branch model.ConfigurationBranch
		var commitBody, branchBody []byte
		if err = rows.Scan(&commit.ID, &commit.ResourceID, &commit.BranchID, &commit.Version, &commit.SourceCommitID, &commit.ContentDigest, &commitBody, &item.Identity, &item.CatalogSummary,
			&branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody); err != nil {
			return model.ConfigurationCommitCatalog{}, err
		}
		selectedBytes += len(commit.ID) + len(commit.ResourceID) + len(commit.BranchID) + len(commit.Version) + len(commit.SourceCommitID) + len(commit.ContentDigest) + len(commitBody) + len(item.Identity) + len(item.CatalogSummary) + len(branch.ID) + len(branch.ResourceID) + len(branch.NameKey) + len(branch.HeadCommitID) + len(branch.Revision) + len(branchBody) + 16
		if len(out.Items) == limit || selectedBytes > MaxResponseBytes {
			return model.ConfigurationCommitCatalog{}, failure("resource_exhausted", "commit catalog exceeds response bound")
		}
		item.Commit, err = configurationDecodeCommitRow(commit, commitBody)
		if err == nil {
			item.Branch, err = configurationDecodeBranchRow(branch, branchBody)
		}
		if err != nil {
			return model.ConfigurationCommitCatalog{}, err
		}
		if branch.ResourceID != q.ID || commit.ResourceID != q.ID || commit.BranchID != branch.ID {
			return model.ConfigurationCommitCatalog{}, failure("data_loss", "commit branch ownership disagrees")
		}
		if err = configurationProjection(item.Commit, item.Identity, item.CatalogSummary); err != nil {
			return model.ConfigurationCommitCatalog{}, err
		}
		encoded, err := encode(item)
		if err != nil {
			return model.ConfigurationCommitCatalog{}, err
		}
		responseBytes += len(encoded)
		if len(out.Items) != 0 {
			responseBytes++
		}
		if responseBytes > MaxResponseBytes {
			return model.ConfigurationCommitCatalog{}, failure("resource_exhausted", "commit catalog exceeds response bound")
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return model.ConfigurationCommitCatalog{}, err
	}
	return out, nil
}
