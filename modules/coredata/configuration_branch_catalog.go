package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadBranchCatalog selects branch metadata and immutable head identities from
// one scoped reader snapshot. A branch may share another branch's immutable
// head; both must belong to the same resource. No payload, manifest or
// reference body is materialized.
func (s *Store) ReadBranchCatalog(ctx context.Context, q model.ConfigurationCatalogRead) (model.ConfigurationBranchCatalog, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationBranchCatalog, error) {
		return configurationBranchCatalog(ctx, tx, scope, q)
	}, func() { q = ownedCatalogRead(q) })
}

const branchCatalogSQL = `SELECT b.id,b.resource_id,b.name_key,b.head_commit_id,b.revision,CASE WHEN length(b.body)<=262144 THEN b.body ELSE X'' END,
coalesce(s.id,''),coalesce(s.resource_id,''),coalesce(s.branch_id,''),coalesce(s.version,0),coalesce(s.source_commit_id,''),coalesce(s.content_digest,''),CASE WHEN length(s.body)<=262144 THEN s.body ELSE X'' END,CASE WHEN length(s.identity_json)<=16384 THEN s.identity_json ELSE X'' END,CASE WHEN length(s.catalog_summary_json)<=16384 THEN s.catalog_summary_json ELSE X'' END,coalesce(owner.resource_id,'')
FROM core_branches b LEFT JOIN core_snapshots s ON s.scope=b.scope AND s.domain=b.domain AND s.id=b.head_commit_id
LEFT JOIN core_branches owner ON owner.scope=s.scope AND owner.domain=s.domain AND owner.id=s.branch_id
WHERE b.scope=? AND b.domain=? AND b.resource_id=?`

func configurationBranchCatalogQuery(includeArchived bool) string {
	query := branchCatalogSQL
	if !includeArchived {
		query += ` AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''`
	}
	return query + ` ORDER BY b.name_key,b.id LIMIT ? OFFSET ?`
}

func configurationBranchCatalog(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead) (model.ConfigurationBranchCatalog, error) {
	var out model.ConfigurationBranchCatalog
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return out, err
	}
	if !textKey(q.ID) || q.ParentID != nil || q.Offset < 0 || q.Limit < 0 || q.Limit > 4096 {
		return out, failure("invalid_argument", "exact resource and bounded branch page required")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 4096
	}
	readLimit := limit
	if q.Limit == 0 {
		readLimit++
	}
	var err error
	out.Resource, err = configurationResource(ctx, tx, scope, q.Domain.Key, q.ID, true)
	if err != nil {
		return model.ConfigurationBranchCatalog{}, err
	}
	out.CurrentMain, err = configurationCurrentMain(ctx, tx, scope, q.Domain.Key, out.Resource, nil)
	if err != nil {
		return model.ConfigurationBranchCatalog{}, err
	}
	rows, err := tx.QueryContext(ctx, configurationBranchCatalogQuery(q.IncludeArchived), scope, q.Domain.Key, q.ID, readLimit, q.Offset)
	if err != nil {
		return model.ConfigurationBranchCatalog{}, err
	}
	defer rows.Close()
	out.Items = []model.ConfigurationBranchHeader{}
	bytes := len(out.CurrentMain.Identity) + 2
	for rows.Next() {
		var item model.ConfigurationBranchHeader
		var branch model.ConfigurationBranch
		var commit model.ConfigurationCommit
		var branchBody, commitBody, summary []byte
		var commitOwner string
		if err = rows.Scan(&branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody, &commit.ID, &commit.ResourceID, &commit.BranchID, &commit.Version, &commit.SourceCommitID, &commit.ContentDigest, &commitBody, &item.Identity, &summary, &commitOwner); err != nil {
			return model.ConfigurationBranchCatalog{}, err
		}
		bytes += len(branchBody) + len(commitBody) + len(item.Identity) + len(summary)
		if len(out.Items) == limit || bytes > MaxResponseBytes {
			return model.ConfigurationBranchCatalog{}, failure("resource_exhausted", "branch catalog exceeds response bound")
		}
		item.Branch, err = configurationDecodeBranchRow(branch, branchBody)
		if err == nil {
			item.Commit, err = configurationDecodeCommitRow(commit, commitBody)
		}
		if err != nil {
			return model.ConfigurationBranchCatalog{}, err
		}
		if branch.ResourceID != q.ID || commit.ResourceID != q.ID || branch.HeadCommitID != commit.ID || commitOwner != q.ID {
			return model.ConfigurationBranchCatalog{}, failure("data_loss", "branch head ownership disagrees")
		}
		if err = configurationProjection(item.Commit, item.Identity, summary); err != nil {
			return model.ConfigurationBranchCatalog{}, err
		}
		encoded, err := encode(item)
		if err != nil {
			return model.ConfigurationBranchCatalog{}, err
		}
		bytes += len(encoded) + 1
		if bytes > MaxResponseBytes {
			return model.ConfigurationBranchCatalog{}, failure("resource_exhausted", "branch catalog exceeds response bound")
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return model.ConfigurationBranchCatalog{}, err
	}
	return out, nil
}
