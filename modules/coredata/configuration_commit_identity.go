package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadCommitIdentity reads a selected immutable identity and current main
// fence without materializing any payload, manifest or reference set.
func (s *Store) ReadCommitIdentity(ctx context.Context, domain model.ConfigurationDomainGuard, commitID string) (model.ConfigurationCommitIdentity, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationCommitIdentity, error) {
		return configurationCommitIdentity(ctx, tx, scope, domain, commitID)
	})
}

func configurationCommitIdentity(ctx context.Context, tx *sql.Tx, scope string, domain model.ConfigurationDomainGuard, commitID string) (out model.ConfigurationCommitIdentity, err error) {
	if _, err = configurationDomain(ctx, tx, domain); err != nil {
		return out, err
	}
	if !textKey(commitID) {
		return out, failure("invalid_argument", "exact commit identity required")
	}
	var row model.ConfigurationCommit
	var body, summary []byte
	var branch model.ConfigurationBranch
	var branchBody []byte
	err = tx.QueryRowContext(ctx, `SELECT s.id,s.resource_id,s.branch_id,s.version,s.source_commit_id,s.content_digest,CASE WHEN length(s.body)<=262144 THEN s.body ELSE X'' END,CASE WHEN length(s.identity_json)<=16384 THEN s.identity_json ELSE X'' END,CASE WHEN length(s.catalog_summary_json)<=16384 THEN s.catalog_summary_json ELSE X'' END,coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),coalesce(b.revision,0),CASE WHEN length(b.body)<=262144 THEN b.body ELSE X'' END
FROM core_snapshots s LEFT JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.id=s.branch_id
WHERE s.scope=? AND s.domain=? AND s.id=?`, scope, domain.Key, commitID).Scan(&row.ID, &row.ResourceID, &row.BranchID, &row.Version, &row.SourceCommitID, &row.ContentDigest, &body, &out.Identity, &summary, &branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "owned immutable commit not found")
	}
	if err != nil {
		return out, err
	}
	if branch.ResourceID != row.ResourceID || branch.ID != row.BranchID {
		return out, failure("data_loss", "immutable snapshot branch belongs to another resource")
	}
	if len(body)+len(out.Identity)+len(summary)+len(branchBody) > model.MaxConfigurationDecodedBytes {
		return out, failure("resource_exhausted", "commit visibility projection exceeds bound")
	}
	out.Commit, err = configurationDecodeCommitRow(row, body)
	if err == nil {
		out.Branch, err = configurationDecodeBranchRow(branch, branchBody)
	}
	if err != nil {
		return out, err
	}
	if err = configurationProjection(out.Commit, out.Identity, summary); err != nil {
		return out, err
	}
	r, err := configurationResource(ctx, tx, scope, domain.Key, row.ResourceID, true)
	if err != nil {
		return out, err
	}
	out.CurrentMain, err = configurationCurrentMain(ctx, tx, scope, domain.Key, r, nil)
	if err != nil {
		return out, err
	}
	if len(body)+len(out.Identity)+len(summary)+len(branchBody)+len(out.CurrentMain.Identity) > model.MaxConfigurationDecodedBytes {
		return out, failure("resource_exhausted", "commit visibility projection exceeds bound")
	}
	return out, nil
}
