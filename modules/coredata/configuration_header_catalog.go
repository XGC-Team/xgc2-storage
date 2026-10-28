package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadResourceHeaders reads the selected current-main metadata page without
// materializing snapshot payloads, manifests or references. It establishes
// relational identity only: domains with visibility rules must additionally
// admit their own immutable visibility identity before exposing a typed list.
// A zero Limit requests the bounded complete result and rejects overflow; an
// explicit Limit and Offset request exactly that page. ParentID retains the
// catalog selector's all / root / exact namespace meanings.
func (s *Store) ReadResourceHeaders(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationHead, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationHead, error) {
		return configurationResourceHeaders(ctx, tx, scope, q)
	})
}

const configurationResourceHeaderColumns = `r.id,coalesce(r.namespace_id,''),r.name,r.name_key,r.revision,r.main_commit_id,r.archived,r.origin_resource_id,r.origin_commit_id,r.body,
coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),coalesce(b.revision,0),coalesce(b.body,X''),
coalesce(s.id,''),coalesce(s.resource_id,''),coalesce(s.branch_id,''),coalesce(s.version,0),coalesce(s.source_commit_id,''),coalesce(s.content_digest,''),coalesce(s.body,X'')`

func configurationResourceHeadersQuery(q model.ConfigurationCatalogRead, predicate string) string {
	return "SELECT " + configurationResourceHeaderColumns + " FROM " + configurationResourceCatalogTable(q) + configurationResourceCatalogJoin + predicate + " ORDER BY r.id LIMIT ? OFFSET ?"
}

func configurationResourceHeaders(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead) ([]model.ConfigurationHead, error) {
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return nil, err
	}
	predicate, args, limit, err := configurationResourceCatalogSelector(scope, q)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, configurationResourceHeadersQuery(q, predicate), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.ConfigurationHead{}
	selectedBytes, responseBytes := 0, 2
	for rows.Next() {
		var head model.ConfigurationHead
		var resource model.ConfigurationResource
		var branch model.ConfigurationBranch
		var commit model.ConfigurationCommit
		var resourceBody, branchBody, commitBody []byte
		var archived bool
		err = rows.Scan(&resource.ID, &resource.NamespaceID, &resource.Name, &resource.NameKey, &resource.Revision, &resource.MainCommitID, &archived, &resource.OriginResourceID, &resource.OriginCommitID, &resourceBody,
			&branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody,
			&commit.ID, &commit.ResourceID, &commit.BranchID, &commit.Version, &commit.SourceCommitID, &commit.ContentDigest, &commitBody)
		if err != nil {
			return nil, err
		}
		selectedBytes += len(resource.ID) + len(resource.NamespaceID) + len(resource.Name) + len(resource.NameKey) + len(resource.Revision) + len(resource.MainCommitID) + len(resource.OriginResourceID) + len(resource.OriginCommitID) + len(resourceBody) +
			len(branch.ID) + len(branch.ResourceID) + len(branch.NameKey) + len(branch.HeadCommitID) + len(branch.Revision) + len(branchBody) +
			len(commit.ID) + len(commit.ResourceID) + len(commit.BranchID) + len(commit.Version) + len(commit.SourceCommitID) + len(commit.ContentDigest) + len(commitBody)
		if len(out) == limit || selectedBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "configuration header catalog exceeds response bound")
		}
		if head.Resource, err = configurationDecodeResourceRow(resource, archived, resourceBody); err == nil {
			head.Branch, err = configurationDecodeBranchRow(branch, branchBody)
		}
		if err == nil {
			head.Commit, err = configurationDecodeCommitRow(commit, commitBody)
		}
		if err != nil {
			return nil, err
		}
		if branch.ResourceID != resource.ID || branch.NameKey != "main" || head.Branch.ArchivedAt != "" || branch.HeadCommitID != resource.MainCommitID || commit.ID != resource.MainCommitID || commit.ResourceID != resource.ID || commit.BranchID != branch.ID {
			return nil, failure("data_loss", "current main relation disagrees")
		}
		encoded, err := encode(head)
		if err != nil {
			return nil, err
		}
		responseBytes += len(encoded)
		if len(out) > 0 {
			responseBytes++
		}
		if responseBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "configuration header catalog exceeds response bound")
		}
		out = append(out, head)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
