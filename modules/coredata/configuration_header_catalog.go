package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadResourceHeaders reads the selected current-main metadata page without
// materializing snapshot payloads, manifests or references. Domains with
// visibility rules admit the immutable identity before exposing a typed list.
// A zero Limit requests the bounded complete result and rejects overflow; an
// explicit Limit and Offset request exactly that page. ParentID retains the
// catalog selector's all / root / exact namespace meanings.
func (s *Store) ReadResourceHeaders(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationResourceHeader, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationResourceHeader, error) {
		return configurationResourceHeaders(ctx, tx, scope, q)
	})
}

const configurationResourceHeaderColumns = `r.id,coalesce(r.namespace_id,''),r.name,r.name_key,r.revision,r.main_commit_id,r.archived,r.origin_resource_id,r.origin_commit_id,CASE WHEN length(r.body)<=262144 THEN r.body ELSE X'' END,
coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),coalesce(b.revision,0),CASE WHEN length(b.body)<=262144 THEN b.body ELSE X'' END,
coalesce(s.id,''),coalesce(s.resource_id,''),coalesce(s.branch_id,''),coalesce(s.version,0),coalesce(s.source_commit_id,''),coalesce(s.content_digest,''),CASE WHEN length(s.identity_json)<=16384 THEN s.identity_json ELSE X'' END,CASE WHEN length(s.catalog_summary_json)<=16384 THEN s.catalog_summary_json ELSE X'' END,CASE WHEN length(s.body)<=262144 THEN s.body ELSE X'' END`

func configurationResourceHeadersQuery(q model.ConfigurationCatalogRead, predicate string) string {
	return "SELECT " + configurationResourceHeaderColumns + " FROM " + configurationResourceCatalogTable(q) + configurationResourceCatalogJoin + predicate + " ORDER BY r.id LIMIT ? OFFSET ?"
}

func configurationResourceHeaders(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead) ([]model.ConfigurationResourceHeader, error) {
	return configurationHeaderPage(ctx, tx, scope, q, "", false)
}

// ReadSelectedResourceHeaders starts at the exact selector index and reads
// current-main headers only; it never scans unrelated headers or old commits.
func (s *Store) ReadSelectedResourceHeaders(ctx context.Context, q model.ConfigurationSelectedCatalogRead) ([]model.ConfigurationResourceHeader, error) {
	if !textKey(q.Selector) {
		return nil, failure("invalid_argument", "exact selected catalog key required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationResourceHeader, error) {
		return configurationHeaderPage(ctx, tx, scope, q.Catalog, q.Selector, q.OrdinaryOnly)
	})
}

func configurationHeaderPage(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead, selector string, ordinary bool) ([]model.ConfigurationResourceHeader, error) {
	if ordinary && (selector == "" || q.IncludeArchived) || selector != "" && !textKey(selector) {
		return nil, failure("invalid_argument", "exact selected catalog key and live ordinary scope required")
	}
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return nil, err
	}
	return configurationHeaderPageInDomain(ctx, tx, scope, q, selector, ordinary)
}

func configurationHeaderPageInDomain(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead, selector string, ordinary bool) ([]model.ConfigurationResourceHeader, error) {
	predicate, args, limit, err := configurationResourceCatalogSelector(scope, q)
	if err != nil {
		return nil, err
	}
	query := configurationResourceHeadersQuery(q, predicate)
	if selector != "" {
		index := "core_resources_catalog_selector"
		if q.ParentID != nil {
			index += "_namespace"
		}
		if ordinary {
			// Clone origin is provenance, not protection. Filter the explicit
			// System flag before LIMIT/OFFSET with the ordinary selector index.
			index += "_ordinary"
			predicate += " AND json_extract(CAST(r.body AS TEXT),'$.system')=0"
		} else if !q.IncludeArchived {
			index += "_live"
		}
		predicate += " AND json_extract(CAST(r.body AS TEXT),'$.catalog_selector')=?"
		tail := append([]any(nil), args[len(args)-2:]...)
		args = append(args[:len(args)-2], selector)
		args = append(args, tail...)
		query = "SELECT " + configurationResourceHeaderColumns + " FROM core_resources r INDEXED BY " + index + configurationResourceCatalogJoin + predicate + " ORDER BY r.id LIMIT ? OFFSET ?"
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.ConfigurationResourceHeader{}
	selectedBytes, responseBytes := 0, 2
	for rows.Next() {
		var header model.ConfigurationResourceHeader
		var resource model.ConfigurationResource
		var branch model.ConfigurationBranch
		var commit model.ConfigurationCommit
		var resourceBody, branchBody, commitBody []byte
		var archived bool
		err = rows.Scan(&resource.ID, &resource.NamespaceID, &resource.Name, &resource.NameKey, &resource.Revision, &resource.MainCommitID, &archived, &resource.OriginResourceID, &resource.OriginCommitID, &resourceBody,
			&branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody,
			&commit.ID, &commit.ResourceID, &commit.BranchID, &commit.Version, &commit.SourceCommitID, &commit.ContentDigest, &header.Identity, &header.CatalogSummary, &commitBody)
		if err != nil {
			return nil, err
		}
		selectedBytes += len(resource.ID) + len(resource.NamespaceID) + len(resource.Name) + len(resource.NameKey) + len(resource.Revision) + len(resource.MainCommitID) + len(resource.OriginResourceID) + len(resource.OriginCommitID) + len(resourceBody) +
			len(branch.ID) + len(branch.ResourceID) + len(branch.NameKey) + len(branch.HeadCommitID) + len(branch.Revision) + len(branchBody) +
			len(commit.ID) + len(commit.ResourceID) + len(commit.BranchID) + len(commit.Version) + len(commit.SourceCommitID) + len(commit.ContentDigest) + len(header.Identity) + len(header.CatalogSummary) + len(commitBody) + 32 // SQLite integer and boolean cells.
		if len(out) == limit || selectedBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "configuration header catalog exceeds response bound")
		}
		if header.Head.Resource, err = configurationDecodeResourceRow(resource, archived, resourceBody); err == nil {
			header.Head.Branch, err = configurationDecodeBranchRow(branch, branchBody)
		}
		if err == nil {
			header.Head.Commit, err = configurationDecodeCommitRow(commit, commitBody)
		}
		if err != nil {
			return nil, err
		}
		if branch.ResourceID != resource.ID || branch.NameKey != "main" || header.Head.Branch.ArchivedAt != "" || branch.HeadCommitID != resource.MainCommitID || commit.ID != resource.MainCommitID || commit.ResourceID != resource.ID || commit.BranchID != branch.ID {
			return nil, failure("data_loss", "current main relation disagrees")
		}
		if err = configurationProjection(header.Head.Commit, header.Identity, header.CatalogSummary); err != nil {
			return nil, err
		}
		if selector != "" {
			projected, e := configurationSummarySelector(header.CatalogSummary)
			if e != nil || projected != selector || header.Head.Resource.CatalogSelector != selector {
				return nil, failure("data_loss", "current catalog selector disagrees with immutable summary")
			}
		}
		encoded, err := encode(header)
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
		out = append(out, header)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
