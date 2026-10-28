package coredata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadCatalogChoice reads an exact current identity without materializing
// origin, source-commit, branch lineage or any authored document.
func (s *Store) ReadCatalogChoice(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationCatalogChoice, error) {
	if !textKey(q.ID) || q.ParentID != nil || q.IncludeArchived || q.Limit != 1 || q.Offset != 0 {
		return nil, failure("invalid_argument", "one live catalog choice identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationCatalogChoice, error) {
		return configurationChoicePage(ctx, tx, scope, q, "", false)
	})
}

// Only facts needed to admit and verify the choice cross the SQLite driver.
// The immutable projection digest and current-main joins are still checked.
const configurationChoiceColumns = `r.id,r.name,r.name_key,coalesce(r.namespace_id,''),r.main_commit_id,r.archived,CASE WHEN length(r.body)<=262144 THEN 1 ELSE 0 END,
coalesce(json_extract(CAST(r.body AS TEXT),'$.id'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.name'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.name_key'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.namespace_id'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.main_commit_id'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.archived_at'),''),coalesce(json_extract(CAST(r.body AS TEXT),'$.catalog_selector'),''),CASE json_type(CAST(r.body AS TEXT),'$.system') WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE -1 END,coalesce(json_extract(CAST(r.body AS TEXT),'$.system_key'),''),
coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),CASE WHEN length(b.body)<=262144 THEN 1 ELSE 0 END,coalesce(json_extract(CAST(b.body AS TEXT),'$.id'),''),coalesce(json_extract(CAST(b.body AS TEXT),'$.resource_id'),''),coalesce(json_extract(CAST(b.body AS TEXT),'$.name_key'),''),coalesce(json_extract(CAST(b.body AS TEXT),'$.head_commit_id'),''),coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),''),
coalesce(s.id,''),coalesce(s.resource_id,''),coalesce(s.branch_id,''),coalesce(s.version,0),coalesce(s.content_digest,''),CASE WHEN length(s.body)<=262144 THEN 1 ELSE 0 END,coalesce(json_extract(CAST(s.body AS TEXT),'$.id'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.resource_id'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.branch_id'),''),CASE WHEN json_type(CAST(s.body AS TEXT),'$.version')='text' THEN json_extract(CAST(s.body AS TEXT),'$.version') ELSE '' END,coalesce(json_extract(CAST(s.body AS TEXT),'$.content_digest'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.catalog_digest'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.root_digest'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.association_digest'),''),coalesce(json_extract(CAST(s.body AS TEXT),'$.schema_version'),0),coalesce(json_extract(CAST(s.body AS TEXT),'$.association_count'),-1),
CASE WHEN length(s.identity_json)<=16384 THEN s.identity_json ELSE X'' END,CASE WHEN length(s.catalog_summary_json)<=16384 THEN s.catalog_summary_json ELSE X'' END`

func configurationChoiceQuery(q model.ConfigurationCatalogRead, predicate, selector string, ordinary bool) string {
	table := configurationResourceCatalogTable(q)
	if selector != "" {
		index := "core_resources_catalog_selector"
		if q.ParentID != nil {
			index += "_namespace"
		}
		if ordinary {
			index += "_ordinary"
		} else if !q.IncludeArchived {
			index += "_live"
		}
		table = "core_resources r INDEXED BY " + index
	}
	return "SELECT " + configurationChoiceColumns + " FROM " + table + configurationResourceCatalogJoin + predicate + " ORDER BY r.id LIMIT ? OFFSET ?"
}

func configurationChoicePage(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead, selector string, ordinary bool) ([]model.ConfigurationCatalogChoice, error) {
	if ordinary && (selector == "" || q.IncludeArchived) || selector != "" && !textKey(selector) {
		return nil, failure("invalid_argument", "exact selected catalog key and live ordinary scope required")
	}
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return nil, err
	}
	return configurationChoicePageInDomain(ctx, tx, scope, q, selector, ordinary)
}

func configurationChoicePageInDomain(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead, selector string, ordinary bool) ([]model.ConfigurationCatalogChoice, error) {
	predicate, args, limit, err := configurationResourceCatalogSelector(scope, q)
	if err != nil {
		return nil, err
	}
	if selector != "" {
		if ordinary {
			predicate += " AND json_extract(CAST(r.body AS TEXT),'$.system')=0"
		}
		predicate += " AND json_extract(CAST(r.body AS TEXT),'$.catalog_selector')=?"
		tail := append([]any(nil), args[len(args)-2:]...)
		args = append(args[:len(args)-2], selector)
		args = append(args, tail...)
	}
	rows, err := tx.QueryContext(ctx, configurationChoiceQuery(q, predicate, selector, ordinary), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.ConfigurationCatalogChoice{}
	selectedBytes, responseBytes := 0, 2
	for rows.Next() {
		item, rowBytes, err := scanConfigurationCatalogChoice(rows, selector)
		if err != nil {
			return nil, err
		}
		selectedBytes += rowBytes
		responseBytes += 256 + 6*(len(item.ResourceID)+len(item.Name)+len(item.CommitID)) + base64.StdEncoding.EncodedLen(len(item.Identity)) + base64.StdEncoding.EncodedLen(len(item.Summary))
		if len(out) == limit || selectedBytes > MaxResponseBytes || responseBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "configuration choice catalog exceeds response bound")
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scanConfigurationCatalogChoice decodes and validates the current-main thin
// projection for a selected row, including visibility identity and digest.
// Callers aggregate the returned physical bytes under their own result bound.
func scanConfigurationCatalogChoice(rows *sql.Rows, selector string) (model.ConfigurationCatalogChoice, int, error) {
	var item model.ConfigurationCatalogChoice
	var key, namespace, rid, name, nameKey, ns, main, resourceArchived, projectedSelector, systemKey string
	var branch, branchResource, branchName, branchHead, bodyBranch, bodyBranchResource, bodyBranchName, bodyBranchHead, branchArchived string
	var commit, commitResource, commitBranch, version, digest, bodyCommit, bodyCommitResource, bodyCommitBranch, bodyVersion, bodyDigest, catalogDigest, rootDigest, associationDigest string
	var archived, system, schemaVersion, associationCount, resourceBound, branchBound, commitBound int
	if err := rows.Scan(&item.ResourceID, &item.Name, &key, &namespace, &item.CommitID, &archived, &resourceBound,
		&rid, &name, &nameKey, &ns, &main, &resourceArchived, &projectedSelector, &system, &systemKey,
		&branch, &branchResource, &branchName, &branchHead, &branchBound, &bodyBranch, &bodyBranchResource, &bodyBranchName, &bodyBranchHead, &branchArchived,
		&commit, &commitResource, &commitBranch, &version, &digest, &commitBound, &bodyCommit, &bodyCommitResource, &bodyCommitBranch, &bodyVersion, &bodyDigest, &catalogDigest, &rootDigest, &associationDigest, &schemaVersion, &associationCount,
		&item.Identity, &item.Summary); err != nil {
		return model.ConfigurationCatalogChoice{}, 0, err
	}
	if resourceBound != 1 || branchBound != 1 || commitBound != 1 || !textKey(item.ResourceID) || !textKey(item.CommitID) || !utf8.ValidString(item.Name) || item.Name == "" ||
		rid != item.ResourceID || name != item.Name || nameKey != key || ns != namespace || main != item.CommitID || archived < 0 || archived > 1 || (archived != 0) != (resourceArchived != "") || system < 0 || system > 1 ||
		branch == "" || branchResource != item.ResourceID || branchName != "main" || branchHead != item.CommitID ||
		bodyBranch != branch || bodyBranchResource != branchResource || bodyBranchName != branchName || bodyBranchHead != branchHead || branchArchived != "" ||
		commit != item.CommitID || commitResource != item.ResourceID || commitBranch != branch || bodyCommit != commit || bodyCommitResource != commitResource || bodyCommitBranch != commitBranch ||
		bodyVersion != version || !positiveRevision(version) || bodyDigest != digest || !sha256Hex(digest) || !sha256Hex(catalogDigest) || !sha256Hex(rootDigest) || !sha256Hex(associationDigest) || schemaVersion < 1 || associationCount < 0 || associationCount > model.MaxConfigurationAssociations {
		return model.ConfigurationCatalogChoice{}, 0, failure("data_loss", "catalog choice current-main metadata disagrees")
	}
	projected, e := configurationSummarySelector(item.Summary)
	if e != nil || projected != projectedSelector || selector != "" && projected != selector {
		return model.ConfigurationCatalogChoice{}, 0, failure("data_loss", "catalog choice selector disagrees")
	}
	if err := configurationProjection(model.ConfigurationCommit{ContentDigest: digest, CatalogDigest: catalogDigest}, item.Identity, item.Summary); err != nil {
		return model.ConfigurationCatalogChoice{}, 0, err
	}
	item.Protected = system != 0 || systemKey != ""
	// Eight SQLite INTEGER cells, including s.version, are eight bytes each at
	// the driver boundary; bodyVersion is the separate stored JSON string.
	selectedBytes := len(item.ResourceID) + len(item.Name) + len(key) + len(namespace) + len(item.CommitID) + len(rid) + len(name) + len(nameKey) + len(ns) + len(main) + len(resourceArchived) + len(projectedSelector) + len(systemKey) + len(branch) + len(branchResource) + len(branchName) + len(branchHead) + len(bodyBranch) + len(bodyBranchResource) + len(bodyBranchName) + len(bodyBranchHead) + len(branchArchived) + len(commit) + len(commitResource) + len(commitBranch) + len(digest) + len(bodyCommit) + len(bodyCommitResource) + len(bodyCommitBranch) + len(bodyVersion) + len(bodyDigest) + len(catalogDigest) + len(rootDigest) + len(associationDigest) + len(item.Identity) + len(item.Summary) + 64
	return item, selectedBytes, nil
}
