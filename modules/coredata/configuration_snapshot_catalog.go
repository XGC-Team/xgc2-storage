package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReadResourceCatalog returns current-main snapshots in resource-ID order.
// ParentID keeps the existing catalog selector: nil is every namespace,
// pointer-to-empty is structural root, and a nonempty value is one namespace.
// One reader admission owns the selected headers, frozen payloads and references.
// A zero Limit requests the complete bounded catalog and reports overflow;
// an explicit Limit/Offset requests that one bounded page.
func (s *Store) ReadResourceCatalog(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationResourceSnapshot, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationResourceSnapshot, error) {
		return configurationResourceCatalog(ctx, tx, scope, q)
	})
}

func configurationDecodeResourceRow(row model.ConfigurationResource, archived bool, body []byte) (out model.ConfigurationResource, err error) {
	if json.Unmarshal(body, &out) != nil || out.ID != row.ID || out.NamespaceID != row.NamespaceID || out.Name != row.Name || out.NameKey != row.NameKey || out.Revision != row.Revision || out.MainCommitID != row.MainCommitID || out.OriginResourceID != row.OriginResourceID || out.OriginCommitID != row.OriginCommitID || !positiveRevision(out.NextVersion) || out.CatalogSelector != "" && !textKey(out.CatalogSelector) || archived != (out.ArchivedAt != "") {
		return out, failure("data_loss", "resource metadata disagrees with relational identity")
	}
	return out, nil
}

func configurationDecodeBranchRow(row model.ConfigurationBranch, body []byte) (out model.ConfigurationBranch, err error) {
	if json.Unmarshal(body, &out) != nil || out.ID != row.ID || out.ResourceID != row.ResourceID || out.NameKey != row.NameKey || out.HeadCommitID != row.HeadCommitID || out.Revision != row.Revision || !positiveRevision(out.Revision) {
		return out, failure("data_loss", "branch metadata disagrees with relational identity")
	}
	return out, nil
}

func configurationDecodeCommitRow(row model.ConfigurationCommit, body []byte) (out model.ConfigurationCommit, err error) {
	if json.Unmarshal(body, &out) != nil || out.ID != row.ID || out.ResourceID != row.ResourceID || out.BranchID != row.BranchID || out.Version != row.Version || out.SourceCommitID != row.SourceCommitID || out.ContentDigest != row.ContentDigest || !positiveRevision(row.Version) || out.SchemaVersion < 1 || !sha256Hex(out.RootDigest) || !sha256Hex(out.ContentDigest) || !sha256Hex(out.CatalogDigest) || !sha256Hex(out.AssociationDigest) || out.AssociationCount < 0 || out.AssociationCount > model.MaxConfigurationAssociations {
		return out, failure("data_loss", "immutable commit metadata disagrees")
	}
	return out, nil
}

func configurationSnapshotBounds(out model.ConfigurationResourceSnapshot) error {
	manifest, err := model.DecodeConfigurationManifest(out.Manifest)
	if err != nil || manifest.RootDigest != out.Head.Commit.RootDigest {
		return failure("data_loss", "stored generic manifest disagrees")
	}
	metadata := out
	metadata.Payload, metadata.Manifest, metadata.Identity, metadata.CatalogSummary, metadata.CurrentMain.Identity = nil, nil, nil, nil, nil
	body, err := encode(metadata)
	if err != nil {
		return err
	}
	budget := model.ConfigurationBudget{PayloadBytes: int64(len(out.Payload)), ManifestBytes: int64(len(out.Manifest)), ManifestNodes: int64(len(manifest.Nodes)), References: int64(len(out.References)), Associations: int64(len(out.Associations)), IdentityBytes: int64(len(out.Identity)), CatalogSummaryBytes: int64(len(out.CatalogSummary)), MainIdentityBytes: int64(len(out.CurrentMain.Identity)), DecodedBytes: int64(len(out.Payload) + len(out.Manifest) + len(out.Identity) + len(out.CatalogSummary) + len(body) + len(out.CurrentMain.Identity))}
	if err = budget.Validate(); err != nil {
		return failure("resource_exhausted", err.Error())
	}
	return nil
}

// The predicate is reused by the selected-page reference query. It never walks
// namespaces, builds an unbounded ID parameter list, or reopens row snapshots.
func configurationResourceCatalogSelector(scope string, q model.ConfigurationCatalogRead) (string, []any, int, error) {
	if q.Offset < 0 || q.Limit < 0 || q.Limit > 4096 || q.ID != "" && !textKey(q.ID) || q.ParentID != nil && *q.ParentID != "" && !textKey(*q.ParentID) {
		return "", nil, 0, failure("invalid_argument", "invalid catalog selector")
	}
	limit := q.Limit
	if limit == 0 {
		limit = 4096
	}
	predicate, args := "r.scope=? AND r.domain=?", []any{scope, q.Domain.Key}
	if q.ID != "" {
		predicate += " AND r.id=?"
		args = append(args, q.ID)
	}
	if q.ParentID != nil {
		if *q.ParentID == "" {
			predicate += " AND r.namespace_id IS NULL"
		} else {
			predicate += " AND r.namespace_id=?"
			args = append(args, *q.ParentID)
		}
	}
	if !q.IncludeArchived {
		predicate += " AND r.archived=0"
	}
	readLimit := limit
	if q.Limit == 0 {
		readLimit++
	}
	return predicate, append(args, readLimit, q.Offset), limit, nil
}

const configurationResourceCatalogColumns = `r.id,coalesce(r.namespace_id,''),r.name,r.name_key,r.revision,r.main_commit_id,r.archived,r.origin_resource_id,r.origin_commit_id,r.body,
coalesce(b.id,''),coalesce(b.resource_id,''),coalesce(b.name_key,''),coalesce(b.head_commit_id,''),coalesce(b.revision,0),coalesce(b.body,X''),
coalesce(s.id,''),coalesce(s.resource_id,''),coalesce(s.branch_id,''),coalesce(s.version,0),coalesce(s.source_commit_id,''),coalesce(s.content_digest,''),coalesce(s.payload,X''),coalesce(s.manifest,X''),coalesce(s.identity_json,X''),coalesce(s.catalog_summary_json,X''),coalesce(s.body,X'')`

const configurationResourceCatalogJoin = `
LEFT JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
LEFT JOIN core_snapshots s ON s.scope=r.scope AND s.domain=r.domain AND s.resource_id=r.id AND s.id=r.main_commit_id WHERE `

// Fix the access path at the query owner. Otherwise SQLite may choose the full
// namespace index for a live page and walk arbitrarily many archived siblings.
// These are current-schema indexes, not optional planner hints or fallbacks.
func configurationResourceCatalogTable(q model.ConfigurationCatalogRead) string {
	index := "sqlite_autoindex_core_resources_1"
	if q.ParentID != nil {
		index = "core_resources_namespace_all"
		if !q.IncludeArchived {
			index = "core_resources_namespace"
		}
	} else if !q.IncludeArchived {
		index = "core_resources_live_catalog"
	}
	return "core_resources r INDEXED BY " + index
}

func configurationResourceCatalogQuery(q model.ConfigurationCatalogRead, predicate string) string {
	return "SELECT " + configurationResourceCatalogColumns + " FROM " + configurationResourceCatalogTable(q) + configurationResourceCatalogJoin + predicate + " ORDER BY r.id LIMIT ? OFFSET ?"
}

func configurationResourceCatalogReferencesQuery(q model.ConfigurationCatalogRead, predicate string) string {
	return `SELECT commit_id,slot,target_domain,target_resource_id,target_commit_id,body FROM core_references
WHERE scope=? AND domain=? AND commit_id IN (SELECT r.main_commit_id FROM ` + configurationResourceCatalogTable(q) + ` WHERE ` + predicate + ` ORDER BY r.id LIMIT ? OFFSET ?) ORDER BY commit_id,slot LIMIT ?`
}

func configurationResourceCatalogAssociationsQuery(q model.ConfigurationCatalogRead, predicate string) string {
	return `SELECT commit_id,kind,agent_id,slot_id,related_robot_resource_id FROM core_configuration_agent_index
WHERE scope=? AND domain=? AND commit_id IN (SELECT r.main_commit_id FROM ` + configurationResourceCatalogTable(q) + ` WHERE ` + predicate + ` ORDER BY r.id LIMIT ? OFFSET ?) ORDER BY commit_id,kind,agent_id,slot_id,related_robot_resource_id LIMIT ?`
}

// Count the public snapshot representation without allocating base64 copies of
// opaque frozen blobs. All three byte fields are non-nil in an admitted row.
func configurationSnapshotResponseBytes(snapshot model.ConfigurationResourceSnapshot) (int, error) {
	payload, manifest, identity, summary, mainIdentity := len(snapshot.Payload), len(snapshot.Manifest), len(snapshot.Identity), len(snapshot.CatalogSummary), len(snapshot.CurrentMain.Identity)
	snapshot.Payload, snapshot.Manifest, snapshot.Identity, snapshot.CatalogSummary, snapshot.CurrentMain.Identity = nil, nil, nil, nil, nil
	body, err := encode(snapshot)
	if err != nil {
		return 0, err
	}
	return len(body) - 10 + base64.StdEncoding.EncodedLen(payload) + base64.StdEncoding.EncodedLen(manifest) + base64.StdEncoding.EncodedLen(identity) + base64.StdEncoding.EncodedLen(summary) + base64.StdEncoding.EncodedLen(mainIdentity), nil
}

func configurationResourceCatalog(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationCatalogRead) ([]model.ConfigurationResourceSnapshot, error) {
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return nil, err
	}
	predicate, args, limit, err := configurationResourceCatalogSelector(scope, q)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, configurationResourceCatalogQuery(q, predicate), args...)
	if err != nil {
		return nil, err
	}
	out, selectedBytes, responseBytes := []model.ConfigurationResourceSnapshot{}, 2, 2
	byCommit := map[string]int{}
	for rows.Next() {
		var snapshot model.ConfigurationResourceSnapshot
		var resource model.ConfigurationResource
		var branch model.ConfigurationBranch
		var commit model.ConfigurationCommit
		var resourceBody, branchBody, commitBody []byte
		var archived bool
		err = rows.Scan(&resource.ID, &resource.NamespaceID, &resource.Name, &resource.NameKey, &resource.Revision, &resource.MainCommitID, &archived, &resource.OriginResourceID, &resource.OriginCommitID, &resourceBody,
			&branch.ID, &branch.ResourceID, &branch.NameKey, &branch.HeadCommitID, &branch.Revision, &branchBody,
			&commit.ID, &commit.ResourceID, &commit.BranchID, &commit.Version, &commit.SourceCommitID, &commit.ContentDigest, &snapshot.Payload, &snapshot.Manifest, &snapshot.Identity, &snapshot.CatalogSummary, &commitBody)
		if err != nil {
			rows.Close()
			return nil, err
		}
		selectedBytes += len(resourceBody) + len(branchBody) + len(commitBody) + len(snapshot.Payload) + len(snapshot.Manifest) + len(snapshot.Identity) + len(snapshot.CatalogSummary)
		if len(out) == limit || selectedBytes > MaxResponseBytes {
			rows.Close()
			return nil, failure("resource_exhausted", "configuration snapshot catalog exceeds response bound")
		}
		if snapshot.Head.Resource, err = configurationDecodeResourceRow(resource, archived, resourceBody); err == nil {
			snapshot.Head.Branch, err = configurationDecodeBranchRow(branch, branchBody)
		}
		if err == nil {
			snapshot.Head.Commit, err = configurationDecodeCommitRow(commit, commitBody)
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		if branch.ResourceID != resource.ID || branch.NameKey != "main" || snapshot.Head.Branch.ArchivedAt != "" || branch.HeadCommitID != resource.MainCommitID || commit.ID != resource.MainCommitID || commit.ResourceID != resource.ID || commit.BranchID != branch.ID {
			rows.Close()
			return nil, failure("data_loss", "current main relation disagrees")
		}
		identity, e := configurationIdentity(snapshot.Payload)
		if e != nil || !bytes.Equal(identity, snapshot.Identity) {
			rows.Close()
			return nil, failure("data_loss", "snapshot identity disagrees with its frozen envelope")
		}
		if e = configurationProjection(snapshot.Head.Commit, snapshot.Identity, snapshot.CatalogSummary); e != nil {
			rows.Close()
			return nil, e
		}
		selectedBytes += len(identity)
		if selectedBytes > MaxResponseBytes {
			rows.Close()
			return nil, failure("resource_exhausted", "configuration snapshot catalog exceeds response bound")
		}
		snapshot.CurrentMain = model.ConfigurationVisibilityPin{Branch: model.ConfigurationBranchGuard{ID: branch.ID, ExpectedRevision: branch.Revision, CommitID: commit.ID, ContentDigest: commit.ContentDigest}, SchemaVersion: snapshot.Head.Commit.SchemaVersion, Identity: identity}
		snapshot.References = []model.ConfigurationReference{}
		snapshot.Associations = []model.ConfigurationAgentAssociation{}
		snapshotBytes, e := configurationSnapshotResponseBytes(snapshot)
		if e != nil {
			rows.Close()
			return nil, e
		}
		responseBytes += snapshotBytes
		if len(out) > 0 {
			responseBytes++
		}
		if responseBytes > MaxResponseBytes {
			rows.Close()
			return nil, failure("resource_exhausted", "configuration snapshot catalog exceeds response bound")
		}
		byCommit[commit.ID] = len(out)
		out = append(out, snapshot)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(out) == 0 {
		return out, nil
	}
	// Reuse the exact selected page within this reader snapshot. The references
	// index is entered by scope/domain/commit; unrelated history is not read.
	query := configurationResourceCatalogReferencesQuery(q, predicate)
	referenceArgs := append([]any{scope, q.Domain.Key}, args...)
	referenceArgs = append(referenceArgs, MaxReferences+1)
	refs, err := tx.QueryContext(ctx, query, referenceArgs...)
	if err != nil {
		return nil, err
	}
	referenceCount := 0
	for refs.Next() {
		var commitID, slot, domain, resource, targetCommit string
		var body []byte
		if err = refs.Scan(&commitID, &slot, &domain, &resource, &targetCommit, &body); err != nil {
			refs.Close()
			return nil, err
		}
		referenceCount++
		selectedBytes += len(body)
		if referenceCount > MaxReferences || selectedBytes > MaxResponseBytes {
			refs.Close()
			return nil, failure("resource_exhausted", "configuration snapshot catalog exceeds response bound")
		}
		index, ok := byCommit[commitID]
		if !ok {
			refs.Close()
			return nil, failure("data_loss", "reference escaped selected catalog page")
		}
		var ref model.ConfigurationReference
		if json.Unmarshal(body, &ref) != nil || ref.Slot != slot || ref.TargetDomain != domain || ref.TargetResourceID != resource || ref.TargetCommitID != targetCommit {
			refs.Close()
			return nil, failure("data_loss", "reference metadata disagrees")
		}
		if len(out[index].References) == model.MaxConfigurationReferences {
			refs.Close()
			return nil, failure("resource_exhausted", "immutable snapshot materialization bound exceeded")
		}
		encoded, e := encode(ref)
		if e != nil {
			refs.Close()
			return nil, e
		}
		responseBytes += len(encoded)
		if len(out[index].References) > 0 {
			responseBytes++
		}
		if responseBytes > MaxResponseBytes {
			refs.Close()
			return nil, failure("resource_exhausted", "configuration snapshot catalog exceeds response bound")
		}
		out[index].References = append(out[index].References, ref)
	}
	err, closeErr = refs.Err(), refs.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	query = configurationResourceCatalogAssociationsQuery(q, predicate)
	associationArgs := append([]any{scope, q.Domain.Key}, args...)
	associationArgs = append(associationArgs, model.MaxConfigurationCloneAssociations+1)
	associations, err := tx.QueryContext(ctx, query, associationArgs...)
	if err != nil {
		return nil, err
	}
	count := 0
	for associations.Next() {
		var commitID string
		var fact model.ConfigurationAgentAssociation
		if err = associations.Scan(&commitID, &fact.Kind, &fact.AgentID, &fact.SlotID, &fact.RobotResourceID); err != nil {
			associations.Close()
			return nil, err
		}
		count++
		selectedBytes += len(commitID) + len(fact.Kind) + len(fact.AgentID) + len(fact.SlotID) + len(fact.RobotResourceID)
		if count > model.MaxConfigurationCloneAssociations || selectedBytes > MaxResponseBytes {
			associations.Close()
			return nil, failure("resource_exhausted", "configuration association catalog exceeds response bound")
		}
		index, ok := byCommit[commitID]
		if !ok {
			associations.Close()
			return nil, failure("data_loss", "association escaped selected catalog page")
		}
		if len(out[index].Associations) == model.MaxConfigurationAssociations {
			associations.Close()
			return nil, failure("resource_exhausted", "snapshot association bound exceeded")
		}
		encoded, e := encode(fact)
		if e != nil {
			associations.Close()
			return nil, e
		}
		responseBytes += len(encoded)
		if len(out[index].Associations) != 0 {
			responseBytes++
		}
		if responseBytes > MaxResponseBytes {
			associations.Close()
			return nil, failure("resource_exhausted", "configuration association catalog exceeds response bound")
		}
		out[index].Associations = append(out[index].Associations, fact)
	}
	err, closeErr = associations.Err(), associations.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	for _, snapshot := range out {
		if err = configurationAssociationIntegrity(snapshot.Head.Commit, snapshot.Associations); err != nil {
			return nil, err
		}
		pin, e := model.ConfigurationContentDigest(snapshot.Payload, snapshot.Manifest, snapshot.Identity, snapshot.CatalogSummary, snapshot.References, snapshot.Associations)
		if e != nil || pin != snapshot.Head.Commit.ContentDigest {
			return nil, failure("data_loss", "immutable content digest mismatch")
		}
		if err = configurationSnapshotBounds(snapshot); err != nil {
			return nil, err
		}
	}
	return out, nil
}
