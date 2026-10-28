package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const maxAgentSourcePage = 256

// ReadAgentSources discovers one target's current-main derived facts. It never
// reads authored payloads, manifests, or reference bodies. Callers must apply
// domain visibility and verify the selected complete immutable snapshot before
// using its facts. A continuation is valid only for the same generation.
func (s *Store) ReadAgentSources(ctx context.Context, q model.ConfigurationAgentSourceRead) (model.ConfigurationAgentSourcePage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationAgentSourcePage, error) {
		return configurationAgentSources(ctx, tx, scope, q)
	})
}

const agentSourceJoin = ` JOIN core_snapshots s ON s.scope=a.scope AND s.domain=a.domain AND s.id=a.commit_id
JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id AND r.main_commit_id=s.id AND r.archived=0
LEFT JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')='' `

const agentSourceQuery = `SELECT a.kind,a.agent_id,a.slot_id,a.related_robot_resource_id,` + configurationResourceHeaderColumns + `
FROM core_configuration_agent_index a INDEXED BY core_configuration_agent_target ` + agentSourceJoin + `
WHERE a.scope=? AND a.agent_id=? AND a.domain=? AND (?=0 OR a.kind<>'world') AND (a.commit_id,a.kind,a.slot_id,a.related_robot_resource_id)>(?,?,?,?)
ORDER BY a.commit_id,a.kind,a.slot_id,a.related_robot_resource_id LIMIT ?`

type configurationHeaderScan struct {
	header                               model.ConfigurationResourceHeader
	resource                             model.ConfigurationResource
	branch                               model.ConfigurationBranch
	commit                               model.ConfigurationCommit
	resourceBody, branchBody, commitBody []byte
	archived                             bool
}

func (v *configurationHeaderScan) fields() []any {
	return []any{&v.resource.ID, &v.resource.NamespaceID, &v.resource.Name, &v.resource.NameKey, &v.resource.Revision, &v.resource.MainCommitID, &v.archived, &v.resource.OriginResourceID, &v.resource.OriginCommitID, &v.resourceBody,
		&v.branch.ID, &v.branch.ResourceID, &v.branch.NameKey, &v.branch.HeadCommitID, &v.branch.Revision, &v.branchBody,
		&v.commit.ID, &v.commit.ResourceID, &v.commit.BranchID, &v.commit.Version, &v.commit.SourceCommitID, &v.commit.ContentDigest, &v.header.Identity, &v.header.CatalogSummary, &v.commitBody}
}

// selectedBytes counts actual driver cells, including malformed unknown JSON
// fields that disappear when a valid response object is re-encoded.
func (v *configurationHeaderScan) selectedBytes() int {
	return len(v.resource.ID) + len(v.resource.NamespaceID) + len(v.resource.Name) + len(v.resource.NameKey) + len(v.resource.Revision) + len(v.resource.MainCommitID) + len(v.resource.OriginResourceID) + len(v.resource.OriginCommitID) + len(v.resourceBody) +
		len(v.branch.ID) + len(v.branch.ResourceID) + len(v.branch.NameKey) + len(v.branch.HeadCommitID) + len(v.branch.Revision) + len(v.branchBody) +
		len(v.commit.ID) + len(v.commit.ResourceID) + len(v.commit.BranchID) + len(v.commit.Version) + len(v.commit.SourceCommitID) + len(v.commit.ContentDigest) + len(v.header.Identity) + len(v.header.CatalogSummary) + len(v.commitBody) + 32
}

func (v *configurationHeaderScan) decoded() (model.ConfigurationResourceHeader, error) {
	var err error
	if v.header.Head.Resource, err = configurationDecodeResourceRow(v.resource, v.archived, v.resourceBody); err == nil {
		v.header.Head.Branch, err = configurationDecodeBranchRow(v.branch, v.branchBody)
	}
	if err == nil {
		v.header.Head.Commit, err = configurationDecodeCommitRow(v.commit, v.commitBody)
	}
	if err != nil {
		return model.ConfigurationResourceHeader{}, err
	}
	if v.branch.ResourceID != v.resource.ID || v.branch.NameKey != "main" || v.header.Head.Branch.ArchivedAt != "" || v.branch.HeadCommitID != v.resource.MainCommitID || v.commit.ID != v.resource.MainCommitID || v.commit.ResourceID != v.resource.ID || v.commit.BranchID != v.branch.ID {
		return model.ConfigurationResourceHeader{}, failure("data_loss", "current main relation disagrees")
	}
	if err = configurationProjection(v.header.Head.Commit, v.header.Identity, v.header.CatalogSummary); err != nil {
		return model.ConfigurationResourceHeader{}, err
	}
	return v.header, nil
}

func configurationAgentSources(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationAgentSourceRead) (model.ConfigurationAgentSourcePage, error) {
	out := model.ConfigurationAgentSourcePage{Items: []model.ConfigurationAgentSource{}}
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return model.ConfigurationAgentSourcePage{}, err
	}
	var err error
	out.Generation, err = configurationGeneration(ctx, tx, scope)
	if err != nil {
		return model.ConfigurationAgentSourcePage{}, err
	}
	if q.Limit < 1 || q.Limit > maxAgentSourcePage || len(q.AgentID) == 0 || len(q.AgentID) > 128 {
		return model.ConfigurationAgentSourcePage{}, failure("invalid_argument", "invalid Agent source page")
	}
	var after model.ConfigurationAgentSourceCursor
	if q.After != nil {
		after = *q.After
		if after.ScopeDigest != digest(scope) || after.DomainKey != q.Domain.Key || after.AgentID != q.AgentID || after.ExcludeWorld != q.ExcludeWorld || !textKey(after.CommitID) || len(after.Kind) == 0 || len(after.Kind) > 32 || len(after.SlotID) > 255 || len(after.RobotResourceID) > 255 {
			return model.ConfigurationAgentSourcePage{}, failure("invalid_argument", "invalid Agent source continuation")
		}
		if after.Generation != out.Generation {
			return model.ConfigurationAgentSourcePage{}, failure("conflict", "Agent source generation changed")
		}
	}
	rows, err := tx.QueryContext(ctx, agentSourceQuery, scope, q.AgentID, q.Domain.Key, q.ExcludeWorld, after.CommitID, after.Kind, after.SlotID, after.RobotResourceID, q.Limit+1)
	if err != nil {
		return model.ConfigurationAgentSourcePage{}, err
	}
	defer rows.Close()
	selectedBytes, responseBytes := 0, 2
	for rows.Next() {
		if len(out.Items) == q.Limit {
			last := out.Items[len(out.Items)-1]
			out.Next = &model.ConfigurationAgentSourceCursor{ScopeDigest: digest(scope), DomainKey: q.Domain.Key, AgentID: q.AgentID, ExcludeWorld: q.ExcludeWorld, Generation: out.Generation, CommitID: last.Header.Head.Commit.ID, Kind: last.Association.Kind, SlotID: last.Association.SlotID, RobotResourceID: last.Association.RobotResourceID}
			break
		}
		var fact model.ConfigurationAgentAssociation
		var scan configurationHeaderScan
		fields := append([]any{&fact.Kind, &fact.AgentID, &fact.SlotID, &fact.RobotResourceID}, scan.fields()...)
		if err = rows.Scan(fields...); err != nil {
			return model.ConfigurationAgentSourcePage{}, err
		}
		selectedBytes += scan.selectedBytes() + len(fact.Kind) + len(fact.AgentID) + len(fact.SlotID) + len(fact.RobotResourceID)
		if selectedBytes > MaxResponseBytes {
			return model.ConfigurationAgentSourcePage{}, failure("resource_exhausted", "Agent source page exceeds selected-byte bound")
		}
		if fact.AgentID != q.AgentID {
			return model.ConfigurationAgentSourcePage{}, failure("data_loss", "Agent association identity disagrees")
		}
		if _, err = model.CanonicalConfigurationAssociations([]model.ConfigurationAgentAssociation{fact}); err != nil {
			return model.ConfigurationAgentSourcePage{}, failure("data_loss", "invalid Agent association")
		}
		header, err := scan.decoded()
		if err != nil {
			return model.ConfigurationAgentSourcePage{}, err
		}
		item := model.ConfigurationAgentSource{Header: header, Association: fact}
		body, err := encode(item)
		if err != nil {
			return model.ConfigurationAgentSourcePage{}, err
		}
		responseBytes += len(body) + 1
		if responseBytes > MaxResponseBytes {
			return model.ConfigurationAgentSourcePage{}, failure("resource_exhausted", "Agent source page exceeds byte bound")
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return model.ConfigurationAgentSourcePage{}, err
	}
	return out, nil
}

// ReadRobotIncoming finds current-main source snapshots whose robot:<slot>
// references address one live Robot resource. A selected Robot's visibility and
// owner must be admitted separately by the caller. No referenced payload or
// reference body is read. Selected snapshots are verified by exact detail.
func (s *Store) ReadRobotIncoming(ctx context.Context, q model.ConfigurationRobotIncomingRead) (model.ConfigurationRobotIncomingPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationRobotIncomingPage, error) {
		return configurationRobotIncoming(ctx, tx, scope, q)
	})
}

const robotIncomingQuery = `SELECT a.target_commit_id,a.commit_id,a.slot,` + configurationResourceHeaderColumns + `
FROM core_references a INDEXED BY core_reference_targets
JOIN core_snapshots s ON s.scope=a.scope AND s.domain=a.domain AND s.id=a.commit_id
JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id AND r.main_commit_id=s.id AND r.archived=0
LEFT JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
WHERE a.scope=? AND a.target_domain=? AND a.target_resource_id=? AND a.domain=? AND substr(a.slot,1,6)='robot:'
AND (a.target_commit_id,a.commit_id,a.slot)>(?,?,?)
ORDER BY a.target_commit_id,a.commit_id,a.slot LIMIT ?`

func configurationRobotIncoming(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationRobotIncomingRead) (model.ConfigurationRobotIncomingPage, error) {
	out := model.ConfigurationRobotIncomingPage{Items: []model.ConfigurationRobotIncoming{}}
	if _, err := configurationDomain(ctx, tx, q.Domain); err != nil {
		return model.ConfigurationRobotIncomingPage{}, err
	}
	var err error
	out.Generation, err = configurationGeneration(ctx, tx, scope)
	if err != nil {
		return model.ConfigurationRobotIncomingPage{}, err
	}
	if _, err := configurationDomain(ctx, tx, q.RobotDomain); err != nil {
		return model.ConfigurationRobotIncomingPage{}, err
	}
	if q.Limit < 1 || q.Limit > maxAgentSourcePage || !textKey(q.RobotID) {
		return model.ConfigurationRobotIncomingPage{}, failure("invalid_argument", "invalid Robot incoming page")
	}
	var after model.ConfigurationRobotIncomingCursor
	// Tracking references have an empty target commit, so the first page uses
	// an empty target with an empty source commit (source commit is never empty).
	if q.After != nil {
		after = *q.After
		if after.ScopeDigest != digest(scope) || after.DomainKey != q.Domain.Key || after.RobotDomainKey != q.RobotDomain.Key || after.RobotID != q.RobotID || !textKey(after.CommitID) || len(after.TargetCommitID) > 255 || len(after.Slot) > 255 {
			return model.ConfigurationRobotIncomingPage{}, failure("invalid_argument", "invalid Robot incoming continuation")
		}
		if after.Generation != out.Generation {
			return model.ConfigurationRobotIncomingPage{}, failure("conflict", "Robot incoming generation changed")
		}
	}
	rows, err := tx.QueryContext(ctx, robotIncomingQuery, scope, q.RobotDomain.Key, q.RobotID, q.Domain.Key, after.TargetCommitID, after.CommitID, after.Slot, q.Limit+1)
	if err != nil {
		return model.ConfigurationRobotIncomingPage{}, err
	}
	defer rows.Close()
	selectedBytes, responseBytes := 0, 2
	for rows.Next() {
		if len(out.Items) == q.Limit {
			last := out.Items[len(out.Items)-1]
			out.Next = &model.ConfigurationRobotIncomingCursor{ScopeDigest: digest(scope), DomainKey: q.Domain.Key, RobotDomainKey: q.RobotDomain.Key, RobotID: q.RobotID, Generation: out.Generation, TargetCommitID: last.TargetCommitID, CommitID: last.Header.Head.Commit.ID, Slot: last.Slot}
			break
		}
		var targetCommit, commit, slot string
		var scan configurationHeaderScan
		fields := append([]any{&targetCommit, &commit, &slot}, scan.fields()...)
		if err = rows.Scan(fields...); err != nil {
			return model.ConfigurationRobotIncomingPage{}, err
		}
		selectedBytes += scan.selectedBytes() + len(targetCommit) + len(commit) + len(slot)
		if selectedBytes > MaxResponseBytes {
			return model.ConfigurationRobotIncomingPage{}, failure("resource_exhausted", "Robot incoming page exceeds selected-byte bound")
		}
		header, err := scan.decoded()
		if err != nil {
			return model.ConfigurationRobotIncomingPage{}, err
		}
		if header.Head.Commit.ID != commit {
			return model.ConfigurationRobotIncomingPage{}, failure("data_loss", "incoming reference commit disagrees")
		}
		item := model.ConfigurationRobotIncoming{Header: header, Slot: slot, TargetCommitID: targetCommit}
		body, err := encode(item)
		if err != nil {
			return model.ConfigurationRobotIncomingPage{}, err
		}
		responseBytes += len(body) + 1
		if responseBytes > MaxResponseBytes {
			return model.ConfigurationRobotIncomingPage{}, failure("resource_exhausted", "Robot incoming page exceeds byte bound")
		}
		out.Items = append(out.Items, item)
	}
	if err = rows.Err(); err != nil {
		return model.ConfigurationRobotIncomingPage{}, err
	}
	return out, nil
}
