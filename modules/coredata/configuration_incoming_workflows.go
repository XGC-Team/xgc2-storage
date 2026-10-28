package coredata

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const MaxWorkflowRobotTargets = 100

// The partial index is owned by the central current-schema writer. Its exact
// predicate excludes unrelated sources, pinned references and non-main targets
// before any source header or reference body is materialized.
const workflowRobotLinksQuery = `WITH selected AS MATERIALIZED (
 SELECT DISTINCT target_resource_id,commit_id
 FROM core_references INDEXED BY core_reference_robot_workflow_inverse
 WHERE scope=? AND target_domain='automation' AND target_resource_id IN (SELECT value FROM json_each(?))
 AND domain='robot' AND current_main_live=1 AND target_commit_id=''
 AND slot>='workflow-binding:' AND slot<'workflow-binding;'
 AND json_extract(CAST(body AS TEXT),'$.mode')='tracking'
 AND json_extract(CAST(body AS TEXT),'$.target_branch')='main'
 AND json_extract(CAST(body AS TEXT),'$.target_component_id')=''
)
SELECT selected.target_resource_id,r.id,s.id FROM selected
JOIN core_snapshots s ON s.scope=? AND s.domain='robot' AND s.id=selected.commit_id
JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id AND r.main_commit_id=s.id AND r.archived=0
JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND b.head_commit_id=s.id AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
ORDER BY selected.target_resource_id,r.id`

// ReadWorkflowRobotBindings obtains selected Workflow heads, distinct current
// Robot identities and their links in one Storage read transaction. It never
// reads reference bodies or editable source payloads during this query.
func (s *Store) ReadWorkflowRobotBindings(ctx context.Context, q model.ConfigurationWorkflowRobotRead) (model.ConfigurationWorkflowRobotResult, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationWorkflowRobotResult, error) {
		return configurationWorkflowRobotBindings(ctx, tx, scope, q)
	})
}

func configurationWorkflowRobotBindings(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationWorkflowRobotRead) (model.ConfigurationWorkflowRobotResult, error) {
	out := model.ConfigurationWorkflowRobotResult{Workflows: []model.ConfigurationCatalogChoice{}, Robots: []model.ConfigurationCatalogChoice{}, Links: []model.ConfigurationWorkflowRobotLink{}}
	if q.WorkflowDomain.Key != "automation" || q.RobotDomain.Key != "robot" || len(q.WorkflowIDs) < 1 || len(q.WorkflowIDs) > MaxWorkflowRobotTargets {
		return out, failure("invalid_argument", "selected Automation IDs and domain guards required")
	}
	if _, err := configurationDomain(ctx, tx, q.WorkflowDomain); err != nil {
		return out, err
	}
	if _, err := configurationDomain(ctx, tx, q.RobotDomain); err != nil {
		return out, err
	}
	ids := q.WorkflowIDs
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; !textKey(id) || duplicate {
			return out, failure("invalid_argument", "Automation IDs must be distinct identifiers")
		}
		seen[id] = struct{}{}
	}
	selected, err := json.Marshal(ids)
	if err != nil {
		return out, err
	}
	bytes := 0
	out.Workflows, err = workflowRobotHeaders(ctx, tx, scope, q.WorkflowDomain.Key, selected, len(ids), &bytes, &out.Work.WorkflowHeaderBytes, &out.Work.WorkflowHeaderDecodes)
	if err != nil {
		return out, err
	}
	if len(out.Workflows) != len(ids) {
		return out, nil
	}
	rows, err := tx.QueryContext(ctx, workflowRobotLinksQuery, scope, string(selected), scope)
	if err != nil {
		return out, err
	}
	robotSet := map[string]string{}
	for rows.Next() {
		var link model.ConfigurationWorkflowRobotLink
		if err = rows.Scan(&link.WorkflowID, &link.RobotID, &link.CommitID); err != nil {
			rows.Close()
			return out, err
		}
		bytes += len(link.WorkflowID) + len(link.RobotID) + len(link.CommitID)
		if len(out.Links) == MaxReferences || bytes > MaxResponseBytes {
			rows.Close()
			return out, failure("resource_exhausted", "selected Workflow Robot bindings exceed response bound")
		}
		if prior, ok := robotSet[link.RobotID]; ok && prior != link.CommitID {
			rows.Close()
			return out, failure("data_loss", "Robot current main commits disagree")
		}
		robotSet[link.RobotID] = link.CommitID
		out.Links = append(out.Links, link)
		out.Work.Links++
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	if err = rows.Close(); err != nil {
		return out, err
	}
	robotIDs := make([]string, 0, len(robotSet))
	for id := range robotSet {
		robotIDs = append(robotIDs, id)
	}
	if len(robotIDs) != 0 {
		selectedRobots, encodeErr := json.Marshal(robotIDs)
		if encodeErr != nil {
			return out, encodeErr
		}
		out.Robots, err = workflowRobotHeaders(ctx, tx, scope, q.RobotDomain.Key, selectedRobots, len(robotIDs), &bytes, &out.Work.RobotHeaderBytes, &out.Work.RobotHeaderDecodes)
		if err != nil {
			return out, err
		}
		if len(out.Robots) != len(robotIDs) {
			return out, failure("data_loss", "selected Robot head disappeared inside its read transaction")
		}
		for _, robot := range out.Robots {
			if robotSet[robot.ResourceID] != robot.CommitID {
				return out, failure("data_loss", "selected Robot head disagrees with its reference")
			}
		}
	}
	return out, nil
}

func workflowRobotHeaders(ctx context.Context, tx *sql.Tx, scope, domain string, selected []byte, count int, bytes, headerBytes, decodes *int) ([]model.ConfigurationCatalogChoice, error) {
	query := `SELECT ` + configurationChoiceColumns + ` FROM core_resources r ` + configurationResourceCatalogJoin + `r.scope=? AND r.domain=? AND r.id IN (SELECT value FROM json_each(?)) AND r.archived=0 ORDER BY r.id`
	rows, err := tx.QueryContext(ctx, query, scope, domain, string(selected))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.ConfigurationCatalogChoice, 0, count)
	for rows.Next() {
		header, size, err := scanConfigurationCatalogChoice(rows, "")
		*decodes++
		if err != nil {
			return nil, err
		}
		*bytes += size
		*headerBytes += size
		if *bytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "selected Workflow Robot headers exceed response bound")
		}
		out = append(out, header)
	}
	return out, rows.Err()
}
