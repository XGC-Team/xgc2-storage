package coredata

import (
	"context"
	"database/sql"
	"reflect"
	"sort"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// This row is an Environment observation in the existing discovery index,
// not a member of the immutable commit's association digest.
const environmentSlotKind = "environment-slot"

// advanceEnvironmentAssociations carries only the selected resource's
// existing discovery relation across a main publication. A prior accepted
// container declaration remains discoverable when its saved slot becomes an
// asset-agent; an existing observed row takes precedence. Immutable rows and
// their digest are never altered.
func advanceEnvironmentAssociations(ctx context.Context, tx *sql.Tx, scope, domain, previous, next string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE core_configuration_agent_index SET commit_id=? WHERE scope=? AND domain=? AND commit_id=? AND kind=? AND EXISTS (SELECT 1 FROM core_references r WHERE r.scope=? AND r.domain=? AND r.commit_id=? AND r.slot='robot:'||core_configuration_agent_index.slot_id AND r.target_domain='robot' AND r.target_resource_id=core_configuration_agent_index.related_robot_resource_id)`, next, scope, domain, previous, environmentSlotKind, scope, domain, next); err != nil {
		return err
	}
	prune := func(commit, extra string) error {
		where := ` WHERE scope=? AND domain=? AND commit_id=? AND kind=?` + extra
		args := []any{scope, domain, commit, environmentSlotKind}
		var count, bytes int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(kind)+length(agent_id)+length(slot_id)+length(related_robot_resource_id)),0) FROM core_configuration_agent_index`+where, args...).Scan(&count, &bytes); err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM core_configuration_agent_index`+where, args...); err != nil {
			return err
		}
		return charge(ctx, tx, scope, -count, -bytes)
	}
	if err := prune(previous, ""); err != nil {
		return err
	}
	if err := prune(next, ` AND EXISTS (SELECT 1 FROM core_configuration_agent_index declared WHERE declared.scope=core_configuration_agent_index.scope AND declared.domain=core_configuration_agent_index.domain AND declared.commit_id=core_configuration_agent_index.commit_id AND declared.slot_id=core_configuration_agent_index.slot_id AND declared.kind='onboard-slot')`); err != nil {
		return err
	}
	// Copy the formerly accepted container identity only for the same Robot
	// reference, and only if no current observed identity or declaration exists.
	from := ` FROM core_configuration_agent_index a WHERE a.scope=? AND a.domain=? AND a.commit_id=? AND a.kind='onboard-slot'
AND EXISTS (SELECT 1 FROM core_references r WHERE r.scope=a.scope AND r.domain=a.domain AND r.commit_id=? AND r.slot='robot:'||a.slot_id AND r.target_domain='robot' AND r.target_resource_id=a.related_robot_resource_id)
AND NOT EXISTS (SELECT 1 FROM core_configuration_agent_index current WHERE current.scope=a.scope AND current.domain=a.domain AND current.commit_id=? AND current.slot_id=a.slot_id AND current.kind IN ('onboard-slot','environment-slot'))`
	args := []any{scope, domain, previous, next, next}
	var count, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length('environment-slot')+length(a.agent_id)+length(a.slot_id)+length(a.related_robot_resource_id)),0)`+from, args...).Scan(&count, &bytes); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	if err := reserve(ctx, tx, scope, count, bytes); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO core_configuration_agent_index SELECT a.scope,a.domain,?,'environment-slot',a.agent_id,a.slot_id,a.related_robot_resource_id`+from, append([]any{next}, args...)...)
	return err
}

// ReplaceEnvironmentAssociations updates the selected current-main
// Experiment's rebuildable discovery rows without changing its snapshot.
// The caller supplies facts observed by the Environment owner.
func (s *Store) ReplaceEnvironmentAssociations(ctx context.Context, domain model.ConfigurationDomainGuard, resourceID, commitID string, slotIDs []string, facts []model.ConfigurationAgentAssociation) ([]string, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) ([]string, error) {
		if _, err := configurationDomain(ctx, tx, domain); err != nil {
			return nil, err
		}
		var head string
		if err := tx.QueryRowContext(ctx, `SELECT main_commit_id FROM core_resources WHERE scope=? AND domain=? AND id=? AND archived=0`, scope, domain.Key, resourceID).Scan(&head); err != nil {
			return nil, err
		}
		if head != commitID {
			return nil, failure("conflict", "Environment projection source changed")
		}
		selected := make(map[string]bool, len(slotIDs))
		for _, slotID := range slotIDs {
			selected[slotID] = true
		}
		canonical, err := model.CanonicalConfigurationAssociations(facts)
		if err != nil {
			return nil, failure("invalid_argument", err.Error())
		}
		for _, fact := range canonical {
			if fact.Kind != model.AgentAssociationOnboardSlot || !selected[fact.SlotID] {
				return nil, failure("invalid_argument", "Environment projection requires a slot association")
			}
			var found int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM core_references WHERE scope=? AND domain=? AND commit_id=? AND slot=? AND target_domain='robot' AND target_resource_id=?`, scope, domain.Key, commitID, "robot:"+fact.SlotID, fact.RobotResourceID).Scan(&found); err != nil {
				return nil, err
			}
			if found != 1 {
				return nil, failure("conflict", "Environment projection differs from selected Robot reference")
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT agent_id,slot_id,related_robot_resource_id FROM core_configuration_agent_index WHERE scope=? AND domain=? AND commit_id=? AND kind=? ORDER BY agent_id,slot_id,related_robot_resource_id`, scope, domain.Key, commitID, environmentSlotKind)
		if err != nil {
			return nil, err
		}
		var previous []model.ConfigurationAgentAssociation
		for rows.Next() {
			fact := model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot}
			if err = rows.Scan(&fact.AgentID, &fact.SlotID, &fact.RobotResourceID); err != nil {
				rows.Close()
				return nil, err
			}
			if selected[fact.SlotID] {
				previous = append(previous, fact)
			}
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		if len(previous) == len(canonical) && (len(previous) == 0 || reflect.DeepEqual(previous, canonical)) {
			return nil, nil
		}
		for _, slotID := range slotIDs {
			if _, err = tx.ExecContext(ctx, `DELETE FROM core_configuration_agent_index WHERE scope=? AND domain=? AND commit_id=? AND kind=? AND slot_id=?`, scope, domain.Key, commitID, environmentSlotKind, slotID); err != nil {
				return nil, err
			}
		}
		var oldBytes, newBytes int64
		for _, fact := range previous {
			oldBytes += int64(len(environmentSlotKind) + len(fact.AgentID) + len(fact.SlotID) + len(fact.RobotResourceID))
		}
		for _, fact := range canonical {
			newBytes += int64(len(environmentSlotKind) + len(fact.AgentID) + len(fact.SlotID) + len(fact.RobotResourceID))
		}
		if err = reserve(ctx, tx, scope, int64(len(canonical)-len(previous)), newBytes-oldBytes); err != nil {
			return nil, err
		}
		for _, fact := range canonical {
			if _, err = tx.ExecContext(ctx, `INSERT INTO core_configuration_agent_index VALUES(?,?,?,?,?,?,?)`, scope, domain.Key, commitID, environmentSlotKind, fact.AgentID, fact.SlotID, fact.RobotResourceID); err != nil {
				return nil, err
			}
		}
		affected := map[string]bool{}
		for _, fact := range previous {
			affected[fact.AgentID] = true
		}
		for _, fact := range canonical {
			affected[fact.AgentID] = true
		}
		ids := make([]string, 0, len(affected))
		for id := range affected {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids, bumpConfigurationGeneration(ctx, tx, scope)
	})
}
