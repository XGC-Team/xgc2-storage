package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationAssociations(ctx context.Context, tx *sql.Tx, scope, domain, commit string) ([]model.ConfigurationAgentAssociation, int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT kind,agent_id,slot_id,related_robot_resource_id FROM core_configuration_agent_index
WHERE scope=? AND domain=? AND commit_id=? ORDER BY kind,agent_id,slot_id,related_robot_resource_id LIMIT ?`, scope, domain, commit, model.MaxConfigurationAssociations+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	facts := []model.ConfigurationAgentAssociation{}
	size := 0
	for rows.Next() {
		var fact model.ConfigurationAgentAssociation
		if err = rows.Scan(&fact.Kind, &fact.AgentID, &fact.SlotID, &fact.RobotResourceID); err != nil {
			return nil, 0, err
		}
		size += len(fact.Kind) + len(fact.AgentID) + len(fact.SlotID) + len(fact.RobotResourceID)
		if len(facts) == model.MaxConfigurationAssociations || size > model.MaxConfigurationDecodedBytes {
			return nil, 0, failure("resource_exhausted", "agent association set exceeds bound")
		}
		facts = append(facts, fact)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, err
	}
	return facts, size, nil
}

func configurationAssociationIntegrity(commit model.ConfigurationCommit, facts []model.ConfigurationAgentAssociation) error {
	digest, err := model.ConfigurationAssociationDigest(facts)
	if err != nil || len(facts) != commit.AssociationCount || digest != commit.AssociationDigest {
		return failure("data_loss", "immutable agent association set disagrees")
	}
	return nil
}
