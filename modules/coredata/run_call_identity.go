package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The CASE prevents the driver from allocating a malformed oversized blob.
// Non-child rows do not read or decompress trigger_json at all.
const runCallIdentityQuery = `SELECT id,target_id,root_run_id,parent_run_id,call_node_id,depth,
CASE WHEN parent_run_id<>'' AND call_node_id<>'' AND depth>0 AND length(trigger_json)<=? THEN trigger_json ELSE x'' END,
CASE WHEN parent_run_id<>'' AND call_node_id<>'' AND depth>0 THEN length(trigger_json) ELSE 0 END
FROM runs WHERE scope=? AND id=?`

func getRunCallIdentity(ctx context.Context, tx *sql.Tx, scope, id string) (model.RunCallIdentity, error) {
	if !textKey(id) {
		return model.RunCallIdentity{}, failure("invalid_argument", "run identity required")
	}
	var out model.RunCallIdentity
	var trigger []byte
	var size int
	err := tx.QueryRowContext(ctx, runCallIdentityQuery, model.MaxStoredRunCallTriggerBytes, scope, id).Scan(
		&out.ID, &out.TargetID, &out.RootRunID, &out.ParentRunID, &out.CallNodeID, &out.Depth, &trigger, &size)
	if errors.Is(err, sql.ErrNoRows) {
		return model.RunCallIdentity{}, failure("not_found", "run not found")
	}
	if err != nil {
		return model.RunCallIdentity{}, err
	}
	if size > model.MaxStoredRunCallTriggerBytes {
		return model.RunCallIdentity{}, failure("data_loss", "stored Run call trigger exceeds its byte limit")
	}
	raw, err := decodeRunPayload(trigger, model.MaxRunCallTriggerBytes)
	if err != nil {
		return model.RunCallIdentity{}, err
	}
	out.Trigger = rawOrNil(raw)
	return out, nil
}

// GetRunCallIdentity reads one canonical child-call cause and scalar lineage.
// It does not select execution results, nodes, inputs or observation metadata.
func (s *Store) GetRunCallIdentity(ctx context.Context, id string) (model.RunCallIdentity, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RunCallIdentity, error) {
		return getRunCallIdentity(ctx, tx, scope, id)
	})
}
