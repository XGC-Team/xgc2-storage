package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// GetRunRestartReceipt selects one restart's bounded private admission proof
// and public observation without loading its node graph, result or cleanup.
func (s *Store) GetRunRestartReceipt(ctx context.Context, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		const columns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,status,termination,error_summary_json,x'',x'',x'',created_at,started_at,finished_at,revision,length(result_json)>0,source_observation_json,error_summary_json,cleanup_failed"
		run, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM runs WHERE scope=? AND id=?", scope, id), model.RunObservation)
		if errors.Is(err, sql.ErrNoRows) {
			return model.Run{}, failure("not_found", "run not found")
		}
		return run, err
	})
}
