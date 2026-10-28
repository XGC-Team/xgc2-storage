package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// GetRunRestartSource reads one scalar Run identity and its bounded immutable
// cause, never the recorded inputs, result, node graph or cleanup payload.
func (s *Store) GetRunRestartSource(ctx context.Context, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		const columns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,x'',trigger_json,status,termination,x'',x'',x'',x'',created_at,started_at,finished_at,revision,length(result_json)>0,x'',x'',0"
		run, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM runs WHERE scope=? AND id=?", scope, id), model.RunIdentity)
		if errors.Is(err, sql.ErrNoRows) {
			return model.Run{}, failure("not_found", "run not found")
		}
		return run, err
	})
}
