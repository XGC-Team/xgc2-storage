package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// nodeProjectionColumns retains the existing row's complete execution error,
// observation metadata and cleanup count. Discarded columns never enter the
// driver/decoder. Nodes remain one compressed column, scanned by the consumer.
func nodeProjectionColumns(includeInput bool) (string, []any) {
	inputs := "x''"
	args := []any{}
	if includeInput {
		inputs = "CASE WHEN length(inputs_json)<=? THEN inputs_json ELSE x'00' END"
		args = append(args, 2*model.MaxRunInputsBytes)
	}
	columns := "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id," +
		inputs + ",x'',status,termination,CASE WHEN length(error_json)<=? THEN error_json ELSE x'00' END,"
	args = append(args, 2*model.MaxRunErrorBytes)
	result := "CASE WHEN length(result_json)<=? THEN result_json ELSE x'00' END"
	args = append(args, 2*model.MaxRunResultBytes)
	columns += result + ",CASE WHEN length(nodes_json)<=? THEN nodes_json ELSE x'00' END,x'',created_at,started_at,finished_at,revision,length(result_json)>0," +
		"CASE WHEN length(source_observation_json)<=? THEN source_observation_json ELSE x'00' END," +
		"CASE WHEN length(error_summary_json)<=? THEN error_summary_json ELSE x'00' END,cleanup_failed," +
		"incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released"
	args = append(args, 2*model.MaxRunNodesBytes, model.MaxRunSourceObservationBytes, model.MaxRunErrorSummaryBytes)
	return columns, args
}

func getRunNodeProjection(ctx context.Context, tx *sql.Tx, scope, id string, includeInput bool) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	columns, args := nodeProjectionColumns(includeInput)
	args = append(args, scope, id)
	run, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM runs WHERE scope=? AND id=?", args...), model.RunNodeProjection)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	return run, err
}

// GetRunNodeProjection reads the exact execution header, result and nodes. The true
// variant also retains accepted inputs for explicit runs.read consumers.
// Neither variant selects trigger or cleanup-message detail. All existing
// encoded/decoded payload bounds and scope/not-found semantics remain in force.
func (s *Store) GetRunNodeProjection(ctx context.Context, id string, includeInput bool) (model.Run, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return getRunNodeProjection(ctx, tx, scope, id, includeInput)
	})
}
