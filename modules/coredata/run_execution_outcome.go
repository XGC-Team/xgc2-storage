package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The encoded bounds are conservative ceilings above the encoder's maximum
// valid output. CASE keeps a corrupt oversized blob out of the Go driver;
// the sentinel is rejected by the ordinary bounded payload decoder.
const executionOutcomeColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,x'',x'',status,termination," +
	"CASE WHEN length(error_json)<=? THEN error_json ELSE x'00' END," +
	"CASE WHEN length(result_json)<=? THEN result_json ELSE x'00' END," +
	"x'',x'',created_at,started_at,finished_at,revision,length(result_json)>0," +
	"CASE WHEN length(source_observation_json)<=? THEN source_observation_json ELSE x'00' END," +
	"CASE WHEN length(error_summary_json)<=? THEN error_summary_json ELSE x'00' END,cleanup_failed," +
	"incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released"

func getRunExecutionOutcome(ctx context.Context, tx *sql.Tx, scope, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	run, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+executionOutcomeColumns+" FROM runs WHERE scope=? AND id=?",
		2*model.MaxRunErrorBytes, 2*model.MaxRunResultBytes,
		model.MaxRunSourceObservationBytes, model.MaxRunErrorSummaryBytes, scope, id), model.RunExecutionOutcome)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	return run, err
}

// GetRunExecutionOutcome reads the exact Run header, full error and full result.
// It does not select or decompress inputs, trigger, nodes or cleanup details.
func (s *Store) GetRunExecutionOutcome(ctx context.Context, id string) (model.Run, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return getRunExecutionOutcome(ctx, tx, scope, id)
	})
}
