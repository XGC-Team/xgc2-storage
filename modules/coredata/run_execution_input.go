package coredata

import (
	"context"
	"database/sql"
	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strings"
)

// GetRunExecutionInput selects one accepted input and thin observation header;
// it never selects or decompresses node, result, trigger, or cleanup payloads.
func (s *Store) GetRunExecutionInput(ctx context.Context, id string) (model.Run, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return getRunExecutionInput(ctx, tx, scope, id)
	})
}
func getRunExecutionInput(ctx context.Context, tx *sql.Tx, scope, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	columns := strings.Replace(runObservationColumns, "action_id,x'',x'',status", "action_id,CASE WHEN length(inputs_json)<=? THEN inputs_json ELSE x'00' END,x'',status", 1)
	row, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM runs WHERE scope=? AND id=?", 2*model.MaxRunInputsBytes, scope, id), model.RunExecutionInput)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	return row, err
}
