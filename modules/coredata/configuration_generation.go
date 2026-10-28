package coredata

import (
	"context"
	"database/sql"
	"errors"
)

// ReadConfigurationGeneration returns the current scope's derived revision.
// It is an invalidation fence, not a second configuration authority. Every
// actual configuration publication increments it in the same owner transaction.
func (s *Store) ReadConfigurationGeneration(ctx context.Context) (int64, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (int64, error) {
		return configurationGeneration(ctx, tx, scope)
	})
}

func configurationGeneration(ctx context.Context, tx *sql.Tx, scope string) (int64, error) {
	var generation int64
	err := tx.QueryRowContext(ctx, `SELECT configuration_generation FROM core_data_usage WHERE scope=?`, scope).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if generation < 0 {
		return 0, failure("data_loss", "invalid configuration generation")
	}
	return generation, nil
}
func bumpConfigurationGeneration(ctx context.Context, tx *sql.Tx, scope string) error {
	result, err := tx.ExecContext(ctx, `UPDATE core_data_usage SET configuration_generation=configuration_generation+1 WHERE scope=? AND configuration_generation<9223372036854775807`, scope)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return failure("resource_exhausted", "configuration generation exhausted")
	}
	return nil
}
