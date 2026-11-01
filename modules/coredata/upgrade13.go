package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/engine"
)

// Upgrade13To14 preserves the accepted database and adds only the explicit
// typed Run recovery field. Existing empty evidence remains unknown. This is
// an operator-invoked offline cut; normal startup never calls it.
func Upgrade13To14(ctx context.Context, config engine.Config, backupDestination string) (engine.CoreData13UpgradeReceipt, error) {
	return engine.UpgradeCoreData13To14(ctx, config, backupDestination, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "ALTER TABLE runs ADD COLUMN resource_recovery_json BLOB NOT NULL DEFAULT x''")
		return err
	})
}
