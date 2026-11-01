package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// CoreData13UpgradeReceipt records an explicit offline conversion and its
// independently verified, non-overwriting pre-change backup.
type CoreData13UpgradeReceipt struct {
	DatabaseID string           `json:"database_id"`
	From       int              `json:"from"`
	To         int              `json:"to"`
	Backup     BackupReceipt    `json:"backup"`
	Checkpoint CheckpointResult `json:"checkpoint"`
}

// UpgradeCoreData13To14 is the single authorized offline Core-data cut.
// The compiled Core-data module supplies the one typed-column operation. The
// Storage engine owns the exclusive database lock, backup and transaction;
// this entry is never called by Open or ordinary application startup.
func UpgradeCoreData13To14(ctx context.Context, config Config, destination string, addRunRecovery func(context.Context, *sql.Tx) error) (receipt CoreData13UpgradeReceipt, resultErr error) {
	if config.Create || addRunRecovery == nil {
		return receipt, fail("invalid_argument", "an existing database and the Core-data 13-to-14 operation are required")
	}
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || filepath.Ext(destination) != ".db" || strings.ContainsAny(destination, "\x00?#") {
		return receipt, fail("invalid_argument", "canonical backup .db grant required")
	}
	store, err := open(ctx, config, true)
	if err != nil {
		return receipt, err
	}
	defer func() { resultErr = errors.Join(resultErr, store.Close()) }()

	if err := store.Integrity(ctx); err != nil {
		return receipt, err
	}
	backup, err := store.Backup(ctx, destination)
	if err != nil {
		return receipt, err
	}
	receipt = CoreData13UpgradeReceipt{DatabaseID: store.dbid, From: 13, To: 14, Backup: backup}
	err = store.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		var version int
		if err := tx.QueryRowContext(ctx, "SELECT version FROM schema_versions WHERE module='coredata'").Scan(&version); err != nil {
			return err
		}
		if version != 13 {
			return fail("failed_precondition", "Core-data schema changed during the offline cut")
		}
		if err := addRunRecovery(ctx, tx); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, "UPDATE schema_versions SET version=14 WHERE module='coredata' AND version=13")
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return fail("failed_precondition", "Core-data schema marker changed during the offline cut")
		}
		return nil
	})
	if err != nil {
		return receipt, err
	}
	// Verify through the same current-schema gate used by ordinary Open while
	// retaining the exclusive owner lock. No old-schema reader is installed.
	if err := store.admitCurrentSchema(ctx); err != nil {
		return receipt, fmt.Errorf("upgraded schema did not pass current admission: %w", err)
	}
	if err := store.Integrity(ctx); err != nil {
		return receipt, err
	}
	receipt.Checkpoint, err = store.Checkpoint(ctx)
	if err != nil {
		return receipt, err
	}
	if receipt.Checkpoint.Busy != 0 || receipt.Checkpoint.LogPages != receipt.Checkpoint.CheckpointedPages {
		return receipt, fail("conflict", "upgraded database checkpoint did not join")
	}
	return receipt, nil
}

// admitCoreData13ForUpgrade does not run the usual index synchronizer: the
// explicit cut must not alter collections or any accepted configuration.
func (s *Store) admitCoreData13ForUpgrade(ctx context.Context) error {
	versions, err := s.storedVersions(ctx)
	if err != nil {
		return err
	}
	if versions["engine"] != engineVersion {
		return fail("failed_precondition", "offline Core-data cut requires the current engine schema")
	}
	used := s.usedModules()
	core, ok := s.modules["coredata"]
	if !ok || !used["coredata"] || core.Version != 14 || versions["coredata"] != 13 {
		return fail("failed_precondition", "offline cut requires compiled Core-data 14 and stored Core-data 13")
	}
	for _, id := range sortedModuleIDs(s.modules, used) {
		if id != "coredata" && versions[id] != s.modules[id].Version {
			return fail("failed_precondition", "offline cut refuses a noncurrent unrelated module")
		}
	}
	declared := make(map[[2]string]string)
	for _, namespace := range s.config.Manifest.Namespaces {
		for _, collection := range namespace.Collections {
			declared[[2]string{namespace.ID, collection.ID}] = indexSignature(collection)
		}
	}
	rows, err := s.writer.QueryContext(ctx, "SELECT namespace,collection,signature FROM collection_indexes")
	if err != nil {
		return err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var namespace, collection, signature string
		if err := rows.Scan(&namespace, &collection, &signature); err != nil {
			return err
		}
		if expected, ok := declared[[2]string{namespace, collection}]; !ok || expected != signature {
			return fail("failed_precondition", "offline cut requires the existing collection declarations")
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seen != len(declared) {
		return fail("failed_precondition", "offline cut requires the existing collection declarations")
	}
	return nil
}
