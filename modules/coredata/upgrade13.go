package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Upgrade13To14 preserves the accepted database and adds only the explicit
// typed Run recovery field. Existing empty evidence remains unknown. This is
// an operator-invoked offline cut; normal startup never calls it.
func Upgrade13To14(ctx context.Context, config engine.Config, backupDestination string) (engine.CoreData13UpgradeReceipt, error) {
	return Upgrade13To14WithLocalScope(ctx, config, backupDestination, nil)
}

// LegacyLocalScopeHandoff is a narrowly selected operator maintenance fact:
// all local OS work of the named legacy incoming Runs belonged to the old
// kernel scope. It asserts nothing about external or remote child work.
type LegacyLocalScopeHandoff struct {
	PreviousKernelBootID    string
	ReplacementKernelBootID string
	PreviousAgentBootID     string
	RunIDs                  []string
}

// Upgrade13To14WithLocalScope applies an optional explicit witnessed handoff
// within the same exclusive backup and schema transaction. Ordinary 13→14
// upgrades pass nil and leave every historical resource fact unknown.
func Upgrade13To14WithLocalScope(ctx context.Context, config engine.Config, backupDestination string, handoff *LegacyLocalScopeHandoff) (engine.CoreData13UpgradeReceipt, error) {
	if handoff != nil {
		if !textKey(handoff.PreviousKernelBootID) || !textKey(handoff.ReplacementKernelBootID) || handoff.PreviousKernelBootID == handoff.ReplacementKernelBootID || !textKey(handoff.PreviousAgentBootID) || len(handoff.RunIDs) == 0 || len(handoff.RunIDs) > model.MaxPageSize {
			return engine.CoreData13UpgradeReceipt{}, failure("invalid_argument", "exact previous and replacement kernels, Agent boot and selected Runs are required")
		}
		seen := make(map[string]bool, len(handoff.RunIDs))
		for _, id := range handoff.RunIDs {
			if !textKey(id) || seen[id] {
				return engine.CoreData13UpgradeReceipt{}, failure("invalid_argument", "selected Run IDs must be exact and unique")
			}
			seen[id] = true
		}
	}
	return engine.UpgradeCoreData13To14(ctx, config, backupDestination, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE runs ADD COLUMN resource_recovery_json BLOB NOT NULL DEFAULT x''"); err != nil {
			return err
		}
		if handoff == nil {
			return nil
		}
		for _, id := range handoff.RunIDs {
			var count int
			if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM runs WHERE id=?", id).Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				return failure("failed_precondition", "selected legacy Run must exist in exactly one scope")
			}
			var scope, status, target, incoming string
			var released, cleanupFailed int
			if err := tx.QueryRowContext(ctx, "SELECT scope,status,target_id,incoming_remote_target_id,incoming_hold_released,cleanup_failed FROM runs WHERE id=?", id).Scan(&scope, &status, &target, &incoming, &released, &cleanupFailed); err != nil {
				return err
			}
			if model.RunStatus(status) != model.RunInterrupted || incoming == "" || released != 0 || cleanupFailed != 0 {
				return failure("failed_precondition", "selected legacy Run is not an interrupted retained incoming owner")
			}
			var localChild bool
			if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM runs WHERE scope=? AND parent_run_id=? AND target_id=?)", scope, id, target).Scan(&localChild); err != nil {
				return err
			}
			if localChild {
				return failure("failed_precondition", "selected legacy Run has a local descendant without complete recorded ownership")
			}
			state := model.RunResourceRecovery{AgentBootID: handoff.PreviousAgentBootID, Resources: []model.RunResourceAdmission{}, LegacyLocalScope: &model.RunLegacyLocalScope{PreviousKernelBootID: handoff.PreviousKernelBootID, ReplacementKernelBootID: handoff.ReplacementKernelBootID}}
			if err := writeRunRecovery(ctx, tx, scope, id, state, false); err != nil {
				return err
			}
			if _, err := sealRunResourceRecovery(ctx, tx, scope, id, handoff.PreviousAgentBootID); err != nil {
				return err
			}
		}
		return nil
	})
}
