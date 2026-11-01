package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// JoinRunKernelScope records the Supervisor's replacement-kernel fact for
// one exact interrupted Run. The originating kernel was committed with Run
// acceptance; an absent originating identity is unknown. This joins only
// local OS processes, not remote children or uncovered external effects.
func (s *Store) JoinRunKernelScope(ctx context.Context, runID string, want model.IncomingRunRelease, currentKernelBootID string, policy model.RunRetentionPolicy) (model.RunRecoveryTransition, error) {
	for _, value := range []string{runID, want.AgentBootID, want.ParentRunID, want.RootRunID, want.CallNodeID, want.TargetID, want.PrincipalID, currentKernelBootID} {
		if !textKey(value) {
			return model.RunRecoveryTransition{}, engine.WriteRefused(failure("invalid_argument", "exact Run and execution scope identities are required"))
		}
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunRecoveryTransition{}, engine.WriteRefused(err)
	}
	var result model.RunRecoveryTransition
	err := s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		var parent, root, call, target, principal string
		if err := tx.QueryRowContext(ctx, `SELECT parent_run_id,root_run_id,call_node_id,incoming_remote_target_id,incoming_remote_principal_id FROM runs WHERE scope=? AND id=?`, s.scope, runID).Scan(&parent, &root, &call, &target, &principal); err != nil {
			return err
		}
		if parent != want.ParentRunID || root != want.RootRunID || call != want.CallNodeID || target != want.TargetID || principal != want.PrincipalID {
			return failure("failed_precondition", "incoming hold identity or caller facts disagree")
		}
		status, state, found, err := readRunRecovery(ctx, tx, s.scope, runID)
		if err != nil {
			return err
		}
		if !found || model.RunStatus(status) != model.RunInterrupted || state.AgentBootID != want.AgentBootID || state.KernelBootID == "" || state.KernelBootID == currentKernelBootID {
			return failure("failed_precondition", "interrupted Run has no ended kernel execution scope")
		}
		if state.KernelReplacedBy == "" {
			state.KernelReplacedBy = currentKernelBootID
			if err := writeRunRecovery(ctx, tx, s.scope, runID, state, true); err != nil {
				return err
			}
		}
		var cut retentionChanges
		sealed, err := sealRunResourceRecovery(ctx, tx, s.scope, runID, want.AgentBootID)
		if err != nil {
			return err
		}
		if sealed {
			cut.touch(runID)
		}
		result.Recovered, err = completeLocalAncestors(ctx, tx, s.scope, runID, &cut, true)
		if err != nil {
			return err
		}
		if err := cut.finish(ctx, tx, s.scope); err != nil {
			return err
		}
		result.Retention, err = retentionEffect(ctx, tx, s.scope, &cut, policy)
		return err
	})
	return result, err
}

// WitnessRunKernelScope records an explicitly observed old-to-current kernel
// transition for a selected current-format incoming Run whose original scope
// was not recorded. Its caller must own an exclusive, offline Storage host
// and must have independently observed the old Agent's actual kernel scope.
// It never releases the incoming hold or changes the business outcome.
func (s *Store) WitnessRunKernelScope(ctx context.Context, runID, agentBootID, previousKernelBootID, currentKernelBootID string, policy model.RunRetentionPolicy) (model.RunRecoveryTransition, error) {
	for _, value := range []string{runID, agentBootID, previousKernelBootID, currentKernelBootID} {
		if !textKey(value) {
			return model.RunRecoveryTransition{}, engine.WriteRefused(failure("invalid_argument", "exact witnessed scope identities are required"))
		}
	}
	if previousKernelBootID == currentKernelBootID {
		return model.RunRecoveryTransition{}, engine.WriteRefused(failure("failed_precondition", "the kernel execution scope has not been replaced"))
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunRecoveryTransition{}, engine.WriteRefused(err)
	}
	var result model.RunRecoveryTransition
	err := s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		status, state, found, err := readRunRecovery(ctx, tx, s.scope, runID)
		if err != nil {
			return err
		}
		var incoming string
		var released int
		if err := tx.QueryRowContext(ctx, "SELECT incoming_remote_target_id,incoming_hold_released FROM runs WHERE scope=? AND id=?", s.scope, runID).Scan(&incoming, &released); err != nil {
			return err
		}
		if !found || model.RunStatus(status) != model.RunInterrupted || state.AgentBootID != agentBootID || incoming == "" || released != 0 ||
			state.KernelBootID != "" && state.KernelBootID != previousKernelBootID || state.KernelReplacedBy != "" && state.KernelReplacedBy != currentKernelBootID {
			return failure("failed_precondition", "selected Run does not match the witnessed incoming scope")
		}
		if state.KernelBootID == "" || state.KernelReplacedBy == "" {
			state.KernelBootID, state.KernelReplacedBy = previousKernelBootID, currentKernelBootID
			if err := writeRunRecovery(ctx, tx, s.scope, runID, state, true); err != nil {
				return err
			}
		}
		var cut retentionChanges
		sealed, err := sealRunResourceRecovery(ctx, tx, s.scope, runID, agentBootID)
		if err != nil {
			return err
		}
		if sealed {
			cut.touch(runID)
		}
		result.Recovered, err = completeLocalAncestors(ctx, tx, s.scope, runID, &cut, true)
		if err != nil {
			return err
		}
		if err := cut.finish(ctx, tx, s.scope); err != nil {
			return err
		}
		result.Retention, err = retentionEffect(ctx, tx, s.scope, &cut, policy)
		return err
	})
	return result, err
}
