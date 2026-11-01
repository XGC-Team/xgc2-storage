package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A local child may inherit recovery authority only after its exact caller
// admitted that child in the same Run-owned typed control state.
func validateLocalRecoveryChild(ctx context.Context, tx *sql.Tx, scope string, q model.NewRun) error {
	if !textKey(q.ParentRunID) || !textKey(q.CallNodeID) {
		return failure("invalid_argument", "local recovery child requires its exact caller and call node")
	}
	status, parent, found, err := readRunRecovery(ctx, tx, scope, q.ParentRunID)
	if err != nil {
		return err
	}
	if !found || !model.RunStatus(status).Open() || parent.AgentBootID != q.RecoveryAgentBootID || parent.Joined {
		return failure("failed_precondition", "local child has no matching open recovery owner")
	}
	if parent.KernelBootID != q.RecoveryKernelBootID {
		return failure("failed_precondition", "local child has a different kernel execution scope")
	}
	var root, target string
	var depth int
	if err := tx.QueryRowContext(ctx, "SELECT root_run_id,target_id,depth FROM runs WHERE scope=? AND id=?", scope, q.ParentRunID).Scan(&root, &target, &depth); err != nil {
		return err
	}
	if root != q.RootRunID || target != q.TargetID || q.Depth != depth+1 {
		return failure("failed_precondition", "local child lineage or target differs from its owner")
	}
	for _, a := range parent.Resources {
		if a.ID == q.ID && a.NodeID == q.CallNodeID && a.Kind == "local-call" && !a.LocalPhysicalJoined && !a.LocalJoined {
			return nil
		}
	}
	return failure("failed_precondition", "local child was not admitted by its exact caller")
}

// JoinLocalRun records the live orchestration owner's actual physical join.
// A nested remote or process join can arrive later. The physical fact is kept
// separately, so neither a terminal status nor absence of a row becomes proof.
// Returned observations are only newly sealed interrupted local ancestors.
func (s *Store) JoinLocalRun(ctx context.Context, parentID, nodeID, childID, bootID string, policy model.RunRetentionPolicy) (model.RunRecoveryTransition, error) {
	for _, value := range []string{parentID, nodeID, childID, bootID} {
		if !textKey(value) {
			return model.RunRecoveryTransition{}, engine.WriteRefused(failure("invalid_argument", "exact local child and boot are required"))
		}
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunRecoveryTransition{}, engine.WriteRefused(err)
	}
	var result model.RunRecoveryTransition
	err := s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		var cut retentionChanges
		_, parent, found, err := readRunRecovery(ctx, tx, s.scope, parentID)
		if err != nil {
			return err
		}
		if !found || parent.AgentBootID != bootID {
			return failure("failed_precondition", "local caller recovery owner differs")
		}
		var admission *model.RunResourceAdmission
		for i := range parent.Resources {
			a := &parent.Resources[i]
			if a.ID == childID && a.NodeID == nodeID && a.Kind == "local-call" {
				admission = a
				break
			}
		}
		if admission == nil {
			return failure("failed_precondition", "local child was not admitted by this caller")
		}
		valid, err := exactFinishedLocalChild(ctx, tx, s.scope, parentID, nodeID, childID, bootID)
		if err != nil {
			return err
		}
		if !valid {
			return failure("failed_precondition", "local child has no matching clean durable terminal")
		}
		if !admission.LocalPhysicalJoined {
			admission.LocalPhysicalJoined = true
			if err := writeRunRecovery(ctx, tx, s.scope, parentID, parent, false); err != nil {
				return err
			}
		}
		result.Recovered, err = completeLocalAncestors(ctx, tx, s.scope, childID, &cut, true)
		if err != nil {
			return err
		}
		if err := cut.finish(ctx, tx, s.scope); err != nil {
			return err
		}
		result.Retention, err = retentionEffect(ctx, tx, s.scope, &cut, policy)
		return err
	})
	if err != nil {
		// An admitted callback may still own result until physical settlement.
		return model.RunRecoveryTransition{}, err
	}
	return result, err
}

// exactFinishedLocalChild verifies identity and the already durable terminal;
// this is necessary but never sufficient without the live physical join.
func exactFinishedLocalChild(ctx context.Context, tx *sql.Tx, scope, parentID, nodeID, childID, bootID string) (bool, error) {
	var childParent, childCall, childRoot, childTarget, childStatus string
	var cleanupFailed, childDepth int
	err := tx.QueryRowContext(ctx, "SELECT parent_run_id,call_node_id,root_run_id,target_id,status,cleanup_failed,depth FROM runs WHERE scope=? AND id=?", scope, childID).
		Scan(&childParent, &childCall, &childRoot, &childTarget, &childStatus, &cleanupFailed, &childDepth)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var parentRoot, parentTarget string
	var parentDepth int
	if err := tx.QueryRowContext(ctx, "SELECT root_run_id,target_id,depth FROM runs WHERE scope=? AND id=?", scope, parentID).Scan(&parentRoot, &parentTarget, &parentDepth); err != nil {
		return false, err
	}
	if childParent != parentID || childCall != nodeID || childRoot != parentRoot || childTarget != parentTarget || childDepth != parentDepth+1 || cleanupFailed != 0 || model.RunStatus(childStatus).Open() {
		return false, nil
	}
	switch model.RunStatus(childStatus) {
	case model.RunSucceeded, model.RunFailed, model.RunStopped, model.RunCanceled, model.RunInterrupted:
	default:
		return false, nil
	}
	_, child, found, err := readRunRecovery(ctx, tx, scope, childID)
	return found && child.AgentBootID == bootID, err
}

// completeLocalAncestors follows only exact previously admitted parent links.
// Its input is one changed child, not a Run-history scan. Each step requires
// the previously recorded physical owner join and all actual child resources.
func completeLocalAncestors(ctx context.Context, tx *sql.Tx, scope, childID string, cut *retentionChanges, reportExisting bool) ([]model.Run, error) {
	var recovered []model.Run
	for depth := 0; depth < model.MaxRunDepth; depth++ {
		var parentID, nodeID string
		err := tx.QueryRowContext(ctx, "SELECT parent_run_id,call_node_id FROM runs WHERE scope=? AND id=?", scope, childID).Scan(&parentID, &nodeID)
		if errors.Is(err, sql.ErrNoRows) {
			return recovered, nil
		}
		if err != nil {
			return nil, err
		}
		if parentID == "" || nodeID == "" {
			return recovered, nil
		}
		_, parent, found, err := readRunRecovery(ctx, tx, scope, parentID)
		if err != nil {
			var fact *api.Error
			if errors.As(err, &fact) && fact.Code == "not_found" {
				return recovered, nil // incoming parent belongs to another target
			}
			return nil, err
		}
		if !found || parent.AgentBootID == "" {
			return recovered, nil
		}
		var admission *model.RunResourceAdmission
		for i := range parent.Resources {
			a := &parent.Resources[i]
			if a.ID == childID && a.NodeID == nodeID && a.Kind == "local-call" {
				admission = a
				break
			}
		}
		if admission == nil || !admission.LocalPhysicalJoined {
			return recovered, nil
		}
		valid, err := exactFinishedLocalChild(ctx, tx, scope, parentID, nodeID, childID, parent.AgentBootID)
		if err != nil || !valid {
			return recovered, err
		}
		_, child, _, err := readRunRecovery(ctx, tx, scope, childID)
		if err != nil {
			return nil, err
		}
		for _, a := range child.Resources {
			switch a.Kind {
			case "completed-effect":
				// This exact effect returned successfully before its owner ended.
			case "workflow-call":
				// Its actual outbound relation must have closed below.
			case "local-call":
				if !a.LocalJoined {
					return recovered, nil
				}
			case "process", "command":
				if a.Join == nil && child.KernelReplacedBy == "" {
					return recovered, nil
				}
			default:
				return recovered, nil
			}
		}
		var pending bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM run_remote_pending WHERE scope=? AND owner_run_id=?)", scope, childID).Scan(&pending); err != nil {
			return nil, err
		}
		if pending {
			return recovered, nil
		}
		if !admission.LocalJoined {
			admission.LocalJoined = true
			if err := writeRunRecovery(ctx, tx, scope, parentID, parent, false); err != nil {
				return nil, err
			}
			cut.touch(childID)
		}
		sealed := parent.Joined && reportExisting
		if !parent.Joined {
			var err error
			sealed, err = sealRunResourceRecovery(ctx, tx, scope, parentID, parent.AgentBootID)
			if err != nil {
				return nil, err
			}
		}
		if sealed {
			row, err := scanRun(tx.QueryRowContext(ctx, "SELECT "+runObservationColumns+" FROM runs WHERE scope=? AND id=?", scope, parentID), model.RunObservation)
			if err != nil {
				return nil, err
			}
			if row.Status == model.RunInterrupted {
				recovered = append(recovered, row)
			}
		}
		childID = parentID
	}
	return nil, failure("data_loss", "local Run lineage exceeds its maximum depth")
}
