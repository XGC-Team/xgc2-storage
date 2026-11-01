package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func validateRemoteProviderSelection(item model.RunRemoteProviderSelection) error {
	if err := validateRemoteLink(item.Link); err != nil {
		return err
	}
	if item.Link.BootID == "" || item.Link.Phase != "" && item.Link.Phase != model.RemoteRunOpen || item.Link.SettledDisposition != "" || !item.Link.SettledAt.IsZero() || !item.Link.ReleasedAt.IsZero() {
		return failure("invalid_argument", "selected provider requires an open boot-bound relation")
	}
	for _, value := range []string{item.SessionID, item.NodeID, item.ResourceID, item.Incarnation, item.WorkflowResourceID, item.WorkflowCommitID, item.WorkflowDigest} {
		if !textKey(value) {
			return failure("invalid_argument", "complete selected provider identity is required")
		}
	}
	return nil
}

func sameSelectedLink(a, b model.RemoteRunLink) bool {
	return a.OwnerRunID == b.OwnerRunID && a.RootRunID == b.RootRunID && a.TargetID == b.TargetID && a.RunID == b.RunID && a.CallNodeID == b.CallNodeID && a.BootID == b.BootID
}

// AdmitRemoteProviderRecovery records the exact authenticated selection on its
// existing Run before its live provider can be borrowed. A committed duplicate
// is idempotent; a different incarnation never replaces the first.
func (s *Store) AdmitRemoteProviderRecovery(ctx context.Context, item model.RunRemoteProviderSelection) error {
	if err := validateRemoteProviderSelection(item); err != nil {
		return engine.WriteRefused(err)
	}
	item.Link.Phase = ""
	return s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		var root, call, boot, phase, session, ownerRoot, ownerTarget string
		err := tx.QueryRowContext(ctx, `SELECT p.root_run_id,p.call_node_id,p.boot_id,p.phase,r.session_id,r.root_run_id,r.target_id FROM run_remote_pending p JOIN runs r ON r.scope=p.scope AND r.id=p.owner_run_id WHERE p.scope=? AND p.owner_run_id=? AND p.target_id=? AND p.child_run_id=?`, s.scope, item.Link.OwnerRunID, item.Link.TargetID, item.Link.RunID).Scan(&root, &call, &boot, &phase, &session, &ownerRoot, &ownerTarget)
		if errors.Is(err, sql.ErrNoRows) {
			return failure("failed_precondition", "the selected remote relation is unavailable")
		}
		if err != nil {
			return err
		}
		if root != ownerRoot || item.Link.TargetID == ownerTarget {
			return failure("data_loss", "remote relation disagrees with its Run owner")
		}
		if root != item.Link.RootRunID || call != item.Link.CallNodeID || boot != item.Link.BootID || phase != string(model.RemoteRunOpen) || session != item.SessionID {
			return failure("failed_precondition", "the selected provider differs from its admitted relation or Session")
		}
		status, nodes, state, found, err := readRecoveryNodes(ctx, tx, s.scope, item.Link.OwnerRunID)
		if err != nil {
			return err
		}
		if !found || !state.TrackRemoteProviders || !model.RunStatus(status).Open() {
			return failure("failed_precondition", "the Run cannot admit remote provider recovery")
		}
		for _, prior := range state.RemoteProviders {
			if prior.Link.TargetID == item.Link.TargetID && prior.Link.RunID == item.Link.RunID {
				if prior == item {
					return nil
				}
				return failure("failed_precondition", "the selected remote provider identity already differs")
			}
		}
		// A long-lived Run can resolve many sequential remote calls. Retain
		// selections only for its bounded currently pending relation set.
		rows, err := tx.QueryContext(ctx, `SELECT target_id,child_run_id FROM run_remote_pending WHERE scope=? AND owner_run_id=?`, s.scope, item.Link.OwnerRunID)
		if err != nil {
			return err
		}
		active := make(map[string]bool)
		for rows.Next() {
			var target, run string
			if err = rows.Scan(&target, &run); err != nil {
				break
			}
			active[target+"\x00"+run] = true
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		retained := state.RemoteProviders[:0]
		for _, prior := range state.RemoteProviders {
			if active[prior.Link.TargetID+"\x00"+prior.Link.RunID] {
				retained = append(retained, prior)
			}
		}
		state.RemoteProviders = append(retained, item)
		return writeRecoveryNodes(ctx, tx, s.scope, item.Link.OwnerRunID, nodes, state, true)
	})
}

// GetRemoteProviderRecovery reads only the Run belonging to the exact
// currently retained relation. No historical or default selection is inferred.
func (s *Store) GetRemoteProviderRecovery(ctx context.Context, link model.RemoteRunLink) (model.RunRemoteProviderSelection, bool, error) {
	if err := validateRemoteLink(link); err != nil || link.BootID == "" {
		return model.RunRemoteProviderSelection{}, false, failure("invalid_argument", "exact boot-bound relation required")
	}
	type result struct {
		item  model.RunRemoteProviderSelection
		found bool
	}
	out, err := read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (result, error) {
		var root, call, boot, session, ownerRoot, ownerTarget string
		var tracked bool
		err := tx.QueryRowContext(ctx, `SELECT p.root_run_id,p.call_node_id,p.boot_id,r.session_id,r.root_run_id,r.target_id,coalesce(substr(r.nodes_json,1,?)=?,0) FROM run_remote_pending p JOIN runs r ON r.scope=p.scope AND r.id=p.owner_run_id WHERE p.scope=? AND p.owner_run_id=? AND p.target_id=? AND p.child_run_id=?`, len(resourceRecoveryPrefix), resourceRecoveryPrefix, scope, link.OwnerRunID, link.TargetID, link.RunID).Scan(&root, &call, &boot, &session, &ownerRoot, &ownerTarget, &tracked)
		if errors.Is(err, sql.ErrNoRows) {
			return result{}, nil
		}
		if err != nil {
			return result{}, err
		}
		if root != ownerRoot || link.TargetID == ownerTarget {
			return result{}, failure("data_loss", "remote relation disagrees with its Run owner")
		}
		if root != link.RootRunID || call != link.CallNodeID || boot != link.BootID {
			return result{}, failure("failed_precondition", "retained relation identity differs")
		}
		if !tracked {
			return result{}, nil
		}
		_, _, state, found, err := readRecoveryNodes(ctx, tx, scope, link.OwnerRunID)
		if err != nil || !found || !state.TrackRemoteProviders {
			return result{}, err
		}
		for _, item := range state.RemoteProviders {
			if sameSelectedLink(item.Link, link) && item.SessionID == session {
				return result{item: item, found: true}, nil
			}
		}
		return result{}, nil
	})
	return out.item, out.found, err
}
