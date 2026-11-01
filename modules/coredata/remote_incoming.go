package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ReleaseIncomingRun releases the exact incoming hold stamped on a
// remote-called child Run. The presented facts must equal the stamped caller
// identity and the Run's immutable call lineage: any disagreement is a
// foreign or stale call and refuses. The stamped caller PrincipalID is the
// caller's stable core identity delivered on the accepted transport at
// admission — not an ephemeral boot — so the same caller authenticates after
// its own restart without weakening live-session fences elsewhere. A missing
// Run is an idempotent success — the hold cannot outlive its row — and an
// already released hold reports no change. Release requires the caller's
// clean-terminal/join proof: the child must have reached a clean terminal
// disposition (succeeded/failed/stopped/canceled with no failed cleanup).
// An interrupted child can also be released after its local resource owner
// confirms join and its actual remote descendants have joined. Release never
// reads the child's prunable payload.
// Transport authentication of the current legitimate caller is the ingress
// owner's job; this store only compares the exact historical call facts it
// stamped.
func (s *Store) ReleaseIncomingRun(ctx context.Context, runID string, want model.IncomingRunRelease) (bool, error) {
	if !textKey(runID) {
		return false, engine.WriteRefused(failure("invalid_argument", "run identity required"))
	}
	for _, key := range []string{want.ParentRunID, want.RootRunID, want.CallNodeID, want.TargetID, want.PrincipalID} {
		if !textKey(key) {
			return false, engine.WriteRefused(failure("invalid_argument", "exact incoming release facts are required"))
		}
	}
	state := &incomingReleaseFaultState{scope: s.scope, runID: runID, want: want}
	var changed bool
	err := s.db.WriteWithFaultProbe(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		changed, e = releaseIncomingRun(ctx, tx, s.scope, runID, want, &state.detail)
		state.applied = changed && e == nil
		return e
	}, state.probe)
	var disposition *engine.WriteError
	if err != nil && errors.As(err, &disposition) && disposition.Disposition == engine.WriteUnknown {
		return false, &RemoteRunIncomingReleaseError{RunID: runID, Requested: want, cause: err, state: state}
	}
	return changed, err
}

func releaseIncomingRun(ctx context.Context, tx *sql.Tx, scope, runID string, want model.IncomingRunRelease, detail *incomingReleaseChangeDetail) (bool, error) {
	var status, parent, root, call, target, principal string
	var released, cleanupFailed int
	err := tx.QueryRowContext(ctx, `SELECT status,parent_run_id,root_run_id,call_node_id,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released,cleanup_failed FROM runs WHERE scope=? AND id=?`, scope, runID).Scan(&status, &parent, &root, &call, &target, &principal, &released, &cleanupFailed)
	if errors.Is(err, sql.ErrNoRows) {
		// The hold cannot outlive its Run row: a pruned child has no hold left
		// to release, so an exact retry of a committed release is a no-op.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if target == "" {
		return false, failure("failed_precondition", "this Run carries no incoming hold")
	}
	if detail != nil {
		detail.validated = true
		detail.parent, detail.root, detail.call, detail.target, detail.principal, detail.released = parent, root, call, target, principal, released != 0
	}
	if want.ParentRunID != parent || want.RootRunID != root || want.CallNodeID != call || want.TargetID != target || want.PrincipalID != principal {
		return false, failure("failed_precondition", "incoming hold identity or caller facts disagree")
	}
	if released != 0 {
		return false, nil
	}
	// The local join is supplied by the Agent resource owner, never inferred
	// from the interrupted result. Remote descendants have their own durable
	// owners and must also be resolved before this hold can be released.
	switch model.RunStatus(status) {
	case model.RunSucceeded, model.RunFailed, model.RunStopped, model.RunCanceled:
	case model.RunInterrupted:
		if !want.LocalJoined {
			return false, failure("failed_precondition", "the interrupted child has no local join")
		}
		open, err := interruptedChildHasOpenLocal(ctx, tx, scope, runID, root)
		if err != nil {
			return false, err
		}
		if open {
			return false, failure("failed_precondition", "the interrupted child still owns a local Run")
		}
		pending, err := interruptedChildHasPendingRemote(ctx, tx, scope, runID, root)
		if err != nil {
			return false, err
		}
		if pending {
			return false, failure("failed_precondition", "the interrupted child still owns remote work")
		}
	default:
		return false, failure("failed_precondition", "the incoming child Run has no clean terminal disposition")
	}
	if cleanupFailed != 0 {
		return false, failure("failed_precondition", "the incoming child Run still reports failed cleanup")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE runs SET incoming_hold_released=1 WHERE scope=? AND id=?`, scope, runID); err != nil {
		return false, err
	}
	// Releasing unblocks this child's prune guards in the same transaction.
	var cut retentionChanges
	cut.touch(runID)
	return true, cut.finish(ctx, tx, scope)
}

func interruptedChildHasOpenLocal(ctx context.Context, tx *sql.Tx, scope, runID, root string) (bool, error) {
	const q = `WITH RECURSIVE descendants(id) AS (
 SELECT ?2 UNION
 SELECT r.id FROM runs r JOIN descendants parent ON r.parent_run_id=parent.id
 WHERE r.scope=?1 AND r.root_run_id=?3
)
SELECT EXISTS(SELECT 1 FROM descendants child JOIN runs r ON r.scope=?1 AND r.id=child.id
 WHERE child.id<>?2 AND r.status IN ('queued','running','stopping'))`
	var open bool
	err := tx.QueryRowContext(ctx, q, scope, runID, root).Scan(&open)
	return open, err
}

// Pending remote owners can belong to local descendants. Walk only the
// selected root's existing pending owners towards their recorded parents; a
// missing intermediate parent is unknown and cannot establish this join.
// An unrelated sibling with intact lineage does not block this child.
func interruptedChildHasPendingRemote(ctx context.Context, tx *sql.Tx, scope, runID, root string) (bool, error) {
	const q = `WITH RECURSIVE lineage(id,parent,depth) AS (
 SELECT r.id,r.parent_run_id,r.depth
 FROM run_remote_pending p LEFT JOIN runs r ON r.scope=p.scope AND r.id=p.owner_run_id
 WHERE p.scope=?1 AND p.root_run_id=?2
 UNION
 SELECT parent.id,parent.parent_run_id,parent.depth
 FROM lineage child JOIN runs parent ON parent.scope=?1 AND parent.id=child.parent
 WHERE child.id<>?3 AND child.parent<>?2 AND parent.root_run_id=?2 AND parent.depth=child.depth-1
)
SELECT EXISTS(SELECT 1 FROM lineage child
 WHERE child.id IS NULL OR child.id=?3 OR
 (child.id<>?2 AND child.id<>?3 AND child.parent<>?2 AND child.parent<>'' AND NOT EXISTS (
   SELECT 1 FROM runs parent WHERE parent.scope=?1 AND parent.id=child.parent AND parent.root_run_id=?2 AND parent.depth=child.depth-1
 )))`
	var pending bool
	err := tx.QueryRowContext(ctx, q, scope, root, runID).Scan(&pending)
	return pending, err
}

// RemoteRunIncomingReleaseError retains the requested run and presented facts
// of a hold release whose durable outcome is unknown.
type RemoteRunIncomingReleaseError struct {
	RunID     string
	Requested model.IncomingRunRelease
	cause     error
	state     *incomingReleaseFaultState
}

func (e *RemoteRunIncomingReleaseError) Error() string { return e.cause.Error() }
func (e *RemoteRunIncomingReleaseError) Unwrap() error { return e.cause }

// RemoteRunIncomingReleaseFact is an exact post-join observation of the
// child Run's hold state, not a COMMIT receipt.
type RemoteRunIncomingReleaseFact struct {
	RunID        string
	Requested    model.IncomingRunRelease
	Validated    bool
	Applied      bool
	NonCommitted bool
	RunPresent   bool
	HoldPresent  bool
	Released     bool
	Current      model.IncomingRunRelease
}

type incomingReleaseChangeDetail struct {
	validated bool
	parent    string
	root      string
	call      string
	target    string
	principal string
	released  bool
}

type incomingReleaseFaultState struct {
	scope   string
	runID   string
	want    model.IncomingRunRelease
	detail  incomingReleaseChangeDetail
	applied bool
	fact    RemoteRunIncomingReleaseFact
}

func (s *incomingReleaseFaultState) candidate() RemoteRunIncomingReleaseFact {
	return RemoteRunIncomingReleaseFact{RunID: s.runID, Requested: s.want, Validated: s.detail.validated, Applied: s.applied}
}

func (s *incomingReleaseFaultState) noCommitFact() RemoteRunIncomingReleaseFact {
	out := s.candidate()
	out.NonCommitted = true
	if out.Validated {
		out.RunPresent, out.HoldPresent, out.Released = true, s.detail.target != "", s.detail.released
		out.Current = model.IncomingRunRelease{ParentRunID: s.detail.parent, RootRunID: s.detail.root, CallNodeID: s.detail.call, TargetID: s.detail.target, PrincipalID: s.detail.principal}
	}
	return out
}

func (s *incomingReleaseFaultState) probe(ctx context.Context, tx *sql.Tx, possibleCommit bool) error {
	if !possibleCommit {
		s.fact = s.noCommitFact()
		return nil
	}
	out := s.candidate()
	if !out.Validated {
		s.fact = out
		return nil
	}
	var parent, root, call, target, principal string
	var released int
	err := tx.QueryRowContext(ctx, `SELECT parent_run_id,root_run_id,call_node_id,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released FROM runs WHERE scope=? AND id=?`, s.scope, s.runID).Scan(&parent, &root, &call, &target, &principal, &released)
	if errors.Is(err, sql.ErrNoRows) {
		s.fact = out
		return nil
	}
	if err != nil {
		return err
	}
	out.RunPresent = true
	out.HoldPresent = target != ""
	out.Released = released != 0
	out.Current = model.IncomingRunRelease{ParentRunID: parent, RootRunID: root, CallNodeID: call, TargetID: target, PrincipalID: principal}
	s.fact = out
	return nil
}

func (e *RemoteRunIncomingReleaseError) WaitRevalidation(ctx context.Context) (RemoteRunIncomingReleaseFact, error) {
	var disposition *engine.WriteError
	if !errors.As(e.cause, &disposition) {
		return RemoteRunIncomingReleaseFact{}, failure("unavailable", "original writer disposition unavailable")
	}
	if err := disposition.WaitSettlement(ctx); err != nil {
		if ctx.Err() == nil && disposition.NonCommittedAfterJoin() {
			return e.state.noCommitFact(), nil
		}
		return RemoteRunIncomingReleaseFact{}, err
	}
	return e.state.fact, nil
}
