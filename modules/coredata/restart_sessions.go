package coredata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The predecessor's failed-cleanup fact was read with the Session itself. Each
// remaining unsafe subset is independently indexed by the exact Session. The
// pending relation has its own Session selector so a real link is checked
// even when its owner's counter is inconsistent.
const sessionUnsafeActivation = `SELECT CASE
 WHEN EXISTS (SELECT 1 FROM runs INDEXED BY runs_session_instance_open WHERE scope=?1 AND session_id=?2 AND session_id<>'' AND status IN ('queued','running','stopping')) THEN 1
 WHEN EXISTS (SELECT 1 FROM runs INDEXED BY runs_session_pending WHERE scope=?1 AND session_id=?2 AND session_id<>'' AND pending_remote_count>0) THEN 1
 WHEN EXISTS (SELECT 1 FROM run_remote_pending INDEXED BY run_remote_pending_session WHERE scope=?1 AND owner_session_id=?2 AND owner_session_id<>'') THEN 1
 ELSE 0 END`

// QueueRestartSession durably reserves a successor without acquiring the live
// target slot. An existing successor is returned for exact caller comparison.
func (s *Store) QueueRestartSession(ctx context.Context, q model.NewSession) (session model.Session, created bool, err error) {
	if !textKey(q.ID) || !textKey(q.TargetID) || !textKey(q.PredecessorSessionID) || q.ID == q.PredecessorSessionID || !textKey(q.ExperimentResourceID) || !textKey(q.ExperimentCommitID) || !textKey(q.RunMode) || len(q.RunMode) > 64 || len(q.ExperimentName) > 256 || !utf8.ValidString(q.ExperimentName) || strings.IndexFunc(q.ExperimentName, unicode.IsControl) >= 0 {
		return model.Session{}, false, failure("invalid_argument", "invalid queued Session identity or frozen source")
	}
	err = s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		previous, e := selectSession(ctx, tx, s.scope, "id=?", q.ID)
		if e == nil {
			session = previous
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		previous, e = selectSession(ctx, tx, s.scope, "target_id=? AND predecessor_session_id=? AND predecessor_session_id<>''", q.TargetID, q.PredecessorSessionID)
		if e == nil {
			session = previous
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		old, e := selectSession(ctx, tx, s.scope, "id=?", q.PredecessorSessionID)
		if errors.Is(e, sql.ErrNoRows) {
			return failure("not_found", "predecessor Session not found")
		}
		if e != nil {
			return e
		}
		if old.TargetID != q.TargetID || old.ExperimentResourceID != q.ExperimentResourceID || old.Status != model.SessionOpen {
			return failure("failed_precondition", "predecessor Session is not the open target owner")
		}
		var other bool
		if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE scope=? AND target_id=? AND status='queued')", s.scope, q.TargetID).Scan(&other); e != nil {
			return e
		}
		if other {
			return failure("conflict", "target already has a queued Session")
		}
		session = model.Session{ID: q.ID, TargetID: q.TargetID, PredecessorSessionID: q.PredecessorSessionID, ExperimentResourceID: q.ExperimentResourceID, ExperimentCommitID: q.ExperimentCommitID, ExperimentName: q.ExperimentName, RunMode: q.RunMode, Status: model.SessionQueued, OpenedAt: nowOr(q.At), Revision: 1}
		size := sessionBytes(session)
		if e = reserve(ctx, tx, s.scope, 1, size); e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, "INSERT INTO sessions(scope,id,target_id,predecessor_session_id,experiment_resource_id,experiment_commit_id,run_mode,experiment_name,root_run_id,status,stop_intent_json,outcome,error_json,opened_at,closed_at,revision,bytes,failed_cleanup) VALUES(?,?,?,?,?,?,?,?,'','queued',x'','',x'',?,0,1,?,(SELECT EXISTS(SELECT 1 FROM runs INDEXED BY runs_session_failed WHERE scope=? AND session_id=? AND session_id<>'' AND cleanup_failed>0)))", s.scope, session.ID, session.TargetID, session.PredecessorSessionID, session.ExperimentResourceID, session.ExperimentCommitID, session.RunMode, session.ExperimentName, nanos(session.OpenedAt), size, s.scope, session.ID)
		if e == nil {
			var cut retentionChanges
			if e = cut.session(ctx, tx, s.scope, session.TargetID, session.PredecessorSessionID, true); e != nil {
				return e
			}
			if e = cut.finish(ctx, tx, s.scope); e != nil {
				return e
			}
			created = true
		}
		return e
	})
	if err != nil {
		return model.Session{}, false, err
	}
	return session, created, nil
}

// ActivateRestartSession promotes the same queued identity after the exact old
// Session closed cleanly and all of its durable remote and cleanup facts joined.
func (s *Store) ActivateRestartSession(ctx context.Context, id string, policy model.RunRetentionPolicy) (model.SessionMutationResult, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return model.SessionMutationResult{}, err
	}
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.SessionMutationResult, error) {
		next, e := getSession(ctx, tx, scope, id)
		if e != nil {
			return model.SessionMutationResult{}, e
		}
		if next.Status != model.SessionQueued {
			return model.SessionMutationResult{Session: next}, nil
		}
		old, failedCleanup, e := getSessionWithCleanup(ctx, tx, scope, next.PredecessorSessionID)
		if e != nil {
			return model.SessionMutationResult{}, e
		}
		if old.TargetID != next.TargetID || old.Status != model.SessionClosed || old.Outcome != model.SessionStopped {
			return model.SessionMutationResult{}, failure("failed_precondition", "predecessor Session did not close cleanly")
		}
		if failedCleanup {
			return model.SessionMutationResult{}, failure("failed_precondition", "predecessor Session owns unresolved or failed Run cleanup")
		}
		var unsafe bool
		if e = tx.QueryRowContext(ctx, sessionUnsafeActivation, scope, old.ID).Scan(&unsafe); e != nil {
			return model.SessionMutationResult{}, e
		}
		if unsafe {
			return model.SessionMutationResult{}, failure("failed_precondition", "predecessor Session owns unresolved or failed Run cleanup")
		}
		var live bool
		if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE scope=? AND target_id=? AND status IN ('open','stopping'))", scope, next.TargetID).Scan(&live); e != nil {
			return model.SessionMutationResult{}, e
		}
		if live {
			return model.SessionMutationResult{}, failure("conflict", "target still has a live Session")
		}
		if _, e = tx.ExecContext(ctx, "UPDATE sessions SET status='open',revision=revision+1 WHERE scope=? AND id=? AND status='queued'", scope, id); e != nil {
			return model.SessionMutationResult{}, e
		}
		var cut retentionChanges
		if e = cut.session(ctx, tx, scope, next.TargetID, next.PredecessorSessionID, false); e != nil {
			return model.SessionMutationResult{}, e
		}
		if e = cut.finish(ctx, tx, scope); e != nil {
			return model.SessionMutationResult{}, e
		}
		effect, e := retentionEffect(ctx, tx, scope, &cut, policy)
		if e != nil {
			return model.SessionMutationResult{}, e
		}
		next, e = getSession(ctx, tx, scope, id)
		return model.SessionMutationResult{Session: next, Retention: effect}, e
	})
}
