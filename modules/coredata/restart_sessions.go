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
		_, e = tx.ExecContext(ctx, "INSERT INTO sessions(scope,id,target_id,predecessor_session_id,experiment_resource_id,experiment_commit_id,run_mode,experiment_name,root_run_id,status,stop_intent_json,outcome,error_json,opened_at,closed_at,revision,bytes) VALUES(?,?,?,?,?,?,?,?,'','queued',x'','',x'',?,0,1,?)", s.scope, session.ID, session.TargetID, session.PredecessorSessionID, session.ExperimentResourceID, session.ExperimentCommitID, session.RunMode, session.ExperimentName, nanos(session.OpenedAt), size)
		if e == nil {
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
func (s *Store) ActivateRestartSession(ctx context.Context, id string) (model.Session, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Session, error) {
		next, e := getSession(ctx, tx, scope, id)
		if e != nil {
			return next, e
		}
		if next.Status != model.SessionQueued {
			return next, nil
		}
		old, e := getSession(ctx, tx, scope, next.PredecessorSessionID)
		if e != nil {
			return model.Session{}, e
		}
		if old.TargetID != next.TargetID || old.Status != model.SessionClosed || old.Outcome != model.SessionStopped {
			return model.Session{}, failure("failed_precondition", "predecessor Session did not close cleanly")
		}
		var unsafe bool
		if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM runs r WHERE r.scope=? AND r.session_id=? AND (r.pending_remote_count>0 OR r.cleanup_failed>0 OR r.finished_at=0 OR EXISTS (SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.owner_run_id=r.id)))", scope, old.ID).Scan(&unsafe); e != nil {
			return model.Session{}, e
		}
		if unsafe {
			return model.Session{}, failure("failed_precondition", "predecessor Session owns unresolved or failed Run cleanup")
		}
		var live bool
		if e = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM sessions WHERE scope=? AND target_id=? AND status IN ('open','stopping'))", scope, next.TargetID).Scan(&live); e != nil {
			return model.Session{}, e
		}
		if live {
			return model.Session{}, failure("conflict", "target still has a live Session")
		}
		if _, e = tx.ExecContext(ctx, "UPDATE sessions SET status='open',revision=revision+1 WHERE scope=? AND id=? AND status='queued'", scope, id); e != nil {
			return model.Session{}, e
		}
		return getSession(ctx, tx, scope, id)
	})
}
