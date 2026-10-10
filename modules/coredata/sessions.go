package coredata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const sessionColumns = "id,target_id,experiment_resource_id,experiment_commit_id,root_run_id,status,stop_intent_json,opened_at,closed_at,revision"

func scanSession(row scanner) (model.Session, error) {
	var v model.Session
	var status string
	var intent []byte
	var opened, closed int64
	if err := row.Scan(&v.ID, &v.TargetID, &v.ExperimentResourceID, &v.ExperimentCommitID, &v.RootRunID, &status, &intent, &opened, &closed, &v.Revision); err != nil {
		return v, err
	}
	v.Status, v.StopIntent = model.SessionStatus(status), rawOrNil(intent)
	v.OpenedAt, v.ClosedAt = instant(opened), instant(closed)
	return v, nil
}

func selectSession(ctx context.Context, tx *sql.Tx, scope, where string, args ...any) (model.Session, error) {
	return scanSession(tx.QueryRowContext(ctx, "SELECT "+sessionColumns+" FROM sessions WHERE scope=? AND "+where, append([]any{scope}, args...)...))
}

func sessionBytes(v model.Session) int64 {
	return int64(rowOverhead + len(v.ID) + len(v.TargetID) + len(v.ExperimentResourceID) + len(v.ExperimentCommitID) + len(v.RootRunID) + len(v.StopIntent))
}

func openSession(ctx context.Context, tx *sql.Tx, scope string, q model.NewSession) (model.Session, bool, error) {
	if !textKey(q.ID) || !textKey(q.TargetID) || !textKey(q.ExperimentResourceID) || !textKey(q.ExperimentCommitID) {
		return model.Session{}, false, failure("invalid_argument", "session, target, experiment resource and commit identities are required")
	}
	if err := optionalKey("root run", q.RootRunID); err != nil {
		return model.Session{}, false, err
	}
	existing, err := selectSession(ctx, tx, scope, "id=?", q.ID)
	if err == nil {
		// Opening the same Session again is a replay of the same start.
		if existing.TargetID != q.TargetID || existing.ExperimentCommitID != q.ExperimentCommitID {
			return model.Session{}, false, failure("conflict", "session identity already used for another target or experiment commit")
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.Session{}, false, err
	}
	var live string
	switch err = tx.QueryRowContext(ctx, "SELECT id FROM sessions WHERE scope=? AND target_id=? AND status IN ('open','stopping')", scope, q.TargetID).Scan(&live); {
	case err == nil:
		return model.Session{}, false, failure("conflict", "target already has a live session "+live)
	case !errors.Is(err, sql.ErrNoRows):
		return model.Session{}, false, err
	}
	v := model.Session{ID: q.ID, TargetID: q.TargetID, ExperimentResourceID: q.ExperimentResourceID, ExperimentCommitID: q.ExperimentCommitID, RootRunID: q.RootRunID,
		Status: model.SessionOpen, OpenedAt: nowOr(q.At), Revision: 1}
	bytes := sessionBytes(v)
	if err = reserve(ctx, tx, scope, 1, bytes); err != nil {
		return model.Session{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO sessions VALUES(?,?,?,?,?,?,?,x'',?,0,1,?)", scope, v.ID, v.TargetID, v.ExperimentResourceID, v.ExperimentCommitID, v.RootRunID, string(v.Status), nanos(v.OpenedAt), bytes)
	return v, true, err
}

func requestStop(ctx context.Context, tx *sql.Tx, scope, id string, intent []byte) (model.Session, error) {
	v, err := getSession(ctx, tx, scope, id)
	if err != nil {
		return v, err
	}
	// The intent is immutable: only a Session that is still open takes one, and
	// repeating the request returns what was recorded.
	if v.Status != model.SessionOpen {
		return v, nil
	}
	if err = charge(ctx, tx, scope, 0, int64(len(intent))); err != nil {
		return v, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sessions SET status='stopping',stop_intent_json=?,revision=revision+1,bytes=bytes+? WHERE scope=? AND id=? AND status='open'", intent, len(intent), scope, id); err != nil {
		return v, err
	}
	return getSession(ctx, tx, scope, id)
}

func closeSession(ctx context.Context, tx *sql.Tx, scope, id string, at time.Time) (model.Session, error) {
	v, err := getSession(ctx, tx, scope, id)
	if err != nil || !v.Status.Live() {
		return v, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE sessions SET status='closed',closed_at=?,revision=revision+1 WHERE scope=? AND id=? AND status IN ('open','stopping')", nanos(nowOr(at)), scope, id); err != nil {
		return v, err
	}
	return getSession(ctx, tx, scope, id)
}

func getSession(ctx context.Context, tx *sql.Tx, scope, id string) (model.Session, error) {
	if !textKey(id) {
		return model.Session{}, failure("invalid_argument", "session identity required")
	}
	v, err := selectSession(ctx, tx, scope, "id=?", id)
	if errors.Is(err, sql.ErrNoRows) {
		return v, failure("not_found", "session not found")
	}
	return v, err
}

func listSessions(ctx context.Context, tx *sql.Tx, scope string, f model.SessionFilter) (model.SessionPage, error) {
	limit, err := pageLimit(f.Limit)
	if err != nil {
		return model.SessionPage{}, err
	}
	where, args := []string{"scope=?"}, []any{scope}
	if f.TargetID != "" {
		where, args = append(where, "target_id=?"), append(args, f.TargetID)
	}
	if f.ExperimentResourceID != "" {
		where, args = append(where, "experiment_resource_id=?"), append(args, f.ExperimentResourceID)
	}
	if len(f.Statuses) > 0 {
		marks := make([]string, len(f.Statuses))
		for i, s := range f.Statuses {
			if s != model.SessionOpen && s != model.SessionStopping && s != model.SessionClosed && s != model.SessionInterrupted {
				return model.SessionPage{}, failure("invalid_argument", "unknown session status")
			}
			marks[i], args = "?", append(args, string(s))
		}
		where = append(where, "status IN ("+strings.Join(marks, ",")+")")
	}
	if f.Cursor != "" {
		at, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return model.SessionPage{}, err
		}
		where, args = append(where, "(opened_at,id)<(?,?)"), append(args, at, id)
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+sessionColumns+" FROM sessions WHERE "+strings.Join(where, " AND ")+" ORDER BY opened_at DESC,id DESC LIMIT ?", append(args, limit+1)...)
	if err != nil {
		return model.SessionPage{}, err
	}
	defer rows.Close()
	var page model.SessionPage
	for rows.Next() {
		v, err := scanSession(rows)
		if err != nil {
			return page, err
		}
		if len(page.Sessions) == limit {
			last := page.Sessions[limit-1]
			page.Next = encodeCursor(nanos(last.OpenedAt), last.ID)
			break
		}
		page.Sessions = append(page.Sessions, v)
	}
	return page, rows.Err()
}

func closeOpenSessions(ctx context.Context, tx *sql.Tx, scope string, at time.Time) (int, error) {
	result, err := tx.ExecContext(ctx, "UPDATE sessions SET status='interrupted',closed_at=?,revision=revision+1 WHERE scope=? AND status IN ('open','stopping')", nanos(nowOr(at)), scope)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}

// OpenSession durably opens a Session. Only one Session per target is live: a
// second one on the same target is a conflict, while opening the same Session
// again returns it with created false.
func (s *Store) OpenSession(ctx context.Context, q model.NewSession) (session model.Session, created bool, err error) {
	err = s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) (e error) {
		session, created, e = openSession(ctx, tx, s.scope, q)
		return e
	})
	if err != nil {
		return model.Session{}, false, err
	}
	return session, created, nil
}

// RequestStop durably records the operator's stop intent (a JSON object that
// carries its own facts, such as who asked and when) and moves an open Session
// to stopping. The intent is immutable: later calls return the Session as it is.
func (s *Store) RequestStop(ctx context.Context, id string, intent []byte) (model.Session, error) {
	stored, err := jsonField("stop intent", intent, model.MaxStopIntentBytes, true, true)
	if err != nil {
		return model.Session{}, err
	}
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Session, error) {
		return requestStop(ctx, tx, scope, id, stored)
	})
}

// CloseSession durably closes a live Session.
func (s *Store) CloseSession(ctx context.Context, id string, at time.Time) (model.Session, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Session, error) {
		return closeSession(ctx, tx, scope, id, at)
	})
}

func (s *Store) GetSession(ctx context.Context, id string) (model.Session, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Session, error) {
		return getSession(ctx, tx, scope, id)
	})
}

// ListSessions pages through Sessions newest first.
func (s *Store) ListSessions(ctx context.Context, f model.SessionFilter) (model.SessionPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.SessionPage, error) {
		return listSessions(ctx, tx, scope, f)
	})
}

// CloseOpenSessions marks every Session a stopped Core left live as interrupted.
// Core calls it at boot, before it opens any Session.
func (s *Store) CloseOpenSessions(ctx context.Context, at time.Time) (int, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (int, error) {
		return closeOpenSessions(ctx, tx, scope, at)
	})
}
