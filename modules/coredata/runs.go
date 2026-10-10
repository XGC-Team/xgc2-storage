package coredata

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func instant(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func nowOr(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

func optionalKey(name, value string) error {
	if value != "" && !textKey(value) {
		return failure("invalid_argument", name+" must be a bounded identifier")
	}
	return nil
}

// jsonField validates a JSON column: valid, within max bytes and, when
// required, a JSON object. An empty optional value is stored as empty.
func jsonField(name string, raw json.RawMessage, max int, required, object bool) ([]byte, error) {
	if len(raw) == 0 {
		if required {
			return nil, failure("invalid_argument", name+" is required")
		}
		return []byte{}, nil
	}
	if len(raw) > max {
		return nil, failure("resource_exhausted", name+" exceeds its byte limit")
	}
	if !json.Valid(raw) {
		return nil, failure("invalid_argument", name+" must be valid JSON")
	}
	if object && !isObject(raw) {
		return nil, failure("invalid_argument", name+" must be a JSON object")
	}
	return raw, nil
}

func isObject(raw []byte) bool {
	for _, c := range raw {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		}
		return c == '{'
	}
	return false
}

func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(append([]byte(nil), b...))
}

// A cursor is the sort position of the last row of a page.
func encodeCursor(at int64, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(at, 10) + "\x00" + id))
}

func decodeCursor(cursor string) (int64, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err == nil {
		if at, id, ok := strings.Cut(string(raw), "\x00"); ok {
			if n, e := strconv.ParseInt(at, 10, 64); e == nil && id != "" {
				return n, id, nil
			}
		}
	}
	return 0, "", failure("invalid_argument", "invalid page cursor")
}

func pageLimit(limit int) (int, error) {
	if limit < 0 || limit > model.MaxPageSize {
		return 0, failure("invalid_argument", "page limit outside 1.."+strconv.Itoa(model.MaxPageSize))
	}
	if limit == 0 {
		return 100, nil
	}
	return limit, nil
}

type scanner interface{ Scan(dest ...any) error }

const runColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,status,termination,error_json,result_json,nodes_json,cleanup_errors_json,created_at,started_at,finished_at,revision"

// runSummaryColumns replaces the large JSON columns by empty values.
const runSummaryColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,workflow_resource_id,workflow_commit_id,definition_digest,action_id,x'',x'',status,termination,x'',x'',x'',x'',created_at,started_at,finished_at,revision"

func scanRun(row scanner) (model.Run, error) {
	var r model.Run
	var status string
	var inputs, trigger, errored, result, nodes, cleanup []byte
	var created, started, finished int64
	if err := row.Scan(&r.ID, &r.TargetID, &r.RootRunID, &r.ParentRunID, &r.CallNodeID, &r.Depth, &r.SessionID, &r.IdempotencyKey, &r.WorkflowResourceID, &r.WorkflowCommitID, &r.DefinitionDigest, &r.ActionID, &inputs, &trigger, &status, &r.Termination, &errored, &result, &nodes, &cleanup, &created, &started, &finished, &r.Revision); err != nil {
		return r, err
	}
	r.Status = model.RunStatus(status)
	r.Inputs, r.Trigger, r.Error, r.Result, r.Nodes, r.CleanupErrors = rawOrNil(inputs), rawOrNil(trigger), rawOrNil(errored), rawOrNil(result), rawOrNil(nodes), rawOrNil(cleanup)
	r.CreatedAt, r.StartedAt, r.FinishedAt = instant(created), instant(started), instant(finished)
	return r, nil
}

func selectRun(ctx context.Context, tx *sql.Tx, scope, where string, args ...any) (model.Run, error) {
	return scanRun(tx.QueryRowContext(ctx, "SELECT "+runColumns+" FROM runs WHERE scope=? AND "+where, append([]any{scope}, args...)...))
}

// byKey selects the Run of an idempotency key. The second term states the
// condition of the partial index, which the planner cannot infer from a parameter.
const byKey = "idempotency_key=? AND idempotency_key<>''"

// rowOverhead is the fixed cost charged to the live quota for every row.
const rowOverhead = 256

func runBytes(r model.Run) int64 {
	n := rowOverhead
	for _, s := range []string{r.ID, r.TargetID, r.RootRunID, r.ParentRunID, r.CallNodeID, r.SessionID, r.IdempotencyKey, r.WorkflowResourceID, r.WorkflowCommitID, r.DefinitionDigest, r.ActionID, r.Termination} {
		n += len(s)
	}
	return int64(n + len(r.Inputs) + len(r.Trigger) + len(r.Error) + len(r.Result) + len(r.Nodes) + len(r.CleanupErrors))
}

func validateNewRun(q *model.NewRun) error {
	if !textKey(q.ID) || !textKey(q.TargetID) || !textKey(q.WorkflowResourceID) || !textKey(q.WorkflowCommitID) || !textKey(q.DefinitionDigest) || !textKey(q.ActionID) {
		return failure("invalid_argument", "run, target, workflow resource, commit, definition digest and action identities are required")
	}
	for name, value := range map[string]string{"parent run": q.ParentRunID, "call node": q.CallNodeID, "session": q.SessionID, "idempotency key": q.IdempotencyKey} {
		if err := optionalKey(name, value); err != nil {
			return err
		}
	}
	if q.RootRunID == "" {
		q.RootRunID = q.ID
	}
	if !textKey(q.RootRunID) || q.Depth < 0 || q.Depth > model.MaxRunDepth {
		return failure("invalid_argument", "root run and a depth within 0.."+strconv.Itoa(model.MaxRunDepth)+" required")
	}
	// A root Run is its own root at depth 0; a child has a parent, another root and depth > 0.
	if (q.ParentRunID == "") != (q.RootRunID == q.ID) || (q.ParentRunID == "") != (q.Depth == 0) {
		return failure("invalid_argument", "run lineage is inconsistent: a root has no parent, is its own root and has depth 0")
	}
	switch q.Status {
	case "":
		q.Status = model.RunQueued
	case model.RunQueued, model.RunRunning:
	default:
		return failure("invalid_argument", "a Run is accepted as queued or running")
	}
	return nil
}

func createRun(ctx context.Context, tx *sql.Tx, scope string, q model.NewRun) (model.Run, bool, error) {
	if err := validateNewRun(&q); err != nil {
		return model.Run{}, false, err
	}
	inputs, err := jsonField("inputs", q.Inputs, model.MaxRunInputsBytes, true, true)
	if err != nil {
		return model.Run{}, false, err
	}
	trigger, err := jsonField("trigger", q.Trigger, model.MaxRunTriggerBytes, true, true)
	if err != nil {
		return model.Run{}, false, err
	}
	if q.IdempotencyKey != "" {
		existing, err := selectRun(ctx, tx, scope, byKey, q.IdempotencyKey)
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return model.Run{}, false, err
		}
	}
	var taken bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM runs WHERE scope=? AND id=?)", scope, q.ID).Scan(&taken); err != nil {
		return model.Run{}, false, err
	}
	if taken {
		return model.Run{}, false, failure("conflict", "run identity already exists")
	}
	at := nowOr(q.At)
	r := model.Run{ID: q.ID, TargetID: q.TargetID, RootRunID: q.RootRunID, ParentRunID: q.ParentRunID, CallNodeID: q.CallNodeID, Depth: q.Depth, SessionID: q.SessionID,
		IdempotencyKey: q.IdempotencyKey, WorkflowResourceID: q.WorkflowResourceID, WorkflowCommitID: q.WorkflowCommitID, DefinitionDigest: q.DefinitionDigest, ActionID: q.ActionID,
		Inputs: inputs, Trigger: trigger, Status: q.Status, CreatedAt: at, Revision: 1}
	if r.Status == model.RunRunning {
		r.StartedAt = at
	}
	bytes := runBytes(r)
	if err = reserve(ctx, tx, scope, 1, bytes); err != nil {
		return model.Run{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,x'',x'',x'',x'',?,?,0,1,?)",
		scope, r.ID, r.TargetID, r.RootRunID, r.ParentRunID, r.CallNodeID, r.Depth, r.SessionID, r.IdempotencyKey, r.WorkflowResourceID, r.WorkflowCommitID, r.DefinitionDigest, r.ActionID,
		[]byte(inputs), []byte(trigger), string(r.Status), "", nanos(r.CreatedAt), nanos(r.StartedAt), bytes)
	return r, true, err
}

func updateRunStatuses(ctx context.Context, tx *sql.Tx, scope string, updates []model.RunStatusUpdate) error {
	if len(updates) == 0 || len(updates) > model.MaxPageSize {
		return failure("invalid_argument", "1.."+strconv.Itoa(model.MaxPageSize)+" run status updates required")
	}
	for _, u := range updates {
		if !textKey(u.ID) || (u.Status != model.RunRunning && u.Status != model.RunStopping) {
			return failure("invalid_argument", "a run identity and the status running or stopping are required; finish a run with FinishRun")
		}
		var current string
		err := tx.QueryRowContext(ctx, "SELECT status FROM runs WHERE scope=? AND id=?", scope, u.ID).Scan(&current)
		if errors.Is(err, sql.ErrNoRows) {
			return failure("not_found", "run not found")
		}
		if err != nil {
			return err
		}
		from := model.RunStatus(current)
		if from == u.Status {
			continue
		}
		// queued -> running | stopping, running -> stopping; nothing returns.
		if !(from == model.RunQueued || from == model.RunRunning && u.Status == model.RunStopping) {
			return failure("failed_precondition", fmt.Sprintf("run %s is %s and cannot become %s", u.ID, from, u.Status))
		}
		at := nowOr(u.At)
		if _, err = tx.ExecContext(ctx, "UPDATE runs SET status=?,started_at=CASE WHEN ?='running' THEN ? ELSE started_at END,revision=revision+1 WHERE scope=? AND id=? AND status=?", string(u.Status), string(u.Status), nanos(at), scope, u.ID, current); err != nil {
			return err
		}
	}
	return nil
}

func finishRun(ctx context.Context, tx *sql.Tx, scope string, f model.RunFinish) (model.Run, error) {
	if !textKey(f.ID) || !f.Status.Terminal() || len(f.Termination) > 255 || strings.ContainsAny(f.Termination, "\x00\r\n") {
		return model.Run{}, failure("invalid_argument", "a run identity, a terminal status and a short termination reason are required")
	}
	errored, err := jsonField("error", f.Error, model.MaxRunErrorBytes, false, false)
	if err != nil {
		return model.Run{}, err
	}
	result, err := jsonField("result", f.Result, model.MaxRunResultBytes, false, false)
	if err != nil {
		return model.Run{}, err
	}
	nodes, err := jsonField("nodes", f.Nodes, model.MaxRunNodesBytes, false, false)
	if err != nil {
		return model.Run{}, err
	}
	cleanup, err := jsonField("cleanup errors", f.CleanupErrors, model.MaxRunCleanupErrorsBytes, false, false)
	if err != nil {
		return model.Run{}, err
	}
	var current string
	err = tx.QueryRowContext(ctx, "SELECT status FROM runs WHERE scope=? AND id=?", scope, f.ID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	if err != nil {
		return model.Run{}, err
	}
	if !model.RunStatus(current).Open() {
		return model.Run{}, failure("failed_precondition", "run already finished")
	}
	// An open Run carries none of these columns, so the row only grows by them.
	grown := int64(len(f.Termination) + len(errored) + len(result) + len(nodes) + len(cleanup))
	if err = charge(ctx, tx, scope, 0, grown); err != nil {
		return model.Run{}, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE runs SET status=?,termination=?,error_json=?,result_json=?,nodes_json=?,cleanup_errors_json=?,finished_at=?,revision=revision+1,bytes=bytes+? WHERE scope=? AND id=? AND status=?",
		string(f.Status), f.Termination, []byte(errored), []byte(result), []byte(nodes), []byte(cleanup), nanos(nowOr(f.At)), grown, scope, f.ID, current); err != nil {
		return model.Run{}, err
	}
	return selectRun(ctx, tx, scope, "id=?", f.ID)
}

func getRun(ctx context.Context, tx *sql.Tx, scope, id string) (model.Run, error) {
	if !textKey(id) {
		return model.Run{}, failure("invalid_argument", "run identity required")
	}
	r, err := selectRun(ctx, tx, scope, "id=?", id)
	if errors.Is(err, sql.ErrNoRows) {
		return r, failure("not_found", "run not found")
	}
	return r, err
}

func findRunByKey(ctx context.Context, tx *sql.Tx, scope, key string) (model.Run, bool, error) {
	if !textKey(key) {
		return model.Run{}, false, failure("invalid_argument", "idempotency key required")
	}
	r, err := selectRun(ctx, tx, scope, byKey, key)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, false, nil
	}
	return r, err == nil, err
}

// runQuery builds the statement of one page of Runs. Every term is a fixed
// column comparison with a bound value; the filter never reaches the SQL text.
func runQuery(scope string, f model.RunFilter) (query string, args []any, limit int, err error) {
	if limit, err = pageLimit(f.Limit); err != nil {
		return "", nil, 0, err
	}
	where, args := []string{"scope=?"}, []any{scope}
	for column, value := range map[string]string{"target_id": f.TargetID, "root_run_id": f.RootRunID, "session_id": f.SessionID, "workflow_resource_id": f.WorkflowResourceID} {
		if value == "" {
			continue
		}
		if !textKey(value) {
			return "", nil, 0, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		where, args = append(where, column+"=?"), append(args, value)
		if column == "session_id" {
			// The condition of the partial index on session_id.
			where = append(where, "session_id<>''")
		}
	}
	if len(f.Statuses) > 0 {
		marks, open := make([]string, len(f.Statuses)), true
		for i, s := range f.Statuses {
			if !s.Open() && !s.Terminal() {
				return "", nil, 0, failure("invalid_argument", "unknown run status")
			}
			open = open && s.Open()
			marks[i], args = "?", append(args, string(s))
		}
		where = append(where, "status IN ("+strings.Join(marks, ",")+")")
		if open {
			// The condition of the partial index on open Runs, which makes
			// "what is running" independent of the length of the history.
			where = append(where, "status IN ('queued','running','stopping')")
		}
	}
	if !f.Since.IsZero() {
		where, args = append(where, "created_at>=?"), append(args, nanos(f.Since))
	}
	if !f.Until.IsZero() {
		where, args = append(where, "created_at<?"), append(args, nanos(f.Until))
	}
	if f.Cursor != "" {
		at, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return "", nil, 0, err
		}
		where, args = append(where, "(created_at,id)<(?,?)"), append(args, at, id)
	}
	columns := runColumns
	if f.OmitPayloads {
		columns = runSummaryColumns
	}
	return "SELECT " + columns + " FROM runs WHERE " + strings.Join(where, " AND ") + " ORDER BY created_at DESC,id DESC LIMIT ?", append(args, limit+1), limit, nil
}

func listRuns(ctx context.Context, tx *sql.Tx, scope string, f model.RunFilter) (model.RunPage, error) {
	query, args, limit, err := runQuery(scope, f)
	if err != nil {
		return model.RunPage{}, err
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return model.RunPage{}, err
	}
	defer rows.Close()
	var page model.RunPage
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return page, err
		}
		if len(page.Runs) == limit {
			last := page.Runs[limit-1]
			page.Next = encodeCursor(nanos(last.CreatedAt), last.ID)
			break
		}
		page.Runs = append(page.Runs, r)
	}
	return page, rows.Err()
}

// The statements below are fixed text, so that the plan test can run them as written.
const (
	interruptOpen = "UPDATE runs SET status='interrupted',termination='interrupted',finished_at=?,revision=revision+1,bytes=bytes+? WHERE scope=? AND status IN ('queued','running','stopping')"

	// A finished Run is only eligible for pruning when its root is finished too.
	eligibleRuns = "scope=? AND finished_at>0 AND NOT EXISTS (SELECT 1 FROM runs p WHERE p.scope=r.scope AND p.id=r.root_run_id AND p.finished_at=0)"
	pruneByAge   = "SELECT id,bytes FROM runs r WHERE " + eligibleRuns + " AND finished_at<? ORDER BY finished_at,id LIMIT ?"
	pruneByCount = "SELECT id,bytes FROM runs r WHERE " + eligibleRuns + " ORDER BY finished_at DESC,id DESC LIMIT ? OFFSET ?"
)

func interruptOpenRuns(ctx context.Context, tx *sql.Tx, scope string, at time.Time) (int, error) {
	result, err := tx.ExecContext(ctx, interruptOpen, nanos(nowOr(at)), len("interrupted"), scope)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return 0, err
	}
	return int(n), charge(ctx, tx, scope, 0, n*int64(len("interrupted")))
}

func pruneRuns(ctx context.Context, tx *sql.Tx, scope string, r model.RunRetention) (int, error) {
	limit := r.Limit
	if limit == 0 {
		limit = 1000
	}
	if limit < 0 || limit > 10000 || r.KeepNewest < 0 || (r.OlderThan.IsZero() && r.KeepNewest == 0) {
		return 0, failure("invalid_argument", "an age or count limit and a limit within 1..10000 are required")
	}
	type victim struct {
		id    string
		bytes int64
	}
	var victims []victim
	seen := map[string]bool{}
	collect := func(query string, args ...any) error {
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var v victim
			if err = rows.Scan(&v.id, &v.bytes); err != nil {
				return err
			}
			if !seen[v.id] && len(victims) < limit {
				seen[v.id] = true
				victims = append(victims, v)
			}
		}
		return rows.Err()
	}
	if !r.OlderThan.IsZero() {
		if err := collect(pruneByAge, scope, nanos(r.OlderThan), limit); err != nil {
			return 0, err
		}
	}
	if r.KeepNewest > 0 {
		if err := collect(pruneByCount, scope, limit, r.KeepNewest); err != nil {
			return 0, err
		}
	}
	var freed int64
	for _, v := range victims {
		if _, err := tx.ExecContext(ctx, "DELETE FROM runs WHERE scope=? AND id=?", scope, v.id); err != nil {
			return 0, err
		}
		freed += v.bytes
	}
	if len(victims) > 0 {
		if err := release(ctx, tx, scope, int64(len(victims)), freed); err != nil {
			return 0, err
		}
	}
	return len(victims), nil
}

// Runs

// CreateRun durably accepts a Run. With an idempotency key, a repeated call
// returns the stored Run and created is false, so a replayed start finds its
// Run instead of making another.
func (s *Store) CreateRun(ctx context.Context, q model.NewRun) (run model.Run, created bool, err error) {
	err = s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) (e error) {
		run, created, e = createRun(ctx, tx, s.scope, q)
		return e
	})
	if err != nil {
		return model.Run{}, false, err
	}
	return run, created, nil
}

// UpdateRunStatus moves open Runs to running or stopping in one relaxed
// transaction: it survives a process crash and may lose the last transitions
// on power loss, which a restart repairs by interrupting open Runs. Repeating
// a transition is a no-op; a status never goes back.
func (s *Store) UpdateRunStatus(ctx context.Context, updates ...model.RunStatusUpdate) error {
	return s.db.Write(ctx, engine.Relaxed, func(ctx context.Context, tx *sql.Tx) error {
		return updateRunStatuses(ctx, tx, s.scope, updates)
	})
}

// FinishRun durably records how a Run ended. A Run finishes once.
func (s *Store) FinishRun(ctx context.Context, f model.RunFinish) (model.Run, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return finishRun(ctx, tx, scope, f)
	})
}

func (s *Store) GetRun(ctx context.Context, id string) (model.Run, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Run, error) {
		return getRun(ctx, tx, scope, id)
	})
}

// ListRuns pages through Runs newest first.
func (s *Store) ListRuns(ctx context.Context, f model.RunFilter) (model.RunPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RunPage, error) {
		return listRuns(ctx, tx, scope, f)
	})
}

// FindRunByIdempotencyKey returns the Run that a start with this key created.
func (s *Store) FindRunByIdempotencyKey(ctx context.Context, key string) (run model.Run, found bool, err error) {
	err = s.db.Read(ctx, func(ctx context.Context, tx *sql.Tx) (e error) {
		run, found, e = findRunByKey(ctx, tx, s.scope, key)
		return e
	})
	return run, found, err
}

// InterruptOpenRuns ends every Run that a stopped Core left open. Core calls it
// at boot, before it starts any Run, and no half-run graph resumes.
func (s *Store) InterruptOpenRuns(ctx context.Context, at time.Time) (int, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (int, error) {
		return interruptOpenRuns(ctx, tx, scope, at)
	})
}

// PruneRuns deletes finished Runs by age and count and frees their quota. It
// is relaxed housekeeping; call it again while it returns the limit.
func (s *Store) PruneRuns(ctx context.Context, r model.RunRetention) (int, error) {
	return write(ctx, s, engine.Relaxed, func(ctx context.Context, tx *sql.Tx, scope string) (int, error) {
		return pruneRuns(ctx, tx, scope, r)
	})
}
