package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	if len(cursor) > 1024 {
		return 0, "", failure("invalid_argument", "invalid page cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err == nil {
		if at, id, ok := strings.Cut(string(raw), "\x00"); ok {
			if n, e := strconv.ParseInt(at, 10, 64); e == nil && textKey(id) && strconv.FormatInt(n, 10) == at {
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

const runColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,status,termination,error_json,result_json,nodes_json,cleanup_errors_json,created_at,started_at,finished_at,revision,length(result_json)>0,source_observation_json,error_summary_json,cleanup_failed,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released"

// runSummaryColumns replaces the large JSON columns by empty values.
const runSummaryColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,x'',x'',status,termination,x'',x'',x'',x'',created_at,started_at,finished_at,revision,length(result_json)>0,x'',x'',0,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released"

const runObservationColumns = "id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,x'',x'',status,termination,error_summary_json,x'',x'',x'',created_at,started_at,finished_at,revision,length(result_json)>0,source_observation_json,error_summary_json,cleanup_failed,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released"

func scanRun(row scanner, projection ...model.RunProjection) (model.Run, error) {
	var r model.Run
	var status string
	var inputs, trigger, errored, result, nodes, cleanup []byte
	var created, started, finished int64
	if err := row.Scan(&r.ID, &r.TargetID, &r.RootRunID, &r.ParentRunID, &r.CallNodeID, &r.Depth, &r.SessionID, &r.IdempotencyKey, &r.PredecessorRunID, &r.WorkflowResourceID, &r.WorkflowCommitID, &r.DefinitionDigest, &r.ActionID, &inputs, &trigger, &status, &r.Termination, &errored, &result, &nodes, &cleanup, &created, &started, &finished, &r.Revision, &r.HasResult, &r.SourceObservation, &r.ErrorSummary, &r.CleanupFailed, &r.IncomingHold.TargetID, &r.IncomingHold.PrincipalID, &r.IncomingHold.Released); err != nil {
		return r, err
	}
	r.Status = model.RunStatus(status)
	if _, err := jsonField("source observation", r.SourceObservation, model.MaxRunSourceObservationBytes, false, true); err != nil {
		return model.Run{}, err
	}
	if _, err := jsonField("error summary", r.ErrorSummary, model.MaxRunErrorSummaryBytes, false, false); err != nil {
		return model.Run{}, err
	}
	if len(projection) > 0 {
		r.Projection = projection[0]
	}
	for _, field := range []struct {
		stored []byte
		limit  int
		target *json.RawMessage
	}{
		{inputs, model.MaxRunInputsBytes, &r.Inputs},
		{trigger, model.MaxRunTriggerBytes, &r.Trigger},
		{errored, model.MaxRunErrorBytes, &r.Error},
		{result, model.MaxRunResultBytes, &r.Result},
		{nodes, model.MaxRunNodesBytes, &r.Nodes},
		{cleanup, model.MaxRunCleanupErrorsBytes, &r.CleanupErrors},
	} {
		if r.Projection == model.RunObservation && field.target == &r.Error {
			r.Error = rawOrNil(field.stored)
			continue
		}
		raw, err := decodeRunPayload(field.stored, field.limit)
		if err != nil {
			return r, err
		}
		*field.target = rawOrNil(raw)
	}
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
	for _, s := range []string{r.ID, r.TargetID, r.RootRunID, r.ParentRunID, r.CallNodeID, r.SessionID, r.IdempotencyKey, r.PredecessorRunID, r.WorkflowResourceID, r.WorkflowCommitID, r.DefinitionDigest, r.ActionID, r.Termination, r.IncomingHold.TargetID, r.IncomingHold.PrincipalID} {
		n += len(s)
	}
	if r.IncomingHold.TargetID != "" {
		n++
	}
	return int64(n + len(r.Inputs) + len(r.Trigger) + len(r.Error) + len(r.Result) + len(r.Nodes) + len(r.CleanupErrors) + len(r.SourceObservation) + len(r.ErrorSummary))
}

func validateNewRun(q *model.NewRun) error {
	if !textKey(q.ID) || !textKey(q.TargetID) || !textKey(q.WorkflowResourceID) || !textKey(q.WorkflowCommitID) || !textKey(q.DefinitionDigest) || !textKey(q.ActionID) {
		return failure("invalid_argument", "run, target, workflow resource, commit, definition digest and action identities are required")
	}
	for name, value := range map[string]string{"parent run": q.ParentRunID, "call node": q.CallNodeID, "session": q.SessionID, "idempotency key": q.IdempotencyKey, "predecessor run": q.PredecessorRunID} {
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
	if q.PredecessorRunID != "" && (q.PredecessorRunID == q.ID || q.ParentRunID != "" || q.Depth != 0) {
		return failure("invalid_argument", "a restart predecessor must be a different root Run")
	}
	switch q.Status {
	case "":
		q.Status = model.RunQueued
	case model.RunQueued, model.RunRunning:
	default:
		return failure("invalid_argument", "a Run is accepted as queued or running")
	}
	// An incoming hold is the stamped stable identity of the authenticated
	// remote caller (its enrolled target and stable CoreID on the accepted
	// transport, not an ephemeral boot). It requires the accepted call
	// lineage (parent + call node), names a caller target different from
	// this Run's own, and is always created active; release is a separate
	// durable write.
	if (q.IncomingHold.TargetID == "") != (q.IncomingHold.PrincipalID == "") || q.IncomingHold.Released {
		return failure("invalid_argument", "an incoming hold requires the caller's stable target and core identity and cannot be created released")
	}
	if q.IncomingHold.TargetID != "" {
		if !textKey(q.IncomingHold.TargetID) {
			return failure("invalid_argument", "incoming caller target identity is required")
		}
		if err := optionalKey("incoming caller principal identity", q.IncomingHold.PrincipalID); err != nil {
			return err
		}
		if q.IncomingHold.TargetID == q.TargetID || q.ParentRunID == "" || q.CallNodeID == "" {
			return failure("invalid_argument", "an incoming hold requires a remote caller and the accepted call lineage")
		}
	}
	return nil
}

// Session Action instances are normalized by the execution owner to at most
// 128 Unicode code points. The byte bound also permits four-byte code points.
func sessionInstanceKey(id string) bool {
	return id != "" && len(id) <= 512 && utf8.ValidString(id) && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\x00\r\n")
}

// sessionInstanceFromTrigger follows the execution owner's Session Action
// trigger decoder. It is a derived selector, never an execution authority.
func sessionInstanceFromTrigger(sessionID string, trigger []byte) (string, error) {
	if sessionID == "" {
		return "", nil
	}
	var action struct {
		Kind       string `json:"kind"`
		InstanceID string `json:"instanceId"`
		PresetID   string `json:"presetId"`
		ActionID   string `json:"actionId"`
	}
	if json.Unmarshal(trigger, &action) != nil || action.Kind != "session-action" || action.InstanceID == "" {
		return "", nil
	}
	if !sessionInstanceKey(action.InstanceID) {
		return "", failure("invalid_argument", "Session Action instance must be a bounded identifier")
	}
	return action.InstanceID, nil
}

func createRun(ctx context.Context, tx *sql.Tx, scope string, q model.NewRun) (model.Run, bool, error) {
	r, created, err := createRunRaw(ctx, tx, scope, q)
	if err != nil || !created {
		return r, created, err
	}
	var cut retentionChanges
	cut.created(r.ID, r.PredecessorRunID)
	return r, created, cut.finish(ctx, tx, scope)
}

func createRunRaw(ctx context.Context, tx *sql.Tx, scope string, q model.NewRun) (model.Run, bool, error) {
	if err := validateNewRun(&q); err != nil {
		return model.Run{}, false, err
	}
	inputs, err := jsonField("inputs", q.Inputs, model.MaxRunInputsBytes, true, true)
	if err != nil {
		return model.Run{}, false, err
	}
	if q.ParentRunID != "" && len(q.Trigger) > model.MaxRunCallTriggerBytes {
		return model.Run{}, false, failure("resource_exhausted", "child Run trigger exceeds its byte limit")
	}
	trigger, err := jsonField("trigger", q.Trigger, model.MaxRunTriggerBytes, true, true)
	if err != nil {
		return model.Run{}, false, err
	}
	source, err := jsonField("source observation", q.SourceObservation, model.MaxRunSourceObservationBytes, false, true)
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
	// Return the already accepted successor so one conflicting create cannot
	// roll back unrelated journal work in the same durable batch. The execution
	// owner compares the complete intent and rejects a different call identity.
	if q.PredecessorRunID != "" {
		existing, err := selectRun(ctx, tx, scope, "target_id=? AND predecessor_run_id=? AND predecessor_run_id<>''", q.TargetID, q.PredecessorRunID)
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
	sessionInstance, err := sessionInstanceFromTrigger(q.SessionID, trigger)
	if err != nil {
		return model.Run{}, false, err
	}
	at := nowOr(q.At)
	r := model.Run{ID: q.ID, TargetID: q.TargetID, RootRunID: q.RootRunID, ParentRunID: q.ParentRunID, CallNodeID: q.CallNodeID, Depth: q.Depth, SessionID: q.SessionID,
		IdempotencyKey: q.IdempotencyKey, PredecessorRunID: q.PredecessorRunID, WorkflowResourceID: q.WorkflowResourceID, WorkflowCommitID: q.WorkflowCommitID, DefinitionDigest: q.DefinitionDigest, ActionID: q.ActionID,
		Inputs: inputs, Trigger: trigger, SourceObservation: source, Status: q.Status, CreatedAt: at, Revision: 1, IncomingHold: q.IncomingHold}
	if r.Status == model.RunRunning {
		r.StartedAt = at
	}
	storedInputs, err := encodeRunPayload(inputs)
	if err != nil {
		return model.Run{}, false, err
	}
	storedTrigger, err := encodeRunPayload(trigger)
	if err != nil {
		return model.Run{}, false, err
	}
	bytes := runBytes(r) - int64(len(inputs)+len(trigger)) + int64(len(storedInputs)+len(storedTrigger)+len(sessionInstance))
	if err = reserve(ctx, tx, scope, 1, bytes); err != nil {
		return model.Run{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO runs(scope,id,target_id,root_run_id,parent_run_id,call_node_id,depth,session_id,session_instance_id,idempotency_key,predecessor_run_id,workflow_resource_id,workflow_commit_id,definition_digest,action_id,inputs_json,trigger_json,source_observation_json,status,termination,error_json,error_summary_json,result_json,nodes_json,cleanup_errors_json,cleanup_failed,created_at,started_at,finished_at,revision,bytes,incoming_remote_target_id,incoming_remote_principal_id,incoming_hold_released) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,x'',x'',x'',x'',x'',0,?,?,0,1,?,?,?,0)",
		scope, r.ID, r.TargetID, r.RootRunID, r.ParentRunID, r.CallNodeID, r.Depth, r.SessionID, sessionInstance, r.IdempotencyKey, r.PredecessorRunID, r.WorkflowResourceID, r.WorkflowCommitID, r.DefinitionDigest, r.ActionID,
		storedInputs, storedTrigger, []byte(source), string(r.Status), "", nanos(r.CreatedAt), nanos(r.StartedAt), bytes, r.IncomingHold.TargetID, r.IncomingHold.PrincipalID)
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
		// Same status is a no-op. A terminal row keeps its outcome when a late
		// running or stopping update arrives, so a retried batch can still finish.
		// An open Run never moves backward.
		if from == u.Status || from.Terminal() {
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
	return finishRunWithDetail(ctx, tx, scope, f, true)
}

// A batch discards each finished Run. It needs the existing detail only when
// validating a terminal replay; reading it after a changed finish would fetch
// and decode payloads the caller never requested.
func finishRunWithDetail(ctx context.Context, tx *sql.Tx, scope string, f model.RunFinish, wantDetail bool) (model.Run, error) {
	var cut retentionChanges
	run, err := finishRunTransition(ctx, tx, scope, f, wantDetail, &cut, "", map[string]struct{}{f.ID: {}})
	if err != nil {
		return model.Run{}, err
	}
	if err = cut.finish(ctx, tx, scope); err != nil {
		return model.Run{}, err
	}
	return run, nil
}

// planned contains only actual open successors of this Run which are also
// being finished in this batch. A later invalid finish rolls the transaction
// back. Replays are already non-open and cannot suppress a live guard.
func finishRunTransition(ctx context.Context, tx *sql.Tx, scope string, f model.RunFinish, wantDetail bool, cut *retentionChanges, planned string, plannedSet map[string]struct{}) (model.Run, error) {
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
	preview, err := errorSummary(errored, f.ErrorSummary)
	if err != nil {
		return model.Run{}, err
	}
	cleanupFailed, err := cleanupCardinality(cleanup)
	if err != nil {
		return model.Run{}, err
	}
	var current, rootID string
	err = tx.QueryRowContext(ctx, "SELECT status,root_run_id FROM runs WHERE scope=? AND id=?", scope, f.ID).Scan(&current, &rootID)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Run{}, failure("not_found", "run not found")
	}
	if err != nil {
		return model.Run{}, err
	}
	if !model.RunStatus(current).Open() {
		existing, err := selectRun(ctx, tx, scope, "id=?", f.ID)
		if err != nil {
			return model.Run{}, err
		}
		if !sameRunFinish(existing, f, errored, result, nodes, cleanup) || !bytes.Equal(existing.ErrorSummary, preview) || existing.CleanupFailed != cleanupFailed {
			return model.Run{}, failure("failed_precondition", "run already finished")
		}
		return existing, nil
	}
	for _, field := range []*[]byte{&errored, &result, &nodes, &cleanup} {
		*field, err = encodeRunPayload(*field)
		if err != nil {
			return model.Run{}, err
		}
	}
	// An open Run carries none of these columns, so the row only grows by them.
	grown := int64(len(f.Termination) + len(errored) + len(result) + len(nodes) + len(cleanup) + len(preview))
	if err = charge(ctx, tx, scope, 0, grown); err != nil {
		return model.Run{}, err
	}
	var eligible int64
	var predecessor, failedSession string
	_, rootFinishes := plannedSet[rootID]
	if err = tx.QueryRowContext(ctx, retentionFinish,
		string(f.Status), f.Termination, []byte(errored), []byte(preview), []byte(result), []byte(nodes), []byte(cleanup), cleanupFailed, nanos(nowOr(f.At)), grown, scope, f.ID, current, rootFinishes, planned).Scan(&eligible, &predecessor, &failedSession); err != nil {
		return model.Run{}, err
	}
	// A Session can outlive the display Run. Record the failure on that actual
	// Session in the same transaction before the Run becomes prunable.
	if failedSession != "" {
		if _, err = tx.ExecContext(ctx, "UPDATE sessions SET failed_cleanup=1 WHERE scope=? AND id=? AND failed_cleanup=0", scope, failedSession); err != nil {
			return model.Run{}, err
		}
	}
	cut.finished(f.ID, predecessor, eligible)
	if !wantDetail {
		return model.Run{}, nil
	}
	return selectRun(ctx, tx, scope, "id=?", f.ID)
}

// sameRunFinish reports whether f repeats the outcome already stored on existing.
// JSON fields are the normalized uncompressed bytes from jsonField. A zero At
// matches any stored finish time; a non-zero At must be that exact instant.
func sameRunFinish(existing model.Run, f model.RunFinish, errored, result, nodes, cleanup []byte) bool {
	if existing.Status != f.Status || existing.Termination != f.Termination {
		return false
	}
	if !bytes.Equal(existing.Error, errored) || !bytes.Equal(existing.Result, result) || !bytes.Equal(existing.Nodes, nodes) || !bytes.Equal(existing.CleanupErrors, cleanup) {
		return false
	}
	return f.At.IsZero() || nanos(f.At) == nanos(existing.FinishedAt)
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
	if f.SessionInstanceID != "" {
		if f.SessionID == "" || !sessionInstanceKey(f.SessionInstanceID) {
			return "", nil, 0, failure("invalid_argument", "a Session instance filter requires a Session and a bounded instance identifier")
		}
		where, args = append(where, "session_instance_id=?"), append(args, f.SessionInstanceID)
	}
	for column, value := range map[string]string{"id": f.ID, "target_id": f.TargetID, "root_run_id": f.RootRunID, "session_id": f.SessionID, "workflow_resource_id": f.WorkflowResourceID, "source_experiment_id": f.ExperimentResourceID, "source_instance_id": f.WorkflowInstanceID} {
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
	switch f.Projection {
	case "", model.RunComplete:
	case model.RunIdentity:
		columns = runSummaryColumns
	case model.RunObservation:
		columns = runObservationColumns
	default:
		return "", nil, 0, failure("invalid_argument", "unknown Run projection")
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
	// Keep room for the page envelope and the largest admitted cursor (a signed
	// nanosecond timestamp, separator and a 255-byte identifier, then base64).
	// This budgets this SDK response, not OS I/O or a different wire wrapper.
	const envelopeBudget = 1024
	used := envelopeBudget
	for rows.Next() {
		if len(page.Runs) == limit {
			last := page.Runs[len(page.Runs)-1]
			page.Next = encodeCursor(nanos(last.CreatedAt), last.ID)
			break
		}
		r, err := scanRun(rows, f.Projection)
		if err != nil {
			return model.RunPage{}, err
		}
		encoded, err := json.Marshal(r)
		if err != nil {
			return model.RunPage{}, err
		}
		if used+len(encoded)+1 > model.MaxResponseBytes {
			if len(page.Runs) == 0 {
				return model.RunPage{}, failure("resource_exhausted", "one Run exceeds the list response budget; request its exact detail")
			}
			last := page.Runs[len(page.Runs)-1]
			page.Next = encodeCursor(nanos(last.CreatedAt), last.ID)
			break
		}
		used += len(encoded) + 1
		page.Runs = append(page.Runs, r)
	}
	if err := rows.Err(); err != nil {
		return model.RunPage{}, err
	}
	return page, nil
}

// The statements below are fixed text, so that the plan test can run them as written.
const (
	// All open roots and successors in this scope end in this same statement.
	// Their old open state must not guard another row at the final cut.
	bootLocal     = retentionPendingClear
	interruptOpen = `UPDATE runs AS r INDEXED BY runs_open SET status='interrupted',termination='interrupted',finished_at=?1,revision=revision+1,bytes=bytes+?2,
 (retention_local_ok,retention_eligible)=(WITH local(value) AS MATERIALIZED (SELECT CASE WHEN ?1>0 THEN (` + bootLocal + `) ELSE 0 END) SELECT value,CASE WHEN value THEN (` + retentionSessionClear + `) ELSE 0 END FROM local)
 WHERE scope=?3 AND status IN ('queued','running','stopping') RETURNING id,predecessor_run_id,retention_eligible`

	// Recheck all active guards before deletion. An absent root is not live.
	eligibleRuns    = "scope=? AND finished_at>0 AND pending_remote_count=0 AND (r.incoming_remote_target_id='' OR r.incoming_hold_released<>0) AND NOT EXISTS (SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.owner_run_id=r.id) AND NOT EXISTS (SELECT 1 FROM runs p WHERE p.scope=r.scope AND p.id=r.root_run_id AND p.finished_at=0) AND NOT EXISTS (SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.root_run_id=r.id) AND NOT EXISTS (SELECT 1 FROM runs successor WHERE successor.scope=r.scope AND successor.predecessor_run_id=r.id AND successor.predecessor_run_id<>'' AND successor.finished_at=0) AND NOT EXISTS (SELECT 1 FROM sessions successor_session WHERE successor_session.scope=r.scope AND successor_session.target_id=r.target_id AND successor_session.predecessor_session_id=r.session_id AND successor_session.predecessor_session_id<>'' AND successor_session.status='queued')"
	pruneOldest     = "SELECT finished_at FROM runs INDEXED BY runs_retention_eligible WHERE scope=? AND retention_eligible=1 ORDER BY finished_at,id LIMIT 1"
	pruneCandidates = "SELECT id,bytes,finished_at FROM runs INDEXED BY runs_retention_eligible WHERE scope=? AND retention_eligible=1 ORDER BY finished_at,id LIMIT ?"
)

func interruptOpenRuns(ctx context.Context, tx *sql.Tx, scope string, at time.Time, cut *retentionChanges) (int, error) {
	rows, err := tx.QueryContext(ctx, interruptOpen, nanos(nowOr(at)), len("interrupted"), scope)
	if err != nil {
		return 0, err
	}
	n := 0
	for rows.Next() {
		var id, predecessor string
		var eligible int64
		if err = rows.Scan(&id, &predecessor, &eligible); err != nil {
			break
		}
		cut.finished(id, predecessor, eligible)
		n++
	}
	if err == nil {
		err = rows.Err()
	}
	if e := rows.Close(); err == nil {
		err = e
	}
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	return n, charge(ctx, tx, scope, 0, int64(n)*int64(len("interrupted")))
}

func pruneRuns(ctx context.Context, tx *sql.Tx, scope string, r model.RunRetention) (model.RunPruneResult, error) {
	if err := validateRetentionPolicy(model.RunRetentionPolicy{OlderThan: r.OlderThan, KeepNewest: r.KeepNewest}); err != nil {
		return model.RunPruneResult{}, err
	}
	limit := r.Limit
	if limit == 0 {
		limit = 1000
	}
	if limit < 0 || limit > 10000 || r.KeepNewest < 0 || (r.OlderThan.IsZero() && r.KeepNewest == 0) {
		return model.RunPruneResult{}, failure("invalid_argument", "an age or count limit and a limit within 1..10000 are required")
	}
	var eligible int64
	if err := tx.QueryRowContext(ctx, "SELECT coalesce((SELECT retention_eligible_count FROM core_data_usage WHERE scope=?),0)", scope).Scan(&eligible); err != nil {
		return model.RunPruneResult{}, err
	}
	excess := int64(0)
	if r.KeepNewest > 0 && eligible > int64(r.KeepNewest) {
		excess = eligible - int64(r.KeepNewest)
	}
	type victim struct {
		id              string
		bytes, finished int64
	}
	var victims []victim
	rows, err := tx.QueryContext(ctx, pruneCandidates, scope, limit)
	if err != nil {
		return model.RunPruneResult{}, err
	}
	for rows.Next() {
		var v victim
		if err = rows.Scan(&v.id, &v.bytes, &v.finished); err != nil {
			break
		}
		if int64(len(victims)) >= excess && (r.OlderThan.IsZero() || v.finished >= nanos(r.OlderThan)) {
			break
		}
		victims = append(victims, v)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return model.RunPruneResult{}, err
	}
	var freed int64
	for _, v := range victims {
		// A stale or corrupt derived flag must never authorize a deletion.
		deleted, err := tx.ExecContext(ctx, "DELETE FROM runs AS r WHERE "+eligibleRuns+" AND id=? AND retention_eligible=1", scope, v.id)
		if err != nil {
			return model.RunPruneResult{}, err
		}
		n, err := deleted.RowsAffected()
		if err != nil {
			return model.RunPruneResult{}, err
		}
		if n != 1 {
			return model.RunPruneResult{}, failure("data_loss", "retention eligibility disagrees with protections")
		}
		freed += v.bytes
	}
	if len(victims) > 0 {
		count := int64(len(victims))
		updated, err := tx.ExecContext(ctx, "UPDATE core_data_usage SET rows=rows-?,bytes=bytes-?,retention_eligible_count=retention_eligible_count-? WHERE scope=? AND rows>=? AND bytes>=? AND retention_eligible_count>=?", count, freed, count, scope, count, freed, count)
		if err != nil {
			return model.RunPruneResult{}, err
		}
		n, err := updated.RowsAffected()
		if err != nil {
			return model.RunPruneResult{}, err
		}
		if n != 1 {
			return model.RunPruneResult{}, failure("data_loss", "retention quota disagrees")
		}
	}
	result := model.RunPruneResult{Deleted: len(victims)}
	var oldest int64
	if err := tx.QueryRowContext(ctx, pruneOldest, scope).Scan(&oldest); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.RunPruneResult{}, err
	}
	result.OldestEligibleFinishedAt = instant(oldest)
	result.HasVictim = r.KeepNewest > 0 && eligible-int64(len(victims)) > int64(r.KeepNewest) || !r.OlderThan.IsZero() && oldest > 0 && oldest < nanos(r.OlderThan)
	return result, nil
}

// Runs

// CreateRun durably accepts a Run. With an idempotency key, a repeated call
// returns the stored Run and created is false, so a replayed start finds its
// Run instead of making another.
func (s *Store) CreateRun(ctx context.Context, q model.NewRun) (model.Run, bool, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.Run{}, false, err
	}
	var run model.Run
	var created bool
	state := &runFaultState{scope: s.scope, tracksWrites: true}
	err := s.db.WriteWithFaultProbeInput(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) (e error) {
		if e = input.Own(ctx, func() { q = ownedRun(q) }); e != nil {
			return e
		}
		run, created, e = createRunRaw(ctx, tx, s.scope, q)
		if e != nil {
			return e
		}
		state.created = []model.CreatedRun{{Run: run, Created: created}}
		if !created {
			return nil
		}
		var cut retentionChanges
		cut.created(run.ID, run.PredecessorRunID)
		return cut.finish(ctx, tx, s.scope)
	}, state.probe)
	if err != nil {
		return model.Run{}, false, runFaultError(err, state)
	}
	return run, created, nil
}

// UpdateRunStatus moves open Runs to running or stopping in one relaxed
// transaction: it survives a process crash and may lose the last transitions
// on power loss, which a restart repairs by interrupting open Runs. Repeating
// a transition is a no-op. A running or stopping update of an already terminal
// Run is a no-op and leaves revision unchanged. An open Run never moves backward.
func (s *Store) UpdateRunStatus(ctx context.Context, updates ...model.RunStatusUpdate) error {
	if err := canceledWrite(ctx); err != nil {
		return err
	}
	return s.db.WriteWithInput(ctx, engine.Relaxed, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) error {
		if err := input.Own(ctx, func() {
			if len(updates) <= model.MaxPageSize {
				updates = slices.Clone(updates)
			}
		}); err != nil {
			return err
		}
		return updateRunStatuses(ctx, tx, s.scope, updates)
	})
}

// FinishRun durably records how a Run ended. The same outcome returns the
// stored Run and does not change the row, its revision, or its quota. A
// different outcome is failed_precondition.
func (s *Store) FinishRun(ctx context.Context, f model.RunFinish, policy model.RunRetentionPolicy) (model.RunFinishResult, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunFinishResult{}, engine.WriteRefused(err)
	}
	if err := canceledWrite(ctx); err != nil {
		return model.RunFinishResult{}, err
	}
	state := &runFaultState{scope: s.scope, policy: policy, tracksWrites: true}
	var result model.RunFinishResult
	err := s.db.WriteWithFaultProbeInput(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) error {
		if err := input.Own(ctx, func() { f = ownedFinish(f) }); err != nil {
			return err
		}
		cut := retentionChanges{finishedIDs: &state.finished}
		run, err := finishRunTransition(ctx, tx, s.scope, f, true, &cut, "", map[string]struct{}{f.ID: {}})
		if err != nil {
			return err
		}
		if err = cut.finish(ctx, tx, s.scope); err != nil {
			return err
		}
		effect, err := retentionEffect(ctx, tx, s.scope, &cut, policy)
		if err == nil {
			result = model.RunFinishResult{Run: run, Retention: effect}
			state.attempt = effect
		}
		return err
	}, state.probe)
	if err != nil {
		return model.RunFinishResult{}, runFaultError(err, state)
	}
	return result, nil
}

// Expand the planned set once for a batch and retain only actual open
// successor/predecessor pairs in that set. Each finish then checks only its
// own relevant successor identities, not every unrelated planned ID.
const plannedSuccessorQuery = `WITH planned(id) AS MATERIALIZED (SELECT value FROM json_each(?2))
 SELECT r.id,r.predecessor_run_id FROM runs r
 WHERE r.scope=?1 AND r.id IN (SELECT id FROM planned) AND r.finished_at=0
 AND r.predecessor_run_id<>'' AND r.predecessor_run_id IN (SELECT id FROM planned)`

func batchPlannedSuccessors(ctx context.Context, tx *sql.Tx, scope string, ids []string) (map[string]string, error) {
	if len(ids) < 2 {
		return nil, nil
	}
	encoded, err := json.Marshal(ids)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, plannedSuccessorQuery, scope, string(encoded))
	if err != nil {
		return nil, err
	}
	groups := make(map[string][]string)
	for rows.Next() {
		var id, predecessor string
		if err = rows.Scan(&id, &predecessor); err != nil {
			break
		}
		groups[predecessor] = append(groups[predecessor], id)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(groups))
	for predecessor, successors := range groups {
		value, e := json.Marshal(successors)
		if e != nil {
			return nil, e
		}
		out[predecessor] = string(value)
	}
	return out, nil
}

// ApplyRunOps applies a batch of Run creations, open-status transitions and
// terminal finishes in one transaction (creates, then updates, then finishes).
// The transaction is Durable when creates or finishes is non-empty, and
// Relaxed when only updates are present. An empty batch is a no-op.
func (s *Store) ApplyRunOps(ctx context.Context, creates []model.NewRun, updates []model.RunStatusUpdate, finishes []model.RunFinish, policy model.RunRetentionPolicy) (model.RunOpsResult, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunOpsResult{}, engine.WriteRefused(err)
	}
	if len(creates)+len(updates)+len(finishes) > model.MaxPageSize {
		return model.RunOpsResult{}, engine.WriteRefused(failure("invalid_argument", "at most 1000 run operations per batch"))
	}
	if len(creates)+len(updates)+len(finishes) == 0 {
		return model.RunOpsResult{}, nil
	}
	if err := canceledWrite(ctx); err != nil {
		return model.RunOpsResult{}, err
	}
	mode := engine.Relaxed
	if len(creates) > 0 || len(finishes) > 0 {
		mode = engine.Durable
	}
	var created []model.CreatedRun
	var effect model.RunRetentionEffect
	state := &runFaultState{scope: s.scope, policy: policy, tracksWrites: len(creates)+len(finishes) != 0}
	var probe engine.FaultProbe
	if state.tracksWrites {
		probe = state.probe
	}
	err := s.db.WriteWithFaultProbeInput(ctx, mode, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) error {
		if err := input.Own(ctx, func() {
			creates, updates, finishes = slices.Clone(creates), slices.Clone(updates), slices.Clone(finishes)
			for i := range creates {
				creates[i] = ownedRun(creates[i])
			}
			for i := range finishes {
				finishes[i] = ownedFinish(finishes[i])
			}
		}); err != nil {
			return err
		}
		created = make([]model.CreatedRun, 0, len(creates))
		plannedIDs := make([]string, 0, len(finishes))
		plannedSet := make(map[string]struct{}, len(finishes))
		for _, f := range finishes {
			if _, exists := plannedSet[f.ID]; !exists {
				plannedIDs = append(plannedIDs, f.ID)
				plannedSet[f.ID] = struct{}{}
			}
		}
		cut := retentionChanges{finishedIDs: &state.finished}
		for _, q := range creates {
			run, ok, err := createRunRaw(ctx, tx, s.scope, q)
			if err != nil {
				return err
			}
			created = append(created, model.CreatedRun{Run: run, Created: ok})
			state.created = created
			if ok {
				cut.created(run.ID, run.PredecessorRunID)
			}
		}
		if len(updates) > 0 {
			if err := updateRunStatuses(ctx, tx, s.scope, updates); err != nil {
				return err
			}
		}
		plannedSuccessors, err := batchPlannedSuccessors(ctx, tx, s.scope, plannedIDs)
		if err != nil {
			return err
		}
		for _, finish := range finishes {
			if _, err := finishRunTransition(ctx, tx, s.scope, finish, false, &cut, plannedSuccessors[finish.ID], plannedSet); err != nil {
				return err
			}
		}
		if err := cut.finish(ctx, tx, s.scope); err != nil {
			return err
		}
		var e error
		effect, e = retentionEffect(ctx, tx, s.scope, &cut, policy)
		if e == nil {
			state.attempt = effect
		}
		return e
	}, probe)
	if err != nil {
		return model.RunOpsResult{}, runFaultError(err, state)
	}
	if len(creates) == 0 {
		created = nil
	}
	return model.RunOpsResult{Created: created, Retention: effect}, nil
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
	}, func() { f.Statuses = slices.Clone(f.Statuses) })
}

// FindRunByIdempotencyKey returns the Run that a start with this key created.
func (s *Store) FindRunByIdempotencyKey(ctx context.Context, key string) (model.Run, bool, error) {
	var run model.Run
	var found bool
	err := s.db.Read(ctx, func(ctx context.Context, tx *sql.Tx) (e error) {
		run, found, e = findRunByKey(ctx, tx, s.scope, key)
		return e
	})
	if err != nil {
		return model.Run{}, false, err
	}
	return run, found, nil
}

// InterruptOpenRuns ends every Run that a stopped Core left open. Core calls it
// at boot, before it starts any Run, and no half-run graph resumes.
func (s *Store) InterruptOpenRuns(ctx context.Context, at time.Time, policy model.RunRetentionPolicy) (model.RunInterruptResult, error) {
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RunInterruptResult{}, err
	}
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.RunInterruptResult, error) {
		var cut retentionChanges
		count, err := interruptOpenRuns(ctx, tx, scope, at, &cut)
		if err != nil {
			return model.RunInterruptResult{}, err
		}
		if err = cut.finish(ctx, tx, scope); err != nil {
			return model.RunInterruptResult{}, err
		}
		effect, err := retentionEffect(ctx, tx, scope, &cut, policy)
		return model.RunInterruptResult{Count: count, Retention: effect}, err
	})
}

// PruneRuns deletes finished Runs by age and count and frees their quota. It
// is relaxed housekeeping; continue only while HasVictim is true.
func (s *Store) PruneRuns(ctx context.Context, r model.RunRetention) (model.RunPruneResult, error) {
	return write(ctx, s, engine.Relaxed, func(ctx context.Context, tx *sql.Tx, scope string) (model.RunPruneResult, error) {
		return pruneRuns(ctx, tx, scope, r)
	})
}
