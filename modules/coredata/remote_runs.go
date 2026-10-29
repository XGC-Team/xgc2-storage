package coredata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func validateRemoteLink(link model.RemoteRunLink) error {
	for _, key := range []string{link.OwnerRunID, link.RootRunID, link.TargetID, link.RunID, link.CallNodeID} {
		if !textKey(key) {
			return failure("invalid_argument", "remote control identities must be bounded text keys")
		}
	}
	return optionalKey("remote boot", link.BootID)
}

// remoteLinkBytes preserves the existing encoded-array capacity contract even
// though the durable relationship itself is stored as typed relational fields.
func remoteLinkBytes(link model.RemoteRunLink) (int64, error) {
	raw, err := json.Marshal(link)
	return int64(len(raw)), err
}

// ReserveRemoteRun precedes a remote Start. A later reservation may bind an
// initially unknown boot exactly once; a retry never removes that binding.
func (s *Store) ReserveRemoteRun(ctx context.Context, link model.RemoteRunLink) error {
	return s.reserveRemoteRun(ctx, link)
}

// ResolveRemoteRun requires an exact terminal/joined or definitive unsent
// identity. A Stop acceptance alone is not a reason to call it.
func (s *Store) ResolveRemoteRun(ctx context.Context, link model.RemoteRunLink, policy model.RunRetentionPolicy) (model.RemoteRunResolveResult, error) {
	if err := validateRemoteLink(link); err != nil {
		return model.RemoteRunResolveResult{}, engine.WriteRefused(err)
	}
	if err := validateRetentionPolicy(policy); err != nil {
		return model.RemoteRunResolveResult{}, engine.WriteRefused(err)
	}
	state := &remoteFaultState{scope: s.scope, link: link, policy: policy}
	var result model.RemoteRunResolveResult
	err := s.db.WriteWithFaultProbe(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		result, e = resolveRemoteRunPolicy(ctx, tx, s.scope, link, policy, &state.attempted)
		if e == nil {
			state.candidate = result
		}
		return e
	}, state.probe)
	var disposition *engine.WriteError
	if err != nil && errors.As(err, &disposition) && disposition.Disposition == engine.WriteUnknown {
		failure := &RemoteRunResolveError{cause: err, state: state}
		// A callback held inside a real SQL call can outlive this caller. Its
		// attempted bit is owned until settlement and must not be read here.
		if !disposition.CallbackPending {
			failure.ValidatedAttempt = state.attempted
			if state.attempted {
				failure.AttemptedLink = link
			}
		}
		return model.RemoteRunResolveResult{}, failure
	}
	return result, err
}

// RemoteRunResolveError reports transaction-local attempt evidence only. It
// never says the relation was deleted or that COMMIT succeeded. Inspect the
// wrapped engine.WriteError for outcome and physical settlement.
type RemoteRunResolveError struct {
	ValidatedAttempt bool
	AttemptedLink    model.RemoteRunLink
	cause            error
	state            *remoteFaultState
}

func (e *RemoteRunResolveError) Error() string { return e.cause.Error() }
func (e *RemoteRunResolveError) Unwrap() error { return e.cause }

// RemoteRunFaultFact is an exact post-join current observation, not a COMMIT
// receipt. Released is only true for a transaction-validated changed DELETE
// whose exact relation is absent before any intervening writer.
type RemoteRunFaultFact struct {
	Link             model.RemoteRunLink
	ValidatedAttempt bool
	NonCommitted     bool
	RelationPresent  bool
	Released         bool
	LastForOwner     bool
	LastForRoot      bool
	RootFound        bool
	RootFinishedAt   time.Time
	OwnerFinishedAt  time.Time
	Retention        model.RunRetentionEffect
}

func (e *RemoteRunResolveError) WaitRevalidation(ctx context.Context) (RemoteRunFaultFact, error) {
	var disposition *engine.WriteError
	if !errors.As(e.cause, &disposition) {
		return RemoteRunFaultFact{}, failure("unavailable", "original writer disposition unavailable")
	}
	if err := disposition.WaitSettlement(ctx); err != nil {
		return RemoteRunFaultFact{}, err
	}
	return e.state.fact, nil
}

type remoteFaultState struct {
	scope     string
	link      model.RemoteRunLink
	policy    model.RunRetentionPolicy
	attempted bool
	candidate model.RemoteRunResolveResult
	fact      RemoteRunFaultFact
}

func (s *remoteFaultState) probe(ctx context.Context, tx *sql.Tx, possibleCommit bool) error {
	out := RemoteRunFaultFact{Link: s.link, ValidatedAttempt: s.attempted}
	if !possibleCommit {
		out.NonCommitted = true
		s.fact = out
		return nil
	}
	if !s.candidate.Changed {
		s.fact = out
		return nil
	}
	var root, call, boot string
	err := tx.QueryRowContext(ctx, `SELECT root_run_id,call_node_id,boot_id FROM run_remote_pending WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`,
		s.scope, s.link.OwnerRunID, s.link.TargetID, s.link.RunID).Scan(&root, &call, &boot)
	if err == nil {
		if root != s.link.RootRunID || call != s.link.CallNodeID || boot != s.link.BootID {
			return failure("data_loss", "validated remote identity changed before fault revalidation")
		}
		out.RelationPresent = true
		s.fact = out
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	out.Released = true
	out.LastForOwner, out.LastForRoot = s.candidate.LastForOwner, s.candidate.LastForRoot
	out.RootFound = s.candidate.RootFound
	out.RootFinishedAt, out.OwnerFinishedAt = s.candidate.RootFinishedAt, s.candidate.OwnerFinishedAt
	if s.candidate.Retention.EligibilityReleased {
		cut := retentionChanges{released: true}
		out.Retention, err = retentionEffect(ctx, tx, s.scope, &cut, s.policy)
		if err != nil {
			return err
		}
	}
	s.fact = out
	return nil
}

func resolveRemoteRun(ctx context.Context, tx *sql.Tx, scope string, link model.RemoteRunLink) (model.RemoteRunResolveResult, error) {
	return resolveRemoteRunPolicy(ctx, tx, scope, link, model.RunRetentionPolicy{}, nil)
}

func resolveRemoteRunPolicy(ctx context.Context, tx *sql.Tx, scope string, link model.RemoteRunLink, policy model.RunRetentionPolicy, attempted *bool) (model.RemoteRunResolveResult, error) {
	var detail remoteRunChangeDetail
	changed, err := changeRemoteRun(ctx, tx, scope, link, true, &detail)
	if attempted != nil {
		*attempted = detail.validatedAttempt
	}
	if err != nil {
		return model.RemoteRunResolveResult{}, err
	}
	result := model.RemoteRunResolveResult{RootRunID: link.RootRunID, Changed: changed}
	if !changed {
		return result, nil
	}
	result.LastForOwner = detail.remaining == 0
	if !result.LastForOwner {
		// Other relations of this owner necessarily share its validated root.
		return result, nil
	}
	var rootFinished sql.NullInt64
	err = tx.QueryRowContext(ctx, remoteRunLastResolutionQuery,
		scope, link.RootRunID, scope, link.RootRunID).Scan(&rootFinished)
	last := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.RemoteRunResolveResult{}, err
	}
	if last {
		result.LastForRoot = true
		result.RootFound = rootFinished.Valid
		result.RootFinishedAt = instant(rootFinished.Int64)
		if link.OwnerRunID == link.RootRunID {
			if !rootFinished.Valid {
				return model.RemoteRunResolveResult{}, failure("data_loss", "remote owner disappeared during resolution")
			}
		}
		result.OwnerFinishedAt = instant(detail.ownerFinishedAt)
	}
	var cut retentionChanges
	if detail.ownerFinishedAt > 0 {
		cut.touch(link.OwnerRunID)
	}
	if last && rootFinished.Valid && rootFinished.Int64 > 0 {
		cut.touch(link.RootRunID)
	}
	if err = cut.finish(ctx, tx, scope); err != nil {
		return model.RemoteRunResolveResult{}, err
	}
	result.Retention, err = retentionEffect(ctx, tx, scope, &cut, policy)
	if err != nil {
		return model.RemoteRunResolveResult{}, err
	}
	return result, nil
}

// A row is returned only for the final root relation. The owner's finish time
// was already selected while validating the exact relation in this transaction.
const remoteRunLastResolutionQuery = `SELECT
 (SELECT finished_at FROM runs WHERE scope=? AND id=?)
 WHERE NOT EXISTS (SELECT 1 FROM run_remote_pending INDEXED BY run_remote_pending_root WHERE scope=? AND root_run_id=?)`

func (s *Store) reserveRemoteRun(ctx context.Context, link model.RemoteRunLink) error {
	if err := validateRemoteLink(link); err != nil {
		return engine.WriteRefused(err)
	}
	state := &remoteReserveFaultState{scope: s.scope, requested: link}
	err := s.db.WriteWithFaultProbe(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		changed, e := changeRemoteRun(ctx, tx, s.scope, link, false, &state.detail)
		state.applied = changed && e == nil
		if e != nil || !state.detail.inserted || !state.detail.firstOwnerRelation || link.OwnerRunID == link.RootRunID {
			return e
		}
		var cut retentionChanges
		if e = cut.invalidateRemoteRoot(ctx, tx, s.scope, link.RootRunID); e != nil {
			return e
		}
		return cut.finish(ctx, tx, s.scope)
	}, state.probe)
	var disposition *engine.WriteError
	if err != nil && errors.As(err, &disposition) && disposition.Disposition == engine.WriteUnknown {
		return &RemoteRunReserveError{Requested: link, cause: err, state: state}
	}
	return err
}

// RemoteRunReserveError retains the original requested identity and attempt;
// Requested alone is not proof that the transaction validated a relation.
type RemoteRunReserveError struct {
	Requested model.RemoteRunLink
	cause     error
	state     *remoteReserveFaultState
}

func (e *RemoteRunReserveError) Error() string { return e.cause.Error() }
func (e *RemoteRunReserveError) Unwrap() error { return e.cause }

// RemoteRunReserveFact separates the transaction-local action from the exact
// same-owner row after physical join. Action is "insert", "bind", or "existing"
// only when Validated is true; Applied is an in-transaction candidate, not a
// COMMIT receipt. Current is meaningful only when CurrentKnown and Present.
type RemoteRunReserveFact struct {
	Requested     model.RemoteRunLink
	Validated     bool
	Action        string
	Applied       bool
	BeforePresent bool
	Before        model.RemoteRunLink
	NonCommitted  bool
	CurrentKnown  bool
	Present       bool
	Current       model.RemoteRunLink
}

func (e *RemoteRunReserveError) WaitRevalidation(ctx context.Context) (RemoteRunReserveFact, error) {
	var disposition *engine.WriteError
	if !errors.As(e.cause, &disposition) {
		return RemoteRunReserveFact{}, failure("unavailable", "original writer disposition unavailable")
	}
	if err := disposition.WaitSettlement(ctx); err != nil {
		return RemoteRunReserveFact{}, err
	}
	return e.state.fact, nil
}

type remoteReserveFaultState struct {
	scope     string
	requested model.RemoteRunLink
	detail    remoteRunChangeDetail
	applied   bool
	fact      RemoteRunReserveFact
}

const remoteReserveRevalidateQuery = `SELECT root_run_id,call_node_id,boot_id,encoded_bytes FROM run_remote_pending WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`

func (s *remoteReserveFaultState) candidate() RemoteRunReserveFact {
	return RemoteRunReserveFact{Requested: s.requested, Validated: s.detail.reserveValidated, Action: s.detail.reserveAction,
		Applied: s.applied, BeforePresent: s.detail.reserveBeforePresent, Before: s.detail.reserveBefore}
}

func (s *remoteReserveFaultState) noCommitFact() RemoteRunReserveFact {
	out := s.candidate()
	out.NonCommitted = true
	if out.Validated {
		out.CurrentKnown, out.Present, out.Current = true, out.BeforePresent, out.Before
	}
	return out
}

func (s *remoteReserveFaultState) probe(ctx context.Context, tx *sql.Tx, possibleCommit bool) error {
	if !possibleCommit {
		s.fact = s.noCommitFact()
		return nil
	}
	out := s.candidate()
	if !out.Validated {
		s.fact = out
		return nil
	}
	var root, call, boot string
	var encoded int64
	err := tx.QueryRowContext(ctx, remoteReserveRevalidateQuery,
		s.scope, s.requested.OwnerRunID, s.requested.TargetID, s.requested.RunID).Scan(&root, &call, &boot, &encoded)
	out.CurrentKnown = true
	if errors.Is(err, sql.ErrNoRows) {
		if out.BeforePresent {
			return failure("data_loss", "validated reservation disappeared before revalidation")
		}
		s.fact = out
		return nil
	}
	if err != nil {
		return err
	}
	current := s.requested
	current.RootRunID, current.CallNodeID, current.BootID = root, call, boot
	bytes, err := remoteLinkBytes(current)
	if err != nil || validateRemoteLink(current) != nil || bytes != encoded || root != s.requested.RootRunID || call != s.requested.CallNodeID {
		return failure("data_loss", "validated reservation identity changed before revalidation")
	}
	if out.Action == "existing" && current != out.Before || out.Action == "insert" && current != s.requested ||
		out.Action == "bind" && current != out.Before && current != s.requested {
		return failure("data_loss", "validated reservation boot changed before revalidation")
	}
	out.Present, out.Current = true, current
	s.fact = out
	return nil
}

type remoteRunChangeDetail struct {
	remaining            int
	ownerFinishedAt      int64
	inserted             bool
	firstOwnerRelation   bool
	validatedAttempt     bool
	reserveValidated     bool
	reserveAction        string
	reserveBeforePresent bool
	reserveBefore        model.RemoteRunLink
}

func changeRemoteRun(ctx context.Context, tx *sql.Tx, scope string, link model.RemoteRunLink, resolve bool, detail *remoteRunChangeDetail) (bool, error) {
	if err := validateRemoteLink(link); err != nil {
		return false, err
	}
	var root, target, status string
	var count int
	var total, finishedAt, sessionBytes int64
	err := tx.QueryRowContext(ctx, `SELECT root_run_id,target_id,status,pending_remote_count,pending_remote_bytes,finished_at,length(CAST(session_id AS BLOB)) FROM runs WHERE scope=? AND id=?`, scope, link.OwnerRunID).Scan(&root, &target, &status, &count, &total, &finishedAt, &sessionBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return false, failure("not_found", "remote control owner is unavailable")
	}
	if err != nil {
		return false, err
	}
	if root != link.RootRunID || target == link.TargetID {
		return false, failure("failed_precondition", "remote control owner lineage or target disagrees")
	}
	if count < 0 || count > model.MaxRemoteRunLinks || total < 0 || total > model.MaxRemoteRunLinksBytes || (count == 0) != (total == 0) {
		return false, failure("data_loss", "invalid remote ownership capacity")
	}
	var existingRoot, existingCall, existingBoot string
	var existingBytes int64
	err = tx.QueryRowContext(ctx, `SELECT root_run_id,call_node_id,boot_id,encoded_bytes FROM run_remote_pending WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`, scope, link.OwnerRunID, link.TargetID, link.RunID).Scan(&existingRoot, &existingCall, &existingBoot, &existingBytes)
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if found {
		current := link
		current.RootRunID, current.CallNodeID, current.BootID = existingRoot, existingCall, existingBoot
		encoded, e := remoteLinkBytes(current)
		if e != nil || validateRemoteLink(current) != nil || encoded != existingBytes || count == 0 {
			return false, failure("data_loss", "invalid remote ownership row")
		}
		if current.RootRunID != link.RootRunID || current.CallNodeID != link.CallNodeID ||
			(resolve && current.BootID != link.BootID) || (!resolve && current.BootID != "" && link.BootID != "" && current.BootID != link.BootID) {
			return false, failure("failed_precondition", "remote control identity or boot conflicts")
		}
		if !resolve && detail != nil {
			detail.reserveValidated = true
			detail.reserveAction = "existing"
			detail.reserveBeforePresent = true
			detail.reserveBefore = current
		}
		if resolve {
			logical := existingBytes + 1 // comma
			if count == 1 {
				logical = existingBytes + 2 // enclosing brackets
			}
			if logical > total || count == 1 && logical != total {
				return false, failure("data_loss", "remote ownership accounting disagrees")
			}
			if detail != nil {
				detail.validatedAttempt = true
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM run_remote_pending WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`, scope, link.OwnerRunID, link.TargetID, link.RunID); err != nil {
				return false, err
			}
			charged := logical + rowOverhead + sessionBytes
			if err = release(ctx, tx, scope, 1, charged); err != nil {
				return false, err
			}
			// The count is an accounting summary, not authority to release an
			// owner fence. Check zero/nonzero against the existing owner-keyed
			// relation in this same write, and roll everything back on mismatch.
			var pending bool
			err = tx.QueryRowContext(ctx, `UPDATE runs SET pending_remote_count=?,pending_remote_bytes=?,bytes=bytes-? WHERE scope=? AND id=?
				RETURNING EXISTS(SELECT 1 FROM run_remote_pending WHERE scope=? AND owner_run_id=?)`, count-1, total-logical, charged, scope, link.OwnerRunID, scope, link.OwnerRunID).Scan(&pending)
			if errors.Is(err, sql.ErrNoRows) {
				return false, failure("data_loss", "remote owner disappeared during resolution")
			}
			if err == nil && pending != (count > 1) {
				return false, failure("data_loss", "remote ownership count disagrees with relations")
			}
			if err == nil && detail != nil {
				detail.remaining, detail.ownerFinishedAt = count-1, finishedAt
			}
			return true, err
		}
		if current.BootID != "" || link.BootID == "" {
			return false, nil
		}
		nextBytes, err := remoteLinkBytes(link)
		if err != nil {
			return false, err
		}
		delta := nextBytes - existingBytes
		if delta <= 0 || total+delta > model.MaxRemoteRunLinksBytes {
			return false, failure("resource_exhausted", "remote control owner capacity reached")
		}
		if detail != nil {
			detail.reserveAction = "bind"
		}
		if err = reserve(ctx, tx, scope, 0, delta); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE run_remote_pending SET boot_id=?,encoded_bytes=? WHERE scope=? AND owner_run_id=? AND target_id=? AND child_run_id=?`, link.BootID, nextBytes, scope, link.OwnerRunID, link.TargetID, link.RunID); err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE runs SET pending_remote_bytes=?,bytes=bytes+? WHERE scope=? AND id=?`, total+delta, delta, scope, link.OwnerRunID)
		return true, err
	}
	if resolve {
		return false, nil
	}
	if !model.RunStatus(status).Open() {
		return false, failure("failed_precondition", "closed owner cannot reserve a new remote Run")
	}
	encoded, err := remoteLinkBytes(link)
	if err != nil {
		return false, err
	}
	logical := encoded + 1
	if count == 0 {
		logical = encoded + 2
	}
	if count == model.MaxRemoteRunLinks || total+logical > model.MaxRemoteRunLinksBytes {
		return false, failure("resource_exhausted", "remote control owner capacity reached")
	}
	if detail != nil {
		detail.reserveValidated = true
		detail.reserveAction = "insert"
	}
	charged := logical + rowOverhead + sessionBytes
	if err = reserve(ctx, tx, scope, 1, charged); err != nil {
		return false, err
	}
	inserted, err := tx.ExecContext(ctx, `INSERT INTO run_remote_pending(scope,owner_run_id,root_run_id,target_id,child_run_id,call_node_id,boot_id,encoded_bytes,owner_session_id) SELECT ?,?,?,?,?,?,?,?,session_id FROM runs WHERE scope=? AND id=?`, scope, link.OwnerRunID, link.RootRunID, link.TargetID, link.RunID, link.CallNodeID, link.BootID, encoded, scope, link.OwnerRunID)
	if err != nil {
		return false, err
	}
	if n, e := inserted.RowsAffected(); e != nil {
		return false, e
	} else if n != 1 {
		return false, failure("data_loss", "remote owner disappeared during reservation")
	}
	_, err = tx.ExecContext(ctx, `UPDATE runs SET pending_remote_count=?,pending_remote_bytes=?,bytes=bytes+? WHERE scope=? AND id=?`, count+1, total+logical, charged, scope, link.OwnerRunID)
	if err == nil && detail != nil {
		detail.inserted = true
		detail.firstOwnerRelation = count == 0
	}
	return true, err
}

type remoteRunCursor struct {
	Version int    `json:"v"`
	Scope   string `json:"scope"`
	Target  string `json:"target"`
	Owner   string `json:"owner"`
	Run     string `json:"run"`
}

func remoteScope(scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return hex.EncodeToString(sum[:])
}

func remotePageCursor(scope string, link model.RemoteRunLink) string {
	raw, _ := json.Marshal(remoteRunCursor{Version: 1, Scope: remoteScope(scope), Target: link.TargetID, Owner: link.OwnerRunID, Run: link.RunID})
	return base64.RawURLEncoding.EncodeToString(raw)
}

// The covering target index seeks directly to one target and keyset position.
// The owner join validates immutable lineage only for selected relationships.
const pendingRemoteTargetQuery = `SELECT p.owner_run_id,p.root_run_id,p.target_id,p.child_run_id,p.call_node_id,p.boot_id,p.encoded_bytes,r.root_run_id,r.target_id
FROM run_remote_pending p INDEXED BY run_remote_pending_target
JOIN runs r ON r.scope=p.scope AND r.id=p.owner_run_id
WHERE p.scope=? AND p.target_id=? AND (p.owner_run_id,p.child_run_id)>(?,?)
ORDER BY p.owner_run_id,p.child_run_id LIMIT ?`

func (s *Store) ListPendingRemoteRuns(ctx context.Context, target, cursor string, limit int) (model.RemoteRunPage, error) {
	if !textKey(target) || limit < 1 || limit > model.MaxRemoteRunPageSize {
		return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote page")
	}
	var position remoteRunCursor
	if cursor != "" {
		if len(cursor) > 16384 {
			return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote cursor")
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
		if err != nil || len(raw) > 8192 {
			return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote cursor")
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&position) != nil || decoder.Decode(new(any)) != io.EOF || position.Version != 1 || position.Target != target || !textKey(position.Owner) || !textKey(position.Run) || !sha256Hex(position.Scope) {
			return model.RemoteRunPage{}, failure("invalid_argument", "pending remote cursor identity disagrees")
		}
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RemoteRunPage, error) {
		return pendingRemotePage(ctx, tx, scope, target, cursor, limit, position)
	})
}

func pendingRemotePage(ctx context.Context, tx *sql.Tx, scope, target, cursor string, limit int, position remoteRunCursor) (model.RemoteRunPage, error) {
	if cursor != "" && position.Scope != remoteScope(scope) {
		return model.RemoteRunPage{}, failure("invalid_argument", "pending remote cursor scope disagrees")
	}
	rows, err := tx.QueryContext(ctx, pendingRemoteTargetQuery, scope, target, position.Owner, position.Run, limit+1)
	if err != nil {
		return model.RemoteRunPage{}, err
	}
	defer rows.Close()
	var links []model.RemoteRunLink
	more := false
	for rows.Next() {
		var link model.RemoteRunLink
		var ownerRoot, ownerTarget string
		var encoded int64
		if err := rows.Scan(&link.OwnerRunID, &link.RootRunID, &link.TargetID, &link.RunID, &link.CallNodeID, &link.BootID, &encoded, &ownerRoot, &ownerTarget); err != nil {
			return model.RemoteRunPage{}, err
		}
		expected, e := remoteLinkBytes(link)
		if e != nil || validateRemoteLink(link) != nil || link.TargetID != target || link.RootRunID != ownerRoot || link.TargetID == ownerTarget || encoded != expected {
			return model.RemoteRunPage{}, failure("data_loss", "remote control row lineage disagrees")
		}
		if len(links) == limit {
			more = true
			break
		}
		links = append(links, link)
	}
	if err := rows.Err(); err != nil {
		return model.RemoteRunPage{}, err
	}
	page := model.RemoteRunPage{Links: links}
	if more {
		page.Next = remotePageCursor(scope, links[len(links)-1])
	}
	return page, nil
}
