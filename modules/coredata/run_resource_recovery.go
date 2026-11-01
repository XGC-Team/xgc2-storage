package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Run recovery is a separate typed control field on the Run, never an
// interpretation of the public node outcome. An empty field means unknown.
const maxRunRecoveryBytes = 2 * model.MaxRunNodesBytes

func encodeRunRecovery(state model.RunResourceRecovery) ([]byte, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxRunRecoveryBytes {
		return nil, failure("resource_exhausted", "Run resource admissions exceed their storage budget")
	}
	return raw, nil
}

func decodeRunRecovery(raw []byte) (model.RunResourceRecovery, bool, error) {
	if len(raw) == 0 {
		return model.RunResourceRecovery{}, false, nil
	}
	if len(raw) > maxRunRecoveryBytes || !json.Valid(raw) {
		return model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource recovery fact")
	}
	var state model.RunResourceRecovery
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource recovery fact")
	}
	if state.AgentBootID == "" {
		if !state.TrackRemoteProviders || state.Resources != nil || state.Joined {
			return model.RunResourceRecovery{}, false, failure("data_loss", "invalid local Run resource coverage")
		}
	} else if !textKey(state.AgentBootID) || state.Resources == nil {
		return model.RunResourceRecovery{}, false, failure("data_loss", "invalid local Run resource coverage")
	}
	if len(state.RemoteProviders) > model.MaxRemoteRunLinks || !state.TrackRemoteProviders && len(state.RemoteProviders) != 0 {
		return model.RunResourceRecovery{}, false, failure("data_loss", "untracked remote provider selection")
	}
	selected := make(map[string]bool, len(state.RemoteProviders))
	for _, item := range state.RemoteProviders {
		key := item.Link.TargetID + "\x00" + item.Link.RunID
		if validateRemoteProviderSelection(item) != nil || selected[key] {
			return model.RunResourceRecovery{}, false, failure("data_loss", "invalid remote provider selection")
		}
		selected[key] = true
	}
	seen := make(map[string]bool, len(state.Resources))
	for _, resource := range state.Resources {
		if validateRecoveryAdmission(resource) != nil || seen[resource.ID] {
			return model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource admission")
		}
		seen[resource.ID] = true
		if resource.Join != nil && (validateRecoveryJoin(*resource.Join) != nil || resource.Join.ResourceID != resource.ID || resource.Join.NodeID != resource.NodeID || resource.Join.AgentBootID != state.AgentBootID || resource.Join.Kind != resource.Kind || resource.Kind == "uncovered-effect") {
			return model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource join")
		}
		if state.Joined && resource.Join == nil && resource.Kind != "workflow-call" {
			return model.RunResourceRecovery{}, false, failure("data_loss", "joined Run retains an unjoined resource")
		}
	}
	if state.Joined && len(state.Resources) == 0 {
		return model.RunResourceRecovery{}, false, failure("data_loss", "empty Run resource coverage has no positive join")
	}
	return state, true, nil
}

func validateRecoveryAdmission(a model.RunResourceAdmission) error {
	if !textKey(a.ID) || !textKey(a.NodeID) || (a.Kind != "process" && a.Kind != "command" && a.Kind != "workflow-call" && a.Kind != "uncovered-effect") {
		return failure("invalid_argument", "exact resource ID, node ID and supported kind are required")
	}
	return nil
}

func validateRecoveryJoin(p model.RunResourceJoin) error {
	for _, id := range []string{p.RunID, p.NodeID, p.ResourceID, p.AgentBootID, p.Incarnation, p.HostBootID} {
		if !textKey(id) {
			return failure("invalid_argument", "exact resource and process owner identities are required")
		}
	}
	if (p.Kind != "process" && p.Kind != "command") || p.PID <= 1 || p.PGID != p.PID || p.StartTicks == 0 || p.OwnerPID < 1 || p.OwnerStartTicks == 0 {
		return failure("invalid_argument", "complete kernel process identities are required")
	}
	return nil
}

func readRunRecovery(ctx context.Context, tx *sql.Tx, scope, runID string) (string, model.RunResourceRecovery, bool, error) {
	var status string
	var stored []byte
	err := tx.QueryRowContext(ctx, "SELECT status,resource_recovery_json FROM runs WHERE scope=? AND id=?", scope, runID).Scan(&status, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return "", model.RunResourceRecovery{}, false, failure("not_found", "run not found")
	}
	if err != nil {
		return "", model.RunResourceRecovery{}, false, err
	}
	state, found, err := decodeRunRecovery(stored)
	if err == nil && found {
		for _, item := range state.RemoteProviders {
			if item.Link.OwnerRunID != runID {
				return "", model.RunResourceRecovery{}, false, failure("data_loss", "selected provider belongs to a different Run")
			}
		}
		for _, resource := range state.Resources {
			if resource.Join != nil && resource.Join.RunID != runID {
				return "", model.RunResourceRecovery{}, false, failure("data_loss", "resource join belongs to a different Run")
			}
		}
	}
	return status, state, found, err
}

func writeRunRecovery(ctx context.Context, tx *sql.Tx, scope, runID string, state model.RunResourceRecovery, reserveGrowth bool) error {
	stored, err := encodeRunRecovery(state)
	if err != nil {
		return err
	}
	var oldLen int
	if err := tx.QueryRowContext(ctx, "SELECT length(resource_recovery_json) FROM runs WHERE scope=? AND id=?", scope, runID).Scan(&oldLen); err != nil {
		return err
	}
	delta := int64(len(stored) - oldLen)
	if reserveGrowth && delta > 0 {
		err = reserve(ctx, tx, scope, 0, delta)
	} else {
		err = charge(ctx, tx, scope, 0, delta)
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE runs SET resource_recovery_json=?,bytes=bytes+? WHERE scope=? AND id=?", stored, delta, scope, runID)
	return err
}

// A changed finish overwrites nodes_json. Subtract the old physical bytes in
// SQLite, without returning or decoding any detail to the batch driver.
func chargeRunFinish(ctx context.Context, tx *sql.Tx, scope, runID string, grown int64) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO core_data_usage(scope,rows,bytes) VALUES(?,0,0) ON CONFLICT(scope) DO NOTHING", scope); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE core_data_usage SET bytes=max(bytes+?-(SELECT length(nodes_json) FROM runs WHERE scope=? AND id=?),0) WHERE scope=?`, grown, scope, runID, scope)
	return err
}

// GetRunResourceRecovery selects one Run's explicit control evidence. A false
// found means no coverage was ever recorded, not that the Run owns nothing.
func (s *Store) GetRunResourceRecovery(ctx context.Context, runID string) (model.RunResourceRecovery, bool, error) {
	if !textKey(runID) {
		return model.RunResourceRecovery{}, false, failure("invalid_argument", "run identity required")
	}
	type result struct {
		state model.RunResourceRecovery
		found bool
	}
	out, err := read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (result, error) {
		_, state, found, err := readRunRecovery(ctx, tx, scope, runID)
		return result{state, found && state.AgentBootID != ""}, err
	})
	return out.state, out.found, err
}

// AdmitRunResource commits an exact owner identity before its target effect.
// A repeated identical admission is a no-op. Storage errors, including an
// unknown COMMIT disposition, must be joined before the caller starts work.
func (s *Store) AdmitRunResource(ctx context.Context, runID, bootID string, admission model.RunResourceAdmission) error {
	if !textKey(runID) || !textKey(bootID) || admission.Join != nil {
		return engine.WriteRefused(failure("invalid_argument", "exact Run and boot admission is required"))
	}
	if err := validateRecoveryAdmission(admission); err != nil {
		return engine.WriteRefused(err)
	}
	return s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		status, state, found, err := readRunRecovery(ctx, tx, s.scope, runID)
		if err != nil {
			return err
		}
		if !found || !model.RunStatus(status).Open() || state.Joined || state.AgentBootID != bootID {
			return failure("failed_precondition", "Run has no matching open resource coverage")
		}
		for _, existing := range state.Resources {
			if existing.ID == admission.ID {
				if existing.NodeID == admission.NodeID && existing.Kind == admission.Kind {
					return nil
				}
				return failure("failed_precondition", "resource identity already has a different admission")
			}
		}
		state.Resources = append(state.Resources, admission)
		return writeRunRecovery(ctx, tx, s.scope, runID, state, true)
	})
}

// JoinRunResource persists only the exact process owner's positive join. The
// source PID evidence may retire only after this durable write is accepted.
func (s *Store) JoinRunResource(ctx context.Context, proof model.RunResourceJoin) error {
	if err := validateRecoveryJoin(proof); err != nil {
		return engine.WriteRefused(err)
	}
	return s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		status, state, found, err := readRunRecovery(ctx, tx, s.scope, proof.RunID)
		if err != nil {
			return err
		}
		if !found || state.AgentBootID != proof.AgentBootID {
			return failure("failed_precondition", "Run has no matching resource coverage")
		}
		for i := range state.Resources {
			a := &state.Resources[i]
			if a.ID != proof.ResourceID {
				continue
			}
			if a.NodeID != proof.NodeID || a.Kind != proof.Kind || a.Kind == "uncovered-effect" {
				return failure("failed_precondition", "join does not match the admitted process")
			}
			if a.Join != nil {
				if *a.Join == proof {
					return nil
				}
				return failure("failed_precondition", "resource already has a different join")
			}
			if !model.RunStatus(status).Open() && model.RunStatus(status) != model.RunInterrupted {
				return failure("failed_precondition", "a normally finished Run cannot acquire a late join")
			}
			if state.Joined {
				return failure("failed_precondition", "Run is already joined")
			}
			a.Join = &proof
			return writeRunRecovery(ctx, tx, s.scope, proof.RunID, state, false)
		}
		return failure("failed_precondition", "resource was not admitted")
	})
}

// SealRunResourceRecovery is a positive exact-Run receipt. Incoming retention
// is deliberately not an outbound dependency; any unresolved outbound child
// or missing/unknown resource evidence still refuses the seal.
func (s *Store) SealRunResourceRecovery(ctx context.Context, runID, bootID string) error {
	if !textKey(runID) || !textKey(bootID) {
		return engine.WriteRefused(failure("invalid_argument", "exact Run and Agent boot are required"))
	}
	return s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		joined, err := sealRunResourceRecovery(ctx, tx, s.scope, runID, bootID)
		if err != nil {
			return err
		}
		if !joined {
			return failure("failed_precondition", "Run resource coverage is unknown or incomplete")
		}
		return nil
	})
}

// Called on one exact newly interrupted Run. Empty bootID is internal to the
// interruption transaction; it selects the already stamped original boot.
func sealRunResourceRecovery(ctx context.Context, tx *sql.Tx, scope, runID, bootID string) (bool, error) {
	status, state, found, err := readRunRecovery(ctx, tx, scope, runID)
	if err != nil {
		return false, err
	}
	if !found || state.AgentBootID == "" || model.RunStatus(status) != model.RunInterrupted || (bootID != "" && state.AgentBootID != bootID) {
		return false, nil
	}
	if state.Joined {
		return true, nil
	}
	if len(state.Resources) == 0 {
		return false, nil
	}
	for _, a := range state.Resources {
		// Every remote child of a workflow.call must first acquire a durable
		// outbound relation. A local child is separately recorded as uncovered
		// until its own exact join has a durable owner.
		if a.Kind != "workflow-call" && a.Join == nil {
			return false, nil
		}
	}
	var pending bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM run_remote_pending WHERE scope=? AND owner_run_id=?)", scope, runID).Scan(&pending); err != nil {
		return false, err
	}
	if pending {
		return false, nil
	}
	state.Joined = true
	if err := writeRunRecovery(ctx, tx, scope, runID, state, false); err != nil {
		return false, err
	}
	return true, nil
}
