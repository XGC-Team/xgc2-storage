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

// The existing Run detail holds low-frequency resource ownership, alongside
// its public node detail. Older rows have no marker and remain unknown.
const resourceRecoveryFormat = "xgc2-run-resource-recovery-v1"
const maxStoredRunNodesBytes = 2 * model.MaxRunNodesBytes

// Ordinary historical node payloads are always gzip streams produced by
// encodeRunPayload, regardless of their JSON contents. This non-gzip storage
// prefix is written only at the new Run admission cut. Consequently an old
// caller's arbitrary Nodes JSON cannot impersonate admitted coverage.
var resourceRecoveryPrefix = []byte("XGC2-RUN-RESOURCES\x00\x01")

type resourceRecoveryNodes struct {
	Format   string                    `json:"$runResourceRecovery"`
	Nodes    json.RawMessage           `json:"nodes,omitempty"`
	Recovery model.RunResourceRecovery `json:"recovery"`
}

func encodeRecoveryNodes(nodes []byte, recovery model.RunResourceRecovery) ([]byte, error) {
	if len(nodes) > model.MaxRunNodesBytes || len(nodes) > 0 && !json.Valid(nodes) {
		return nil, failure("resource_exhausted", "Run node detail exceeds its existing limit")
	}
	raw, err := json.Marshal(resourceRecoveryNodes{Format: resourceRecoveryFormat, Nodes: nodes, Recovery: recovery})
	if err != nil {
		return nil, err
	}
	if len(raw) > maxStoredRunNodesBytes {
		return nil, failure("resource_exhausted", "Run resource admissions exceed their storage budget")
	}
	return raw, nil
}

func encodeRecoveryPayload(raw []byte) ([]byte, error) {
	compressed, err := encodeRunPayload(raw)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), resourceRecoveryPrefix...), compressed...), nil
}

func decodeStoredNodes(stored []byte) ([]byte, model.RunResourceRecovery, bool, error) {
	if !bytes.HasPrefix(stored, resourceRecoveryPrefix) {
		raw, err := decodeRunPayload(stored, model.MaxRunNodesBytes)
		return raw, model.RunResourceRecovery{}, false, err
	}
	raw, err := decodeRunPayload(stored[len(resourceRecoveryPrefix):], maxStoredRunNodesBytes)
	if err != nil {
		return nil, model.RunResourceRecovery{}, false, err
	}
	nodes, recovery, found, err := decodeRecoveryNodes(raw)
	if err == nil && !found {
		return nil, model.RunResourceRecovery{}, false, failure("data_loss", "missing Run resource recovery fact")
	}
	return nodes, recovery, found, err
}

// decodeRecoveryNodes distinguishes a positively marked new Run from every
// historical value; an absent marker is never interpreted as empty coverage.
func decodeRecoveryNodes(raw []byte) ([]byte, model.RunResourceRecovery, bool, error) {
	if len(raw) == 0 || !isObject(raw) {
		return nil, model.RunResourceRecovery{}, false, nil
	}
	var header map[string]json.RawMessage
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run node detail")
	}
	if _, exists := header["$runResourceRecovery"]; !exists {
		return nil, model.RunResourceRecovery{}, false, nil
	}
	var envelope resourceRecoveryNodes
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil || envelope.Format != resourceRecoveryFormat || !textKey(envelope.Recovery.AgentBootID) || envelope.Recovery.Resources == nil || len(envelope.Nodes) > model.MaxRunNodesBytes {
		return nil, model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource recovery fact")
	}
	seen := make(map[string]bool, len(envelope.Recovery.Resources))
	for _, resource := range envelope.Recovery.Resources {
		if validateRecoveryAdmission(resource) != nil || seen[resource.ID] {
			return nil, model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource admission")
		}
		seen[resource.ID] = true
		if resource.Join != nil && (validateRecoveryJoin(*resource.Join) != nil || resource.Join.ResourceID != resource.ID || resource.Join.NodeID != resource.NodeID || resource.Join.AgentBootID != envelope.Recovery.AgentBootID || resource.Join.Kind != resource.Kind || resource.Kind == "uncovered-effect") {
			return nil, model.RunResourceRecovery{}, false, failure("data_loss", "invalid Run resource join")
		}
		if envelope.Recovery.Joined && resource.Join == nil {
			return nil, model.RunResourceRecovery{}, false, failure("data_loss", "joined Run retains an unjoined resource")
		}
	}
	if envelope.Recovery.Joined && len(envelope.Recovery.Resources) == 0 {
		return nil, model.RunResourceRecovery{}, false, failure("data_loss", "empty Run resource coverage has no positive join")
	}
	return envelope.Nodes, envelope.Recovery, true, nil
}

func validateRecoveryAdmission(a model.RunResourceAdmission) error {
	if !textKey(a.ID) || !textKey(a.NodeID) || (a.Kind != "process" && a.Kind != "command" && a.Kind != "uncovered-effect") {
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

func readRecoveryNodes(ctx context.Context, tx *sql.Tx, scope, runID string) (string, []byte, model.RunResourceRecovery, bool, error) {
	var status string
	var stored []byte
	err := tx.QueryRowContext(ctx, "SELECT status,nodes_json FROM runs WHERE scope=? AND id=?", scope, runID).Scan(&status, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, model.RunResourceRecovery{}, false, failure("not_found", "run not found")
	}
	if err != nil {
		return "", nil, model.RunResourceRecovery{}, false, err
	}
	nodes, state, found, err := decodeStoredNodes(stored)
	if err == nil && found {
		for _, resource := range state.Resources {
			if resource.Join != nil && resource.Join.RunID != runID {
				return "", nil, model.RunResourceRecovery{}, false, failure("data_loss", "resource join belongs to a different Run")
			}
		}
	}
	return status, nodes, state, found, err
}

func writeRecoveryNodes(ctx context.Context, tx *sql.Tx, scope, runID string, nodes []byte, state model.RunResourceRecovery, reserveGrowth bool) error {
	raw, err := encodeRecoveryNodes(nodes, state)
	if err != nil {
		return err
	}
	stored, err := encodeRecoveryPayload(raw)
	if err != nil {
		return err
	}
	var oldLen int
	if err := tx.QueryRowContext(ctx, "SELECT length(nodes_json) FROM runs WHERE scope=? AND id=?", scope, runID).Scan(&oldLen); err != nil {
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
	_, err = tx.ExecContext(ctx, "UPDATE runs SET nodes_json=?,bytes=bytes+? WHERE scope=? AND id=?", stored, delta, scope, runID)
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
		_, _, state, found, err := readRecoveryNodes(ctx, tx, scope, runID)
		return result{state, found}, err
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
		status, nodes, state, found, err := readRecoveryNodes(ctx, tx, s.scope, runID)
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
		return writeRecoveryNodes(ctx, tx, s.scope, runID, nodes, state, true)
	})
}

// JoinRunResource persists only the exact process owner's positive join. The
// source PID evidence may retire only after this durable write is accepted.
func (s *Store) JoinRunResource(ctx context.Context, proof model.RunResourceJoin) error {
	if err := validateRecoveryJoin(proof); err != nil {
		return engine.WriteRefused(err)
	}
	return s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		status, nodes, state, found, err := readRecoveryNodes(ctx, tx, s.scope, proof.RunID)
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
			return writeRecoveryNodes(ctx, tx, s.scope, proof.RunID, nodes, state, false)
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
	status, nodes, state, found, err := readRecoveryNodes(ctx, tx, scope, runID)
	if err != nil {
		return false, err
	}
	if !found || model.RunStatus(status) != model.RunInterrupted || (bootID != "" && state.AgentBootID != bootID) {
		return false, nil
	}
	if state.Joined {
		return true, nil
	}
	if len(state.Resources) == 0 {
		return false, nil
	}
	for _, a := range state.Resources {
		if a.Join == nil {
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
	if err := writeRecoveryNodes(ctx, tx, scope, runID, nodes, state, false); err != nil {
		return false, err
	}
	return true, nil
}
