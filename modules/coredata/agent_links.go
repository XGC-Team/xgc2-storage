package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

var (
	// These two actual transaction refusals are distinguishable with errors.Is
	// and also carry an api.Error with code conflict.
	ErrAgentLinkVersionConflict     = errors.New("agent link version conflict")
	ErrAgentLinkFingerprintConflict = errors.New("agent link fingerprint conflict")
	errAgentLinkNoChange            = &api.Error{Code: "agent_link_no_change", Message: "agent link already has these facts"}
)

func agentLinkConflict(which error, message string) error {
	return errors.Join(which, failure("conflict", message))
}

// ListAgentLinks returns one coherent static enrollment projection, including
// the identity needed to form an absent-ID guard from this complete list.
func (s *Store) ListAgentLinks(ctx context.Context) (model.AgentLinkList, error) {
	if s == nil || ctx == nil {
		return model.AgentLinkList{}, failure("invalid_argument", "agent link owner and context required")
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.AgentLinkList, error) {
		out := model.AgentLinkList{Identity: s.agentLinkIdentity, Records: make([]model.AgentLinkStored, 0)}
		rows, err := tx.QueryContext(ctx, `SELECT agent_id,version,enrollment,fingerprint,CASE WHEN length(facts_json)<=? THEN facts_json ELSE NULL END,length(facts_json),created_at,updated_at,record_bytes FROM core_agent_links WHERE scope=? ORDER BY agent_id`, model.MaxAgentLinkRecordBytes, scope)
		if err != nil {
			return model.AgentLinkList{}, err
		}
		defer rows.Close()
		var total, selectedBytes int64
		for rows.Next() {
			var id, enrollment string
			var version, factsLength, created, updated, recordBytes int64
			var fingerprint sql.NullString
			var factsJSON []byte
			if err := rows.Scan(&id, &version, &enrollment, &fingerprint, &factsJSON, &factsLength, &created, &updated, &recordBytes); err != nil {
				return model.AgentLinkList{}, err
			}
			if len(out.Records) >= model.MaxAgentLinkRecords || factsLength < 0 || factsLength > model.MaxAgentLinkRecordBytes || recordBytes <= 0 || recordBytes > model.MaxAgentLinkRecordBytes {
				return model.AgentLinkList{}, failure("data_loss", "invalid agent link record bounds")
			}
			total += recordBytes + int64(len(id))
			selectedBytes += factsLength + int64(len(id)+len(enrollment)+len(fingerprint.String))
			if total > model.MaxAgentLinkBytes || selectedBytes > model.MaxAgentLinkBytes {
				return model.AgentLinkList{}, failure("data_loss", "invalid agent link collection bounds")
			}
			pin := ""
			if fingerprint.Valid {
				pin = fingerprint.String
				if pin == "" {
					return model.AgentLinkList{}, failure("data_loss", "invalid agent link metadata")
				}
			}
			if !validAgentLinkIdentity(id, enrollment, pin) || version <= 0 || created == 0 || updated == 0 || !utf8.Valid(factsJSON) {
				return model.AgentLinkList{}, failure("data_loss", "invalid agent link metadata")
			}
			facts, err := decodeAgentLinkFacts(factsJSON)
			if err != nil {
				return model.AgentLinkList{}, failure("data_loss", "invalid agent link facts")
			}
			record := model.AgentLinkRecord{AgentID: id, Enrollment: enrollment, PublicKeyFingerprint: pin, AgentLinkFacts: facts}
			out.Records = append(out.Records, agentLinkStored(s.agentLinkIdentity, record, version, created, updated))
		}
		if err := rows.Err(); err != nil {
			return model.AgentLinkList{}, err
		}
		return out, nil
	})
}

// Decode the current fixed projection once, with exact fields and duplicate
// rejection. Nested manifest values remain owned raw bytes for Core's strict
// domain decode; their keys are checked without another encoding.
func decodeAgentLinkFacts(raw []byte) (model.AgentLinkFacts, error) {
	var facts model.AgentLinkFacts
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return facts, errors.New("facts must be an object")
	}
	seen := make(map[string]bool, 8)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return facts, err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return facts, errors.New("duplicate facts field")
		}
		seen[key] = true
		var stringTarget *string
		var uintTarget *uint32
		switch key {
		case "protocolMajor":
			uintTarget = &facts.ProtocolMajor
		case "protocolMinor":
			uintTarget = &facts.ProtocolMinor
		case "displayName":
			stringTarget = &facts.DisplayName
		case "buildIdentity":
			stringTarget = &facts.BuildIdentity
		case "advertisedManagementEndpoint":
			stringTarget = &facts.AdvertisedManagementEndpoint
		case "persistentAccessToken":
			stringTarget = &facts.PersistentAccessToken
		case "capabilityManifest":
			if err := d.Decode(&facts.CapabilityManifest); err != nil {
				return facts, err
			}
		case "capabilities":
			start, err := d.Token()
			if err != nil {
				return facts, err
			}
			if start != nil {
				if start != json.Delim('[') {
					return facts, errors.New("invalid capabilities")
				}
				facts.Capabilities = make([]string, 0)
				for d.More() {
					value, err := d.Token()
					text, ok := value.(string)
					if err != nil || !ok {
						return facts, errors.New("invalid capability")
					}
					facts.Capabilities = append(facts.Capabilities, text)
				}
				end, err := d.Token()
				if err != nil || end != json.Delim(']') {
					return facts, errors.New("invalid capabilities")
				}
			}
		default:
			return facts, errors.New("unknown facts field")
		}
		if stringTarget != nil || uintTarget != nil {
			value, err := d.Token()
			if err != nil {
				return facts, err
			}
			if stringTarget != nil {
				text, ok := value.(string)
				if !ok {
					return facts, errors.New("invalid facts text")
				}
				*stringTarget = text
			} else {
				number, ok := value.(json.Number)
				if !ok {
					return facts, errors.New("invalid protocol version")
				}
				n, err := strconv.ParseUint(string(number), 10, 32)
				if err != nil {
					return facts, err
				}
				*uintTarget = uint32(n)
			}
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || len(seen) != 8 {
		return facts, errors.New("incomplete facts object")
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		return facts, errors.New("trailing facts value")
	}
	manifest := json.NewDecoder(bytes.NewReader(facts.CapabilityManifest))
	manifest.UseNumber()
	value, err := agentJSONValue(manifest)
	if err != nil {
		return facts, err
	}
	if _, ok := value.(map[string]any); !ok {
		return facts, errors.New("manifest must be an object")
	}
	if _, err = manifest.Token(); !errors.Is(err, io.EOF) {
		return facts, errors.New("trailing manifest value")
	}
	return facts, nil
}

// WriteAgentLink performs exact Agent-version and nonempty fingerprint guards
// in the existing owner transaction. A matching no-op rolls back without a
// durable commit. It does not use a scope revision or a mutation receipt.
func (s *Store) WriteAgentLink(ctx context.Context, q model.AgentLinkWrite) (model.AgentLinkWriteResult, error) {
	if s == nil || ctx == nil {
		return model.AgentLinkWriteResult{}, failure("invalid_argument", "agent link owner and context required")
	}
	if !validAgentLinkIdentity(q.Record.AgentID, q.Record.Enrollment, q.Record.PublicKeyFingerprint) {
		return model.AgentLinkWriteResult{}, failure("invalid_argument", "invalid agent link identity or enrollment")
	}
	if q.Expected.Identity != s.agentLinkIdentity || q.Expected.AgentID != q.Record.AgentID {
		return model.AgentLinkWriteResult{}, failure("failed_precondition", "agent link owner or identity changed")
	}
	if len(q.Expected.Version) > 19 {
		return model.AgentLinkWriteResult{}, failure("invalid_argument", "canonical agent link version required")
	}
	version, err := strconv.ParseInt(q.Expected.Version, 10, 64)
	if err != nil || version < 0 || strconv.FormatInt(version, 10) != q.Expected.Version {
		return model.AgentLinkWriteResult{}, failure("invalid_argument", "canonical agent link version required")
	}
	if err := agentLinkInputBound(q.Record); err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	if err := canceledWrite(ctx); err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	var out model.AgentLinkWriteResult
	err = s.db.WriteWithInput(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) error {
		if err := input.Own(ctx, func() {
			q.Record.CapabilityManifest = bytes.Clone(q.Record.CapabilityManifest)
			q.Record.Capabilities = slices.Clone(q.Record.Capabilities)
		}); err != nil {
			return err
		}
		var e error
		out, e = agentLinkWrite(ctx, tx, s.scope, s.agentLinkIdentity, version, q.Record)
		return e
	})
	if errors.Is(err, errAgentLinkNoChange) && engine.DefinitelyNotCommitted(err) {
		return out, nil
	}
	if err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	return out, nil
}

func validAgentLinkIdentity(id, enrollment, fingerprint string) bool {
	if !textKey(id) || (enrollment != "known" && enrollment != "enrolled" && enrollment != "revoked") {
		return false
	}
	if fingerprint != "" && !sha256Hex(fingerprint) {
		return false
	}
	return enrollment != "enrolled" || fingerprint != ""
}

// Bound caller-owned input before admission or allocation. Encoding happens
// inside the existing writer admission, so at most one encoded record is in
// flight at this owner; waiting callers do not retain additional encoded copies.
func agentLinkInputBound(record model.AgentLinkRecord) error {
	f := record.AgentLinkFacts
	if len(f.CapabilityManifest) > model.MaxAgentLinkRecordBytes || len(f.Capabilities) > model.MaxAgentLinkRecordBytes/3 {
		return failure("resource_exhausted", "agent link record too large")
	}
	total := len(f.CapabilityManifest)
	for _, text := range []string{record.AgentID, record.Enrollment, record.PublicKeyFingerprint, f.DisplayName, f.BuildIdentity, f.AdvertisedManagementEndpoint, f.PersistentAccessToken} {
		if len(text) > model.MaxAgentLinkRecordBytes-total {
			return failure("resource_exhausted", "agent link record too large")
		}
		if !utf8.ValidString(text) {
			return failure("invalid_argument", "agent link text is not UTF-8")
		}
		total += len(text)
	}
	for _, text := range f.Capabilities {
		if len(text) > model.MaxAgentLinkRecordBytes-total {
			return failure("resource_exhausted", "agent link record too large")
		}
		if !utf8.ValidString(text) {
			return failure("invalid_argument", "agent link text is not UTF-8")
		}
		total += len(text)
	}
	return nil
}

func agentLinkWrite(ctx context.Context, tx *sql.Tx, scope string, identity model.AgentLinkIdentity, expected int64, record model.AgentLinkRecord) (model.AgentLinkWriteResult, error) {
	var oldVersion, oldCreated, oldUpdated, oldBytes int64
	var oldEnrollment string
	var oldFingerprint sql.NullString
	var oldFacts []byte
	err := tx.QueryRowContext(ctx, `SELECT version,enrollment,fingerprint,CASE WHEN length(facts_json)<=? THEN facts_json ELSE NULL END,created_at,updated_at,record_bytes FROM core_agent_links WHERE scope=? AND agent_id=?`, model.MaxAgentLinkRecordBytes, scope, record.AgentID).Scan(&oldVersion, &oldEnrollment, &oldFingerprint, &oldFacts, &oldCreated, &oldUpdated, &oldBytes)
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.AgentLinkWriteResult{}, err
	}
	if found && (oldVersion <= 0 || oldCreated == 0 || oldUpdated == 0 || oldBytes <= 0 || oldBytes > model.MaxAgentLinkRecordBytes || oldFacts == nil) {
		return model.AgentLinkWriteResult{}, failure("data_loss", "invalid agent link stored facts")
	}
	oldPin := ""
	if oldFingerprint.Valid {
		oldPin = oldFingerprint.String
		if oldPin == "" {
			return model.AgentLinkWriteResult{}, failure("data_loss", "invalid agent link stored identity")
		}
	}
	if found && !validAgentLinkIdentity(record.AgentID, oldEnrollment, oldPin) {
		return model.AgentLinkWriteResult{}, failure("data_loss", "invalid agent link stored identity")
	}
	if (!found && expected != 0) || (found && expected != oldVersion) {
		return model.AgentLinkWriteResult{}, agentLinkConflict(ErrAgentLinkVersionConflict, "agent link version changed")
	}
	if record.PublicKeyFingerprint != "" && (!found || oldPin != record.PublicKeyFingerprint) {
		var owner string
		err = tx.QueryRowContext(ctx, `SELECT agent_id FROM core_agent_links INDEXED BY core_agent_links_fingerprint WHERE scope=? AND fingerprint=?`, scope, record.PublicKeyFingerprint).Scan(&owner)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return model.AgentLinkWriteResult{}, err
		}
		if err == nil && owner != record.AgentID {
			return model.AgentLinkWriteResult{}, agentLinkConflict(ErrAgentLinkFingerprintConflict, "agent link fingerprint belongs to another agent")
		}
	}
	manifest, err := canonicalAgentManifest(record.CapabilityManifest)
	if err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	record.CapabilityManifest = manifest
	record.Capabilities = slices.Clone(record.Capabilities)
	facts, err := json.Marshal(record.AgentLinkFacts)
	if err != nil {
		return model.AgentLinkWriteResult{}, failure("invalid_argument", "invalid agent link facts")
	}
	if found && oldEnrollment == record.Enrollment && oldPin == record.PublicKeyFingerprint && bytes.Equal(oldFacts, facts) {
		return model.AgentLinkWriteResult{Stored: agentLinkStored(identity, record, oldVersion, oldCreated, oldUpdated)}, errAgentLinkNoChange
	}
	if found && oldVersion == math.MaxInt64 {
		return model.AgentLinkWriteResult{}, failure("resource_exhausted", "agent link version exhausted")
	}
	now := time.Now().UTC()
	created := now.UnixNano()
	if found {
		created = oldCreated
	}
	updated := now.UnixNano()
	// The old collection's quota charged the complete canonical static record
	// plus its key. Facts and the disjoint scalar envelope each encode once;
	// merging their nonempty JSON objects changes the length by exactly one.
	envelope, err := json.Marshal(struct {
		AgentID              string    `json:"agentId"`
		Enrollment           string    `json:"enrollment"`
		PublicKeyFingerprint string    `json:"publicKeyFingerprint"`
		CreatedAt            time.Time `json:"createdAt"`
		UpdatedAt            time.Time `json:"updatedAt"`
	}{record.AgentID, record.Enrollment, record.PublicKeyFingerprint, instant(created), instant(updated)})
	if err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	recordBytes := int64(len(facts) + len(envelope) - 1)
	if recordBytes > model.MaxAgentLinkRecordBytes {
		return model.AgentLinkWriteResult{}, failure("resource_exhausted", "agent link record too large")
	}
	rowDelta, byteDelta := int64(0), recordBytes-oldBytes
	if !found {
		rowDelta, byteDelta = 1, recordBytes+int64(len(record.AgentID))
	}
	if rowDelta != 0 || byteDelta != 0 {
		if !found {
			if _, err = tx.ExecContext(ctx, `INSERT INTO core_data_usage(scope,rows,bytes) VALUES(?,0,0) ON CONFLICT(scope) DO NOTHING`, scope); err != nil {
				return model.AgentLinkWriteResult{}, err
			}
		}
		quota, err := tx.ExecContext(ctx, `UPDATE core_data_usage SET agent_link_rows=agent_link_rows+?,agent_link_bytes=agent_link_bytes+? WHERE scope=? AND agent_link_rows+? BETWEEN 0 AND ? AND agent_link_bytes+? BETWEEN 0 AND ?`, rowDelta, byteDelta, scope, rowDelta, model.MaxAgentLinkRecords, byteDelta, model.MaxAgentLinkBytes)
		if err != nil {
			return model.AgentLinkWriteResult{}, err
		}
		if n, e := quota.RowsAffected(); e != nil {
			return model.AgentLinkWriteResult{}, e
		} else if n != 1 {
			return model.AgentLinkWriteResult{}, failure("resource_exhausted", "agent link quota reached")
		}
	}
	var fingerprint any
	if record.PublicKeyFingerprint != "" {
		fingerprint = record.PublicKeyFingerprint
	}
	newVersion := oldVersion + 1
	if found {
		var res sql.Result
		res, err = tx.ExecContext(ctx, `UPDATE core_agent_links SET version=?,enrollment=?,fingerprint=?,facts_json=?,updated_at=?,record_bytes=? WHERE scope=? AND agent_id=? AND version=?`, newVersion, record.Enrollment, fingerprint, facts, updated, recordBytes, scope, record.AgentID, oldVersion)
		if err == nil {
			var n int64
			n, err = res.RowsAffected()
			if err == nil && n != 1 {
				return model.AgentLinkWriteResult{}, failure("data_loss", "agent link update lost its row")
			}
		}
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO core_agent_links(scope,agent_id,version,enrollment,fingerprint,facts_json,created_at,updated_at,record_bytes) VALUES(?,?,?,?,?,?,?,?,?)`, scope, record.AgentID, newVersion, record.Enrollment, fingerprint, facts, created, updated, recordBytes)
	}
	if err != nil {
		return model.AgentLinkWriteResult{}, err
	}
	return model.AgentLinkWriteResult{Stored: agentLinkStored(identity, record, newVersion, created, updated), Changed: true}, nil
}

func agentLinkStored(identity model.AgentLinkIdentity, record model.AgentLinkRecord, version, created, updated int64) model.AgentLinkStored {
	return model.AgentLinkStored{Record: record, Guard: model.AgentLinkGuard{Identity: identity, AgentID: record.AgentID, Version: strconv.FormatInt(version, 10)}, CreatedAt: instant(created), UpdatedAt: instant(updated)}
}

// Canonicalize the manifest once, rejecting duplicate keys at every level.
func canonicalAgentManifest(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > model.MaxAgentLinkRecordBytes || !utf8.Valid(raw) {
		return nil, failure("invalid_argument", "agent link capability manifest must be one object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	value, err := agentJSONValue(d)
	if err != nil {
		return nil, failure("invalid_argument", "invalid agent link capability manifest")
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, failure("invalid_argument", "agent link capability manifest must be one object")
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		return nil, failure("invalid_argument", "invalid agent link capability manifest")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, failure("invalid_argument", "invalid agent link capability manifest")
	}
	return encoded, nil
}

func agentJSONValue(d *json.Decoder) (any, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for d.More() {
			keyToken, e := d.Token()
			if e != nil {
				return nil, e
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("invalid object key")
			}
			if _, exists := object[key]; exists {
				return nil, errors.New("duplicate object key")
			}
			value, e := agentJSONValue(d)
			if e != nil {
				return nil, e
			}
			object[key] = value
		}
		end, e := d.Token()
		if e != nil || end != json.Delim('}') {
			return nil, errors.New("invalid object")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for d.More() {
			value, e := agentJSONValue(d)
			if e != nil {
				return nil, e
			}
			array = append(array, value)
		}
		end, e := d.Token()
		if e != nil || end != json.Delim(']') {
			return nil, errors.New("invalid array")
		}
		return array, nil
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}
