package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"
)

// Full plan identity includes operation and every typed payload field, including
// exact frozen bytes, guards, allocation IDs and ordered changes/references.
// The transport RequestID is outside this plan and never the product key.
func ConfigurationCreatePlanDigest(r ConfigurationResourceCreate) (string, error) {
	return configurationPlanDigest(ResourceCreateOperation, r)
}
func ConfigurationCommitPlanDigest(r ConfigurationResourceCommit) (string, error) {
	return configurationPlanDigest(ResourceCommitOperation, r)
}
func configurationPlanDigest(operation string, r any) (string, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(r); err != nil {
		return "", err
	}
	if b.Len() > MaxRequestBytes {
		return "", fmt.Errorf("configuration plan byte limit exceeded")
	}
	h := sha256.New()
	h.Write([]byte(operation + "\x00"))
	h.Write(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Tracking stores a logical address; only pinned mode carries immutable facts.
// This check is shared by snapshot hashing and writer plan admission, including
// noop/location-only plans whose proposed immutable bytes are not published.
func ValidateConfigurationReferenceMode(ref ConfigurationReference) error {
	switch ref.Mode {
	case "tracking":
		if ref.TargetCommitID != "" || ref.TargetVersion != "" || ref.TargetRootDigest != "" {
			return fmt.Errorf("tracking reference pin fields must all be empty")
		}
	case "pinned":
		if err := ValidateConfigurationIdentifier(ref.TargetCommitID); err != nil {
			return err
		}
		if err := ValidateConfigurationDecimal(ref.TargetVersion, false); err != nil {
			return err
		}
		root, err := hex.DecodeString(ref.TargetRootDigest)
		if err != nil || len(root) != sha256.Size || hex.EncodeToString(root) != ref.TargetRootDigest {
			return fmt.Errorf("pinned reference requires a canonical SHA256 root digest")
		}
	default:
		return fmt.Errorf("reference mode must be tracking or pinned")
	}
	return nil
}

// ConfigurationContentDigest shares the exact frozen-byte snapshot algorithm
// with namespace operations. References are a complete set sorted by Slot;
// caller slices are never mutated. This digest is distinct from business root.
func ConfigurationContentDigest(payload, manifest, identity, summary []byte, refs []ConfigurationReference, associations []ConfigurationAgentAssociation) (string, error) {
	if len(payload) > MaxConfigurationPayloadBytes || len(manifest) > MaxConfigurationManifestBytes || len(refs) > MaxConfigurationReferences {
		return "", fmt.Errorf("configuration snapshot bound exceeded")
	}
	out := make([]Reference, 0, len(refs))
	seen := map[string]bool{}
	for _, ref := range refs {
		if err := ValidateConfigurationReferenceMode(ref); err != nil {
			return "", err
		}
		if ValidateConfigurationName(ConfigurationSlotName, ref.Slot) != nil || seen[ref.Slot] {
			return "", fmt.Errorf("invalid or duplicate reference slot")
		}
		seen[ref.Slot] = true
		var b bytes.Buffer
		e := json.NewEncoder(&b)
		e.SetEscapeHTML(false)
		if err := e.Encode(ref); err != nil {
			return "", err
		}
		out = append(out, Reference{Slot: ref.Slot, TargetDomain: ref.TargetDomain, TargetResourceID: ref.TargetResourceID, TargetCommitID: ref.TargetCommitID, Body: bytes.TrimSuffix(b.Bytes(), []byte("\n"))})
	}
	return SnapshotDigest(payload, manifest, identity, summary, out, associations)
}

// CanonicalConfigurationAssociations validates and sorts the derived facts
// without changing the caller's slice. Empty and nil are equivalent for hash
// computation; write admission separately requires an explicit empty set.
func CanonicalConfigurationAssociations(facts []ConfigurationAgentAssociation) ([]ConfigurationAgentAssociation, error) {
	if len(facts) > MaxConfigurationAssociations {
		return nil, fmt.Errorf("agent association count exceeded")
	}
	out := append([]ConfigurationAgentAssociation{}, facts...)
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.AgentID != b.AgentID {
			return a.AgentID < b.AgentID
		}
		if a.SlotID != b.SlotID {
			return a.SlotID < b.SlotID
		}
		return a.RobotResourceID < b.RobotResourceID
	})
	for i, fact := range out {
		if len(fact.AgentID) == 0 || len(fact.AgentID) > 128 || !utf8.ValidString(fact.AgentID) || bytes.ContainsAny([]byte(fact.AgentID), "\x00\r\n") {
			return nil, fmt.Errorf("invalid agent association identity")
		}
		switch fact.Kind {
		case AgentAssociationRobotOwner, AgentAssociationWorld:
			if fact.SlotID != "" || fact.RobotResourceID != "" {
				return nil, fmt.Errorf("unexpected association target")
			}
		case AgentAssociationSlotHost:
			if ValidateConfigurationIdentifier(fact.SlotID) != nil || fact.RobotResourceID != "" {
				return nil, fmt.Errorf("invalid slot association")
			}
		case AgentAssociationOnboardSlot:
			if ValidateConfigurationIdentifier(fact.SlotID) != nil || fact.RobotResourceID != "" && ValidateConfigurationIdentifier(fact.RobotResourceID) != nil {
				return nil, fmt.Errorf("invalid onboard association")
			}
		default:
			return nil, fmt.Errorf("unknown agent association kind")
		}
		if i > 0 && out[i-1] == fact {
			return nil, fmt.Errorf("duplicate agent association")
		}
	}
	return out, nil
}

func ConfigurationAssociationDigest(facts []ConfigurationAgentAssociation) (string, error) {
	canonical, err := CanonicalConfigurationAssociations(facts)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("coredata.agent-associations.v1\x00"))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// The projection is bounded independently of the full frozen document.
func ValidateConfigurationCatalogProjection(identity, summary []byte) error {
	if len(identity) > MaxConfigurationIdentityBytes || len(summary) > MaxConfigurationCatalogSummaryBytes || !object(identity) || !object(summary) {
		return fmt.Errorf("bounded identity and catalog summary JSON objects required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(summary, &fields); err != nil {
		return fmt.Errorf("invalid catalog summary")
	}
	if raw, ok := fields["_agentSyncDigest"]; ok {
		var digest string
		if json.Unmarshal(raw, &digest) != nil {
			return fmt.Errorf("invalid Agent synchronization digest")
		}
		decoded, err := hex.DecodeString(digest)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
			return fmt.Errorf("invalid Agent synchronization digest")
		}
	}
	return nil
}

// CatalogDigest lets a thin read verify its immutable projection without
// reading the document. ContentDigest independently covers the same bytes.
func ConfigurationCatalogDigest(contentDigest string, identity, summary []byte) (string, error) {
	decoded, err := hex.DecodeString(contentDigest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != contentDigest {
		return "", fmt.Errorf("canonical content digest required")
	}
	if err := ValidateConfigurationCatalogProjection(identity, summary); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("coredata.catalog.projection.v1\x00"))
	for _, part := range [][]byte{[]byte(contentDigest), identity, summary} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		h.Write(size[:])
		h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
