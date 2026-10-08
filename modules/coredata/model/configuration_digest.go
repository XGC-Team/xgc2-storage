package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
func ConfigurationContentDigest(payload, manifest []byte, refs []ConfigurationReference) (string, error) {
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
	return SnapshotDigest(payload, manifest, out)
}
