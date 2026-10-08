// SnapshotDigest pins typed JSON and sorted reference data without any SQL dependency.
package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

func object(raw json.RawMessage) bool {
	return len(raw) > 0 && json.Valid(raw) && bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{"))
}
func digest(v any) string {
	var buffer bytes.Buffer
	e := json.NewEncoder(&buffer)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	h := sha256.Sum256(bytes.TrimSuffix(buffer.Bytes(), []byte("\n")))
	return hex.EncodeToString(h[:])
}

// SnapshotDigest lets Core construct exact immutable data pins using this new
// model. It does not validate a product's typed schema or freeze execution plans.
func SnapshotDigest(payload, manifest json.RawMessage, references []Reference) (string, error) {
	if !object(payload) || !object(manifest) {
		return "", fmt.Errorf("invalid snapshot object")
	}
	references = append([]Reference{}, references...)
	sort.Slice(references, func(i, j int) bool { return references[i].Slot < references[j].Slot })
	return digest(struct {
		Payload, Manifest json.RawMessage
		References        []Reference
	}{payload, manifest, references}), nil
}
