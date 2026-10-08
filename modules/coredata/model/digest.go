// SnapshotDigest pins typed JSON and sorted reference data without any SQL dependency.
package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	// The frozen data bytes are not JSON-normalized. Reference bodies contain
	// control metadata; normalize their object key order with exact JSON numbers.
	for i := range references {
		if !object(references[i].Body) {
			return "", fmt.Errorf("invalid reference object")
		}
		var value any
		d := json.NewDecoder(bytes.NewReader(references[i].Body))
		d.UseNumber()
		if err := d.Decode(&value); err != nil {
			return "", err
		}
		var b bytes.Buffer
		e := json.NewEncoder(&b)
		e.SetEscapeHTML(false)
		if err := e.Encode(value); err != nil {
			return "", err
		}
		references[i].Body = bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	}
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(references); err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("coredata.snapshot.bytes.v1\x00"))
	for _, part := range [][]byte{payload, manifest, bytes.TrimSuffix(b.Bytes(), []byte("\n"))} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		h.Write(size[:])
		h.Write(part)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
