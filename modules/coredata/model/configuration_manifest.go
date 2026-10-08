package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// NormalizeConfigurationName is the common data-name rule: NFC display and
// folded NFC key. It performs no domain policy, compilation or provider IO.
func NormalizeConfigurationName(class ConfigurationNameClass, raw string) (display, key string, err error) {
	display = norm.NFC.String(strings.TrimSpace(raw))
	if display == "." || display == ".." || strings.ContainsAny(display, "/\x00") {
		return "", "", fmt.Errorf("invalid configuration name")
	}
	key = norm.NFC.String(cases.Fold().String(display))
	if err = ValidateConfigurationName(class, display); err != nil {
		return "", "", err
	}
	if err = ValidateConfigurationName(class, key); err != nil {
		return "", "", err
	}
	return display, key, nil
}

// ConfigurationManifestNode is the existing generic NodeDraft representation.
// Domain body codecs do not belong to this manifest or the storage module.
type ConfigurationManifestNode struct {
	ID            string `json:"id"`
	ParentID      string `json:"parentId"`
	Name          string `json:"name"`
	Kind          string `json:"kind"`
	DocumentKind  string `json:"documentKind,omitempty"`
	PayloadDigest string `json:"payloadDigest,omitempty"`
	SortOrder     int    `json:"sortOrder"`
}
type ConfigurationManifest struct {
	Nodes []ConfigurationManifestNode `json:"nodes"`
}
type ValidatedConfigurationManifest struct {
	RootDigest string
	Nodes      []ConfigurationManifestNode
	keys       map[string]string
	byID       map[string]ConfigurationManifestNode
}

func (v ValidatedConfigurationManifest) Contains(id string) bool { _, ok := v.byID[id]; return ok }

// NameKey returns the canonical key computed by validation, without normalizing
// again or exposing the private map. Root, missing and zero-value return empty.
func (v ValidatedConfigurationManifest) NameKey(id string) string { return v.keys[id] }

func configurationSHA(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == sha256.Size && strings.ToLower(s) == s
}

// ValidateConfigurationManifest checks only the bounded generic tree, retaining
// the established business root digest. The input slice is never mutated.
func ValidateConfigurationManifest(m ConfigurationManifest) (out ValidatedConfigurationManifest, err error) {
	if len(m.Nodes) == 0 || len(m.Nodes) > MaxConfigurationManifestNodes {
		return out, fmt.Errorf("manifest node budget exceeded")
	}
	out.byID = make(map[string]ConfigurationManifestNode, len(m.Nodes))
	out.keys = make(map[string]string, len(m.Nodes))
	root := ""
	siblings := map[string]map[string]bool{}
	for _, n := range m.Nodes {
		if n.ID == "" || len(n.ID) > 128 || len(n.ParentID) > 128 || !utf8.ValidString(n.ID) || !utf8.ValidString(n.ParentID) || strings.TrimSpace(n.ID) == "" || strings.ContainsAny(n.ID+n.ParentID, "\x00") || n.SortOrder < 0 {
			return out, fmt.Errorf("invalid manifest node identity")
		}
		if _, ok := out.byID[n.ID]; ok {
			return out, fmt.Errorf("duplicate manifest node")
		}
		switch n.Kind {
		case "root":
			if root != "" || n.ParentID != "" || n.Name != "" || n.DocumentKind != "" || n.PayloadDigest != "" {
				return out, fmt.Errorf("invalid manifest root")
			}
			root = n.ID
		case "namespace", "document":
			display, key, e := NormalizeConfigurationName(ConfigurationResourceName, n.Name)
			if e != nil {
				return out, e
			}
			n.Name = display
			out.keys[n.ID] = key
			if n.Kind == "namespace" {
				if n.DocumentKind != "" || n.PayloadDigest != "" {
					return out, fmt.Errorf("namespace carries payload")
				}
			} else if n.DocumentKind == "" || strings.TrimSpace(n.DocumentKind) != n.DocumentKind || len(n.DocumentKind) > 128 || !utf8.ValidString(n.DocumentKind) || strings.ContainsRune(n.DocumentKind, 0) || !configurationSHA(n.PayloadDigest) {
				return out, fmt.Errorf("invalid document metadata")
			}
		default:
			return out, fmt.Errorf("invalid manifest node kind")
		}
		out.byID[n.ID] = n
	}
	if root == "" {
		return out, fmt.Errorf("missing manifest root")
	}
	for id, n := range out.byID {
		if id == root {
			continue
		}
		p, ok := out.byID[n.ParentID]
		if !ok || p.Kind == "document" {
			return out, fmt.Errorf("invalid manifest parent")
		}
		if siblings[n.ParentID] == nil {
			siblings[n.ParentID] = map[string]bool{}
		}
		if siblings[n.ParentID][out.keys[id]] {
			return out, fmt.Errorf("manifest sibling collision")
		}
		siblings[n.ParentID][out.keys[id]] = true
	}
	state := map[string]uint8{root: 2}
	path := make([]string, 0, len(m.Nodes))
	for id := range out.byID {
		path = path[:0]
		cursor := id
		for state[cursor] != 2 {
			if state[cursor] == 1 {
				return out, fmt.Errorf("manifest cycle")
			}
			n, ok := out.byID[cursor]
			if !ok || n.ParentID == "" {
				return out, fmt.Errorf("disconnected manifest")
			}
			state[cursor] = 1
			path = append(path, cursor)
			cursor = n.ParentID
		}
		for _, id := range path {
			state[id] = 2
		}
	}
	out.Nodes = make([]ConfigurationManifestNode, 0, len(out.byID))
	for _, n := range out.byID {
		out.Nodes = append(out.Nodes, n)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })
	h := sha256.New()
	for _, n := range out.Nodes {
		for _, f := range []string{n.ID, n.ParentID, n.Name, out.keys[n.ID], n.Kind, n.DocumentKind, n.PayloadDigest, strconv.Itoa(n.SortOrder)} {
			h.Write([]byte(strconv.Itoa(len(f)) + ":" + f + "|"))
		}
	}
	out.RootDigest = hex.EncodeToString(h.Sum(nil))
	return out, nil
}
func DecodeConfigurationManifest(raw []byte) (ValidatedConfigurationManifest, error) {
	if len(raw) > MaxConfigurationManifestBytes {
		return ValidatedConfigurationManifest{}, fmt.Errorf("manifest byte budget exceeded")
	}
	var m ConfigurationManifest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return ValidatedConfigurationManifest{}, err
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return ValidatedConfigurationManifest{}, fmt.Errorf("one manifest required")
	}
	v, e := ValidateConfigurationManifest(m)
	if e != nil {
		return v, e
	}
	// Frozen manifests are canonical ID-ordered, normalized data, but their exact
	// JSON bytes remain authoritative and are not re-encoded for persistence.
	for i, n := range m.Nodes {
		if n != v.Nodes[i] {
			return v, fmt.Errorf("manifest nodes must be normalized and ID-ordered")
		}
	}
	return v, nil
}
func configurationPaths(v ValidatedConfigurationManifest) (map[string]string, error) {
	out := map[string]string{}
	path := make([]string, 0, len(v.Nodes))
	size := 0
	for _, node := range v.Nodes {
		path = path[:0]
		id := node.ID
		for {
			if _, ok := out[id]; ok {
				break
			}
			n := v.byID[id]
			path = append(path, id)
			if n.ParentID == "" {
				out[id] = "/"
				path = path[:len(path)-1]
				break
			}
			id = n.ParentID
		}
		for i := len(path) - 1; i >= 0; i-- {
			n := v.byID[path[i]]
			p := strings.TrimSuffix(out[n.ParentID], "/") + "/" + n.Name
			size += len(p)
			if size > MaxConfigurationDecodedBytes {
				return nil, fmt.Errorf("manifest path budget exceeded")
			}
			out[n.ID] = p
		}
	}
	return out, nil
}

// DiffConfigurationManifests returns the complete established ID-ordered diff.
// A zero before value represents resource creation.
func DiffConfigurationManifests(before, after ValidatedConfigurationManifest) ([]ConfigurationNodeChange, string, error) {
	bp, e := configurationPaths(before)
	if e != nil {
		return nil, "", e
	}
	ap, e := configurationPaths(after)
	if e != nil {
		return nil, "", e
	}
	ids := map[string]bool{}
	for id := range before.byID {
		ids[id] = true
	}
	for id := range after.byID {
		ids[id] = true
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	out := []ConfigurationNodeChange{}
	counts := map[string]int{}
	for _, id := range ordered {
		old, had := before.byID[id]
		n, has := after.byID[id]
		d := ConfigurationNodeChange{NodeID: id, BeforePath: bp[id], AfterPath: ap[id]}
		switch {
		case !had:
			d.Operation = "create"
			d.AfterDigest = n.PayloadDigest
		case !has:
			d.Operation = "delete"
			d.BeforeDigest = old.PayloadDigest
		case old.ParentID != n.ParentID || before.keys[id] != after.keys[id]:
			d.Operation = "move"
			d.BeforeDigest = old.PayloadDigest
			d.AfterDigest = n.PayloadDigest
		case old.Kind != n.Kind || old.DocumentKind != n.DocumentKind || old.PayloadDigest != n.PayloadDigest || old.SortOrder != n.SortOrder:
			d.Operation = "update"
			d.BeforeDigest = old.PayloadDigest
			d.AfterDigest = n.PayloadDigest
		default:
			continue
		}
		out = append(out, d)
		counts[d.Operation]++
	}
	return out, fmt.Sprintf("create %d, update %d, delete %d, move %d", counts["create"], counts["update"], counts["delete"], counts["move"]), nil
}
