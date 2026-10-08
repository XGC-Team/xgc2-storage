package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConfigurationCommonNamesManifestAndExactDigests(t *testing.T) {
	display, key, e := NormalizeConfigurationName(ConfigurationResourceName, "  Cafe\u0301 STRAẞE  ")
	if e != nil || display != "Café STRAẞE" || key != "café strasse" {
		t.Fatalf("NFC/fold %q %q %v", display, key, e)
	}
	_, _, e = NormalizeConfigurationName(ConfigurationResourceName, "../unsafe")
	if e == nil {
		t.Fatal("invalid path name accepted")
	}
	collision := ConfigurationManifest{Nodes: []ConfigurationManifestNode{{ID: "root", Kind: "root"}, {ID: "a", ParentID: "root", Name: "Straße", Kind: "namespace"}, {ID: "b", ParentID: "root", Name: "STRASSE", Kind: "namespace"}}}
	if _, e = ValidateConfigurationManifest(collision); e == nil {
		t.Fatal("folded sibling collision accepted")
	}
	cycle := ConfigurationManifest{Nodes: []ConfigurationManifestNode{{ID: "root", Kind: "root"}, {ID: "a", ParentID: "b", Name: "A", Kind: "namespace"}, {ID: "b", ParentID: "a", Name: "B", Kind: "namespace"}}}
	if _, e = ValidateConfigurationManifest(cycle); e == nil {
		t.Fatal("disconnected cycle accepted")
	}
	good := ConfigurationManifest{Nodes: []ConfigurationManifestNode{{ID: "root", Kind: "root"}}}
	v, e := ValidateConfigurationManifest(good)
	if e != nil {
		t.Fatal(e)
	}
	manifest, _ := json.Marshal(good)
	refs := []ConfigurationReference{{Slot: "z", Mode: "tracking", TargetDomain: "d", TargetResourceID: "r", TargetBranch: "main"}, {Slot: "a", Mode: "tracking", TargetDomain: "d", TargetResourceID: "r", TargetBranch: "main"}}
	payload := []byte(`{ "identity":{}, "body":{"number":9007199254740993,"precision":1.0} }`)
	a, e := ConfigurationContentDigest(payload, manifest, refs)
	if e != nil {
		t.Fatal(e)
	}
	b, e := ConfigurationContentDigest([]byte(strings.Replace(string(payload), "1.0", "1", 1)), manifest, refs)
	if e != nil || a == b {
		t.Fatal("exact numeric frozen bytes normalized")
	}
	b, _ = ConfigurationContentDigest([]byte(strings.Replace(string(payload), "{ ", "{", 1)), manifest, refs)
	if a == b {
		t.Fatal("frozen whitespace not pinned")
	}
	reversed := []ConfigurationReference{refs[1], refs[0]}
	b, _ = ConfigurationContentDigest(payload, manifest, reversed)
	if a != b || refs[0].Slot != "z" {
		t.Fatal("reference order changed pin or caller slice")
	}
	q := ConfigurationResourceCreate{ResourceID: "r", Snapshot: PreparedConfigurationSnapshot{Payload: payload, Manifest: manifest, References: refs, RootDigest: v.RootDigest}}
	p, _ := ConfigurationCreatePlanDigest(q)
	q.Namespace.ExpectedRevision = "2"
	p2, _ := ConfigurationCreatePlanDigest(q)
	if p == p2 {
		t.Fatal("full plan omitted exact guard")
	}
	generic := []Reference{}
	for _, r := range refs {
		body, _ := json.Marshal(r)
		generic = append(generic, Reference{Slot: r.Slot, TargetDomain: r.TargetDomain, TargetResourceID: r.TargetResourceID, TargetCommitID: r.TargetCommitID, Body: body})
	}
	same, e := SnapshotDigest(payload, manifest, generic)
	if e != nil || same != a {
		t.Fatal("namespace and configuration content algorithms differ")
	}
}
