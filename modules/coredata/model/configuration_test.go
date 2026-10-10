package model_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestConfigurationFrozenBytesAndDecimalWire(t *testing.T) {
	// Whitespace, raw extension escaping and exact numeric spelling survive the
	// base64 outer wire; they must not be decoded through a float64 or reencoded.
	payload := []byte("{ \"identity\":{\"active\":true}, \"body\":{\"n\":9007199254740993, \"literal\":\"<>&\\u003c\", \"v\":1.0} }\n")
	manifest := []byte("{\"nodes\":[]}\n")
	identity := []byte("{ \"active\":false, \"catalog\":9007199254740993 }\n")
	before := model.ConfigurationResourceSnapshot{Head: model.ConfigurationHead{Resource: model.ConfigurationResource{Revision: "9007199254740993", NextVersion: "9223372036854775807"}, Branch: model.ConfigurationBranch{Revision: "9007199254740995"}, Commit: model.ConfigurationCommit{ID: "history", Version: "9007199254740997", SchemaVersion: 1}}, CurrentMain: model.ConfigurationVisibilityPin{Branch: model.ConfigurationBranchGuard{ID: "main", ExpectedRevision: "9007199254740999", CommitID: "current-main", ContentDigest: strings.Repeat("e", 64)}, SchemaVersion: 2, Identity: identity}, Payload: payload, Manifest: manifest}
	wire, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	var after model.ConfigurationResourceSnapshot
	if err = json.Unmarshal(wire, &after); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, after.Payload) || !bytes.Equal(manifest, after.Manifest) || !bytes.Equal(identity, after.CurrentMain.Identity) || after.Head != before.Head || after.CurrentMain.Branch != before.CurrentMain.Branch || after.CurrentMain.SchemaVersion != before.CurrentMain.SchemaVersion {
		t.Fatal("frozen bytes or decimal versions changed")
	}
	// Publishing a nonmain snapshot sends the exact visibility pin from the
	// same read, independently of the selected immutable history identity.
	commit := model.ConfigurationResourceCommit{Main: after.CurrentMain.Branch}
	publishWire, err := json.Marshal(commit)
	if err != nil {
		t.Fatal(err)
	}
	var published model.ConfigurationResourceCommit
	if err = json.Unmarshal(publishWire, &published); err != nil || published.Main != before.CurrentMain.Branch {
		t.Fatal("current-main pin changed on the publication wire")
	}
	for _, value := range []string{"1", "9007199254740993", "9223372036854775807"} {
		if model.ValidateConfigurationDecimal(value, false) != nil {
			t.Fatalf("valid decimal rejected: %s", value)
		}
	}
	for _, value := range []string{"", "0", "01", "+1", "1.0", "1e3", "-1", "9223372036854775808"} {
		if model.ValidateConfigurationDecimal(value, false) == nil {
			t.Fatalf("noncanonical/overflowing decimal accepted: %s", value)
		}
	}
	if model.ValidateConfigurationDecimal("0", true) != nil {
		t.Fatal("structural root zero rejected")
	}
}

func TestConfigurationMultibyteNameIsSeparateFromIdentifier(t *testing.T) {
	for _, c := range []struct {
		class model.ConfigurationNameClass
		runes int
	}{{model.ConfigurationNamespaceName, 128}, {model.ConfigurationResourceName, 160}, {model.ConfigurationBranchName, 80}, {model.ConfigurationSlotName, 64}} {
		name := strings.Repeat("😀", c.runes)
		if model.ValidateConfigurationName(c.class, name) != nil {
			t.Fatalf("valid multibyte %s name rejected", c.class)
		}
		if model.ValidateConfigurationName(c.class, name+"a") == nil {
			t.Fatalf("overflow %s name accepted", c.class)
		}
	}
	if model.ValidateConfigurationIdentifier(strings.Repeat("😀", 64)) == nil {
		t.Fatal("256-byte identifier accepted as a name")
	}
	if model.ValidateConfigurationIdentifier(strings.Repeat("a", 255)) != nil {
		t.Fatal("255-byte identifier rejected")
	}
	if model.ValidateConfigurationName(model.ConfigurationResourceName, strings.Repeat("a", 161)) == nil {
		t.Fatal("rune overflow accepted beneath byte ceiling")
	}
	if model.ValidateConfigurationName(model.ConfigurationResourceName, string([]byte{0xff})) == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestConfigurationIndependentDecodedBudgets(t *testing.T) {
	budget := model.ConfigurationBudget{PayloadBytes: 10 << 20, ManifestBytes: 1 << 20, ManifestNodes: 4096, References: 4096, NodeChanges: 8192, DecodedBytes: 12 << 20, ReceiptResultBytes: 256 << 10, MainIdentityBytes: 16 << 10}
	if budget.Validate() != nil {
		t.Fatal("legal independently measured counters rejected")
	}
	for _, alter := range []func(*model.ConfigurationBudget){func(b *model.ConfigurationBudget) { b.DecodedBytes++ }, func(b *model.ConfigurationBudget) { b.PayloadBytes++ }, func(b *model.ConfigurationBudget) { b.ManifestBytes++ }, func(b *model.ConfigurationBudget) { b.ManifestNodes++ }, func(b *model.ConfigurationBudget) { b.References++ }, func(b *model.ConfigurationBudget) { b.NodeChanges++ }, func(b *model.ConfigurationBudget) { b.ReceiptResultBytes++ }, func(b *model.ConfigurationBudget) { b.MainIdentityBytes++ }} {
		b := budget
		alter(&b)
		if b.Validate() == nil {
			t.Fatal("independent maximum silently bypassed")
		}
	}
	for _, omit := range []func(*model.ConfigurationBudget){func(b *model.ConfigurationBudget) { b.DecodedBytes = b.PayloadBytes + b.ManifestBytes }} {
		b := budget
		omit(&b)
		if b.Validate() == nil {
			t.Fatal("current-main identity omitted from a whole byte counter")
		}
	}
	clone := model.ConfigurationCloneBudget{Namespaces: 1024, Resources: 4096, References: 16384, ManifestNodes: 65536, NodeChangesAndSummary: 65537, NamespaceDepth: 128, DecodedBytes: 12 << 20}
	if clone.Validate() != nil {
		t.Fatal("bounded clone rejected")
	}
	clone.NamespaceDepth++
	if clone.Validate() == nil {
		t.Fatal("clone depth overflow accepted")
	}
}
