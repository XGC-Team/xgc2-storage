package model

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/api"
)

// Every bound applies independently. Payload/manifest bytes are decoded sizes;
// WireBytes is the actual serialized whole Named request/response, including
// its envelope and base64/escaping. A finite atomic plan is never split.
const (
	MaxConfigurationPayloadBytes       = 10 << 20
	MaxConfigurationManifestBytes      = 1 << 20
	MaxConfigurationManifestNodes      = 4096
	MaxConfigurationReferences         = 4096
	MaxConfigurationNodeChanges        = 8192
	MaxConfigurationDecodedBytes       = 12 << 20
	MaxConfigurationReceiptResultBytes = 256 << 10
	MaxConfigurationIdentityBytes      = 16 << 10
	MaxConfigurationCloneNamespaces    = 1024
	MaxConfigurationCloneResources     = 4096
	MaxConfigurationCloneReferences    = 16384
	MaxConfigurationCloneManifestNodes = 65536
	MaxConfigurationCloneChanges       = 65537
	MaxConfigurationNamespaceDepth     = 128
	MaxConfigurationIDBytes            = 255
	MaxConfigurationNamespaceRunes     = 128
	MaxConfigurationNamespaceBytes     = 512
	MaxConfigurationResourceRunes      = 160
	MaxConfigurationResourceBytes      = 640
	MaxConfigurationBranchRunes        = 80
	MaxConfigurationBranchBytes        = 320
	MaxConfigurationSlotRunes          = 64
	MaxConfigurationSlotBytes          = 256
)

// These declarations describe the shared wire contract; they do not register
// an executable module or grant availability. Only coredata.Spec does that.
func ConfigurationFirstGroupOperations() []api.NamedOperation {
	return []api.NamedOperation{
		{ID: ResourceCreateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: ResourceCommitOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: ResourceSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: ConfigurationReceiptOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
	}
}

type ConfigurationNameClass string

const (
	ConfigurationNamespaceName ConfigurationNameClass = "namespace"
	ConfigurationResourceName  ConfigurationNameClass = "resource"
	ConfigurationBranchName    ConfigurationNameClass = "branch"
	ConfigurationSlotName      ConfigurationNameClass = "slot"
)

// ValidateConfigurationName checks storage bounds, not domain normalization or
// syntax. Core owns NFC/folding/reserved-name rules and supplies NameKey. Both
// display name and key are checked against the relevant separate bound. Names
// are never passed through the 255-byte identifier validator.
func ValidateConfigurationName(class ConfigurationNameClass, name string) error {
	var runes, bytes int
	switch class {
	case ConfigurationNamespaceName:
		runes, bytes = MaxConfigurationNamespaceRunes, MaxConfigurationNamespaceBytes
	case ConfigurationResourceName:
		runes, bytes = MaxConfigurationResourceRunes, MaxConfigurationResourceBytes
	case ConfigurationBranchName:
		runes, bytes = MaxConfigurationBranchRunes, MaxConfigurationBranchBytes
	case ConfigurationSlotName:
		runes, bytes = MaxConfigurationSlotRunes, MaxConfigurationSlotBytes
	default:
		return fmt.Errorf("configuration: unknown name class %q", class)
	}
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, '\x00') || len(name) > bytes || utf8.RuneCountInString(name) > runes {
		return fmt.Errorf("configuration: %s name exceeds valid UTF-8 bounds (%d runes/%d bytes)", class, runes, bytes)
	}
	return nil
}

func ValidateConfigurationIdentifier(id string) error {
	if id == "" || len(id) > MaxConfigurationIDBytes || !utf8.ValidString(id) || strings.TrimSpace(id) != id || strings.ContainsAny(id, "\x00\r\n") {
		return fmt.Errorf("configuration: invalid identifier")
	}
	return nil
}

// Decimal revisions/versions are signed-64-bit storage integers transported as
// strings. Only structural-root and explicit absent guards allow zero.
func ValidateConfigurationDecimal(value string, allowZero bool) error {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 || n == 0 && !allowZero || strconv.FormatInt(n, 10) != value {
		return fmt.Errorf("configuration: noncanonical or overflowing decimal value")
	}
	return nil
}

// Counters are owner-computed, never supplied as trusted RPC fields. DecodedBytes
// includes the complete plan/snapshot metadata, references, audit, receipt and
// the current-main identity projection (even when it comes from selected main);
// WireBytes includes the actual whole envelope. Validation does not measure or
// authorize a plan on a caller's behalf.
type ConfigurationBudget struct {
	PayloadBytes, ManifestBytes, ManifestNodes, References, NodeChanges int64
	DecodedBytes, WireBytes, ReceiptResultBytes                         int64
	MainIdentityBytes                                                   int64
}

func (b ConfigurationBudget) Validate() error {
	for _, bound := range []struct {
		name       string
		value, max int64
	}{
		{"payload bytes", b.PayloadBytes, MaxConfigurationPayloadBytes},
		{"manifest bytes", b.ManifestBytes, MaxConfigurationManifestBytes},
		{"manifest nodes", b.ManifestNodes, MaxConfigurationManifestNodes},
		{"references", b.References, MaxConfigurationReferences},
		{"node changes", b.NodeChanges, MaxConfigurationNodeChanges},
		{"decoded whole bytes", b.DecodedBytes, MaxConfigurationDecodedBytes},
		{"serialized whole bytes", b.WireBytes, MaxRequestBytes},
		{"receipt result bytes", b.ReceiptResultBytes, MaxConfigurationReceiptResultBytes},
		{"current-main identity bytes", b.MainIdentityBytes, MaxConfigurationIdentityBytes},
	} {
		if bound.value < 0 || bound.value > bound.max {
			return fmt.Errorf("configuration: %s budget exceeded", bound.name)
		}
	}
	if b.DecodedBytes < b.PayloadBytes+b.ManifestBytes+b.MainIdentityBytes || b.WireBytes < b.PayloadBytes+b.ManifestBytes+b.MainIdentityBytes {
		return fmt.Errorf("configuration: whole byte counters omit decoded payload/manifest/main identity")
	}
	return nil
}

// Clone has its own complete-closure counts. It still obeys the decoded and
// actual wire whole-plan bounds; the individual maxima cannot all coexist.
type ConfigurationCloneBudget struct {
	Namespaces, Resources, References, ManifestNodes, NodeChangesAndSummary int64
	NamespaceDepth, DecodedBytes, WireBytes                                 int64
}

func (b ConfigurationCloneBudget) Validate() error {
	for _, bound := range []struct {
		name       string
		value, max int64
	}{
		{"clone namespaces", b.Namespaces, MaxConfigurationCloneNamespaces},
		{"clone resources", b.Resources, MaxConfigurationCloneResources},
		{"clone references", b.References, MaxConfigurationCloneReferences},
		{"clone manifest nodes", b.ManifestNodes, MaxConfigurationCloneManifestNodes},
		{"clone node changes and summary", b.NodeChangesAndSummary, MaxConfigurationCloneChanges},
		{"namespace depth", b.NamespaceDepth, MaxConfigurationNamespaceDepth},
		{"decoded clone bytes", b.DecodedBytes, MaxConfigurationDecodedBytes},
		{"serialized clone bytes", b.WireBytes, MaxRequestBytes},
	} {
		if bound.value < 0 || bound.value > bound.max {
			return fmt.Errorf("configuration: %s budget exceeded", bound.name)
		}
	}
	return nil
}
