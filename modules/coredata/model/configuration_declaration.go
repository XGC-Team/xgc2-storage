package model

// Deployment input, never a Named data request or a first-read adoption rule.
// Only catalog identity/digest may change on explicit owner synchronization.
// Schema version and capabilities, including visibility/system policy, are fixed.
type ConfigurationDomainDeclaration struct {
	Key                     string   `json:"key"`
	SchemaIdentity          string   `json:"schema_identity"`
	SchemaVersion           int      `json:"schema_version"`
	RegistryDigest          string   `json:"registry_digest"`
	Capabilities            []string `json:"capabilities"`
	MainVisibility          bool     `json:"main_visibility"`
	AllowSystemProvisioning bool     `json:"allow_system_provisioning"`
}
