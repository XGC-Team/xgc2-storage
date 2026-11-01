package model

// Deployment input, never a Named data request or a first-read adoption rule.
// Only catalog identity/digest may change on explicit owner synchronization.
// Capabilities, including visibility/system policy, remain fixed. Schema version
// advances require the exact previous owner declaration; data calls cannot advance it.
type ConfigurationDomainDeclaration struct {
	PreviousSchema          *ConfigurationDomainGuard `json:"previous_schema,omitempty"`
	Key                     string                    `json:"key"`
	SchemaIdentity          string                    `json:"schema_identity"`
	SchemaVersion           int                       `json:"schema_version"`
	RegistryDigest          string                    `json:"registry_digest"`
	Capabilities            []string                  `json:"capabilities"`
	MainVisibility          bool                      `json:"main_visibility"`
	AllowSystemProvisioning bool                      `json:"allow_system_provisioning"`
}
