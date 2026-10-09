package model

const ConfigurationIncomingOperation = "configuration.references.incoming"

type ConfigurationIncomingRead struct {
	Domain     ConfigurationDomainGuard `json:"domain"`
	ResourceID string                   `json:"resource_id"`
}
type ConfigurationIncomingReference struct {
	SourceDomain     string                 `json:"source_domain"`
	SourceResourceID string                 `json:"source_resource_id"`
	SourceBranch     string                 `json:"source_branch"`
	SourceCommitID   string                 `json:"source_commit_id"`
	Reference        ConfigurationReference `json:"reference"`
	CreatedAt        string                 `json:"created_at"`
}
