package model

// ConfigurationResourceMetadata changes catalog identity without fabricating a
// content commit. Protection is monotonic; nil MoveTo retains the location.
const ConfigurationResourceMetadataOperation = "configuration.resource.metadata"

type ConfigurationResourceMetadata struct {
	Domain           ConfigurationDomainGuard     `json:"domain"`
	Mutation         ConfigurationMutation        `json:"mutation"`
	ResourceID       string                       `json:"resource_id"`
	ExpectedRevision string                       `json:"expected_revision"`
	Main             ConfigurationBranchGuard     `json:"main"`
	MoveTo           *ConfigurationNamespaceGuard `json:"move_to,omitempty"`
	Protect          bool                         `json:"protect"`
}

func ConfigurationResourceMetadataPlanDigest(q ConfigurationResourceMetadata) (string, error) {
	return configurationPlanDigest(ConfigurationResourceMetadataOperation, q)
}
