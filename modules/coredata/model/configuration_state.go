package model

// Catalog mutations use the same durable product receipt as snapshot writes.
const (
	ConfigurationBranchCreateOperation  = "configuration.branch.create"
	ConfigurationBranchArchiveOperation = "configuration.branch.archive"
	ConfigurationResourceStateOperation = "configuration.resource.state"
)

type ConfigurationBranchCreate struct {
	Domain                   ConfigurationDomainGuard `json:"domain"`
	Mutation                 ConfigurationMutation    `json:"mutation"`
	ResourceID               string                   `json:"resource_id"`
	ExpectedResourceRevision string                   `json:"expected_resource_revision"`
	Main                     ConfigurationBranchGuard `json:"main"`
	ID                       string                   `json:"id"`
	Name                     string                   `json:"name"`
	FromCommitID             string                   `json:"from_commit_id"`
	FromContentDigest        string                   `json:"from_content_digest"`
}

type ConfigurationBranchArchive struct {
	Domain     ConfigurationDomainGuard `json:"domain"`
	Mutation   ConfigurationMutation    `json:"mutation"`
	ResourceID string                   `json:"resource_id"`
	Main       ConfigurationBranchGuard `json:"main"`
	Branch     ConfigurationBranchGuard `json:"branch"`
}

type ConfigurationResourceState struct {
	Domain           ConfigurationDomainGuard `json:"domain"`
	Mutation         ConfigurationMutation    `json:"mutation"`
	ResourceID       string                   `json:"resource_id"`
	ExpectedRevision string                   `json:"expected_revision"`
	Main             ConfigurationBranchGuard `json:"main"`
	Archived         bool                     `json:"archived"`
}

func ConfigurationBranchCreatePlanDigest(q ConfigurationBranchCreate) (string, error) {
	return configurationPlanDigest(ConfigurationBranchCreateOperation, q)
}
func ConfigurationBranchArchivePlanDigest(q ConfigurationBranchArchive) (string, error) {
	return configurationPlanDigest(ConfigurationBranchArchiveOperation, q)
}
func ConfigurationResourceStatePlanDigest(q ConfigurationResourceState) (string, error) {
	return configurationPlanDigest(ConfigurationResourceStateOperation, q)
}
