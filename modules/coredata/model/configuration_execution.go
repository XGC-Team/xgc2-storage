package model

// ConfigurationMainPin fences an already reviewed current-main resource in
// the execution action's own transaction. RootDigest is the authored tree
// identity, not a client-computed SQL/content projection.
type ConfigurationMainPin struct {
	Domain     ConfigurationDomainGuard `json:"domain"`
	ResourceID string                   `json:"resource_id"`
	CommitID   string                   `json:"commit_id"`
	RootDigest string                   `json:"root_digest"`
}
