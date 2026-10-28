package model

// ConfigurationCommitIdentityState is an exact batched visibility result.
// Valid=false means a missing or inconsistent identity; callers must fail
// closed for that item. Identity is the immutable domain visibility projection.
type ConfigurationCommitIdentityState struct {
	ID       string `json:"id"`
	Identity []byte `json:"identity"`
	Valid    bool   `json:"valid"`
}
