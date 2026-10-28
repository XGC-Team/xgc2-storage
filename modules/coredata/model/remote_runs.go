package model

// RemoteRunLink is an unresolved control/join relation owned by its accepted Run. It is not a mirror of the remote execution state.
type RemoteRunLink struct {
	OwnerRunID string `json:"ownerRunId"`
	RootRunID  string `json:"rootRunId"`
	TargetID   string `json:"targetId"`
	RunID      string `json:"runId"`
	CallNodeID string `json:"callNodeId"`
	BootID     string `json:"bootId,omitempty"`
}

type RemoteRunPage struct {
	Links []RemoteRunLink `json:"links"`
	// Next continues after the last selected target relationship.
	Next string `json:"next,omitempty"`
}

const MaxRemoteRunLinks = 1024
const MaxRemoteRunLinksBytes = 512 << 10
const MaxRemoteRunPageSize = 128
