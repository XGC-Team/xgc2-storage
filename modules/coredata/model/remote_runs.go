package model

import "time"

// RemoteRunLink is an unresolved control/join relation owned by its accepted Run. It is not a mirror of the remote execution state.
type RemoteRunLink struct {
	OwnerRunID string `json:"ownerRunId"`
	RootRunID  string `json:"rootRunId"`
	TargetID   string `json:"targetId"`
	RunID      string `json:"runId"`
	CallNodeID string `json:"callNodeId"`
	BootID     string `json:"bootId,omitempty"`
}

// RemoteRunResolveResult reports the committed transition of one exact
// remote-control relation. LastForOwner is true only when this call removed
// the final relation owned by OwnerRunID. LastForRoot independently reports
// the final indexed relation for RootRunID. The root and owner finish facts
// are meaningful only then; RootFound distinguishes a missing root from a
// present but still-open root. A zero finish time means that Run is open.
// These are facts at the transaction cut, not a promise that later writes
// cannot create a new relation or otherwise protect history.
type RemoteRunResolveResult struct {
	RootRunID       string             `json:"rootRunId"`
	Changed         bool               `json:"changed"`
	LastForOwner    bool               `json:"lastForOwner"`
	LastForRoot     bool               `json:"lastForRoot"`
	RootFound       bool               `json:"rootFound"`
	RootFinishedAt  time.Time          `json:"rootFinishedAt,omitzero"`
	OwnerFinishedAt time.Time          `json:"ownerFinishedAt,omitzero"`
	Retention       RunRetentionEffect `json:"retention"`
}

type RemoteRunPage struct {
	Links []RemoteRunLink `json:"links"`
	// Next continues after the last selected target relationship.
	Next string `json:"next,omitempty"`
}

const MaxRemoteRunLinks = 1024
const MaxRemoteRunLinksBytes = 512 << 10
const MaxRemoteRunPageSize = 128
