package model

import "encoding/json"

// RunCallIdentity is the canonical, exact-id parent/call selection. Trigger
// is returned only for a child-shaped row. Storage leaves interpretation of
// the accepted trigger to the workflow execution owner.
type RunCallIdentity struct {
	ID          string
	TargetID    string
	RootRunID   string
	ParentRunID string
	CallNodeID  string
	Depth       int
	Trigger     json.RawMessage
}

const (
	MaxRunCallTriggerBytes       = 16 << 10
	MaxStoredRunCallTriggerBytes = 20 << 10
)
