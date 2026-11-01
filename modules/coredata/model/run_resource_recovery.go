package model

// RunResourceRecovery is the current-format control fact for one incoming
// remote Run. It is separate from the public node outcome and is absent for
// historical Runs whose effect admissions were not recorded.
type RunResourceRecovery struct {
	AgentBootID          string                       `json:"agentBootId"`
	Resources            []RunResourceAdmission       `json:"resources"`
	Joined               bool                         `json:"joined"`
	TrackRemoteProviders bool                         `json:"trackRemoteProviders,omitempty"`
	RemoteProviders      []RunRemoteProviderSelection `json:"remoteProviders,omitempty"`
}

// RunRemoteProviderSelection is the exact provider admitted on the
// authenticated original Agent epoch, captured before live use is granted.
type RunRemoteProviderSelection struct {
	Link               RemoteRunLink `json:"link"`
	SessionID          string        `json:"sessionId"`
	NodeID             string        `json:"nodeId"`
	ResourceID         string        `json:"resourceId"`
	Incarnation        string        `json:"incarnation"`
	WorkflowResourceID string        `json:"workflowResourceId"`
	WorkflowCommitID   string        `json:"workflowCommitId"`
	WorkflowDigest     string        `json:"workflowDigest"`
}

// RunResourceAdmission precedes a local effect. An admission is never a join:
// only the exact process owner's subsequent proof can fill Join.
type RunResourceAdmission struct {
	ID     string           `json:"id"`
	NodeID string           `json:"nodeId"`
	Kind   string           `json:"kind"`
	Join   *RunResourceJoin `json:"join,omitempty"`
}

// RunResourceJoin binds the physical owner's evidence to the exact admitted
// Run, node, instance, Agent boot and OS-process incarnation.
type RunResourceJoin struct {
	RunID           string `json:"runId"`
	NodeID          string `json:"nodeId"`
	ResourceID      string `json:"resourceId"`
	Kind            string `json:"kind"`
	AgentBootID     string `json:"agentBootId"`
	Incarnation     string `json:"incarnation"`
	HostBootID      string `json:"hostBootId"`
	PID             int    `json:"pid"`
	PGID            int    `json:"pgid"`
	StartTicks      uint64 `json:"startTicks"`
	OwnerPID        int    `json:"ownerPid"`
	OwnerStartTicks uint64 `json:"ownerStartTicks"`
}
