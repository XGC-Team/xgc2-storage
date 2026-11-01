package model

// RunResourceRecovery is the current-format control fact for an accepted Run
// and its exactly admitted local children. It is separate from the public node
// outcome and absent for historical Runs whose effects were not recorded.
type RunResourceRecovery struct {
	AgentBootID string `json:"agentBootId"`
	// KernelBootID is the kernel of the execution owner at Run acceptance.
	// A different replacement kernel ends all processes in that old scope.
	// Older Runs without this identity remain unknown.
	KernelBootID     string                 `json:"kernelBootId,omitempty"`
	KernelReplacedBy string                 `json:"kernelReplacedBy,omitempty"`
	Resources        []RunResourceAdmission `json:"resources"`
	Joined           bool                   `json:"joined"`
	// LocalDriver records the accepted local root's actual in-process driver
	// join, separately from its admitted processes and remote children. It is
	// absent for incoming and local child Runs, whose caller owns that join.
	LocalDriver          string                       `json:"localDriver,omitempty"`
	LegacyLocalScope     *RunLegacyLocalScope         `json:"legacyLocalScope,omitempty"`
	TrackRemoteProviders bool                         `json:"trackRemoteProviders,omitempty"`
	RemoteProviders      []RunRemoteProviderSelection `json:"remoteProviders,omitempty"`
}

// RunLegacyLocalScope is an explicit offline maintenance witness for the
// selected pre-14 local OS scope. It is never inferred on ordinary startup.
type RunLegacyLocalScope struct {
	PreviousKernelBootID    string `json:"previousKernelBootId"`
	ReplacementKernelBootID string `json:"replacementKernelBootId"`
}

// RunRecoveryTransition carries exact newly completed callers and the
// ordinary retention effect of the same durable transition.
type RunRecoveryTransition struct {
	Recovered []Run
	Retention RunRetentionEffect
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
// only the exact process owner can fill Join, and only the exact local Run
// owner can record the separate physical and complete child joins.
type RunResourceAdmission struct {
	ID                  string           `json:"id"`
	NodeID              string           `json:"nodeId"`
	Kind                string           `json:"kind"`
	Join                *RunResourceJoin `json:"join,omitempty"`
	LocalPhysicalJoined bool             `json:"localPhysicalJoined,omitempty"`
	LocalJoined         bool             `json:"localJoined,omitempty"`
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
