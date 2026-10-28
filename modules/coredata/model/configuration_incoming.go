package model

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

// ConfigurationWorkflowRobotRead selects a bounded set of current Automation
// IDs in this scope. Both domains are guarded; no source payload is read.
type ConfigurationWorkflowRobotRead struct {
	WorkflowDomain ConfigurationDomainGuard `json:"workflow_domain"`
	RobotDomain    ConfigurationDomainGuard `json:"robot_domain"`
	WorkflowIDs    []string                 `json:"workflow_ids"`
}

// ConfigurationWorkflowRobotLink is one distinct current-main tracking
// relationship, not one entry per binding slot.
type ConfigurationWorkflowRobotLink struct {
	WorkflowID string `json:"workflow_id"`
	RobotID    string `json:"robot_id"`
	CommitID   string `json:"commit_id"`
}

type ConfigurationWorkflowRobotResult struct {
	Workflows []ConfigurationCatalogChoice     `json:"workflows"`
	Robots    []ConfigurationCatalogChoice     `json:"robots"`
	Links     []ConfigurationWorkflowRobotLink `json:"links"`
	Work      ConfigurationWorkflowRobotWork   `json:"work"`
}

// Work describes this read's materialization, not physical disk IO.
type ConfigurationWorkflowRobotWork struct {
	WorkflowHeaderDecodes int `json:"workflow_header_decodes"`
	RobotHeaderDecodes    int `json:"robot_header_decodes"`
	WorkflowHeaderBytes   int `json:"workflow_header_bytes"`
	RobotHeaderBytes      int `json:"robot_header_bytes"`
	Links                 int `json:"links"`
}
