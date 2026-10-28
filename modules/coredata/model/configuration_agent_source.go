package model

// ConfigurationAgentSourceCursor is the exclusive, index-ordered continuation
// for one Agent in one configuration domain. It is meaningful only with the
// same scope, domain, AgentID and ExcludeWorld selector as the preceding page.
type ConfigurationAgentSourceCursor struct {
	ScopeDigest     string `json:"scopeDigest"`
	DomainKey       string `json:"domainKey"`
	AgentID         string `json:"agentId"`
	ExcludeWorld    bool   `json:"excludeWorld"`
	Generation      int64  `json:"generation"`
	CommitID        string `json:"commitId"`
	Kind            string `json:"kind"`
	SlotID          string `json:"slotId"`
	RobotResourceID string `json:"robotResourceId"`
}

type ConfigurationAgentSourceRead struct {
	ExcludeWorld bool                            `json:"excludeWorld"`
	Domain       ConfigurationDomainGuard        `json:"domain"`
	AgentID      string                          `json:"agentId"`
	Limit        int                             `json:"limit"`
	After        *ConfigurationAgentSourceCursor `json:"after,omitempty"`
}

type ConfigurationAgentSource struct {
	Header      ConfigurationResourceHeader   `json:"header"`
	Association ConfigurationAgentAssociation `json:"association"`
}

type ConfigurationAgentSourcePage struct {
	Generation int64                           `json:"generation"`
	Items      []ConfigurationAgentSource      `json:"items"`
	Next       *ConfigurationAgentSourceCursor `json:"next,omitempty"`
}

// ConfigurationRobotIncomingCursor is exclusive and valid only for the same
// Robot, source/target domain guards, and storage scope as its preceding page.
type ConfigurationRobotIncomingCursor struct {
	ScopeDigest    string `json:"scopeDigest"`
	DomainKey      string `json:"domainKey"`
	RobotDomainKey string `json:"robotDomainKey"`
	RobotID        string `json:"robotId"`
	Generation     int64  `json:"generation"`
	TargetCommitID string `json:"targetCommitId"`
	CommitID       string `json:"commitId"`
	Slot           string `json:"slot"`
}

type ConfigurationRobotIncomingRead struct {
	Domain      ConfigurationDomainGuard          `json:"domain"`
	RobotDomain ConfigurationDomainGuard          `json:"robotDomain"`
	RobotID     string                            `json:"robotId"`
	Limit       int                               `json:"limit"`
	After       *ConfigurationRobotIncomingCursor `json:"after,omitempty"`
}

type ConfigurationRobotIncoming struct {
	Header         ConfigurationResourceHeader `json:"header"`
	Slot           string                      `json:"slot"`
	TargetCommitID string                      `json:"targetCommitId"`
}

type ConfigurationRobotIncomingPage struct {
	Generation int64                             `json:"generation"`
	Items      []ConfigurationRobotIncoming      `json:"items"`
	Next       *ConfigurationRobotIncomingCursor `json:"next,omitempty"`
}
