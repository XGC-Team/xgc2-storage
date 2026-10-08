package model

import "encoding/json"

const MaxSessionWorkflowRuns = 4096
const MaxSessionWorkflowSources = 16384
const MaxSessionWorkflowMembers = 1024

// SessionWorkflowLogRead selects one authorization-scope-local Session graph.
// It has no caller-provided inventory, traversal program or mutable page token.
type SessionWorkflowLogRead struct {
	TargetID  string `json:"target_id"`
	SessionID string `json:"session_id"`
}

// SessionWorkflowLogSnapshot transports complete product metadata objects
// without importing Core's internal packages or decoding numbers through any.
// The storage implementation must select the safe projection fields from its
// authoritative data; a consumer cannot submit a prebuilt snapshot as truth.
type SessionWorkflowLogSnapshot struct {
	Session json.RawMessage         `json:"session"`
	Runs    []SessionWorkflowLogRun `json:"runs"`
	Jobs    []SessionWorkflowLogJob `json:"jobs"`
}

type SessionWorkflowLogRun struct {
	Run         json.RawMessage   `json:"run"`
	Invocations []json.RawMessage `json:"invocations"`
	Attempts    []json.RawMessage `json:"attempts"`
	Relations   json.RawMessage   `json:"relations"`
}

// Origin is explicit because the product Job projection omits its trusted
// origin from JSON. Storage returns it only for the exact selected occurrence.
type SessionWorkflowLogJob struct {
	Origin json.RawMessage `json:"origin"`
	Job    json.RawMessage `json:"job"`
}
