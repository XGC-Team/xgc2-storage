package model

import (
	"encoding/json"
	"time"
)

// Durable facts of workflow execution. Core runs workflows in memory and keeps
// only what a restart or an operator needs afterwards: that a Run was accepted
// and how it ended, which Session owns a target, and which recordings exist.
// Times are UTC instants; the zero time means "not yet".

type RunStatus string

const (
	RunQueued      RunStatus = "queued"
	RunRunning     RunStatus = "running"
	RunStopping    RunStatus = "stopping"
	RunSucceeded   RunStatus = "succeeded"
	RunFailed      RunStatus = "failed"
	RunStopped     RunStatus = "stopped"
	RunCanceled    RunStatus = "canceled"
	RunInterrupted RunStatus = "interrupted"
)

// Open reports whether a Run in this status may still change.
func (s RunStatus) Open() bool { return s == RunQueued || s == RunRunning || s == RunStopping }

// Terminal reports whether the status ends a Run.
func (s RunStatus) Terminal() bool {
	return s == RunSucceeded || s == RunFailed || s == RunStopped || s == RunCanceled || s == RunInterrupted
}

// Run is the durable record of one workflow Run. Inputs hold the frozen inputs
// including the session context; Nodes, written once when the Run ends, hold the
// node records.
type Run struct {
	ID                 string          `json:"id"`
	TargetID           string          `json:"target_id"`
	RootRunID          string          `json:"root_run_id"`
	ParentRunID        string          `json:"parent_run_id,omitempty"`
	CallNodeID         string          `json:"call_node_id,omitempty"`
	Depth              int             `json:"depth"`
	SessionID          string          `json:"session_id,omitempty"`
	IdempotencyKey     string          `json:"idempotency_key,omitempty"`
	WorkflowResourceID string          `json:"workflow_resource_id"`
	WorkflowCommitID   string          `json:"workflow_commit_id"`
	DefinitionDigest   string          `json:"definition_digest"`
	ActionID           string          `json:"action_id"`
	Inputs             json.RawMessage `json:"inputs,omitempty"`
	Trigger            json.RawMessage `json:"trigger,omitempty"`
	Status             RunStatus       `json:"status"`
	Termination        string          `json:"termination,omitempty"`
	Error              json.RawMessage `json:"error,omitempty"`
	Result             json.RawMessage `json:"result,omitempty"`
	Nodes              json.RawMessage `json:"nodes,omitempty"`
	CleanupErrors      json.RawMessage `json:"cleanup_errors,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	StartedAt          time.Time       `json:"started_at,omitzero"`
	FinishedAt         time.Time       `json:"finished_at,omitzero"`
	Revision           int64           `json:"revision"`
}

// NewRun accepts a Run. RootRunID defaults to ID and Status to queued. A
// non-empty IdempotencyKey makes the call idempotent: a second call with the
// same key returns the stored Run instead of creating another.
type NewRun struct {
	ID                 string
	TargetID           string
	RootRunID          string
	ParentRunID        string
	CallNodeID         string
	Depth              int
	SessionID          string
	IdempotencyKey     string
	WorkflowResourceID string
	WorkflowCommitID   string
	DefinitionDigest   string
	ActionID           string
	Inputs             json.RawMessage
	Trigger            json.RawMessage
	Status             RunStatus
	At                 time.Time
}

// RunStatusUpdate moves an open Run to running or stopping.
type RunStatusUpdate struct {
	ID     string
	Status RunStatus
	At     time.Time
}

// RunFinish ends a Run with a terminal status and its outcome.
type RunFinish struct {
	ID            string
	Status        RunStatus
	Termination   string
	Error         json.RawMessage
	Result        json.RawMessage
	Nodes         json.RawMessage
	CleanupErrors json.RawMessage
	At            time.Time
}

// RunFilter selects Runs newest first. Zero fields do not filter.
type RunFilter struct {
	TargetID           string
	RootRunID          string
	SessionID          string
	WorkflowResourceID string
	Statuses           []RunStatus
	// Since is inclusive and Until exclusive, both on the creation time.
	Since, Until time.Time
	// Limit defaults to 100 and is at most 1000.
	Limit int
	// Cursor is the Next value of the previous page.
	Cursor string
	// OmitPayloads leaves out inputs, trigger, error, result, nodes and cleanup
	// errors, which dominate the size of a Run; GetRun returns them.
	OmitPayloads bool
}

type RunPage struct {
	Runs []Run
	// Next is empty after the last page.
	Next string
}

// RunRetention selects finished Runs to delete. A Run whose root Run is still
// open is kept, so a tree is never cut while it is live.
type RunRetention struct {
	// OlderThan deletes Runs that finished before it.
	OlderThan time.Time
	// KeepNewest deletes all but this many finished Runs, newest first (0: no count limit).
	KeepNewest int
	// Limit bounds one call (default 1000, at most 10000); call again while it returns Limit.
	Limit int
}

type SessionStatus string

const (
	SessionOpen     SessionStatus = "open"
	SessionStopping SessionStatus = "stopping"
	SessionClosed   SessionStatus = "closed"
	// SessionInterrupted is a Session that was still live when Core restarted.
	SessionInterrupted SessionStatus = "interrupted"
)

func (s SessionStatus) Live() bool { return s == SessionOpen || s == SessionStopping }

// Session is the durable record of one experiment session on a target. At most
// one Session per target is live. StopIntent is set once and never changes.
type Session struct {
	ID                   string          `json:"id"`
	TargetID             string          `json:"target_id"`
	ExperimentResourceID string          `json:"experiment_resource_id"`
	ExperimentCommitID   string          `json:"experiment_commit_id"`
	RootRunID            string          `json:"root_run_id,omitempty"`
	Status               SessionStatus   `json:"status"`
	StopIntent           json.RawMessage `json:"stop_intent,omitempty"`
	OpenedAt             time.Time       `json:"opened_at"`
	ClosedAt             time.Time       `json:"closed_at,omitzero"`
	Revision             int64           `json:"revision"`
}

type NewSession struct {
	ID                   string
	TargetID             string
	ExperimentResourceID string
	ExperimentCommitID   string
	RootRunID            string
	At                   time.Time
}

type SessionFilter struct {
	TargetID             string
	ExperimentResourceID string
	Statuses             []SessionStatus
	// Limit defaults to 100 and is at most 1000.
	Limit  int
	Cursor string
}

type SessionPage struct {
	Sessions []Session
	Next     string
}

// Recording is an index entry for a recording that a Run registered.
type Recording struct {
	ID        string          `json:"id"`
	SessionID string          `json:"session_id,omitempty"`
	RunID     string          `json:"run_id"`
	Kind      string          `json:"kind"`
	Path      string          `json:"path"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type NewRecording struct {
	ID        string
	SessionID string
	RunID     string
	Kind      string
	Path      string
	Metadata  json.RawMessage
	At        time.Time
}

// RecordingFilter lists recordings oldest first.
type RecordingFilter struct {
	SessionID string
	RunID     string
	Kind      string
	Limit     int
	Cursor    string
}

type RecordingPage struct {
	Recordings []Recording
	Next       string
}

const (
	MaxRunInputsBytes        = 4 << 20
	MaxRunTriggerBytes       = 1 << 20
	MaxRunErrorBytes         = 256 << 10
	MaxRunResultBytes        = 4 << 20
	MaxRunNodesBytes         = 8 << 20
	MaxRunCleanupErrorsBytes = 256 << 10
	MaxStopIntentBytes       = 64 << 10
	MaxRecordingMetadata     = 256 << 10
	MaxRunDepth              = 1024
	MaxPageSize              = 1000
)
