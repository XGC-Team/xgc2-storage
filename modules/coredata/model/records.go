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
	PredecessorRunID   string          `json:"predecessor_run_id,omitempty"`
	WorkflowResourceID string          `json:"workflow_resource_id"`
	WorkflowCommitID   string          `json:"workflow_commit_id"`
	DefinitionDigest   string          `json:"definition_digest"`
	ActionID           string          `json:"action_id"`
	Inputs             json.RawMessage `json:"inputs,omitempty"`
	Trigger            json.RawMessage `json:"trigger,omitempty"`
	// SourceObservation is a bounded projection supplied by the execution owner
	// when the Run is accepted. The separate Session Action selector is derived
	// from the accepted Trigger and is not execution authority.
	SourceObservation json.RawMessage `json:"source_observation,omitempty"`
	Status            RunStatus       `json:"status"`
	Termination       string          `json:"termination,omitempty"`
	Error             json.RawMessage `json:"error,omitempty"`
	ErrorSummary      json.RawMessage `json:"error_summary,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	HasResult         bool            `json:"has_result,omitempty"`
	Nodes             json.RawMessage `json:"nodes,omitempty"`
	CleanupErrors     json.RawMessage `json:"cleanup_errors,omitempty"`
	CleanupFailed     int             `json:"cleanup_failed,omitempty"`
	Projection        RunProjection   `json:"-"`
	CreatedAt         time.Time       `json:"created_at"`
	StartedAt         time.Time       `json:"started_at,omitzero"`
	FinishedAt        time.Time       `json:"finished_at,omitzero"`
	Revision          int64           `json:"revision"`
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
	PredecessorRunID   string
	WorkflowResourceID string
	WorkflowCommitID   string
	DefinitionDigest   string
	ActionID           string
	Inputs             json.RawMessage
	Trigger            json.RawMessage
	SourceObservation  json.RawMessage
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
	ID          string
	Status      RunStatus
	Termination string
	Error       json.RawMessage
	// ErrorSummary is required for an Error larger than MaxRunErrorSummaryBytes.
	// A short Error is its own exact summary; a supplied different value is invalid.
	ErrorSummary  json.RawMessage
	Result        json.RawMessage
	Nodes         json.RawMessage
	CleanupErrors json.RawMessage
	At            time.Time
}

// CreatedRun is the per-item result of creating a Run in ApplyRunOps.
type CreatedRun struct {
	Run     Run
	Created bool
}

type RunProjection string

const (
	RunComplete    RunProjection = "complete"
	RunIdentity    RunProjection = "identity"
	RunObservation RunProjection = "observation"
)

// RunFilter selects Runs newest first. Zero fields do not filter.
type RunFilter struct {
	// These select accepted Experiment invocation identity from the same-row source projection.
	ExperimentResourceID string
	WorkflowInstanceID   string
	// SessionInstanceID selects the accepted Session Action's trigger identity.
	// SessionID must also be specified. It is independent of WorkflowInstanceID.
	SessionInstanceID  string
	ID                 string
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
	// Projection controls the fixed SQL selection. Identity omits all JSON;
	// Observation selects only write-derived source/error metadata and exact
	// cleanup cardinality. It never reads any full execution JSON column.
	// Complete (the default) and GetRun return all payloads.
	Projection RunProjection
}

type RunPage struct {
	Runs []Run
	// Next is empty after the last page.
	Next string
}

// RunRetention selects eligible finished Runs to delete. A live root, pending
// remote relation or live successor Run or queued successor Session protects
// the relevant Run. A missing root is not a live root.
type RunRetention struct {
	// OlderThan deletes Runs that finished before it.
	OlderThan time.Time
	// KeepNewest keeps this many newest eligible Runs (0 disables the count policy).
	// Count and age victims are deleted oldest first, ordered by finish time and ID.
	KeepNewest int
	// Limit bounds one call (default 1000, at most 10000).
	Limit int
}

// RunRetentionPolicy is captured once at the mutation's committed cut. Zero
// values disable the corresponding selector; a disabled policy needs no lookup.
type RunRetentionPolicy struct {
	OlderThan  time.Time `json:"olderThan,omitzero"`
	KeepNewest int       `json:"keepNewest"`
}

// RunRetentionEffect is a same-transaction fact, not a promise about later
// writes. A later prune always rechecks the authoritative protections.
type RunRetentionEffect struct {
	ChangedFinish       bool `json:"changedFinish"`
	EligibilityReleased bool `json:"eligibilityReleased"`
	HasVictim           bool `json:"hasVictim"`
}

type RunFinishResult struct {
	Run       `json:"run"`
	Retention RunRetentionEffect `json:"retention"`
}
type RunOpsResult struct {
	Created   []CreatedRun       `json:"created"`
	Retention RunRetentionEffect `json:"retention"`
}
type RunInterruptResult struct {
	Count     int                `json:"count"`
	Retention RunRetentionEffect `json:"retention"`
}
type SessionMutationResult struct {
	Session   `json:"session"`
	Retention RunRetentionEffect `json:"retention"`
}
type SessionInterruptResult struct {
	Count     int                `json:"count"`
	Retention RunRetentionEffect `json:"retention"`
}

type SessionStatus string

const (
	SessionQueued   SessionStatus = "queued"
	SessionOpen     SessionStatus = "open"
	SessionStopping SessionStatus = "stopping"
	SessionClosed   SessionStatus = "closed"
	// SessionInterrupted is a Session that was still live when Core restarted.
	SessionInterrupted SessionStatus = "interrupted"
)

func (s SessionStatus) Live() bool {
	return s == SessionQueued || s == SessionOpen || s == SessionStopping
}

// SessionOutcome records how a Session was closed.
type SessionOutcome string

const (
	SessionStopped SessionOutcome = "stopped"
	SessionFailed  SessionOutcome = "failed"
)

// Session is the durable record of one experiment session on a target. At most
// one Session per target is live. StopIntent is set once and never changes.
type Session struct {
	ID                   string          `json:"id"`
	TargetID             string          `json:"target_id"`
	PredecessorSessionID string          `json:"predecessor_session_id,omitempty"`
	ExperimentResourceID string          `json:"experiment_resource_id"`
	ExperimentCommitID   string          `json:"experiment_commit_id"`
	RunMode              string          `json:"run_mode"`
	ExperimentName       string          `json:"experiment_name"`
	RootRunID            string          `json:"root_run_id,omitempty"`
	Status               SessionStatus   `json:"status"`
	StopIntent           json.RawMessage `json:"stop_intent,omitempty"`
	Outcome              SessionOutcome  `json:"outcome,omitempty"`
	Error                json.RawMessage `json:"error,omitempty"`
	OpenedAt             time.Time       `json:"opened_at"`
	ClosedAt             time.Time       `json:"closed_at,omitzero"`
	Revision             int64           `json:"revision"`
}

// NewSession requires a RunMode text key of at most 64 bytes. ExperimentName may
// be empty and is at most 256 UTF-8 bytes without control characters.
type NewSession struct {
	ID                   string
	TargetID             string
	PredecessorSessionID string
	ExperimentResourceID string
	ExperimentCommitID   string
	RunMode              string
	ExperimentName       string
	RootRunID            string
	At                   time.Time
}

// SessionClose ends a live Session with a required stopped or failed outcome.
// Error is an optional JSON object bounded by MaxStopIntentBytes.
type SessionClose struct {
	At      time.Time
	Outcome SessionOutcome
	Error   json.RawMessage
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

// RecordingArchiveClass is the immutable archive surface class of a recording.
// It is derived from the accepted kind and path at registration; a caller
// cannot supply or change it.
type RecordingArchiveClass string

const (
	RecordingArchiveROSBag  RecordingArchiveClass = "rosbag"
	RecordingArchiveScreen  RecordingArchiveClass = "screen"
	RecordingArchiveGallery RecordingArchiveClass = "image-gallery"
	RecordingArchiveOther   RecordingArchiveClass = "other"
)

// RecordingOrder selects the chronological page direction. Equal sort instants
// tie-break on the recording identity in the same direction.
type RecordingOrder string

const (
	RecordingOldestFirst RecordingOrder = "asc"
	RecordingNewestFirst RecordingOrder = "desc"
)

// RecordingSummary is the fixed list projection consumed by archive surfaces.
// The JSON names are the field names the product already exposes. Association
// fields come from the accepted Run/Session owner; the rest are projected from
// the registration metadata.
type RecordingSummary struct {
	ExperimentName string    `json:"experimentName,omitempty"`
	RunMode        string    `json:"runMode,omitempty"`
	TargetID       string    `json:"targetId,omitempty"`
	RecordingID    string    `json:"recordingId,omitempty"`
	RecordID       string    `json:"recordId,omitempty"`
	BindingID      string    `json:"bindingId,omitempty"`
	TimeSource     string    `json:"timeSource,omitempty"`
	MediaType      string    `json:"mediaType,omitempty"`
	DurationMS     int64     `json:"durationMs,omitempty"`
	Width          int       `json:"width,omitempty"`
	Height         int       `json:"height,omitempty"`
	Status         string    `json:"status,omitempty"`
	HasPoster      bool      `json:"hasPoster,omitempty"`
	StartedAt      time.Time `json:"startedAt,omitzero"`
	EndedAt        time.Time `json:"endedAt,omitzero"`
	FinishedAt     time.Time `json:"finishedAt,omitzero"`
}

// Recording is one durable registration. ExperimentResourceID, ArchiveClass and
// SortAt are derived inside the registration transaction. List returns Summary
// and leaves Metadata empty; Get and Add return the stored metadata too.
type Recording struct {
	ID                   string                `json:"id"`
	SessionID            string                `json:"session_id,omitempty"`
	RunID                string                `json:"run_id"`
	ExperimentResourceID string                `json:"experiment_resource_id,omitempty"`
	Kind                 string                `json:"kind"`
	ArchiveClass         RecordingArchiveClass `json:"archive_class"`
	Path                 string                `json:"path"`
	Summary              RecordingSummary      `json:"summary"`
	Metadata             json.RawMessage       `json:"metadata,omitempty"`
	SortAt               time.Time             `json:"sort_at"`
	CreatedAt            time.Time             `json:"created_at"`
}

// NewRecording registers one recording. Association, archive class and sort
// order are never caller facts: registration derives them from the accepted
// Run, the optional Session and the metadata's valid startedAt.
type NewRecording struct {
	ID        string
	SessionID string
	RunID     string
	Kind      string
	Path      string
	Metadata  json.RawMessage
	At        time.Time
}

// RecordingDelete is the immutable guard captured from a selected stored row.
// It carries no metadata payload or caller association. DeleteRecording also
// rechecks the owning Run lifecycle inside its durable transaction.
type RecordingDelete struct {
	ID        string
	RunID     string
	SessionID string
	Kind      string
	Path      string
	CreatedAt time.Time
}

// RecordingFilter selects one chronological page. Zero fields do not filter.
// DataFilesOnly selects non-gallery archive files and requires exactly one
// Experiment. Order defaults to oldest first.
type RecordingFilter struct {
	ExperimentResourceID string
	SessionID            string
	RunID                string
	Kind                 string
	ArchiveClass         RecordingArchiveClass
	DataFilesOnly        bool
	Order                RecordingOrder
	// Limit defaults to 100 and is at most 1000.
	Limit int
	// Cursor is the Next value of the previous page; the page keeps its Order.
	Cursor string
}

type RecordingPage struct {
	Recordings []Recording
	Next       string
}

const (
	MaxRunInputsBytes            = 4 << 20
	MaxRunTriggerBytes           = 1 << 20
	MaxRunSourceObservationBytes = 16 << 10
	MaxRunErrorBytes             = 256 << 10
	MaxRunErrorSummaryBytes      = 4 << 10
	MaxRunResultBytes            = 4 << 20
	MaxRunNodesBytes             = 8 << 20
	MaxRunCleanupErrorsBytes     = 256 << 10
	MaxStopIntentBytes           = 64 << 10
	MaxRecordingMetadata         = 256 << 10
	MaxRecordingSummaryBytes     = 4 << 10
	MaxRunDepth                  = 1024
	MaxPageSize                  = 1000
)

// RunPruneResult carries the next age boundary from the same indexed eligible
// set after deletion. A zero timestamp means no eligible terminal row remains.
type RunPruneResult struct {
	Deleted                  int
	OldestEligibleFinishedAt time.Time
	// HasVictim is the exact post-delete continuation for the supplied policy.
	HasVictim bool
}
