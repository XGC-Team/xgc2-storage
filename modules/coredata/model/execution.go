package model

import (
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

const MaxExecutionStateMutations = api.MaxNamedStateMutations

// ExecutionCommit describes one explicit atomic action, not a remote callback.
// Command payloads, terminal results and event payloads retain encoded JSON
// bytes. The consumer decodes seq/offset decimal strings at its domain boundary.
type ExecutionCommit struct {
	CommandAcceptedAt     time.Time                `json:"command_accepted_at,omitempty"`
	ConfigurationPins     []ConfigurationReference `json:"configuration_pins,omitempty"`
	ConfigurationMainPins []ConfigurationMainPin   `json:"configuration_main_pins,omitempty"`
	State                 []api.Mutation           `json:"state,omitempty"`
	Guards                []RecordGuard            `json:"guards,omitempty"`
	Events                []ExecutionEventInput    `json:"events,omitempty"`
	Command               *CommandRequest          `json:"command,omitempty"`
	Completion            *CommandCompletion       `json:"completion,omitempty"`
}

type CommandRequest struct {
	RequestID      string `json:"request_id"`
	IdempotencyKey string `json:"idempotency_key"`
	CommandID      string `json:"command_id"`
	Actor          string `json:"actor"`
	Risk           string `json:"risk"`
	Target         string `json:"target"`
	Action         string `json:"action"`
	Reason         string `json:"reason,omitempty"`
	Payload        []byte `json:"payload,omitempty"`
}

type CommandReceipt struct {
	CommandRequest
	Status       string     `json:"status"`
	ResultRef    string     `json:"result_ref,omitempty"`
	Result       []byte     `json:"result,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

type CommandResult struct {
	Status       string `json:"status"`
	ResultRef    string `json:"result_ref,omitempty"`
	Result       []byte `json:"result,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type CommandCompletion struct {
	CommandID string        `json:"command_id"`
	Result    CommandResult `json:"result"`
}

type ExecutionEventInput struct {
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Type       string    `json:"type"`
	Level      string    `json:"level,omitempty"`
	Payload    []byte    `json:"payload,omitempty"`
	CommandID  string    `json:"command_id,omitempty"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
}

type ExecutionEvent struct {
	ExecutionEventInput
	Offset string `json:"offset"`
	Seq    string `json:"seq"`
}

type ExecutionCommitted struct {
	// Replayed means an exact business-command/terminal replay skipped State and Events.
	// An outer request replay instead returns the original cached result and receipt.
	Replayed       bool             `json:"replayed"`
	State          []api.Record     `json:"state,omitempty"`
	Events         []ExecutionEvent `json:"events,omitempty"`
	Command        *CommandReceipt  `json:"command,omitempty"`
	CommandCreated bool             `json:"command_created"`
}

type CommandRead struct {
	ID             string `json:"id,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type CommandFound struct {
	Found   bool            `json:"found"`
	Receipt *CommandReceipt `json:"receipt,omitempty"`
}

type EventCursor struct {
	StreamID     string `json:"stream_id"`
	LatestOffset string `json:"latest_offset"`
}

type EventRead struct {
	AfterOffset string `json:"after_offset"`
	Through     string `json:"through,omitempty"`
	EntityType  string `json:"entity_type,omitempty"`
	EntityID    string `json:"entity_id,omitempty"`
	Type        string `json:"type,omitempty"`
	AfterSeq    string `json:"after_seq,omitempty"`
	Limit       int    `json:"limit"`
}

type EventPage struct {
	Cursor     EventCursor      `json:"cursor"`
	Through    string           `json:"through"`
	NextOffset string           `json:"next_offset"`
	Events     []ExecutionEvent `json:"events"`
}

// CommandListRead is a finite recovery/read set, never an inventory predicate.
type CommandListRead struct {
	IDs            []string `json:"ids,omitempty"`
	AcceptedAction string   `json:"accepted_action,omitempty"`
}
type CommandList struct {
	Receipts []CommandReceipt `json:"receipts"`
}
type JobEventPageRead struct {
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
	Level    string `json:"level,omitempty"`
	Query    string `json:"query,omitempty"`
}
type JobEventPage struct {
	Events []ExecutionEvent `json:"events"`
	Total  int64            `json:"total"`
}
