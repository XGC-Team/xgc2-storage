package model

import "encoding/json"

const OccurrenceExecutionModel = "orchestration-occurrence-v1"
const StopSetAction = "workflowruntime.stop-set"
const RunCommandTargetPrefix = "orchestration-run:"
const MaxGroupAncestors = 32

// These are authoritative data coordinates stored in runs/invocations, not
// caller assertions of eligibility. The same types form the exact predicates.
// Core must encode the private producer capability explicitly rather than
// marshal a public NodeInvocation whose json:"-" omits it.
type RunPrepareState struct {
	ID             string `json:"id"`
	TargetID       string `json:"targetId"`
	RootRunID      string `json:"rootRunId"`
	ParentRunID    string `json:"parentRunId,omitempty"`
	ExecutionModel string `json:"executionModel"`
	Status         string `json:"status"`
}

type ProducerPrepareState struct {
	ID               string `json:"id"`
	RunID            string `json:"runId"`
	NodeID           string `json:"nodeId"`
	Kind             string `json:"kind"`
	Status           string `json:"status"`
	ChildRunProducer bool   `json:"childRunProducer"`
}

// Ancestors is the complete parent-to-root point-guard chain, including the
// ParentGuard first. A missing ancestor, changed parent edge or cycle fails;
// storage always checks each ancestor's durable stop-set receipts itself.
type GroupPrepareCondition struct {
	Parent    RunPrepareState      `json:"parent"`
	Producer  ProducerPrepareState `json:"producer"`
	Ancestors []RecordGuard        `json:"ancestors"`
}

type GroupMemberRead struct {
	ChildID string `json:"child_id,omitempty"`
	EventID string `json:"event_id,omitempty"`
}

// This is an immutable preparation read, not a claim or proof of dispatch.
// Event/link/member/parameters come from the same sealed authoritative row.
type GroupMemberSnapshot struct {
	Group        GroupPrepared   `json:"group"`
	ParentID     string          `json:"parent_id"`
	InvocationID string          `json:"invocation_id"`
	GroupKey     string          `json:"group_key"`
	Ordinal      int             `json:"ordinal"`
	Body         json.RawMessage `json:"body"`
	Member       GroupMember     `json:"member"`
}
