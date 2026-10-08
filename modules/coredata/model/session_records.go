package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/api"
)

// These coordinates are shared by deployment, authoritative execution writers
// and the bounded reader. They are new data classes, not retired SQL table names.
const (
	SessionsCollection       = "sessions"
	SessionMembersCollection = "session_members"
	RunsCollection           = "runs"
	InvocationsCollection    = "invocations"
	AttemptsCollection       = "attempts"
	RunRelationsCollection   = "run_relations"
	WorkflowJobsCollection   = "workflow_jobs"
	SessionOwnershipIndex    = "by_session"
	RunFactsIndex            = "by_run"
	WorkflowOriginIndex      = "by_origin"
)

// RunRelationRecord stores one authoritative relation, including its private
// facts. Execute projects its allowlisted public fields; no prebuilt aggregate
// or caller-supplied tree is accepted. Kind is an ExecutionRelations slice name.
// Sealed group.prepare facts are read from that operation's own authority.
type RunRelationRecord struct {
	ID    string          `json:"id"`
	RunID string          `json:"runId"`
	Kind  string          `json:"kind"`
	Fact  json.RawMessage `json:"fact"`
}

// WorkflowJobRecord is written only by the trusted workflow Job admission
// path, in the same action as its Job state. Ordinary Job admission has no
// origin. Flat coordinates are the scalar index authority and must exactly
// agree with Origin and the selected Run/invocation. Job may contain private
// durable fields, which are never forwarded by the read projection.
type WorkflowJobRecord struct {
	RunTargetID  string          `json:"runTargetId"`
	RunID        string          `json:"runId"`
	InvocationID string          `json:"invocationId"`
	Origin       json.RawMessage `json:"origin"`
	Job          json.RawMessage `json:"job"`
}

// RelationRecordKey names the kind/identity pair without length-dependent
// concatenation or collisions between different relation kinds.
func RelationRecordKey(kind, id string) string {
	b, _ := json.Marshal([]string{kind, id})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// SessionGraphCollections is the single deployment catalog. Record keys are
// entity IDs except run_relations (RelationRecordKey). No SQL implementation is
// reachable through this package. A fresh slice is returned on each call.
func SessionGraphCollections() []api.Collection {
	var out []api.Collection
	for _, id := range []string{SessionsCollection, SessionMembersCollection, RunsCollection, InvocationsCollection, AttemptsCollection, RunRelationsCollection, WorkflowJobsCollection, "definitions"} {
		c := api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 100000, MaxBytes: 256 << 20,
			Retention: "Core owns current/recovery facts; reviewed explicit retention cleanup only", Recovery: "storage-owned consistent new-data backup/restore"}
		switch id {
		case SessionMembersCollection:
			c.Indexes = []api.Index{{ID: SessionOwnershipIndex, Fields: []string{"targetId", "sessionId"}}}
		case InvocationsCollection, AttemptsCollection, RunRelationsCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}}
		case WorkflowJobsCollection:
			c.Indexes = []api.Index{{ID: WorkflowOriginIndex, Fields: []string{"runTargetId", "runId", "invocationId"}, Unique: true}, {ID: RunFactsIndex, Fields: []string{"runTargetId", "runId"}}}
		}
		out = append(out, c)
	}
	return out
}
