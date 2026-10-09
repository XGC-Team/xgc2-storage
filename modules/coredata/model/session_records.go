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
	SessionsCollection           = "sessions"
	SessionFrontiersCollection   = "session_frontiers"
	SessionMembersCollection     = "session_members"
	SessionBindingsCollection    = "session_bindings"
	SessionStopIntentsCollection = "session_stop_intents"
	ExecutionLeasesCollection    = "execution_leases"
	RunsCollection               = "runs"
	InvocationsCollection        = "invocations"
	AttemptsCollection           = "attempts"
	WorkflowJobsCollection       = "workflow_jobs"
	SessionOwnershipIndex        = "by_session"
	RunFactsIndex                = "by_run"
	WorkflowOriginIndex          = "by_origin"
)

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
// entity IDs. No SQL implementation is
// reachable through this package. A fresh slice is returned on each call.
func SessionGraphCollections() []api.Collection {
	var out []api.Collection
	for _, id := range []string{SessionFrontiersCollection, SessionsCollection, SessionMembersCollection, SessionBindingsCollection, SessionStopIntentsCollection, ExecutionLeasesCollection, RunsCollection, InvocationsCollection, AttemptsCollection, WorkflowJobsCollection, DefinitionsCollection} {
		c := api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 1000000, MaxBytes: 1 << 30,
			Retention: "Core owns current/recovery facts; reviewed explicit retention cleanup only", Recovery: "storage-owned consistent new-data backup/restore"}
		switch id {
		case DefinitionsCollection:
			c.Indexes = []api.Index{{ID: "by_definition", Fields: []string{"id"}}, {ID: "by_execution_identity", Fields: []string{"targetId", "configDigest", "executionPlanDigest", "registryDigest", "digest"}}}
		case RunsCollection:
			c.Indexes = []api.Index{{ID: "by_automation", Fields: []string{"targetId", "automationResourceId"}}, {ID: "by_target", Fields: []string{"targetId"}}, {ID: "by_admission", Fields: []string{"targetId", "admissionKey"}}, {ID: "by_dedupe", Fields: []string{"targetId", "dedupeKey"}, Unique: true}, {ID: "admission_slot", Fields: []string{"targetId", "admissionKey", "admissionSlot"}, Unique: true}, {ID: "by_replaced", Fields: []string{"replacesRunId"}}, {ID: "by_parent", Fields: []string{"parentRunId"}}}
			c.MaxBytes = 4 << 30
		case SessionsCollection:
			c.Indexes = []api.Index{
				{ID: "active_target", Fields: []string{"targetId", "activeSlot"}, Unique: true},
				{ID: "active_experiment", Fields: []string{"targetId", "experimentResourceId", "activeSlot"}},
				{ID: "by_target", Fields: []string{"targetId"}},
				{ID: "by_experiment", Fields: []string{"experimentResourceId"}},
				{ID: "by_target_experiment", Fields: []string{"targetId", "experimentResourceId"}},
				{ID: "by_opening_run", Fields: []string{"targetId", "openingRunId"}},
				{ID: "by_active", Fields: []string{"activeSlot"}},
			}
		case SessionMembersCollection:
			c.Indexes = []api.Index{
				{ID: SessionOwnershipIndex, Fields: []string{"targetId", "sessionId"}},
				{ID: "by_owner", Fields: []string{"targetId", "sessionId", "kind", "ownerId"}, Unique: true},
				{ID: "by_workflow_owner", Fields: []string{"targetId", "kind", "ownerId"}},
			}
		case InvocationsCollection, AttemptsCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}}
		case WorkflowJobsCollection:
			c.Indexes = []api.Index{{ID: WorkflowOriginIndex, Fields: []string{"runTargetId", "runId", "invocationId"}, Unique: true}, {ID: RunFactsIndex, Fields: []string{"runTargetId", "runId"}}}
		}
		out = append(out, c)
	}
	out = append(out, WorkflowCollections()...)
	return append(out, WorkflowRelationCollections()...)
}
