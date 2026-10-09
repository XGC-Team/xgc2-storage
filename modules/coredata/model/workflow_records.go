package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	DefinitionsCollection      = "definitions"
	DefinitionHeadsCollection  = "definition_heads"
	ActivationTokensCollection = "activation_tokens"
	NodeOutputsCollection      = "node_outputs"
	NodeInputsCollection       = "node_inputs"
	InvocationTasksCollection  = "invocation_tasks"
	RunTasksCollection         = "run_tasks"
	RunAdmissionCollection     = "run_admission"
	RunSequencesCollection     = "run_sequences"
)

// WorkflowCollections supplements the Session graph's public coordinates with
// the private immutable lineage and durable scheduling facts used by writers.
func WorkflowCollections() []api.Collection {
	out := []api.Collection{}
	for _, id := range []string{DefinitionHeadsCollection, ActivationTokensCollection, NodeOutputsCollection, NodeInputsCollection, InvocationTasksCollection, RunTasksCollection, RunAdmissionCollection, RunSequencesCollection} {
		c := api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 1000000, MaxBytes: 1 << 30, Retention: "workflow owns immutable lineage and current recovery facts", Recovery: "storage-owned consistent backup/restore"}
		switch id {
		case DefinitionHeadsCollection:
			c.Indexes = []api.Index{{ID: "by_target", Fields: []string{"targetId"}}}
		case ActivationTokensCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}, {ID: "by_node", Fields: []string{"runId", "nodeId"}, Unique: true}}
		case NodeOutputsCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}, {ID: "by_invocation", Fields: []string{"invocationId"}}, {ID: "by_port", Fields: []string{"invocationId", "port"}, Unique: true}}
		case NodeInputsCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}, {ID: "by_invocation", Fields: []string{"consumerInvocationId"}}, {ID: "by_slot", Fields: []string{"consumerInvocationId", "edgeId", "inputKey", "ordinal"}, Unique: true}}
		case InvocationTasksCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}, {ID: "by_invocation", Fields: []string{"invocationId"}}, {ID: "by_generation", Fields: []string{"invocationId", "kind", "generation"}, Unique: true}, {ID: "by_status", Fields: []string{"status"}}}
		case RunTasksCollection:
			c.Indexes = []api.Index{{ID: "by_state", Fields: []string{"state"}}}
		}
		out = append(out, c)
	}
	return out
}
