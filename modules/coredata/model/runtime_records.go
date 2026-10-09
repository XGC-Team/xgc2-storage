package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	ProcessInstancesCollection = "process_instances"
	SchedulesCollection        = "schedules"
	RobotBindingsCollection    = "robot_bindings"
	RobotConnectionsCollection = "robot_connections"
	RobotOperationsCollection  = "robot_operations"
	RobotAttemptsCollection    = "robot_attempts"
	RobotBatchesCollection     = "robot_batches"
	RobotBatchItemsCollection  = "robot_batch_items"
)

// RuntimeCollections declares the existing execution transaction participants.
// All mutations join execution.commit; the records carry no SQL handles.
func RuntimeCollections() []api.Collection {
	indexes := map[string][]api.Index{
		ProcessInstancesCollection: {
			{ID: "by_target", Fields: []string{"targetId"}},
			{ID: "by_definition", Fields: []string{"definitionId"}},
			{ID: "by_owner", Fields: []string{"ownerType", "ownerId"}},
			{ID: "by_reconciliation", Fields: []string{"reconciliation"}},
		},
		SchedulesCollection: {
			{ID: "by_target", Fields: []string{"targetId"}},
			{ID: "by_owner_kind", Fields: []string{"ownerKind"}},
			{ID: "by_owner", Fields: []string{"ownerKind", "ownerId"}, Unique: true},
			{ID: "by_enabled", Fields: []string{"enabled"}},
		},
		RobotBindingsCollection: {
			{ID: "by_run", Fields: []string{"targetId", "runId"}},
			{ID: "by_provider", Fields: []string{"targetId", "runId", "providerDefinitionId"}, Unique: true},
		},
		RobotConnectionsCollection: {
			{ID: "by_run", Fields: []string{"targetId", "runId"}},
			{ID: "by_state", Fields: []string{"state"}},
		},
		RobotOperationsCollection: {
			{ID: "by_request", Fields: []string{"targetId", "idempotencyKey"}, Unique: true},
			{ID: "by_command", Fields: []string{"commandId"}, Unique: true},
			{ID: "by_run", Fields: []string{"targetId", "runId"}},
			{ID: "by_binding_run", Fields: []string{"targetId", "bindingRunId"}},
			{ID: "by_phase", Fields: []string{"phase"}},
			{ID: "by_attempt", Fields: []string{"currentAttemptId"}, Unique: true},
		},
		RobotAttemptsCollection: {
			{ID: "by_operation", Fields: []string{"operationId"}},
			{ID: "by_number", Fields: []string{"operationId", "number"}, Unique: true},
			{ID: "by_adapter", Fields: []string{"adapterIdempotencyKey"}, Unique: true},
			{ID: "by_owner", Fields: []string{"operationId", "ownerAttempt"}, Unique: true},
		},
		RobotBatchesCollection: {
			{ID: "by_state", Fields: []string{"state"}},
			{ID: "by_owner", Fields: []string{"targetId", "initiatingRunId", "rootRunId", "controllerCheckpointDigest"}},
		},
		RobotBatchItemsCollection: {
			{ID: "by_batch", Fields: []string{"batchId"}},
			{ID: "by_attempt", Fields: []string{"attemptId"}, Unique: true},
		},
	}
	var out []api.Collection
	for _, id := range []string{ProcessInstancesCollection, SchedulesCollection, RobotBindingsCollection, RobotConnectionsCollection, RobotOperationsCollection, RobotAttemptsCollection, RobotBatchesCollection, RobotBatchItemsCollection} {
		out = append(out, api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 50000, MaxBytes: 256 << 20, Indexes: indexes[id], Retention: "Core explicit completed-history retention", Recovery: "storage-owned consistent backup/restore"})
	}
	return out
}
