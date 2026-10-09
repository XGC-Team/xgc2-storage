package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	TriggerActivationsCollection       = "trigger_activations"
	TriggerListenersCollection         = "trigger_listeners"
	TriggerEventsCollection            = "trigger_events"
	TriggerTransitionsCollection       = "trigger_transitions"
	TriggerLifecyclesCollection        = "trigger_lifecycles"
	TriggerCompletionsCollection       = "trigger_completions"
	TriggerRemoteActivationsCollection = "trigger_remote_activations"
	TriggerRemoteDeliveriesCollection  = "trigger_remote_deliveries"
)

func TriggerCollections() []api.Collection {
	out := []api.Collection{}
	for _, id := range []string{TriggerActivationsCollection, TriggerListenersCollection, TriggerEventsCollection, TriggerTransitionsCollection, TriggerLifecyclesCollection, TriggerCompletionsCollection, TriggerRemoteActivationsCollection, TriggerRemoteDeliveriesCollection} {
		c := api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 1000000, MaxBytes: 256 << 20, Retention: "trigger identity, delivery and immutable transition facts", Recovery: "storage-owned consistent backup/restore"}
		switch id {
		case TriggerActivationsCollection:
			c.Indexes = []api.Index{{ID: "by_target", Fields: []string{"targetId"}}, {ID: "by_resource", Fields: []string{"resourceId"}}, {ID: "by_public", Fields: []string{"publicId"}, Unique: true}}
		case TriggerListenersCollection:
			c.Indexes = []api.Index{{ID: "by_public", Fields: []string{"publicId"}, Unique: true}, {ID: "by_status", Fields: []string{"status"}}, {ID: "by_resource_status", Fields: []string{"resourceId", "status"}}}
		case TriggerEventsCollection:
			c.Indexes = []api.Index{{ID: "by_run", Fields: []string{"runId"}, Unique: true}, {ID: "by_source", Fields: []string{"sourceKind", "sourceScope", "sourceEventKey"}, Unique: true}, {ID: "by_stream", Fields: []string{"sourceKind", "sourceScope"}}, {ID: "by_dispatch", Fields: []string{"status", "dispatchGateId"}}, {ID: "by_target", Fields: []string{"targetId"}}, {ID: "by_resource", Fields: []string{"resourceId"}}, {ID: "by_listener", Fields: []string{"listenerId"}}, {ID: "by_target_resource", Fields: []string{"targetId", "resourceId"}}}
		case TriggerTransitionsCollection:
			c.Indexes = []api.Index{{ID: "by_event", Fields: []string{"eventId"}}, {ID: "by_revision", Fields: []string{"eventId", "revision"}, Unique: true}}
		case TriggerCompletionsCollection:
			c.Indexes = []api.Index{{ID: "by_state", Fields: []string{"state"}}}
		case TriggerRemoteActivationsCollection:
			c.Indexes = []api.Index{{ID: "by_target", Fields: []string{"targetId"}}, {ID: "by_target_resource", Fields: []string{"targetId", "resourceId"}}, {ID: "by_public", Fields: []string{"publicId"}, Unique: true}}
		case TriggerRemoteDeliveriesCollection:
			c.Indexes = []api.Index{{ID: "by_source", Fields: []string{"publicId", "sourceEventKey"}, Unique: true}, {ID: "by_state", Fields: []string{"state"}}}
		}
		out = append(out, c)
	}
	return out
}
