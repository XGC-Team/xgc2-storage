package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	ResourceGatesCollection            = "resource_gates"
	ChildLinksCollection               = "child_links"
	ChildGroupsCollection              = "child_groups"
	ChildGroupMembersCollection        = "child_group_members"
	WaitsCollection                    = "waits"
	EffectsCollection                  = "effects"
	RuntimeGroupsCollection            = "runtime_groups"
	RuntimeBindingsCollection          = "runtime_bindings"
	RuntimeObservationsCollection      = "runtime_observations"
	RuntimeObservationLatestCollection = "runtime_observation_latest"
	ResourceBindingsCollection         = "resource_bindings"
	ResourceBundlesCollection          = "resource_bundles"
	ResourceMembersCollection          = "resource_members"
	ResourceLeasesCollection           = "resource_leases"
)

// RelationCollection names the existing typed fact authority. Facts stay flat;
// the public reader and writer share these coordinates without a JSON wrapper.
func RelationCollection(kind string) string {
	switch kind {
	case "childRuns":
		return ChildLinksCollection
	case "childRunGroups":
		return ChildGroupsCollection
	case "childRunGroupMembers":
		return ChildGroupMembersCollection
	case "waits":
		return WaitsCollection
	case "effects":
		return EffectsCollection
	case "runtimeGroups":
		return RuntimeGroupsCollection
	case "runtimes":
		return RuntimeBindingsCollection
	case "runtimeAttachmentObservations":
		return RuntimeObservationsCollection
	case "resources":
		return ResourceBindingsCollection
	}
	return ""
}
func WorkflowRelationCollections() []api.Collection {
	out := []api.Collection{}
	for _, id := range []string{ChildLinksCollection, ChildGroupsCollection, ChildGroupMembersCollection, WaitsCollection, EffectsCollection, RuntimeGroupsCollection, RuntimeBindingsCollection, RuntimeObservationsCollection, RuntimeObservationLatestCollection, ResourceGatesCollection, ResourceBindingsCollection, ResourceBundlesCollection, ResourceMembersCollection, ResourceLeasesCollection} {
		c := api.Collection{ID: id, MaxRecordBytes: 3 << 20, MaxRecords: 1000000, MaxBytes: 256 << 20, Retention: "workflow typed lifecycle and immutable lineage facts", Recovery: "storage-owned consistent backup/restore"}
		c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"runId"}}}
		switch id {
		case ChildLinksCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"parentRunId"}}, {ID: "by_parent_slot", Fields: []string{"parentRunId", "parentInvocationId", "ordinal"}, Unique: true}}
		case ChildGroupsCollection:
			c.Indexes = []api.Index{{ID: RunFactsIndex, Fields: []string{"parentRunId"}}, {ID: "by_group_key", Fields: []string{"parentRunId", "producerInvocationId", "groupKey"}, Unique: true}, {ID: "by_state", Fields: []string{"state"}}}
		case ChildGroupMembersCollection:
			c.Indexes = []api.Index{{ID: "by_state", Fields: []string{"state"}}, {ID: "by_group", Fields: []string{"groupId"}}, {ID: "by_child", Fields: []string{"childRunId"}, Unique: true}, {ID: "by_ordinal", Fields: []string{"groupId", "ordinal"}, Unique: true}, {ID: "by_item", Fields: []string{"groupId", "itemKey"}, Unique: true}}

		case EffectsCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}}, api.Index{ID: "by_effect_key", Fields: []string{"invocationId", "effectKey"}, Unique: true})
		case RuntimeGroupsCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}}, api.Index{ID: "by_group_key", Fields: []string{"invocationId", "groupKey"}, Unique: true})
		case RuntimeBindingsCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}}, api.Index{ID: "by_group", Fields: []string{"groupId"}}, api.Index{ID: "by_target", Fields: []string{"targetId"}}, api.Index{ID: "by_state", Fields: []string{"state"}}, api.Index{ID: "by_binding_key", Fields: []string{"groupId", "bindingKey"}, Unique: true}, api.Index{ID: "owned_backend", Fields: []string{"targetId", "backendKind", "backendId", "ownedActiveSlot"}, Unique: true})
		case RuntimeObservationsCollection, RuntimeObservationLatestCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_binding", Fields: []string{"bindingId"}})
		case ResourceGatesCollection:
			c.Indexes = []api.Index{{ID: "by_target", Fields: []string{"targetId"}}}
		case ResourceBindingsCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}}, api.Index{ID: "by_bundle", Fields: []string{"claimBundleId"}}, api.Index{ID: "by_resource_state", Fields: []string{"targetId", "resourceKey", "state"}}, api.Index{ID: "by_state", Fields: []string{"state"}}, api.Index{ID: "by_key", Fields: []string{"invocationId", "bindingKey"}, Unique: true}, api.Index{ID: "by_active_slot", Fields: []string{"targetId", "resourceKey", "activeSlot"}, Unique: true}, api.Index{ID: "by_active_lease", Fields: []string{"leaseResourceKey", "leaseActiveSlot"}, Unique: true})
		case ResourceBundlesCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}}, api.Index{ID: "by_key", Fields: []string{"invocationId", "groupKey"}, Unique: true}, api.Index{ID: "by_state", Fields: []string{"state"}})
		case ResourceMembersCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_bundle", Fields: []string{"bundleId"}}, api.Index{ID: "by_resource", Fields: []string{"targetId", "resourceKey"}}, api.Index{ID: "by_ordinal", Fields: []string{"bundleId", "ordinal"}, Unique: true})
		case ResourceLeasesCollection:
			c.Indexes = []api.Index{{ID: "by_binding", Fields: []string{"bindingId"}}, {ID: "by_generation", Fields: []string{"bindingId", "generation"}, Unique: true}}
		case WaitsCollection:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId", "generation"}, Unique: true}, api.Index{ID: "by_resume", Fields: []string{"resumeTokenDigest"}, Unique: true}, api.Index{ID: "by_state", Fields: []string{"state"}}, api.Index{ID: "by_subject", Fields: []string{"type", "subjectId", "state"}})
		default:
			c.Indexes = append(c.Indexes, api.Index{ID: "by_invocation", Fields: []string{"invocationId"}})
		}
		out = append(out, c)
	}
	return out
}
