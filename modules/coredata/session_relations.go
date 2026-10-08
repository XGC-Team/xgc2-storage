package coredata

import (
	"encoding/json"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func (s *sessionGraphReader) relations(run sessionFact, invocations map[string]sessionFact) (map[string][]json.RawMessage, error) {
	id := run.text("id")
	out := map[string][]json.RawMessage{}
	seen := map[string]bool{}
	for kind := range relationPublicFields {
		out[kind] = []json.RawMessage{}
	}
	appendFact := func(kind string, raw json.RawMessage) error {
		fields, ok := relationPublicFields[kind]
		if !ok {
			return failure("data_loss", "unknown authoritative relation kind")
		}
		f, err := publicSessionFact(raw, fields)
		if err != nil {
			return err
		}
		key := kind + "\x00" + f.text("id")
		if !storedLifecycle(f) || seen[key] {
			return failure("data_loss", "invalid or duplicate relation fact")
		}
		seen[key] = true
		if f.present("targetId") && f.text("targetId") != s.target {
			return failure("data_loss", "cross-target Session relation")
		}
		if f.present("runId") && f.text("runId") != id {
			return failure("data_loss", "cross-Run Session relation")
		}
		if kind != "childRuns" && kind != "childRunGroups" && kind != "childRunGroupMembers" {
			if f.text("runId") != id || invocations[f.text("invocationId")] == nil {
				return failure("data_loss", "relation occurrence ownership is missing")
			}
			if kind != "waits" && f.text("targetId") != s.target {
				return failure("data_loss", "relation target ownership is missing")
			}
			for _, key := range []string{"attemptId", "preparedAttemptId", "boundAttemptId"} {
				if f.present(key) {
					a := s.attemptFacts[f.text(key)]
					if a == nil || a.text("runId") != id || a.text("invocationId") != f.text("invocationId") {
						return failure("data_loss", "relation attempt ownership disagrees")
					}
				}
			}
		}
		if kind == "childRuns" || kind == "childRunGroups" {
			if f.text("parentRunId") != id || f.text("rootRunId") != run.text("rootRunId") || f.text("targetId") != s.target {
				return failure("data_loss", "invalid child ownership coordinates")
			}
			producer, node := "parentInvocationId", "callNodeId"
			if kind == "childRunGroups" {
				producer, node = "producerInvocationId", "producerNodeId"
			}
			inv := invocations[f.text(producer)]
			if inv == nil || inv.text("nodeId") != f.text(node) {
				return failure("data_loss", "child ownership producer is missing")
			}
		}
		if kind == "childRuns" {
			if !storedIdentity(f, "childRunId") || !storedIdentity(f, "ownerRunId") {
				return failure("data_loss", "invalid child ownership identity")
			}
			// A prepared/remote relation cannot claim a local lifecycle. The
			// bound local lifecycle below is read from the same Run authority.
			delete(f, "runStatus")
			delete(f, "runRevision")
			if !f.boolean("targetRoot") && f.present("boundAt") {
				childRaw, e := s.point(model.RunsCollection, f.text("childRunId"))
				if e != nil {
					return e
				}
				child, e := publicSessionFact(childRaw, runPublicFields)
				if e != nil {
					return e
				}
				if !boundChildMatches(run, f, child) {
					return failure("data_loss", "bound child relation is corrupt")
				}
				f["runStatus"], f["runRevision"] = child["status"], child["revision"]
			}
		}
		out[kind] = append(out[kind], f.raw())
		return nil
	}
	records, err := s.read(model.RunRelationsCollection, model.RunFactsIndex, id)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		var r model.RunRelationRecord
		if json.Unmarshal(record.Data, &r) != nil || r.RunID != id || !textKey(r.ID) || record.Key != model.RelationRecordKey(r.Kind, r.ID) {
			return nil, failure("data_loss", "invalid authoritative relation record")
		}
		var fact sessionFact
		_ = json.Unmarshal(r.Fact, &fact)
		if fact.text("id") != r.ID {
			return nil, failure("data_loss", "relation envelope identity disagrees")
		}
		if err = appendFact(r.Kind, r.Fact); err != nil {
			return nil, err
		}
	}
	// group.prepare's immutable member/link facts retain one authority. The
	// parent index selects only this Run's sealed groups, without an inventory.
	rows, err := s.tx.QueryContext(s.ctx, "SELECT id,invocation_id,group_key FROM core_groups WHERE scope=? AND parent_id=? AND sealed=1 ORDER BY id LIMIT ?", s.scope, id, model.MaxSessionWorkflowSources+1)
	if err != nil {
		return nil, err
	}
	type sealedCoordinates struct{ id, invocation, key string }
	groupIDs := []sealedCoordinates{}
	for rows.Next() {
		var groupID sealedCoordinates
		if err = rows.Scan(&groupID.id, &groupID.invocation, &groupID.key); err != nil {
			rows.Close()
			return nil, err
		}
		if err = s.charge(1); err != nil {
			rows.Close()
			return nil, err
		}
		groupIDs = append(groupIDs, groupID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, groupID := range groupIDs {
		group, e := groupSnapshot(s.ctx, s.tx, s.scope, model.GroupRead{ID: groupID.id})
		if e != nil {
			return nil, e
		}
		if e = s.charge(2 * len(group.Members)); e != nil {
			return nil, e
		}
		wire, _ := encode(group)
		s.privateBytes += len(wire)
		if s.privateBytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "Session group materialization byte limit exceeded")
		}
		header, e := publicSessionFact(group.Body, relationPublicFields["childRunGroups"])
		if e != nil {
			return nil, e
		}
		if header.text("id") != groupID.id || header.text("producerInvocationId") != groupID.invocation || header.text("groupKey") != groupID.key || header.integer("memberCount") != int64(group.MemberCount) {
			return nil, failure("data_loss", "sealed group metadata disagrees with indexed authority")
		}
		if e = appendFact("childRunGroups", group.Body); e != nil {
			return nil, e
		}
		for ordinal, member := range group.Members {
			link, e := publicSessionFact(member.Link, relationPublicFields["childRuns"])
			if e != nil {
				return nil, e
			}
			fact, e := publicSessionFact(member.Body, relationPublicFields["childRunGroupMembers"])
			if e != nil {
				return nil, e
			}
			if link.text("childRunId") != member.ChildID || link.text("parentInvocationId") != groupID.invocation || link.integer("ordinal") != int64(ordinal) || fact.text("childRunId") != member.ChildID || fact.text("groupId") != groupID.id || fact.text("itemKey") != member.ItemKey || fact.integer("ordinal") != int64(ordinal) {
				return nil, failure("data_loss", "sealed group member coordinates disagree with authority")
			}
			if e = appendFact("childRuns", member.Link); e != nil {
				return nil, e
			}
			if e = appendFact("childRunGroupMembers", member.Body); e != nil {
				return nil, e
			}
		}
	}
	groups := map[string]sessionFact{}
	links := map[string]bool{}
	groupCounts := map[string]int64{}
	runtimeGroups := map[string]sessionFact{}
	for _, raw := range out["childRunGroups"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		groups[f.text("id")] = f
	}
	for _, raw := range out["childRuns"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		if links[f.text("childRunId")] {
			return nil, failure("data_loss", "duplicate child ownership edge")
		}
		links[f.text("childRunId")] = true
	}
	for _, raw := range out["childRunGroupMembers"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		if groups[f.text("groupId")] == nil || !links[f.text("childRunId")] {
			return nil, failure("data_loss", "orphaned child group member")
		}
		groupCounts[f.text("groupId")]++
	}
	for id, group := range groups {
		if group.integer("memberCount") != groupCounts[id] {
			return nil, failure("data_loss", "incomplete child group membership")
		}
	}
	for _, raw := range out["runtimeGroups"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		runtimeGroups[f.text("id")] = f
	}
	for _, raw := range out["runtimes"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		group := runtimeGroups[f.text("groupId")]
		if group == nil || group.text("invocationId") != f.text("invocationId") {
			return nil, failure("data_loss", "runtime binding group ownership disagrees")
		}
	}
	for _, raw := range out["resources"] {
		var f sessionFact
		_ = json.Unmarshal(raw, &f)
		if f.text("runtimeGroupId") != "" {
			group := runtimeGroups[f.text("runtimeGroupId")]
			if group == nil || group.text("invocationId") != f.text("invocationId") {
				return nil, failure("data_loss", "resource runtime group ownership disagrees")
			}
		}
	}
	for _, facts := range out {
		sortSessionFacts(facts)
	}
	return out, nil
}

func (s *sessionGraphReader) jobs(run sessionFact, invocations map[string]sessionFact) ([]model.SessionWorkflowLogJob, error) {
	out := []model.SessionWorkflowLogJob{}
	records, err := s.read(model.WorkflowJobsCollection, model.RunFactsIndex, s.target, run.text("id"))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, record := range records {
		var r model.WorkflowJobRecord
		if json.Unmarshal(record.Data, &r) != nil || r.RunTargetID != s.target || r.RunID != run.text("id") || invocations[r.InvocationID] == nil || seen[r.InvocationID] {
			return nil, failure("data_loss", "ambiguous or orphaned workflow Job origin")
		}
		seen[r.InvocationID] = true
		origin, e := publicSessionFact(r.Origin, originPublicFields)
		if e != nil {
			return nil, e
		}
		inv := invocations[r.InvocationID]
		if origin.text("RunTargetID") != r.RunTargetID || origin.text("RunID") != r.RunID || origin.text("InvocationID") != r.InvocationID || origin.text("NodeID") != inv.text("nodeId") || origin.text("NodeKind") != inv.text("kind") || origin.integer("NodeTypeVersion") < 1 {
			return nil, failure("data_loss", "workflow Job origin disagrees with occurrence")
		}
		for originKey, runKey := range map[string]string{"RootRunID": "rootRunId", "DefinitionID": "definitionId", "ConfigDigest": "configDigest", "ExecutionPlanDigest": "executionPlanDigest", "RegistryDigest": "registryDigest", "DefinitionDigest": "definitionDigest"} {
			if origin.text(originKey) == "" || origin.text(originKey) != run.text(runKey) {
				return nil, failure("data_loss", "workflow Job origin immutable pins disagree")
			}
		}
		job, e := publicSessionFact(r.Job, jobPublicFields)
		if e != nil {
			return nil, e
		}
		if job.text("ID") != record.Key || job.text("TargetID") != s.target || !storedIdentity(job, "ID") || !storedIdentity(job, "Kind") || !storedIdentity(job, "Status") || job.integer("Revision") < 1 || job.integer("AttemptCount") < 0 {
			return nil, failure("data_loss", "invalid trusted workflow Job metadata")
		}
		var updated time.Time
		if !job.present("AttemptCount") || json.Unmarshal(job["UpdatedAt"], &updated) != nil || updated.IsZero() {
			return nil, failure("data_loss", "invalid Job projection metadata")
		}
		var body map[string]json.RawMessage
		_ = json.Unmarshal(r.Job, &body)
		var attempts []json.RawMessage
		attemptsPresent := false
		for key, raw := range body {
			if key == "Attempts" || key == "attempts" {
				if attemptsPresent || json.Unmarshal(raw, &attempts) != nil {
					return nil, failure("data_loss", "invalid Job attempt lineage")
				}
				attemptsPresent = true
			}
		}
		if e = s.charge(len(attempts)); e != nil {
			return nil, e
		}
		if int64(len(attempts)) != job.integer("AttemptCount") {
			return nil, failure("data_loss", "incomplete Job attempt lineage")
		}
		projected := []json.RawMessage{}
		ids := map[string]bool{}
		numbers := map[int64]bool{}
		for _, raw := range attempts {
			f, e := publicSessionFact(raw, jobAttemptPublicFields)
			if e != nil {
				return nil, e
			}
			var started time.Time
			if !storedIdentity(f, "ID") || f.integer("Number") < 1 || ids[f.text("ID")] || numbers[f.integer("Number")] || json.Unmarshal(f["StartedAt"], &started) != nil || started.IsZero() {
				return nil, failure("data_loss", "invalid Job attempt identity")
			}
			ids[f.text("ID")] = true
			numbers[f.integer("Number")] = true
			if f.present("FinishedAt") {
				var finished time.Time
				if json.Unmarshal(f["FinishedAt"], &finished) != nil || finished.Before(started) {
					return nil, failure("data_loss", "invalid Job attempt time window")
				}
			}
			projected = append(projected, f.raw())
		}
		job["Attempts"], _ = encode(projected)
		out = append(out, model.SessionWorkflowLogJob{Origin: origin.raw(), Job: job.raw()})
	}
	return out, nil
}
