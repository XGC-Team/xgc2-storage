package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type sessionGraphReader struct {
	ctx          context.Context
	tx           *sql.Tx
	scope        string
	target       string
	facts        int
	privateBytes int
	queued       map[string]bool
	edges        map[string][]string
	points       map[string]json.RawMessage
	attemptFacts map[string]sessionFact
}

func (s *sessionGraphReader) charge(n int) error {
	s.facts += n
	if s.facts > model.MaxSessionWorkflowSources {
		return failure("resource_exhausted", "Session workflow dependent fact limit exceeded")
	}
	return nil
}

func (s *sessionGraphReader) read(collection, index string, equal ...string) ([]api.Record, error) {
	q := api.Query{Collection: collection, Index: index}
	for _, v := range equal {
		b, _ := json.Marshal(v)
		q.Equal = append(q.Equal, b)
	}
	r, err := engine.ReadRecords(s.ctx, s.tx, s.scope, q)
	if err != nil {
		return nil, err
	}
	if err = s.charge(len(r.Records)); err != nil {
		return nil, err
	}
	return r.Records, nil
}

func (s *sessionGraphReader) point(collection, key string) (json.RawMessage, error) {
	cacheKey := collection + "\x00" + key
	if raw, ok := s.points[cacheKey]; ok {
		return raw, nil
	}
	r, err := engine.ReadRecords(s.ctx, s.tx, s.scope, api.Query{Collection: collection, Keys: []string{key}})
	if err != nil {
		return nil, err
	}
	if len(r.Records) != 1 || r.Records[0].Missing || r.Records[0].Deleted {
		return nil, failure("data_loss", "Session ownership fact is missing")
	}
	s.points[cacheKey] = r.Records[0].Data
	return r.Records[0].Data, nil
}

func sessionWorkflowLogSnapshot(ctx context.Context, tx *sql.Tx, scope string, r model.SessionWorkflowLogRead) (model.SessionWorkflowLogSnapshot, error) {
	out := model.SessionWorkflowLogSnapshot{Runs: []model.SessionWorkflowLogRun{}, Jobs: []model.SessionWorkflowLogJob{}}
	if !textKey(r.TargetID) || !textKey(r.SessionID) {
		return out, failure("invalid_argument", "exact target and Session identities required")
	}
	s := sessionGraphReader{ctx: ctx, tx: tx, scope: scope, target: r.TargetID, queued: map[string]bool{}, edges: map[string][]string{}, points: map[string]json.RawMessage{}, attemptFacts: map[string]sessionFact{}}
	raw, err := s.point(model.SessionsCollection, r.SessionID)
	if err != nil {
		var e *api.Error
		if a, ok := err.(*api.Error); ok {
			e = a
		}
		if e != nil && e.Code == "data_loss" {
			return out, failure("not_found", "Session not found")
		}
		return out, err
	}
	session, err := publicSessionFact(raw, sessionPublicFields)
	if err != nil {
		return out, err
	}
	if session.text("id") != r.SessionID || session.text("targetId") != r.TargetID {
		return out, failure("not_found", "Session not found on target")
	}
	if !storedLifecycle(session) || !storedIdentity(session, "state") || !storedIdentity(session, "mode") || !storedIdentity(session, "runMode") {
		return out, failure("data_loss", "invalid Session metadata")
	}
	members, err := s.read(model.SessionMembersCollection, model.SessionOwnershipIndex, r.TargetID, r.SessionID)
	if err != nil {
		return out, err
	}
	if len(members) > model.MaxSessionWorkflowMembers {
		return out, failure("resource_exhausted", "Session member limit exceeded")
	}
	memberFacts := []json.RawMessage{}
	queue := []string{}
	runPosition := map[string]int{}
	owners := map[string]bool{}
	for _, record := range members {
		member, e := publicSessionFact(record.Data, memberPublicFields)
		if e != nil {
			return out, e
		}
		if !storedLifecycle(member) || member.text("id") != record.Key || member.text("targetId") != r.TargetID || member.text("sessionId") != r.SessionID || !storedIdentity(member, "ownerId") || !storedIdentity(member, "bindingId") || !storedIdentity(member, "kind") || !storedIdentity(member, "status") {
			return out, failure("data_loss", "invalid Session ownership index fact")
		}
		memberFacts = append(memberFacts, member.raw())
		if member.text("kind") != "workflow_run" && member.text("kind") != "workflow_command" {
			continue
		}
		owner := member.text("kind") + "\x00" + member.text("ownerId")
		if owners[owner] {
			return out, failure("data_loss", "duplicate Session workflow owner")
		}
		owners[owner] = true
		id := member.text("ownerId")
		if !s.queued[id] {
			s.queued[id] = true
			queue = append(queue, id)
		}
	}
	sortSessionFacts(memberFacts)
	out.Session, _ = encode(struct {
		Session json.RawMessage   `json:"session"`
		Members []json.RawMessage `json:"members"`
	}{session.raw(), memberFacts})
	for cursor := 0; cursor < len(queue); cursor++ {
		if len(queue) > model.MaxSessionWorkflowRuns {
			return out, failure("resource_exhausted", "Session Run limit exceeded")
		}
		id := queue[cursor]
		if err = s.charge(1); err != nil {
			return out, err
		}
		if err = ctx.Err(); err != nil {
			return out, err
		}
		raw, err = s.point(model.RunsCollection, id)
		if err != nil {
			return out, err
		}
		run, e := publicSessionFact(raw, runPublicFields)
		if e != nil {
			return out, e
		}
		if !storedLifecycle(run) || run.text("id") != id || run.text("targetId") != r.TargetID || !storedIdentity(run, "rootRunId") || !storedIdentity(run, "definitionId") || run.integer("definitionVersion") < 1 || !storedIdentity(run, "status") {
			return out, failure("data_loss", "invalid Session Run ownership")
		}
		for _, key := range []string{"configDigest", "executionPlanDigest", "registryDigest", "definitionDigest"} {
			if !canonicalSessionPin(run.text(key)) {
				return out, failure("data_loss", "invalid immutable Session Run digest")
			}
		}
		invocations, attempts, invocationFacts, e := s.occurrences(id)
		if e != nil {
			return out, e
		}
		relations, e := s.relations(run, invocationFacts)
		if e != nil {
			return out, e
		}
		for _, linkRaw := range relations["childRuns"] {
			link, _ := publicSessionFact(linkRaw, relationPublicFields["childRuns"])
			// Prepared links and remote target roots are durable relation facts,
			// never a fabricated local Run or permission to expose its lifecycle.
			if link.boolean("targetRoot") || !link.present("boundAt") {
				continue
			}
			var bound time.Time
			if json.Unmarshal(link["boundAt"], &bound) != nil || bound.IsZero() {
				return out, failure("data_loss", "invalid child visibility fence")
			}
			child := link.text("childRunId")
			childRaw, e := s.point(model.RunsCollection, child)
			if e != nil {
				return out, e
			}
			childFact, e := publicSessionFact(childRaw, runPublicFields)
			if e != nil {
				return out, e
			}
			if !boundChildMatches(run, link, childFact) {
				return out, failure("data_loss", "bound child Run disagrees with ownership edge")
			}
			s.edges[id] = append(s.edges[id], child)
			if !s.queued[child] {
				if len(queue) >= model.MaxSessionWorkflowRuns {
					return out, failure("resource_exhausted", "Session Run limit exceeded")
				}
				s.queued[child] = true
				queue = append(queue, child)
			}
		}
		jobs, e := s.jobs(run, invocationFacts)
		if e != nil {
			return out, e
		}
		out.Jobs = append(out.Jobs, jobs...)
		relationObject := map[string]any{"runId": id}
		for kind, facts := range relations {
			relationObject[kind] = facts
		}
		relationRaw, _ := encode(relationObject)
		runPosition[id] = len(out.Runs)
		out.Runs = append(out.Runs, model.SessionWorkflowLogRun{Run: run.raw(), Invocations: invocations, Attempts: attempts, Relations: relationRaw})
	}
	colors := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		if colors[id] == 1 {
			return failure("data_loss", "cyclic Session ownership graph")
		}
		if colors[id] == 2 {
			return nil
		}
		colors[id] = 1
		for _, child := range s.edges[id] {
			if e := visit(child); e != nil {
				return e
			}
		}
		colors[id] = 2
		return nil
	}
	for _, id := range queue {
		if err = visit(id); err != nil {
			return out, err
		}
	}
	sort.Strings(queue)
	orderedRuns := make([]model.SessionWorkflowLogRun, len(queue))
	for i, id := range queue {
		orderedRuns[i] = out.Runs[runPosition[id]]
	}
	out.Runs = orderedRuns
	return out, nil
}

func boundChildMatches(parent, link, child sessionFact) bool {
	if !storedLifecycle(child) || child.text("id") != link.text("childRunId") || child.text("targetId") != parent.text("targetId") || child.text("rootRunId") != parent.text("rootRunId") || child.text("parentRunId") != parent.text("id") || child.text("callNodeId") != link.text("callNodeId") || child.integer("definitionVersion") != link.integer("childDefinitionVersion") {
		return false
	}
	for childKey, linkKey := range map[string]string{"definitionId": "childDefinitionId", "configDigest": "childConfigDigest", "executionPlanDigest": "childExecutionPlanDigest", "registryDigest": "childRegistryDigest", "definitionDigest": "childDefinitionDigest"} {
		if child.text(childKey) == "" || child.text(childKey) != link.text(linkKey) {
			return false
		}
	}
	return true
}

func sortSessionFacts(facts []json.RawMessage) {
	// Decode the ordering coordinates once. Parsing whole JSON objects in a
	// comparison function multiplies allocation by N log N at the exact limit.
	type orderedFact struct {
		raw         json.RawMessage
		created, id string
	}
	ordered := make([]orderedFact, len(facts))
	for i, raw := range facts {
		var f struct {
			ID        string `json:"id"`
			CreatedAt string `json:"createdAt"`
		}
		_ = json.Unmarshal(raw, &f)
		ordered[i] = orderedFact{raw, f.CreatedAt, f.ID}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].created != ordered[j].created {
			return ordered[i].created < ordered[j].created
		}
		return ordered[i].id < ordered[j].id
	})
	for i, fact := range ordered {
		facts[i] = fact.raw
	}
}

func (s *sessionGraphReader) occurrences(runID string) ([]json.RawMessage, []json.RawMessage, map[string]sessionFact, error) {
	invocations, attempts := []json.RawMessage{}, []json.RawMessage{}
	byID := map[string]sessionFact{}
	nodes := map[string]bool{}
	records, err := s.read(model.InvocationsCollection, model.RunFactsIndex, runID)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, record := range records {
		f, e := publicSessionFact(record.Data, invocationPublicFields)
		if e != nil {
			return nil, nil, nil, e
		}
		if !storedLifecycle(f) || f.text("id") != record.Key || f.text("runId") != runID || !storedIdentity(f, "nodeId") || nodes[f.text("nodeId")] || !storedIdentity(f, "kind") || !storedIdentity(f, "status") {
			return nil, nil, nil, failure("data_loss", "invalid or duplicate invocation ownership")
		}
		nodes[f.text("nodeId")] = true
		byID[f.text("id")] = f
	}
	records, err = s.read(model.AttemptsCollection, model.RunFactsIndex, runID)
	if err != nil {
		return nil, nil, nil, err
	}
	seen := map[string]bool{}
	attemptsByID := map[string]sessionFact{}
	for _, record := range records {
		f, e := publicSessionFact(record.Data, attemptPublicFields)
		if e != nil {
			return nil, nil, nil, e
		}
		key := f.text("invocationId") + "\x00" + f.text("phase") + "\x00" + string(f["number"])
		if !storedLifecycle(f) || f.text("id") != record.Key || f.text("runId") != runID || byID[f.text("invocationId")] == nil || f.integer("number") < 1 || seen[key] || !storedIdentity(f, "status") || (f.text("phase") != "execution" && f.text("phase") != "compensation") {
			return nil, nil, nil, failure("data_loss", "invalid or duplicate attempt ownership")
		}
		seen[key] = true
		attemptsByID[f.text("id")] = f
		s.attemptFacts[f.text("id")] = f
		attempts = append(attempts, f.raw())
	}
	for id, f := range byID {
		// Never infer an active attempt from status/number/order. An explicit
		// persistent pointer must resolve in this transaction's run-local index
		// and belong to this exact invocation. Missing/empty means no pointer.
		if f.present("activeAttemptId") {
			var active string
			if json.Unmarshal(f["activeAttemptId"], &active) != nil {
				return nil, nil, nil, failure("data_loss", "invalid active-attempt pointer type")
			}
			if active != "" {
				attempt := attemptsByID[active]
				if !textKey(active) || attempt == nil || attempt.text("runId") != runID || attempt.text("invocationId") != id {
					return nil, nil, nil, failure("data_loss", "active-attempt pointer ownership disagrees")
				}
			}
		}
		invocations = append(invocations, f.raw())
	}
	sortSessionFacts(invocations)
	sortSessionFacts(attempts)
	return invocations, attempts, byID, nil
}
