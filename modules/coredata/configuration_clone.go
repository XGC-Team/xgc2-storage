package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationCloneReceipt(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationNamespaceCloneReceipt) (out model.ConfigurationNamespaceResult, err error) {
	if _, err = configurationDomain(ctx, tx, q.Domain); err != nil {
		return
	}
	if err = configurationMutationValid(q.Mutation); err != nil {
		return
	}
	var intent, op string
	var body []byte
	err = tx.QueryRowContext(ctx, `SELECT intent_digest,operation,body FROM core_catalog_receipts WHERE scope=? AND domain=? AND mutation_key=? UNION ALL SELECT intent_digest,operation,body FROM core_configuration_receipts WHERE scope=? AND domain=? AND mutation_key=?`, scope, q.Domain.Key, q.Mutation.Key, scope, q.Domain.Key, q.Mutation.Key).Scan(&intent, &op, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return
	}
	if intent != q.Mutation.IntentDigest || op != model.ConfigurationNamespaceCloneOperation {
		return out, failure("conflict", "mutation identity reused")
	}
	if len(body) > model.MaxConfigurationReceiptResultBytes || json.Unmarshal(body, &out) != nil || !validAffectedAgentIDs(out.AffectedAgentIDs) || !validAffectedWorkflows(out.AffectedWorkflowIDs) {
		return out, failure("data_loss", "invalid namespace clone receipt")
	}
	out.Replayed = true
	return
}
func configurationClone(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationNamespaceClone, own ...func(*model.ConfigurationNamespaceClone) error) (out model.ConfigurationNamespaceResult, err error) {
	if out, err = configurationCloneReceipt(ctx, tx, scope, model.ConfigurationNamespaceCloneReceipt{Domain: q.Domain, Mutation: q.Mutation}); err != nil || out.Replayed {
		return
	}
	if len(own) != 0 {
		if err = own[0](&q); err != nil {
			return out, err
		}
	}
	if !positiveRevision(q.ExpectedRevision) || len(q.Namespaces) > MaxCloneNamespaces || len(q.Resources) > MaxCloneResources {
		return out, failure("invalid_argument", "invalid bounded subtree clone")
	}
	associationCount := 0
	for _, resource := range q.Resources {
		associationCount += len(resource.Snapshot.Associations)
		if associationCount > model.MaxConfigurationCloneAssociations {
			return out, failure("resource_exhausted", "subtree clone association count exceeded")
		}
		if !configurationSnapshotWithinRawBounds(resource.Snapshot) {
			return out, failure("resource_exhausted", "configuration plan bound exceeded")
		}
	}
	size, err := cloneBytes(q)
	if err != nil {
		return out, err
	}
	if size > model.MaxConfigurationDecodedBytes {
		return out, failure("resource_exhausted", "subtree clone exceeds the decoded plan bound")
	}
	if err = configurationNamespaceGuard(ctx, tx, scope, q.Domain.Key, q.TargetParent); err != nil {
		return
	}
	ns, err := readNamespaces(ctx, tx, scope, model.NamespaceRead{Domain: q.Domain.Key, ID: q.SourceID})
	if err != nil {
		return out, err
	}
	rs, err := readResources(ctx, tx, scope, model.NamespaceRead{Domain: q.Domain.Key, ID: q.SourceID}, false)
	if err != nil {
		return out, err
	}
	if len(ns) != len(q.Namespaces) || len(rs) != len(q.Resources) {
		return out, failure("conflict", "subtree changed")
	}
	mappings := map[string]model.ConfigurationNamespaceMapping{}
	ids := map[string]bool{}
	for _, p := range q.Namespaces {
		if !textKey(p.TargetID) || mappings[p.SourceID].SourceID != "" || ids[p.TargetID] {
			return out, failure("invalid_argument", "duplicate namespace mapping")
		}
		mappings[p.SourceID] = p
		ids[p.TargetID] = true
	}
	children := map[string][]model.NamespaceRow{}
	var root model.NamespaceRow
	for _, n := range ns {
		p := mappings[n.ID]
		if p.SourceID == "" || p.ExpectedRevision != n.Revision {
			return out, failure("conflict", "namespace changed")
		}
		if n.ID == q.SourceID {
			root = n
			if n.Revision != q.ExpectedRevision {
				return out, failure("conflict", "source namespace changed")
			}
		} else {
			children[n.ParentID] = append(children[n.ParentID], n)
		}
	}
	plans := map[string]model.ConfigurationResourceCreate{}
	for _, p := range q.Resources {
		if p.Source == nil || plans[p.Source.ResourceID].ResourceID != "" || ids[p.ResourceID] || ids[p.BranchID] || ids[p.CommitID] {
			return out, failure("invalid_argument", "invalid resource mapping")
		}
		ids[p.ResourceID] = true
		ids[p.BranchID] = true
		ids[p.CommitID] = true
		plans[p.Source.ResourceID] = p
	}
	for _, r := range rs {
		p, ok := plans[r.ID]
		if !ok || p.Domain != q.Domain || p.Source.ExpectedResourceRevision != r.Revision || p.Source.CommitID != r.CommitID || p.Source.ContentDigest != r.ContentDigest || p.Source.Branch != nil || p.Namespace.ID != mappings[r.NamespaceID].TargetID || p.Namespace.ExpectedRevision != "1" || p.Name != r.Name || p.NameKey != r.NameKey || p.System || p.SystemKey != "" {
			return out, failure("conflict", "resource clone source changed")
		}
	}
	// Insert all destination namespaces, then all content writes in one owner tx.
	queue := []model.NamespaceRow{root}
	visited := map[string]bool{}
	for i := 0; i < len(queue); i++ {
		n := queue[i]
		if n.ID == "" || visited[n.ID] {
			return out, failure("data_loss", "namespace cycle")
		}
		visited[n.ID] = true
		parent := q.TargetParent
		name := q.Name
		if n.ID != q.SourceID {
			parent = model.ConfigurationNamespaceGuard{ID: mappings[n.ParentID].TargetID, ExpectedRevision: "1"}
			name = n.Name
		}
		m := q.Mutation
		m.Key = digest(struct{ Key, Kind, ID string }{q.Mutation.Key, "namespace", n.ID})
		result, e := configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceCreateOperation, model.ConfigurationNamespaceWrite{Domain: q.Domain, Mutation: m, ID: mappings[n.ID].TargetID, ExpectedRevision: "0", Parent: &parent, Name: name})
		if e != nil {
			return out, e
		}
		if n.ID == q.SourceID {
			out.Namespace = result.Namespace
		}
		queue = append(queue, children[n.ID]...)
	}
	if len(visited) != len(ns) {
		return out, failure("data_loss", "incomplete namespace topology")
	}
	for _, r := range rs {
		p := plans[r.ID]
		p.Mutation = q.Mutation
		p.Mutation.Key = digest(struct{ Key, Kind, ID string }{q.Mutation.Key, "resource", r.ID})
		created, createErr := configurationCreate(ctx, tx, scope, p)
		if createErr != nil {
			return out, createErr
		}
		out.AffectedAgentIDs, err = mergeAffectedAgents(out.AffectedAgentIDs, created.AffectedAgentIDs)
		if err != nil {
			return out, err
		}
		if len(created.AffectedWorkflowIDs) != 0 {
			out.AffectedWorkflowIDs, err = mergeAffectedWorkflows(out.AffectedWorkflowIDs, created.AffectedWorkflowIDs)
			if err != nil {
				return out, err
			}
		}
	}
	body, err := encode(out)
	if err != nil {
		return out, err
	}
	if len(body) > model.MaxConfigurationReceiptResultBytes {
		return out, failure("resource_exhausted", "namespace clone receipt exceeds bound")
	}
	if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO core_catalog_receipts VALUES(?,?,?,?,?,?)", scope, q.Domain.Key, q.Mutation.Key, q.Mutation.IntentDigest, model.ConfigurationNamespaceCloneOperation, body)
	return out, err
}

// cloneBytes is the decoded size of everything the plan asks the transaction to copy.
func cloneBytes(q model.ConfigurationNamespaceClone) (int, error) {
	control := q
	control.Resources = nil
	header, err := encode(control)
	if err != nil {
		return 0, err
	}
	total := len(header)
	if q.Resources != nil {
		total -= 2
	} // Replace JSON null with an array.
	for i, r := range q.Resources {
		total += len(r.Snapshot.Payload) + len(r.Snapshot.Manifest) + len(r.Snapshot.Identity) + len(r.Snapshot.CatalogSummary)
		if total > model.MaxConfigurationDecodedBytes {
			return total, nil
		}
		r.Snapshot.Payload = nil
		r.Snapshot.Manifest = nil
		r.Snapshot.Identity = nil
		r.Snapshot.CatalogSummary = nil
		encoded, err := encode(r)
		if err != nil {
			return 0, err
		}
		total += len(encoded)
		if i != 0 {
			total++
		}
		if total > model.MaxConfigurationDecodedBytes {
			return total, nil
		}
	}
	return total, nil
}
