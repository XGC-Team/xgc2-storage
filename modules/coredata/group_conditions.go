package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func guardedBody(ctx context.Context, tx *sql.Tx, scope string, g model.RecordGuard) ([]byte, error) {
	switch g.Collection {
	case "runs", "invocations", "definitions":
	default:
		return nil, failure("invalid_argument", "unregistered group guard collection")
	}
	if !textKey(g.Key) || !positiveRevision(g.Version) {
		return nil, failure("invalid_argument", "exact live point guard required")
	}
	var current string
	var deleted int
	var body []byte
	err := tx.QueryRowContext(ctx, "SELECT CAST(version AS TEXT),deleted,data FROM records WHERE scope=? AND collection=? AND key=?", scope, g.Collection, g.Key).Scan(&current, &deleted, &body)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (current != g.Version || deleted != 0) {
		return nil, failure("conflict", "guarded source record changed")
	}
	return body, err
}

func checkGroupCondition(ctx context.Context, tx *sql.Tx, scope string, r model.GroupPrepare) error {
	c := r.Condition
	validParent := c.Parent.ID == r.ParentID && c.Parent.ExecutionModel == model.OccurrenceExecutionModel && (c.Parent.Status == "running" || c.Parent.Status == "waiting") && textKey(c.Parent.TargetID) && textKey(c.Parent.RootRunID)
	validProducer := c.Producer.ID == r.InvocationID && c.Producer.RunID == r.ParentID && textKey(c.Producer.NodeID) && textKey(c.Producer.Kind) && c.Producer.Status == "running" && c.Producer.ChildRunProducer
	validChain := len(c.Ancestors) >= 1 && len(c.Ancestors) <= model.MaxGroupAncestors && c.Ancestors[0] == r.ParentGuard
	if !validParent || !validProducer || !validChain {
		return failure("invalid_argument", "complete active parent/producer predicate and ancestor chain required")
	}
	parentRaw, err := guardedBody(ctx, tx, scope, r.ParentGuard)
	if err != nil {
		return err
	}
	var parent model.RunPrepareState
	if err = json.Unmarshal(parentRaw, &parent); err != nil {
		return failure("data_loss", "invalid parent preparation state")
	}
	if parent != c.Parent {
		return failure("conflict", "parent preparation state does not match predicate")
	}
	producerRaw, err := guardedBody(ctx, tx, scope, r.InvocationGuard)
	if err != nil {
		return err
	}
	var producer model.ProducerPrepareState
	if err = json.Unmarshal(producerRaw, &producer); err != nil {
		return failure("data_loss", "invalid producer preparation state")
	}
	if producer != c.Producer {
		return failure("conflict", "producer is not the guarded active occurrence")
	}
	if _, err = guardedBody(ctx, tx, scope, r.PinGuard); err != nil {
		return err
	}
	seen := make(map[string]bool, len(c.Ancestors))
	expectedID := r.ParentID
	for i, guard := range c.Ancestors {
		if guard.Collection != "runs" || guard.Key != expectedID || seen[guard.Key] {
			return failure("conflict", "ancestor lineage does not match complete guarded chain")
		}
		ancestor := parent
		if i > 0 {
			raw, err := guardedBody(ctx, tx, scope, guard)
			if err != nil {
				return err
			}
			var stored model.RunPrepareState
			if err = json.Unmarshal(raw, &stored); err != nil {
				return failure("data_loss", "invalid ancestor preparation state")
			}
			ancestor = stored
		}
		if ancestor.ID != guard.Key || ancestor.ExecutionModel != model.OccurrenceExecutionModel || ancestor.RootRunID != parent.RootRunID || ancestor.TargetID != parent.TargetID {
			return failure("conflict", "ancestor lineage changed")
		}
		seen[ancestor.ID] = true
		var stopped int
		err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM core_commands WHERE scope=? AND target=? AND action=? AND status IN ('accepted','succeeded'))", scope, model.RunCommandTargetPrefix+ancestor.ID, model.StopSetAction).Scan(&stopped)
		if err != nil {
			return err
		}
		if stopped != 0 {
			return failure("conflict", "ancestor has a durable stop-set fence")
		}
		if i == len(c.Ancestors)-1 {
			if ancestor.ParentRunID != "" || ancestor.ID != parent.RootRunID {
				return failure("conflict", "ancestor guard chain omits its root")
			}
			return nil
		}
		if ancestor.ParentRunID == "" || c.Ancestors[i+1].Key != ancestor.ParentRunID {
			return failure("conflict", "ancestor guard chain contains a wrong parent edge")
		}
		expectedID = ancestor.ParentRunID
	}
	return failure("conflict", "incomplete ancestor guard chain")
}

func groupMemberSnapshot(ctx context.Context, tx *sql.Tx, scope string, r model.GroupMemberRead) (out model.GroupMemberSnapshot, err error) {
	if (r.ChildID == "") == (r.EventID == "") || r.ChildID != "" && !textKey(r.ChildID) || r.EventID != "" && !textKey(r.EventID) {
		return out, failure("invalid_argument", "exactly one prepared child/event identity required")
	}
	column, identity := "m.child_id", r.ChildID
	if r.EventID != "" {
		column, identity = "m.event_id", r.EventID
	}
	var factDigest string
	var groupBody, parameters, event, link, body []byte
	query := "SELECT g.id,g.member_count,g.membership_digest,g.parent_id,g.invocation_id,g.group_key,g.body,m.ordinal,m.item_key,m.child_id,m.event_id,m.parameters,m.fact_digest,m.event_body,m.link_body,m.member_body FROM core_group_members m JOIN core_groups g ON g.scope=m.scope AND g.id=m.group_id WHERE m.scope=? AND " + column + "=? AND g.sealed=1"
	err = tx.QueryRowContext(ctx, query, scope, identity).Scan(&out.Group.ID, &out.Group.MemberCount, &out.Group.MembershipDigest, &out.ParentID, &out.InvocationID, &out.GroupKey, &groupBody, &out.Ordinal, &out.Member.ItemKey, &out.Member.ChildID, &out.Member.EventID, &parameters, &factDigest, &event, &link, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "sealed prepared member not found")
	}
	if err != nil {
		return out, err
	}
	out.Body = groupBody
	out.Member.Parameters, out.Member.Event, out.Member.Link, out.Member.Body = parameters, event, link, body
	if out.Ordinal < 0 || out.Ordinal >= out.Group.MemberCount || factDigest != digest(out) || !object(out.Body) || !object(parameters) || !object(event) || !object(link) || !object(body) {
		return model.GroupMemberSnapshot{}, failure("data_loss", "invalid sealed prepared member")
	}
	return out, nil
}
