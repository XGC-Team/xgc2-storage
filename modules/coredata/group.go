package coredata

import (
	"context"
	"database/sql"

	"encoding/json"
	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func groupSnapshot(ctx context.Context, tx *sql.Tx, scope string, r model.GroupRead) (out model.GroupSnapshot, err error) {
	out.Members = make([]model.GroupMember, 0)
	if !textKey(r.ID) {
		return out, failure("invalid_argument", "group identity required")
	}
	var body []byte
	err = tx.QueryRowContext(ctx, "SELECT id,member_count,membership_digest,body FROM core_groups WHERE scope=? AND id=? AND sealed=1", scope, r.ID).Scan(&out.ID, &out.MemberCount, &out.MembershipDigest, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "sealed group not found")
	}
	if err != nil {
		return out, err
	}
	out.Body = body
	rows, err := tx.QueryContext(ctx, "SELECT ordinal,item_key,child_id,event_id,parameters,event_body,link_body,member_body FROM core_group_members WHERE scope=? AND group_id=? ORDER BY ordinal LIMIT 1001", scope, r.ID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	bytes := len(body)
	for rows.Next() {
		var ordinal int
		var m model.GroupMember
		var parameters, event, link, body []byte
		if err = rows.Scan(&ordinal, &m.ItemKey, &m.ChildID, &m.EventID, &parameters, &event, &link, &body); err != nil {
			return out, err
		}
		if ordinal != len(out.Members) || len(out.Members) >= MaxMembers {
			return out, failure("failed_precondition", "group membership is incomplete")
		}
		m.Parameters, m.Event, m.Link, m.Body = parameters, event, link, body
		bytes += len(parameters) + len(event) + len(link) + len(body)
		if bytes > MaxResponseBytes {
			return out, failure("resource_exhausted", "group snapshot byte limit exceeded")
		}
		out.Members = append(out.Members, m)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Members) != out.MemberCount {
		return out, failure("failed_precondition", "sealed group membership is incomplete")
	}
	if out.MembershipDigest != digest(struct {
		Body    json.RawMessage
		Members []model.GroupMember
	}{out.Body, out.Members}) {
		return out, failure("failed_precondition", "sealed group membership digest changed")
	}
	return out, nil
}

// groupIntentDigest fences immutable preparation, not a renewed admission read.
// Live guards and predicates are checked only for a new preparation. They cannot
// change the identity of an already sealed group when a caller reloads facts.
func groupIntentDigest(r model.GroupPrepare) string {
	return digest(struct {
		ID, ParentID, InvocationID, GroupKey string
		Body                                 json.RawMessage
		Members                              []model.GroupMember
	}{r.ID, r.ParentID, r.InvocationID, r.GroupKey, r.Body, r.Members})
}

func prepareGroup(ctx context.Context, tx *sql.Tx, scope string, r model.GroupPrepare) (out model.GroupPrepared, err error) {
	r.Members = append([]model.GroupMember{}, r.Members...)
	if err = checkSize(r); err != nil {
		return out, err
	}
	if !textKey(r.ID) || !textKey(r.ParentID) || !textKey(r.InvocationID) || !textKey(r.GroupKey) || r.ID == r.ParentID || !object(r.Body) || len(r.Members) > MaxMembers || r.ParentGuard.Key != r.ParentID || r.InvocationGuard.Key != r.InvocationID || r.ParentGuard.Collection != "runs" || r.InvocationGuard.Collection != "invocations" || r.PinGuard.Collection != "definitions" {
		return out, failure("invalid_argument", "invalid group identity, body or member count")
	}
	items, children, events := map[string]bool{}, map[string]bool{}, map[string]bool{}
	parameterBytes := 0
	for _, m := range r.Members {
		if !textKey(m.ItemKey) || !textKey(m.ChildID) || !textKey(m.EventID) || m.ChildID == r.ParentID || items[m.ItemKey] || children[m.ChildID] || events[m.EventID] || !object(m.Parameters) || !object(m.Event) || !object(m.Link) || !object(m.Body) {
			return out, failure("invalid_argument", "invalid or duplicate member data")
		}
		items[m.ItemKey], children[m.ChildID], events[m.EventID] = true, true, true
		parameterBytes += len(m.Parameters)
	}
	if parameterBytes > MaxParameterBytes {
		return out, failure("resource_exhausted", "group parameters exceed 8 MiB")
	}
	planDigest := groupIntentDigest(r)
	var previous string
	var sealed int
	err = tx.QueryRowContext(ctx, "SELECT plan_digest,membership_digest,member_count,sealed FROM core_groups WHERE scope=? AND id=?", scope, r.ID).Scan(&previous, &out.MembershipDigest, &out.MemberCount, &sealed)
	if err == nil {
		if previous != planDigest || sealed != 1 {
			return model.GroupPrepared{}, failure("conflict", "group identity reused with different preparation")
		}
		out.ID = r.ID
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err = checkGroupCondition(ctx, tx, scope, r); err != nil {
		return out, err
	}
	encoded, _ := encode(r)
	if err = reserve(ctx, tx, scope, int64(1+len(r.Members)), int64(len(encoded)+1024*(1+len(r.Members)))); err != nil {
		return out, err
	}
	out = model.GroupPrepared{ID: r.ID, MemberCount: len(r.Members), MembershipDigest: digest(struct {
		Body    json.RawMessage
		Members []model.GroupMember
	}{r.Body, r.Members})}
	if _, err = tx.ExecContext(ctx, "INSERT INTO core_groups VALUES(?,?,?,?,?,?,?,?,?,0)", scope, r.ID, r.ParentID, r.InvocationID, r.GroupKey, []byte(r.Body), planDigest, out.MembershipDigest, len(r.Members)); err != nil {
		return out, err
	}
	for ordinal, m := range r.Members {
		fact := model.GroupMemberSnapshot{Group: out, ParentID: r.ParentID, InvocationID: r.InvocationID, GroupKey: r.GroupKey, Ordinal: ordinal, Body: r.Body, Member: m}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_group_members VALUES(?,?,?,?,?,?,?,?,?,?,?)", scope, r.ID, ordinal, m.ItemKey, m.ChildID, m.EventID, []byte(m.Parameters), digest(fact), []byte(m.Event), []byte(m.Link), []byte(m.Body)); err != nil {
			return out, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE core_groups SET sealed=1 WHERE scope=? AND id=?", scope, r.ID); err != nil {
		return out, err
	}
	return out, nil
}
