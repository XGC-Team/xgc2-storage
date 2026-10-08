package coredata

import (
	"context"
	"database/sql"

	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strconv"
	"strings"
)

// The recursive limit stops expansion as well as bounding the output. UNION
// makes a damaged cycle finite. A separate topology check rejects cycles.
const treeSQL = `WITH RECURSIVE tree(id) AS (
 SELECT id FROM core_namespaces WHERE scope=? AND domain=? AND id=? AND archived=0
 UNION SELECT n.id FROM core_namespaces n JOIN tree t ON n.parent_id=t.id
 WHERE n.scope=? AND n.domain=? AND n.archived=0 LIMIT 1025
) `

func treeArgs(scope string, r model.NamespaceRead) []any {
	return []any{scope, r.Domain, r.ID, scope, r.Domain}
}

// A destination parent need not fit inside a complete subtree snapshot. Return
// only its metadata and exact revision, without loading descendant payloads.
func namespaceGet(ctx context.Context, tx *sql.Tx, scope string, r model.NamespaceRead) (out model.NamespaceRow, err error) {
	if !textKey(r.Domain) || !textKey(r.ID) {
		return out, failure("invalid_argument", "domain and namespace identity required")
	}
	var body []byte
	err = tx.QueryRowContext(ctx, "SELECT id,coalesce(parent_id,''),name,name_key,revision,body FROM core_namespaces WHERE scope=? AND domain=? AND id=? AND archived=0", scope, r.Domain, r.ID).Scan(&out.ID, &out.ParentID, &out.Name, &out.NameKey, &out.Revision, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, failure("not_found", "live namespace not found")
	}
	out.Body = body
	return out, err
}

func readNamespaces(ctx context.Context, tx *sql.Tx, scope string, r model.NamespaceRead) (out []model.NamespaceRow, err error) {
	if !textKey(r.Domain) || !textKey(r.ID) {
		return nil, failure("invalid_argument", "domain and source namespace required")
	}
	args := append(treeArgs(scope, r), scope, r.Domain)
	rows, err := tx.QueryContext(ctx, treeSQL+"SELECT n.id,coalesce(n.parent_id,''),n.name,n.name_key,n.revision,n.body FROM tree JOIN core_namespaces n ON n.id=tree.id WHERE n.scope=? AND n.domain=? ORDER BY n.id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bytes := 0
	for rows.Next() {
		var n model.NamespaceRow
		var body []byte
		if err = rows.Scan(&n.ID, &n.ParentID, &n.Name, &n.NameKey, &n.Revision, &body); err != nil {
			return nil, err
		}
		n.Body = body
		bytes += len(body) + len(n.ID) + len(n.ParentID) + len(n.Name) + len(n.NameKey)
		if bytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "namespace data byte limit exceeded")
		}
		out = append(out, n)
		if len(out) > MaxCloneNamespaces {
			return nil, failure("resource_exhausted", "namespace subtree exceeds 1024 nodes")
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, failure("not_found", "live source namespace not found")
	}
	ids := map[string]bool{}
	for _, n := range out {
		ids[n.ID] = true
	}
	for _, n := range out {
		if n.ID == r.ID && ids[n.ParentID] {
			return nil, failure("failed_precondition", "source namespace cycle")
		}
	}
	return out, nil
}

func readResources(ctx context.Context, tx *sql.Tx, scope string, r model.NamespaceRead, bodies bool) (out []model.ResourceSnapshot, err error) {
	columns := "r.id,r.namespace_id,r.name,r.name_key,r.revision,r.main_commit_id,b.id,b.revision,s.content_digest"
	if bodies {
		columns += ",r.body,b.body,s.body,s.payload,s.manifest"
	}
	args := append(treeArgs(scope, r), scope, r.Domain)
	rows, err := tx.QueryContext(ctx, treeSQL+"SELECT "+columns+` FROM tree JOIN core_resources r ON r.namespace_id=tree.id
 LEFT JOIN core_branches b ON b.scope=r.scope AND b.domain=r.domain AND b.resource_id=r.id AND b.name_key='main' AND b.head_commit_id=r.main_commit_id
 LEFT JOIN core_snapshots s ON s.scope=r.scope AND s.domain=r.domain AND s.id=r.main_commit_id AND s.resource_id=r.id AND s.branch_id=b.id
 WHERE r.scope=? AND r.domain=? AND r.archived=0 ORDER BY r.id LIMIT 4097`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bytes := 0
	for rows.Next() {
		var v model.ResourceSnapshot
		var body, branchBody, commitBody, payload, manifest []byte
		var branch, content sql.NullString
		var revision sql.NullInt64
		values := []any{&v.ID, &v.NamespaceID, &v.Name, &v.NameKey, &v.Revision, &v.CommitID, &branch, &revision, &content}
		if bodies {
			values = append(values, &body, &branchBody, &commitBody, &payload, &manifest)
		}
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}
		if !branch.Valid || !revision.Valid || !content.Valid {
			return nil, failure("failed_precondition", "current main snapshot relation is incomplete")
		}
		v.BranchID, v.BranchRevision, v.ContentDigest = branch.String, strconv.FormatInt(revision.Int64, 10), content.String
		v.Body, v.BranchBody, v.CommitBody, v.Payload, v.Manifest = body, branchBody, commitBody, payload, manifest
		bytes += len(v.Body) + len(v.BranchBody) + len(v.CommitBody) + len(v.Payload) + len(v.Manifest)
		if bytes > MaxResponseBytes {
			return nil, failure("resource_exhausted", "namespace snapshot byte limit exceeded")
		}
		out = append(out, v)
		if len(out) > MaxCloneResources {
			return nil, failure("resource_exhausted", "namespace subtree exceeds 4096 resources")
		}
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func namespaceSnapshot(ctx context.Context, tx *sql.Tx, scope string, r model.NamespaceRead) (out model.NamespaceTree, err error) {
	if out.Namespaces, err = readNamespaces(ctx, tx, scope, r); err != nil {
		return out, err
	}
	if out.Resources, err = readResources(ctx, tx, scope, r, true); err != nil {
		return out, err
	}
	byCommit := map[string]int{}
	for i, v := range out.Resources {
		byCommit[v.CommitID] = i
	}
	args := append(treeArgs(scope, r), scope, r.Domain)
	rows, err := tx.QueryContext(ctx, treeSQL+`SELECT f.commit_id,f.slot,f.target_domain,f.target_resource_id,f.target_commit_id,f.body FROM tree
 JOIN core_resources r ON r.namespace_id=tree.id
 JOIN core_references f ON f.scope=r.scope AND f.domain=r.domain AND f.commit_id=r.main_commit_id
 WHERE r.scope=? AND r.domain=? AND r.archived=0 ORDER BY f.commit_id,f.slot LIMIT 16385`, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	count, bytes := 0, 0
	for rows.Next() {
		var commit string
		var f model.Reference
		var body []byte
		if err = rows.Scan(&commit, &f.Slot, &f.TargetDomain, &f.TargetResourceID, &f.TargetCommitID, &body); err != nil {
			return out, err
		}
		f.Body = body
		count++
		bytes += len(f.Body)
		if count > MaxReferences || bytes > MaxResponseBytes {
			return out, failure("resource_exhausted", "namespace reference bound exceeded")
		}
		i, ok := byCommit[commit]
		if !ok {
			return out, failure("failed_precondition", "reference has no current source snapshot")
		}
		out.Resources[i].References = append(out.Resources[i].References, f)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	for _, v := range out.Resources {
		pin, e := model.SnapshotDigest(v.Payload, v.Manifest, v.References)
		if e != nil || pin != v.ContentDigest {
			return out, failure("failed_precondition", "current main content digest changed")
		}
	}
	return out, nil
}

func cloneNamespace(ctx context.Context, tx *sql.Tx, scope string, r model.NamespaceClone) (out model.NamespaceCloned, err error) {
	if err = checkSize(r); err != nil {
		return out, err
	}
	if !textKey(r.Domain) || !textKey(r.SourceID) || !textKey(r.TargetID) || model.ValidateConfigurationName(model.ConfigurationNamespaceName, r.Name) != nil || model.ValidateConfigurationName(model.ConfigurationNamespaceName, r.NameKey) != nil || strings.TrimSpace(r.Name) != r.Name || strings.TrimSpace(r.NameKey) != r.NameKey || strings.ContainsAny(r.Name+r.NameKey, "\r\n") || !positiveRevision(r.ExpectedRevision) || !textKey(r.ChangeID) || !object(r.Change) || len(r.Namespaces) > MaxCloneNamespaces || len(r.Resources) > MaxCloneResources {
		return out, failure("invalid_argument", "invalid namespace clone identity or bounds")
	}
	if r.TargetParentID == "" {
		if r.ExpectedTargetParentRevision != "0" {
			return out, failure("invalid_argument", "root target has no parent revision")
		}
	} else {
		if !textKey(r.TargetParentID) || !positiveRevision(r.ExpectedTargetParentRevision) {
			return out, failure("invalid_argument", "target parent guard required")
		}
		var revision string
		err = tx.QueryRowContext(ctx, "SELECT revision FROM core_namespaces WHERE scope=? AND domain=? AND id=? AND archived=0", scope, r.Domain, r.TargetParentID).Scan(&revision)
		if errors.Is(err, sql.ErrNoRows) || err == nil && revision != r.ExpectedTargetParentRevision {
			return out, failure("conflict", "target parent changed")
		}
		if err != nil {
			return out, err
		}
	}
	namespaces, err := readNamespaces(ctx, tx, scope, model.NamespaceRead{r.Domain, r.SourceID})
	if err != nil {
		return out, err
	}
	resources, err := readResources(ctx, tx, scope, model.NamespaceRead{r.Domain, r.SourceID}, false)
	if err != nil {
		return out, err
	}
	if len(namespaces) != len(r.Namespaces) || len(resources) != len(r.Resources) {
		return out, failure("conflict", "clone plan does not cover the current complete subtree")
	}
	namespacePlans := map[string]model.NamespaceCopy{}
	targets := map[string]bool{}
	for _, p := range r.Namespaces {
		if !textKey(p.SourceID) || !textKey(p.TargetID) || !positiveRevision(p.ExpectedRevision) || !object(p.Body) || namespacePlans[p.SourceID].SourceID != "" || targets[p.TargetID] {
			return out, failure("invalid_argument", "invalid namespace mapping")
		}
		namespacePlans[p.SourceID] = p
		targets[p.TargetID] = true
	}
	for _, n := range namespaces {
		p, ok := namespacePlans[n.ID]
		if !ok || p.ExpectedRevision != n.Revision || n.ID == r.SourceID && (n.Revision != r.ExpectedRevision || p.TargetID != r.TargetID) {
			return out, failure("conflict", "source namespace changed or mapping incomplete")
		}
	}
	resourcePlans := map[string]model.ResourceCopy{}
	references := 0
	for _, p := range r.Resources {
		if !textKey(p.SourceID) || !textKey(p.TargetID) || !textKey(p.TargetBranchID) || !textKey(p.TargetCommitID) || !positiveRevision(p.ExpectedRevision) || !positiveRevision(p.ExpectedBranchRevision) || !object(p.Body) || !object(p.BranchBody) || !object(p.CommitBody) || !object(p.Payload) || !object(p.Manifest) || resourcePlans[p.SourceID].SourceID != "" {
			return out, failure("invalid_argument", "invalid resource clone data")
		}
		for _, id := range []string{p.TargetID, p.TargetBranchID, p.TargetCommitID} {
			if targets[id] {
				return out, failure("invalid_argument", "duplicate target identity")
			}
			targets[id] = true
		}
		slots := map[string]bool{}
		for _, f := range p.References {
			if !textKey(f.Slot) || !textKey(f.TargetDomain) || !textKey(f.TargetResourceID) || f.TargetCommitID != "" && !textKey(f.TargetCommitID) || !object(f.Body) || slots[f.Slot] {
				return out, failure("invalid_argument", "invalid reference data")
			}
			slots[f.Slot] = true
		}
		references += len(p.References)
		resourcePlans[p.SourceID] = p
	}
	if references > MaxReferences {
		return out, failure("resource_exhausted", "clone reference count exceeded")
	}
	for _, v := range resources {
		p, ok := resourcePlans[v.ID]
		if !ok || p.ExpectedRevision != v.Revision || p.ExpectedBranchRevision != v.BranchRevision || p.SourceCommitID != v.CommitID || p.SourceContentDigest != v.ContentDigest {
			return out, failure("conflict", "source current main snapshot changed")
		}
	}
	encoded, _ := encode(r)
	rowCount := len(namespaces) + 3*len(resources) + references + 1
	if err = reserve(ctx, tx, scope, int64(rowCount), int64(len(encoded)+1024*rowCount)); err != nil {
		return out, err
	}
	// Insert in dependency order, without recursion on the Go stack. The
	// original read set was fully materialized before any target becomes visible.
	children := map[string][]model.NamespaceRow{}
	var root model.NamespaceRow
	for _, n := range namespaces {
		if n.ID == r.SourceID {
			root = n
		} else {
			children[n.ParentID] = append(children[n.ParentID], n)
		}
	}
	queue := []model.NamespaceRow{root}
	visited := map[string]bool{}
	for i := 0; i < len(queue); i++ {
		n := queue[i]
		if n.ID == "" || visited[n.ID] {
			return out, failure("failed_precondition", "source namespace cycle")
		}
		visited[n.ID] = true
		p := namespacePlans[n.ID]
		parent, name, nameKey := r.TargetParentID, r.Name, r.NameKey
		if n.ID != r.SourceID {
			parent = namespacePlans[n.ParentID].TargetID
			name, nameKey = n.Name, n.NameKey
		}
		var parentValue any
		if parent != "" {
			parentValue = parent
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_namespaces VALUES(?,?,?,?,?,?,1,0,?)", scope, r.Domain, p.TargetID, parentValue, name, nameKey, []byte(p.Body)); err != nil {
			return out, err
		}
		queue = append(queue, children[n.ID]...)
	}
	if len(visited) != len(namespaces) {
		return out, failure("failed_precondition", "source namespace topology is incomplete")
	}
	for _, v := range resources {
		p := resourcePlans[v.ID]
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_resources VALUES(?,?,?,?,?,?,1,?,0,?,?,?)", scope, r.Domain, p.TargetID, namespacePlans[v.NamespaceID].TargetID, v.Name, v.NameKey, p.TargetCommitID, v.ID, v.CommitID, []byte(p.Body)); err != nil {
			return out, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_branches VALUES(?,?,?,?, 'main',?,1,?)", scope, r.Domain, p.TargetBranchID, p.TargetID, p.TargetCommitID, []byte(p.BranchBody)); err != nil {
			return out, err
		}
		contentDigest, _ := model.SnapshotDigest(p.Payload, p.Manifest, p.References)
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_snapshots VALUES(?,?,?,?,?,1,?,?,?,?,?)", scope, r.Domain, p.TargetCommitID, p.TargetID, p.TargetBranchID, v.CommitID, contentDigest, []byte(p.Payload), []byte(p.Manifest), []byte(p.CommitBody)); err != nil {
			return out, err
		}
		for _, f := range p.References {
			if _, err = tx.ExecContext(ctx, "INSERT INTO core_references VALUES(?,?,?,?,?,?,?,?)", scope, r.Domain, p.TargetCommitID, f.Slot, f.TargetDomain, f.TargetResourceID, f.TargetCommitID, []byte(f.Body)); err != nil {
				return out, err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO core_changes VALUES(?,?,?,?,?)", scope, r.ChangeID, r.Domain, r.TargetID, []byte(r.Change)); err != nil {
		return out, err
	}
	return model.NamespaceCloned{r.TargetID, len(namespaces), len(resources)}, nil
}
