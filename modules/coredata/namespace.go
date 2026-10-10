package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strconv"
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
