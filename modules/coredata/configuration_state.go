package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationMainGuard(ctx context.Context, tx *sql.Tx, scope, domain string, r model.ConfigurationResource, g model.ConfigurationBranchGuard) error {
	current, e := configurationCurrentMain(ctx, tx, scope, domain, r, nil)
	if e != nil {
		return e
	}
	if current.Branch != g {
		return failure("conflict", "current main visibility pin changed")
	}
	return nil
}

func configurationStateReceipt(ctx context.Context, tx *sql.Tx, scope, op string, d model.ConfigurationDomainGuard, m model.ConfigurationMutation) (model.ConfigurationMutationResult, error) {
	if e := configurationMutationValid(m); e != nil {
		return model.ConfigurationMutationResult{}, e
	}
	return configurationReceipt(ctx, tx, scope, model.ConfigurationReceipt{Domain: d, Key: m.Key, IntentDigest: m.IntentDigest, Operations: []string{op}})
}

func configurationStateFinish(ctx context.Context, tx *sql.Tx, scope, op string, d model.ConfigurationDomainGuard, m model.ConfigurationMutation, plan, disposition string, h model.ConfigurationHead, changed bool) (out model.ConfigurationMutationResult, err error) {
	if changed {
		c := model.ConfigurationChange{ID: digest(struct{ Domain, Key string }{d.Key, m.Key}), Summary: op, Nodes: []model.ConfigurationNodeChange{}}
		if err = configurationChange(ctx, tx, scope, d.Key, h.Resource.ID, h.Commit.ID, op, m, c, "", ""); err != nil {
			return out, err
		}
	}
	out = model.ConfigurationMutationResult{Found: true, Key: m.Key, Domain: d.Key, Operation: op, IntentDigest: m.IntentDigest, PlanDigest: plan, Result: model.ConfigurationPublished{Disposition: disposition, Head: h}}
	err = configurationSaveReceipt(ctx, tx, scope, out)
	return
}

// Incoming discovery stays on the target index. An archived owner no longer
// contributes live tracking edges. Shared immutable heads count for every live
// branch, including a fork whose snapshot was authored on another branch.
func configurationArchiveBlocked(ctx context.Context, tx *sql.Tx, scope, domain, resource, branch string) error {
	var blocked bool
	e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_references f
 JOIN core_snapshots s ON s.scope=f.scope AND s.domain=f.domain AND s.id=f.commit_id
 JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id
 JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.resource_id=s.resource_id AND b.head_commit_id=s.id
 WHERE f.scope=? AND f.target_domain=? AND f.target_resource_id=?
 AND json_extract(CAST(f.body AS TEXT),'$.mode')='tracking'
 AND (?='' OR json_extract(CAST(f.body AS TEXT),'$.target_branch')=?)
 AND r.archived=0 AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
 AND NOT(s.domain=? AND s.resource_id=? AND (?='' OR b.name_key=?)))`, scope, domain, resource, branch, branch, domain, resource, branch, branch).Scan(&blocked)
	if e != nil {
		return e
	}
	if blocked {
		return failure("failed_precondition", "live tracking references prevent archive")
	}
	return nil
}

func configurationBranchCreate(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationBranchCreate) (out model.ConfigurationMutationResult, err error) {
	const op = model.ConfigurationBranchCreateOperation
	out, err = configurationStateReceipt(ctx, tx, scope, op, q.Domain, q.Mutation)
	if err != nil || out.Found {
		return
	}
	if !textKey(q.ResourceID) || !textKey(q.ID) || !textKey(q.FromCommitID) || !sha256Hex(q.FromContentDigest) || !positiveRevision(q.ExpectedResourceRevision) {
		return out, failure("invalid_argument", "exact resource/source pin and allocated branch identity required")
	}
	name, key, e := model.NormalizeConfigurationName(model.ConfigurationBranchName, q.Name)
	if e != nil || name != q.Name || key == "main" {
		return out, failure("invalid_argument", "canonical named branch required")
	}
	r, e := configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, false)
	if e != nil {
		return out, e
	}
	if r.Revision != q.ExpectedResourceRevision {
		return out, failure("conflict", "resource revision changed")
	}
	if e = configurationMainGuard(ctx, tx, scope, q.Domain.Key, r, q.Main); e != nil {
		return out, e
	}
	source, e := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, q.FromCommitID)
	if e != nil {
		return out, e
	}
	if source.Head.Commit.ContentDigest != q.FromContentDigest {
		return out, failure("conflict", "immutable source pin changed")
	}
	var exists bool
	e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_branches WHERE scope=? AND domain=? AND (id=? OR (resource_id=? AND name_key=? AND coalesce(json_extract(CAST(body AS TEXT),'$.archived_at'),'')='')))`, scope, q.Domain.Key, q.ID, r.ID, key).Scan(&exists)
	if e != nil {
		return out, e
	}
	if exists {
		return out, failure("conflict", "branch identity or live name already exists")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	b := model.ConfigurationBranch{ID: q.ID, ResourceID: r.ID, Name: name, NameKey: key, Revision: "1", HeadCommitID: q.FromCommitID, CreatedFromCommitID: q.FromCommitID, CreatedAt: now, UpdatedAt: now}
	manifest, e := model.DecodeConfigurationManifest(source.Manifest)
	if e != nil {
		return out, failure("data_loss", "source manifest is invalid")
	}
	if e = configurationValidateReferences(ctx, tx, scope, q.Domain.Key, r, b, source.Head.Commit, manifest, source.References); e != nil {
		return out, e
	}
	body, e := encode(b)
	if e != nil {
		return out, e
	}
	if e = reserve(ctx, tx, scope, 1, int64(len(body))); e != nil {
		return out, e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", scope, q.Domain.Key, b.ID, r.ID, key, b.HeadCommitID, b.Revision, body); e != nil {
		return out, e
	}
	plan, e := model.ConfigurationBranchCreatePlanDigest(q)
	if e != nil {
		return out, e
	}
	return configurationStateFinish(ctx, tx, scope, op, q.Domain, q.Mutation, plan, "created", model.ConfigurationHead{Resource: r, Branch: b, Commit: source.Head.Commit}, true)
}

func configurationBranchArchive(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationBranchArchive) (out model.ConfigurationMutationResult, err error) {
	const op = model.ConfigurationBranchArchiveOperation
	out, err = configurationStateReceipt(ctx, tx, scope, op, q.Domain, q.Mutation)
	if err != nil || out.Found {
		return
	}
	r, e := configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, false)
	if e != nil {
		return out, e
	}
	if e = configurationMainGuard(ctx, tx, scope, q.Domain.Key, r, q.Main); e != nil {
		return out, e
	}
	b, e := configurationBranch(ctx, tx, scope, q.Domain.Key, q.Branch.ID)
	if e != nil {
		return out, e
	}
	if b.ResourceID != r.ID || b.NameKey == "main" {
		return out, failure("invalid_argument", "owned nonmain branch required")
	}
	source, e := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, b.HeadCommitID)
	if e != nil {
		return out, e
	}
	check := b
	check.ArchivedAt = ""
	if e = configurationGuard(check, source.Head.Commit, q.Branch); e != nil {
		return out, e
	}
	changed := b.ArchivedAt == ""
	if changed {
		if e = configurationArchiveBlocked(ctx, tx, scope, q.Domain.Key, r.ID, b.NameKey); e != nil {
			return out, e
		}
		old, _ := encode(b)
		b.ArchivedAt = time.Now().UTC().Format(time.RFC3339Nano)
		b.UpdatedAt = b.ArchivedAt
		b.Revision, e = configurationBump(b.Revision)
		if e != nil {
			return out, e
		}
		body, e := encode(b)
		if e != nil {
			return out, e
		}
		if e = reserve(ctx, tx, scope, 0, int64(len(body)-len(old))); e != nil {
			return out, e
		}
		if _, e = tx.ExecContext(ctx, "UPDATE core_branches SET revision=?,body=? WHERE scope=? AND domain=? AND id=?", b.Revision, body, scope, q.Domain.Key, b.ID); e != nil {
			return out, e
		}
	}
	plan, e := model.ConfigurationBranchArchivePlanDigest(q)
	if e != nil {
		return out, e
	}
	disposition := "noop"
	if changed {
		disposition = "archived"
	}
	return configurationStateFinish(ctx, tx, scope, op, q.Domain, q.Mutation, plan, disposition, model.ConfigurationHead{Resource: r, Branch: b, Commit: source.Head.Commit}, changed)
}

func configurationResourceState(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationResourceState) (out model.ConfigurationMutationResult, err error) {
	const op = model.ConfigurationResourceStateOperation
	out, err = configurationStateReceipt(ctx, tx, scope, op, q.Domain, q.Mutation)
	if err != nil || out.Found {
		return
	}
	if !positiveRevision(q.ExpectedRevision) {
		return out, failure("invalid_argument", "canonical resource revision required")
	}
	r, e := configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, true)
	if e != nil {
		return out, e
	}
	if r.Revision != q.ExpectedRevision {
		return out, failure("conflict", "resource revision changed")
	}
	if e = configurationMainGuard(ctx, tx, scope, q.Domain.Key, r, q.Main); e != nil {
		return out, e
	}
	main, e := configurationBranchNamed(ctx, tx, scope, q.Domain.Key, r.ID, "main")
	if e != nil {
		return out, e
	}
	source, e := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, r.MainCommitID)
	if e != nil {
		return out, e
	}
	if q.Archived && r.System {
		return out, failure("failed_precondition", "system resource is protected")
	}
	changed := q.Archived != (r.ArchivedAt != "")
	if changed {
		if q.Archived {
			if e = configurationArchiveBlocked(ctx, tx, scope, q.Domain.Key, r.ID, ""); e != nil {
				return out, e
			}
		} else {
			if r.NamespaceID != "" {
				n, _, e := configurationNamespace(ctx, tx, scope, q.Domain.Key, r.NamespaceID)
				if e != nil {
					return out, e
				}
				if n.ArchivedAt != "" {
					return out, failure("failed_precondition", "restore requires a live namespace")
				}
			}
			if e = configurationNameFree(ctx, tx, scope, q.Domain.Key, r.NamespaceID, r.NameKey, r.ID); e != nil {
				return out, e
			}
		}
		old, _ := encode(r)
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		r.ArchivedAt = ""
		if q.Archived {
			r.ArchivedAt = r.UpdatedAt
		}
		r.Revision, e = configurationBump(r.Revision)
		if e != nil {
			return out, e
		}
		body, e := encode(r)
		if e != nil {
			return out, e
		}
		if e = reserve(ctx, tx, scope, 0, int64(len(body)-len(old))); e != nil {
			return out, e
		}
		if _, e = tx.ExecContext(ctx, "UPDATE core_resources SET revision=?,archived=?,body=? WHERE scope=? AND domain=? AND id=?", r.Revision, q.Archived, body, scope, q.Domain.Key, r.ID); e != nil {
			return out, e
		}
		if !q.Archived {
			// Validate the prospective restored graph in this same transaction. Any
			// invalid target rolls the row, quota, audit and receipt back together.
			rows, e := tx.QueryContext(ctx, `SELECT body FROM core_branches WHERE scope=? AND domain=? AND resource_id=? AND coalesce(json_extract(CAST(body AS TEXT),'$.archived_at'),'')='' LIMIT 4097`, scope, q.Domain.Key, r.ID)
			if e != nil {
				return out, e
			}
			branches := []model.ConfigurationBranch{}
			for rows.Next() {
				var body []byte
				var b model.ConfigurationBranch
				if e = rows.Scan(&body); e != nil {
					break
				}
				if json.Unmarshal(body, &b) != nil {
					e = failure("data_loss", "invalid restored branch")
					break
				}
				branches = append(branches, b)
				if len(branches) > 4096 {
					e = failure("resource_exhausted", "restore branch bound exceeded")
					break
				}
			}
			if e == nil {
				e = rows.Err()
			}
			rows.Close()
			if e != nil {
				return out, e
			}
			size := 0
			for _, b := range branches {
				s, e := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, b.HeadCommitID)
				if e != nil {
					return out, e
				}
				size += len(s.Payload) + len(s.Manifest)
				if size > model.MaxConfigurationDecodedBytes {
					return out, failure("resource_exhausted", "restore head materialization bound exceeded")
				}
				m, e := model.DecodeConfigurationManifest(s.Manifest)
				if e != nil {
					return out, e
				}
				if e = configurationValidateReferences(ctx, tx, scope, q.Domain.Key, r, b, s.Head.Commit, m, s.References); e != nil {
					return out, e
				}
			}
		}
	}
	plan, e := model.ConfigurationResourceStatePlanDigest(q)
	if e != nil {
		return out, e
	}
	disposition := "noop"
	if changed {
		disposition = "restored"
		if q.Archived {
			disposition = "archived"
		}
	}
	return configurationStateFinish(ctx, tx, scope, op, q.Domain, q.Mutation, plan, disposition, model.ConfigurationHead{Resource: r, Branch: main, Commit: source.Head.Commit}, changed)
}
