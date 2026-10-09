package coredata

import (
	"context"
	"database/sql"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationResourceMetadata(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationResourceMetadata) (out model.ConfigurationMutationResult, err error) {
	const op = model.ConfigurationResourceMetadataOperation
	out, err = configurationStateReceipt(ctx, tx, scope, op, q.Domain, q.Mutation)
	if err != nil || out.Found {
		return
	}
	if !positiveRevision(q.ExpectedRevision) || !q.Protect && q.MoveTo == nil {
		return out, failure("invalid_argument", "exact revision and an explicit metadata change required")
	}
	r, err := configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, false)
	if err != nil {
		return out, err
	}
	if r.Revision != q.ExpectedRevision {
		return out, failure("conflict", "resource revision changed")
	}
	if err = configurationMainGuard(ctx, tx, scope, q.Domain.Key, r, q.Main); err != nil {
		return out, err
	}
	main, err := configurationBranchNamed(ctx, tx, scope, q.Domain.Key, r.ID, "main")
	if err != nil {
		return out, err
	}
	snapshot, err := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, r.MainCommitID)
	if err != nil {
		return out, err
	}
	changed := q.Protect && !r.System
	before := r.NamespaceID
	if q.MoveTo != nil {
		if err = configurationNamespaceGuard(ctx, tx, scope, q.Domain.Key, *q.MoveTo); err != nil {
			return out, err
		}
		if r.System && q.MoveTo.ID != r.NamespaceID {
			return out, failure("failed_precondition", "system resource location is protected")
		}
		if err = configurationNameFree(ctx, tx, scope, q.Domain.Key, q.MoveTo.ID, r.NameKey, r.ID); err != nil {
			return out, err
		}
		changed = changed || r.NamespaceID != q.MoveTo.ID
	}
	if changed {
		old, _ := encode(r)
		if q.Protect {
			r.System = true
		}
		if q.MoveTo != nil {
			r.NamespaceID = q.MoveTo.ID
		}
		r.Revision, err = configurationBump(r.Revision)
		if err != nil {
			return out, err
		}
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		body, err := encode(r)
		if err != nil {
			return out, err
		}
		if err = reserve(ctx, tx, scope, 0, int64(len(body)-len(old))); err != nil {
			return out, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE core_resources SET namespace_id=?,revision=?,body=? WHERE scope=? AND domain=? AND id=?", configurationNullable(r.NamespaceID), r.Revision, body, scope, q.Domain.Key, r.ID); err != nil {
			return out, err
		}
		change := model.ConfigurationChange{ID: digest(struct{ Domain, Key string }{q.Domain.Key, q.Mutation.Key}), Summary: op, Nodes: []model.ConfigurationNodeChange{}}
		if err = configurationChange(ctx, tx, scope, q.Domain.Key, r.ID, snapshot.Head.Commit.ID, op, q.Mutation, change, before, r.NamespaceID); err != nil {
			return out, err
		}
	}
	plan, err := model.ConfigurationResourceMetadataPlanDigest(q)
	if err != nil {
		return out, err
	}
	disposition := "noop"
	if changed {
		disposition = "identity-only"
	}
	// The audit above includes the exact location change; do not append a second
	// generic state audit when storing the one durable product receipt.
	return configurationStateFinish(ctx, tx, scope, op, q.Domain, q.Mutation, plan, disposition, model.ConfigurationHead{Resource: r, Branch: main, Commit: snapshot.Head.Commit}, false)
}
