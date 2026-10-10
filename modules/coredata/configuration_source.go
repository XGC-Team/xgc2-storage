package coredata

import (
	"context"
	"database/sql"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationSource(ctx context.Context, tx *sql.Tx, scope, domain string, p model.ConfigurationSourcePin) (model.ConfigurationResourceSnapshot, error) {
	var zero model.ConfigurationResourceSnapshot
	if !positiveRevision(p.ExpectedResourceRevision) || !sha256Hex(p.ContentDigest) {
		return zero, failure("invalid_argument", "exact source resource/content pin required")
	}
	r, err := configurationResource(ctx, tx, scope, domain, p.ResourceID, false)
	if err != nil {
		return zero, err
	}
	if r.Revision != p.ExpectedResourceRevision {
		return zero, failure("conflict", "source resource changed")
	}
	if err = configurationMainGuard(ctx, tx, scope, domain, r, p.Main); err != nil {
		return zero, err
	}
	s, err := configurationCommitRow(ctx, tx, scope, domain, r.ID, p.CommitID)
	if err != nil {
		return zero, err
	}
	if s.Head.Commit.ContentDigest != p.ContentDigest {
		return zero, failure("conflict", "immutable source content differs")
	}
	if p.Branch != nil {
		b, err := configurationBranch(ctx, tx, scope, domain, p.Branch.ID)
		if err != nil {
			return zero, err
		}
		if b.ResourceID != r.ID || b.NameKey == "main" {
			return zero, failure("invalid_argument", "owned named promotion branch required")
		}
		if err = configurationGuard(b, s.Head.Commit, *p.Branch); err != nil {
			return zero, err
		}
		s.Head.Branch = b
	}
	s.Head.Resource = r
	return s, nil
}
