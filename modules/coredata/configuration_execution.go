package coredata

import (
	"context"
	"database/sql"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Runtime pins join the native execution action; validating them in a separate
// RPC before acceptance would permit the referenced catalog to change between
// the proof and the durable frozen Run. Pinned audit facts live with that Run.
func configurationValidatePinnedTargets(ctx context.Context, tx *sql.Tx, scope string, refs []model.ConfigurationReference) error {
	if len(refs) > model.MaxConfigurationReferences {
		return failure("resource_exhausted", "runtime configuration pin bound exceeded")
	}
	for _, ref := range refs {
		if ref.Mode != "pinned" {
			return failure("invalid_argument", "runtime references must be immutable pins")
		}
		if err := model.ValidateConfigurationReferenceMode(ref); err != nil {
			return failure("invalid_argument", err.Error())
		}
	}
	return configurationValidateReferences(ctx, tx, scope, "", model.ConfigurationResource{}, model.ConfigurationBranch{}, model.ConfigurationCommit{}, model.ValidatedConfigurationManifest{}, refs)
}

func configurationValidateMainPins(ctx context.Context, tx *sql.Tx, scope string, pins []model.ConfigurationMainPin) error {
	if len(pins) > model.MaxConfigurationReferences {
		return failure("resource_exhausted", "reviewed main pin bound exceeded")
	}
	for _, p := range pins {
		if _, err := configurationDomain(ctx, tx, p.Domain); err != nil {
			return err
		}
		if !textKey(p.ResourceID) || !textKey(p.CommitID) || !canonicalSessionPin(p.RootDigest) {
			return failure("invalid_argument", "exact reviewed main pin required")
		}
		r, err := configurationResource(ctx, tx, scope, p.Domain.Key, p.ResourceID, false)
		if err != nil {
			return err
		}
		if r.MainCommitID != p.CommitID {
			return failure("conflict", "reviewed current main changed")
		}
		s, err := configurationCommitRow(ctx, tx, scope, p.Domain.Key, r.ID, p.CommitID)
		if err != nil {
			return err
		}
		if s.Head.Commit.RootDigest != p.RootDigest {
			return failure("conflict", "reviewed main root changed")
		}
	}
	return nil
}
