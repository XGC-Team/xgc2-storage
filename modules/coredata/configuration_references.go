package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type configurationTarget struct {
	commit   model.ConfigurationCommit
	manifest model.ValidatedConfigurationManifest
}

// Targets are indexed points and bounded generic manifests, never table-wide
// plans, domain callbacks, or client SQL. The replacing branch is a prospective
// in-transaction target; all other branches retain their actual current heads.
func configurationValidateReferences(ctx context.Context, tx *sql.Tx, scope, domain string, r model.ConfigurationResource, b model.ConfigurationBranch, c model.ConfigurationCommit, m model.ValidatedConfigurationManifest, refs []model.ConfigurationReference) error {
	cache := map[string]configurationTarget{}
	seen := map[string]bool{}
	bytes := 0
	for _, ref := range refs {
		_, slot, e := model.NormalizeConfigurationName(model.ConfigurationSlotName, ref.Slot)
		if e != nil || slot != ref.Slot || seen[slot] {
			return failure("invalid_argument", "reference slots must be unique canonical keys")
		}
		seen[slot] = true
		_, branch, e := model.NormalizeConfigurationName(model.ConfigurationBranchName, ref.TargetBranch)
		if e != nil || branch != ref.TargetBranch || !textKey(ref.TargetDomain) || !textKey(ref.TargetResourceID) || ref.TargetComponentID != "" && !textKey(ref.TargetComponentID) {
			return failure("invalid_argument", "explicit canonical reference target required")
		}
		if ref.Mode != "tracking" && ref.Mode != "pinned" {
			return failure("invalid_argument", "reference mode must be tracking or pinned")
		}
		hasPin := ref.TargetCommitID != "" || ref.TargetVersion != "" || ref.TargetRootDigest != ""
		if (ref.Mode == "pinned" || hasPin) && (!textKey(ref.TargetCommitID) || !positiveRevision(ref.TargetVersion) || !canonicalSessionPin(ref.TargetRootDigest)) {
			return failure("invalid_argument", "complete exact reference pin required")
		}
		key := ref.TargetDomain + "\x00" + ref.TargetResourceID + "\x00" + ref.TargetBranch
		if ref.Mode == "pinned" {
			key += "\x00" + ref.TargetCommitID
		}
		target, ok := cache[key]
		prospective := ref.TargetDomain == domain && ref.TargetResourceID == r.ID && ref.TargetBranch == b.NameKey && (ref.Mode == "tracking" || ref.TargetCommitID == c.ID)
		if prospective {
			target = configurationTarget{commit: c, manifest: m}
			ok = true
		}
		if !ok {
			tr, e := configurationResource(ctx, tx, scope, ref.TargetDomain, ref.TargetResourceID, false)
			if e != nil {
				return e
			}
			tb, e := configurationBranchNamed(ctx, tx, scope, ref.TargetDomain, tr.ID, ref.TargetBranch)
			if e != nil {
				return e
			}
			if tb.ArchivedAt != "" {
				return failure("not_found", "live target branch not found")
			}
			id := tb.HeadCommitID
			if ref.Mode == "pinned" {
				id = ref.TargetCommitID
			}
			var body, manifest []byte
			e = tx.QueryRowContext(ctx, "SELECT body,manifest FROM core_snapshots WHERE scope=? AND domain=? AND resource_id=? AND id=?", scope, ref.TargetDomain, tr.ID, id).Scan(&body, &manifest)
			if errors.Is(e, sql.ErrNoRows) {
				return failure("not_found", "exact owned reference target not found")
			}
			if e != nil {
				return e
			}
			bytes += len(body) + len(manifest)
			if bytes > model.MaxConfigurationDecodedBytes {
				return failure("resource_exhausted", "reference target materialization exceeds bound")
			}
			if json.Unmarshal(body, &target.commit) != nil || target.commit.ID != id || target.commit.ResourceID != tr.ID || target.commit.BranchID != tb.ID {
				return failure("data_loss", "reference commit ownership disagrees")
			}
			target.manifest, e = model.DecodeConfigurationManifest(manifest)
			if e != nil || target.manifest.RootDigest != target.commit.RootDigest {
				return failure("data_loss", "reference manifest disagrees")
			}
			cache[key] = target
		}
		if hasPin && (ref.TargetCommitID != target.commit.ID || ref.TargetVersion != target.commit.Version || ref.TargetRootDigest != target.commit.RootDigest) {
			return failure("conflict", "reference target pin changed")
		}
		if ref.TargetComponentID != "" && !target.manifest.Contains(ref.TargetComponentID) {
			return failure("not_found", "reference component absent in transaction post-state")
		}
	}
	return nil
}

func configurationIncoming(ctx context.Context, tx *sql.Tx, scope, domain, resource string, b model.ConfigurationBranch, m model.ValidatedConfigurationManifest) error {
	// The target-leading index bounds discovery. Immutable historical outgoing
	// rows only block through a live source branch whose head is that commit.
	rows, e := tx.QueryContext(ctx, `SELECT f.body FROM core_references f
 JOIN core_snapshots s ON s.scope=f.scope AND s.domain=f.domain AND s.id=f.commit_id
 JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id
 JOIN core_branches b ON b.scope=s.scope AND b.domain=s.domain AND b.resource_id=s.resource_id AND b.head_commit_id=s.id
 WHERE f.scope=? AND f.target_domain=? AND f.target_resource_id=?
 AND json_extract(CAST(f.body AS TEXT),'$.mode')='tracking'
 AND json_extract(CAST(f.body AS TEXT),'$.target_branch')=?
 AND r.archived=0 AND coalesce(json_extract(CAST(b.body AS TEXT),'$.archived_at'),'')=''
 AND b.head_commit_id=s.id
 AND NOT(s.domain=? AND s.resource_id=? AND b.id=?) LIMIT 16385`, scope, domain, resource, b.NameKey, domain, resource, b.ID)
	if e != nil {
		return e
	}
	defer rows.Close()
	count, size := 0, 0
	for rows.Next() {
		var body []byte
		if e = rows.Scan(&body); e != nil {
			return e
		}
		count++
		size += len(body)
		if count > MaxReferences || size > model.MaxConfigurationDecodedBytes {
			return failure("resource_exhausted", "live incoming reference materialization bound exceeded")
		}
		var ref model.ConfigurationReference
		if json.Unmarshal(body, &ref) != nil {
			return failure("data_loss", "invalid live incoming reference")
		}
		if ref.TargetComponentID != "" && !m.Contains(ref.TargetComponentID) {
			return failure("failed_precondition", "commit removes component referenced by another live source")
		}
	}
	return rows.Err()
}
