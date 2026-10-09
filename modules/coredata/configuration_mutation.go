package coredata

import (
	"context"

	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationMutationValid(m model.ConfigurationMutation) error {
	if !textKey(m.Key) || !canonicalSessionPin(m.IntentDigest) || m.Actor == "" || len(m.Actor) > 255 || !utf8.ValidString(m.Actor) || len(m.Reason) > 4096 || !utf8.ValidString(m.Reason) {
		return failure("invalid_argument", "invalid bounded product mutation metadata")
	}
	return nil
}
func configurationName(name, key string) error {
	display, canonical, e := model.NormalizeConfigurationName(model.ConfigurationResourceName, name)
	if e != nil || display != name || canonical != key {
		return failure("invalid_argument", "resource name and folded key must be canonical")
	}
	return nil
}
func configurationPrepared(s model.PreparedConfigurationSnapshot, control any) (model.ValidatedConfigurationManifest, string, error) {
	var zero model.ValidatedConfigurationManifest
	if e := configurationPlanBound(s, control, 0); e != nil {
		return zero, "", e
	}
	if !textKey(s.Change.ID) || len(s.Change.Summary) > 4096 || !utf8.ValidString(s.Change.Summary) {
		return zero, "", failure("invalid_argument", "bounded complete change required")
	}
	if _, e := configurationIdentity(s.Payload); e != nil {
		return zero, "", e
	}
	v, e := model.DecodeConfigurationManifest(s.Manifest)
	if e != nil {
		return zero, "", failure("invalid_argument", e.Error())
	}
	if v.RootDigest != s.RootDigest {
		return zero, "", failure("invalid_argument", "business root disagrees with generic manifest")
	}
	pin, e := model.ConfigurationContentDigest(s.Payload, s.Manifest, s.References)
	if e != nil {
		return zero, "", failure("invalid_argument", e.Error())
	}
	return v, pin, nil
}

func configurationPlanBound(s model.PreparedConfigurationSnapshot, control any, extra int) error {
	if len(s.Payload) > model.MaxConfigurationPayloadBytes || len(s.Manifest) > model.MaxConfigurationManifestBytes || len(s.References) > model.MaxConfigurationReferences || len(s.Change.Nodes) > model.MaxConfigurationNodeChanges {
		return failure("resource_exhausted", "configuration plan bound exceeded")
	}
	for _, ref := range s.References {
		if e := model.ValidateConfigurationReferenceMode(ref); e != nil {
			return failure("invalid_argument", e.Error())
		}
	}
	b, e := encode(control)
	if e != nil {
		return e
	}
	if len(b)+len(s.Payload)+len(s.Manifest)+extra > model.MaxConfigurationDecodedBytes {
		return failure("resource_exhausted", "complete decoded plan/audit/receipt exceeds12MiB")
	}
	return nil
}
func configurationFinishReceipt(ctx context.Context, tx *sql.Tx, scope string, out model.ConfigurationMutationResult, s model.PreparedConfigurationSnapshot, control any, mainBytes int) error {
	b, e := encode(out)
	if e != nil {
		return e
	}
	if e = configurationPlanBound(s, control, len(b)+mainBytes); e != nil {
		return e
	}
	return configurationSaveReceipt(ctx, tx, scope, out)
}
func configurationDiff(before, after model.ValidatedConfigurationManifest, c model.ConfigurationChange) error {
	nodes, summary, e := model.DiffConfigurationManifests(before, after)
	if e != nil {
		return failure("resource_exhausted", e.Error())
	}
	// Empty slice and nil both mean the complete empty diff.
	if len(nodes) != len(c.Nodes) || len(nodes) > 0 && !reflect.DeepEqual(nodes, c.Nodes) || summary != c.Summary {
		return failure("invalid_argument", "change must equal complete ordered before/after manifest diff")
	}
	return nil
}
func configurationFree(ctx context.Context, tx *sql.Tx, scope, domain string, r, b, c, change string) error {
	if !textKey(r) || !textKey(b) || !textKey(c) || !textKey(change) {
		return failure("invalid_argument", "bounded allocated identities required")
	}
	var exists bool
	e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_resources WHERE scope=? AND domain=? AND id=?) OR EXISTS(SELECT 1 FROM core_branches WHERE scope=? AND domain=? AND id=?) OR EXISTS(SELECT 1 FROM core_snapshots WHERE scope=? AND domain=? AND id=?) OR EXISTS(SELECT 1 FROM core_changes WHERE scope=? AND id=?)`, scope, domain, r, scope, domain, b, scope, domain, c, scope, change).Scan(&exists)
	if e == nil && exists {
		return failure("conflict", "allocated identity already exists")
	}
	return e
}
func configurationNameFree(ctx context.Context, tx *sql.Tx, scope, domain, namespace, key, exclude string) error {
	var id string
	e := tx.QueryRowContext(ctx, "SELECT id FROM core_resources WHERE scope=? AND domain=? AND coalesce(namespace_id,'')=? AND name_key=? AND archived=0 AND id<>?", scope, domain, namespace, key, exclude).Scan(&id)
	if errors.Is(e, sql.ErrNoRows) {
		return nil
	}
	if e == nil {
		return failure("conflict", "live resource name already exists")
	}
	return e
}
func configurationGuard(b model.ConfigurationBranch, c model.ConfigurationCommit, g model.ConfigurationBranchGuard) error {
	if !textKey(g.ID) || !positiveRevision(g.ExpectedRevision) || !textKey(g.CommitID) || !canonicalSessionPin(g.ContentDigest) {
		return failure("invalid_argument", "exact branch point/content guard required")
	}
	if b.ArchivedAt != "" || g.ID != b.ID || g.ExpectedRevision != b.Revision || g.CommitID != b.HeadCommitID || g.CommitID != c.ID || g.ContentDigest != c.ContentDigest {
		return failure("conflict", "branch point or immutable pin changed")
	}
	return nil
}
func configurationCreate(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationResourceCreate) (out model.ConfigurationMutationResult, err error) {
	if err = configurationMutationValid(q.Mutation); err != nil {
		return out, err
	}
	out, err = configurationReceipt(ctx, tx, scope, model.ConfigurationReceipt{Domain: q.Domain, Key: q.Mutation.Key, IntentDigest: q.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}})
	if err != nil || out.Found {
		return out, err
	}
	d, err := configurationDomain(ctx, tx, q.Domain)
	if err != nil {
		return out, err
	}
	if q.Source != nil {
		if _, err = configurationSource(ctx, tx, scope, q.Domain.Key, *q.Source); err != nil {
			return out, err
		}
	}
	if err = configurationName(q.Name, q.NameKey); err != nil {
		return out, err
	}
	if q.System && !d.AllowSystemProvisioning || !q.System && q.SystemKey != "" {
		return out, failure("failed_precondition", "system provisioning is not declared or has invalid identity")
	}
	if q.SystemKey != "" {
		if _, _, e := model.NormalizeConfigurationName(model.ConfigurationResourceName, q.SystemKey); e != nil || strings.TrimSpace(q.SystemKey) != q.SystemKey || q.Namespace.ID != "" {
			return out, failure("invalid_argument", "system key requires a bounded name and structural root")
		}
		var id string
		e := tx.QueryRowContext(ctx, "SELECT id FROM core_resources WHERE scope=? AND domain=? AND json_extract(CAST(body AS TEXT),'$.system')=1 AND json_extract(CAST(body AS TEXT),'$.system_key')<>'' AND json_extract(CAST(body AS TEXT),'$.system_key')=?", scope, q.Domain.Key, q.SystemKey).Scan(&id)
		if e == nil {
			return out, failure("conflict", "stable system key already owned")
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return out, e
		}
	}
	if err = configurationNamespaceGuard(ctx, tx, scope, q.Domain.Key, q.Namespace); err != nil {
		return out, err
	}
	if err = configurationNameFree(ctx, tx, scope, q.Domain.Key, q.Namespace.ID, q.NameKey, ""); err != nil {
		return out, err
	}
	if err = configurationFree(ctx, tx, scope, q.Domain.Key, q.ResourceID, q.BranchID, q.CommitID, q.Snapshot.Change.ID); err != nil {
		return out, err
	}
	control := q
	control.Snapshot.Payload = nil
	control.Snapshot.Manifest = nil
	manifest, pin, err := configurationPrepared(q.Snapshot, control)
	if err != nil {
		return out, err
	}
	if err = configurationDiff(model.ValidatedConfigurationManifest{}, manifest, q.Snapshot.Change); err != nil {
		return out, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	r := model.ConfigurationResource{ID: q.ResourceID, NamespaceID: q.Namespace.ID, Name: q.Name, NameKey: q.NameKey, Revision: "1", MainCommitID: q.CommitID, NextVersion: "2", System: q.System, SystemKey: q.SystemKey, CreatedAt: now, UpdatedAt: now}
	if q.Source != nil {
		r.OriginResourceID = q.Source.ResourceID
		r.OriginCommitID = q.Source.CommitID
	}
	b := model.ConfigurationBranch{ID: q.BranchID, ResourceID: r.ID, Name: "main", NameKey: "main", Revision: "1", HeadCommitID: q.CommitID, CreatedAt: now, UpdatedAt: now}
	c := model.ConfigurationCommit{ID: q.CommitID, ResourceID: r.ID, BranchID: b.ID, Version: "1", BranchRevision: "1", RootDigest: q.Snapshot.RootDigest, ContentDigest: pin, SchemaVersion: d.SchemaVersion, Actor: q.Mutation.Actor, Reason: q.Mutation.Reason, ChangeSummary: q.Snapshot.Change.Summary, CreatedAt: now}
	if q.Source != nil {
		c.SourceCommitID = q.Source.CommitID
		b.CreatedFromCommitID = c.ID
	}
	if err = configurationValidateReferences(ctx, tx, scope, q.Domain.Key, r, b, c, manifest, q.Snapshot.References); err != nil {
		return out, err
	}
	rb, _ := encode(r)
	bb, _ := encode(b)
	if err = reserve(ctx, tx, scope, 2, int64(len(rb)+len(bb))); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO core_resources VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", scope, q.Domain.Key, r.ID, configurationNullable(r.NamespaceID), r.Name, r.NameKey, r.Revision, r.MainCommitID, 0, r.OriginResourceID, r.OriginCommitID, []byte(rb)); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", scope, q.Domain.Key, b.ID, r.ID, b.NameKey, b.HeadCommitID, b.Revision, []byte(bb)); err != nil {
		return out, err
	}
	if err = configurationInsertSnapshot(ctx, tx, scope, q.Domain.Key, c, q.Snapshot); err != nil {
		return out, err
	}
	if err = configurationChange(ctx, tx, scope, q.Domain.Key, r.ID, c.ID, model.ResourceCreateOperation, q.Mutation, q.Snapshot.Change, "", ""); err != nil {
		return out, err
	}
	plan, err := model.ConfigurationCreatePlanDigest(q)
	if err != nil {
		return out, err
	}
	out = model.ConfigurationMutationResult{Found: true, Key: q.Mutation.Key, Domain: q.Domain.Key, Operation: model.ResourceCreateOperation, IntentDigest: q.Mutation.IntentDigest, PlanDigest: plan, Result: model.ConfigurationPublished{Disposition: "created", Head: model.ConfigurationHead{Resource: r, Branch: b, Commit: c}}}
	err = configurationFinishReceipt(ctx, tx, scope, out, q.Snapshot, control, 0)
	return out, err
}
func configurationCommit(ctx context.Context, tx *sql.Tx, scope string, q model.ConfigurationResourceCommit) (out model.ConfigurationMutationResult, err error) {
	if err = configurationMutationValid(q.Mutation); err != nil {
		return out, err
	}
	out, err = configurationReceipt(ctx, tx, scope, model.ConfigurationReceipt{Domain: q.Domain, Key: q.Mutation.Key, IntentDigest: q.Mutation.IntentDigest, Operations: []string{model.ResourceCommitOperation}})
	if err != nil || out.Found {
		return out, err
	}
	d, err := configurationDomain(ctx, tx, q.Domain)
	if err != nil {
		return out, err
	}
	if !textKey(q.ResourceID) {
		return out, failure("invalid_argument", "resource identity required")
	}
	if err = configurationName(q.Name, q.NameKey); err != nil {
		return out, err
	}
	r, err := configurationResource(ctx, tx, scope, q.Domain.Key, q.ResourceID, false)
	if err != nil {
		return out, err
	}
	b, err := configurationBranch(ctx, tx, scope, q.Domain.Key, q.Branch.ID)
	if err != nil {
		return out, err
	}
	if b.ResourceID != r.ID {
		return out, failure("invalid_argument", "branch is not owned by resource")
	}
	base, err := configurationCommitRow(ctx, tx, scope, q.Domain.Key, r.ID, b.HeadCommitID)
	if err != nil {
		return out, err
	}
	if err = configurationGuard(b, base.Head.Commit, q.Branch); err != nil {
		return out, err
	}
	if q.Source != nil {
		source, e := configurationSource(ctx, tx, scope, q.Domain.Key, *q.Source)
		if e != nil {
			return out, e
		}
		if source.Head.Resource.ID != r.ID || q.Source.Branch == nil || b.NameKey != "main" {
			return out, failure("invalid_argument", "promotion needs an owned named source branch")
		}
	}
	mainBytes := 0
	if d.MainVisibility || q.Main.ID != "" {
		main, err := configurationCurrentMain(ctx, tx, scope, q.Domain.Key, r, &base)
		if err != nil {
			return out, err
		}
		if !textKey(q.Main.ID) || !positiveRevision(q.Main.ExpectedRevision) || !textKey(q.Main.CommitID) || !canonicalSessionPin(q.Main.ContentDigest) {
			return out, failure("invalid_argument", "current main visibility pin required")
		}
		if main.Branch != q.Main {
			return out, failure("conflict", "current main visibility pin changed")
		}
		mainBytes = len(main.Identity)
	}
	ns := r.NamespaceID
	if q.MoveTo != nil {
		if err = configurationNamespaceGuard(ctx, tx, scope, q.Domain.Key, *q.MoveTo); err != nil {
			return out, err
		}
		ns = q.MoveTo.ID
	}
	identity := ns != r.NamespaceID || q.NameKey != r.NameKey
	if r.System && identity && !(q.AllowSystemRename && d.AllowSystemProvisioning && ns == r.NamespaceID) {
		return out, failure("failed_precondition", "protected resource identity cannot change")
	}
	if b.NameKey != "main" && identity {
		return out, failure("invalid_argument", "named branch cannot change resource identity")
	}
	if identity && (q.ExpectedResourceRevision == "" || q.ExpectedResourceRevision != r.Revision) {
		return out, failure("conflict", "resource identity revision changed")
	}
	if q.ExpectedResourceRevision != "" && !positiveRevision(q.ExpectedResourceRevision) {
		return out, failure("invalid_argument", "canonical resource revision required")
	}
	if q.ExpectedResourceRevision != "" && q.ExpectedResourceRevision != r.Revision {
		return out, failure("conflict", "resource revision changed")
	}
	plan, err := model.ConfigurationCommitPlanDigest(q)
	if err != nil {
		return out, err
	}
	control := q
	control.Snapshot.Payload = nil
	control.Snapshot.Manifest = nil
	if err = configurationPlanBound(q.Snapshot, control, mainBytes); err != nil {
		return out, err
	}
	// C1 is evaluated before noop/identity-only. A discarded rename is invalid.
	if base.Head.Commit.RootDigest == q.Snapshot.RootDigest && !q.AppendUnchangedSnapshot {
		if q.NameKey != r.NameKey {
			return out, failure("invalid_argument", "canonical rename requires a changed typed snapshot")
		}
		disposition := "noop"
		if ns != r.NamespaceID {
			if !textKey(q.Snapshot.Change.ID) {
				return out, failure("invalid_argument", "move audit identity required")
			}
			if err = configurationNameFree(ctx, tx, scope, q.Domain.Key, ns, r.NameKey, r.ID); err != nil {
				return out, err
			}
			beforePath, e := configurationResourcePath(ctx, tx, scope, q.Domain.Key, r.NamespaceID, r.Name)
			if e != nil {
				return out, e
			}
			afterPath, e := configurationResourcePath(ctx, tx, scope, q.Domain.Key, ns, r.Name)
			if e != nil {
				return out, e
			}
			mainBytes += len(beforePath) + len(afterPath)
			r.NamespaceID = ns
			r.Revision, err = configurationBump(r.Revision)
			if err != nil {
				return out, err
			}
			r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			rb, _ := encode(r)
			if err = reserve(ctx, tx, scope, 0, int64(len(rb))); err != nil {
				return out, err
			}
			if _, err = tx.ExecContext(ctx, "UPDATE core_resources SET namespace_id=?,revision=?,body=? WHERE scope=? AND domain=? AND id=?", configurationNullable(ns), r.Revision, []byte(rb), scope, q.Domain.Key, r.ID); err != nil {
				return out, err
			}
			if err = configurationChange(ctx, tx, scope, q.Domain.Key, r.ID, base.Head.Commit.ID, "move", q.Mutation, model.ConfigurationChange{ID: q.Snapshot.Change.ID, Summary: "resource location changed", Nodes: []model.ConfigurationNodeChange{}}, beforePath, afterPath); err != nil {
				return out, err
			}
			disposition = "identity-only"
		}
		out = model.ConfigurationMutationResult{Found: true, Key: q.Mutation.Key, Domain: q.Domain.Key, Operation: model.ResourceCommitOperation, IntentDigest: q.Mutation.IntentDigest, PlanDigest: plan, Result: model.ConfigurationPublished{Disposition: disposition, Head: model.ConfigurationHead{Resource: r, Branch: b, Commit: base.Head.Commit}}}
		err = configurationFinishReceipt(ctx, tx, scope, out, q.Snapshot, control, mainBytes)
		return out, err
	}
	manifest, pin, err := configurationPrepared(q.Snapshot, control)
	if err != nil {
		return out, err
	}
	before, err := model.DecodeConfigurationManifest(base.Manifest)
	if err != nil || before.RootDigest != base.Head.Commit.RootDigest {
		return out, failure("data_loss", "guarded base manifest disagrees")
	}
	if err = configurationDiff(before, manifest, q.Snapshot.Change); err != nil {
		return out, err
	}
	if !textKey(q.CommitID) || !textKey(q.Snapshot.Change.ID) {
		return out, failure("invalid_argument", "allocated commit and change identities required")
	}
	// Branch/resource IDs are existing; only the newly allocated immutable/audit
	// identities and live name must be free.
	var exists bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM core_snapshots WHERE scope=? AND domain=? AND id=?) OR EXISTS(SELECT 1 FROM core_changes WHERE scope=? AND id=?)`, scope, q.Domain.Key, q.CommitID, scope, q.Snapshot.Change.ID).Scan(&exists)
	if err != nil {
		return out, err
	}
	if exists {
		return out, failure("conflict", "allocated immutable or audit identity already exists")
	}
	if b.NameKey == "main" {
		if r.MainCommitID != b.HeadCommitID {
			return out, failure("data_loss", "main pointer disagrees")
		}
		if err = configurationNameFree(ctx, tx, scope, q.Domain.Key, ns, q.NameKey, r.ID); err != nil {
			return out, err
		}
	}
	if err = configurationIncoming(ctx, tx, scope, q.Domain.Key, r.ID, b, manifest); err != nil {
		return out, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	version := r.NextVersion
	r.NextVersion, err = configurationBump(r.NextVersion)
	if err != nil {
		return out, err
	}
	r.Revision, err = configurationBump(r.Revision)
	if err != nil {
		return out, err
	}
	b.Revision, err = configurationBump(b.Revision)
	if err != nil {
		return out, err
	}
	if b.NameKey == "main" {
		r.MainCommitID = q.CommitID
		r.Name = q.Name
		r.NameKey = q.NameKey
		r.NamespaceID = ns
	}
	r.UpdatedAt = now
	b.HeadCommitID = q.CommitID
	b.UpdatedAt = now
	c := model.ConfigurationCommit{ID: q.CommitID, ResourceID: r.ID, BranchID: b.ID, Version: version, BranchRevision: b.Revision, ParentCommitID: base.Head.Commit.ID, RootDigest: q.Snapshot.RootDigest, ContentDigest: pin, SchemaVersion: d.SchemaVersion, Actor: q.Mutation.Actor, Reason: q.Mutation.Reason, ChangeSummary: q.Snapshot.Change.Summary, CreatedAt: now}
	if q.Source != nil {
		c.SourceCommitID = q.Source.CommitID
	}
	if err = configurationValidateReferences(ctx, tx, scope, q.Domain.Key, r, b, c, manifest, q.Snapshot.References); err != nil {
		return out, err
	}
	rb, _ := encode(r)
	bb, _ := encode(b)
	if err = reserve(ctx, tx, scope, 0, int64(len(rb)+len(bb))); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE core_resources SET namespace_id=?,name=?,name_key=?,revision=?,main_commit_id=?,body=? WHERE scope=? AND domain=? AND id=?", configurationNullable(r.NamespaceID), r.Name, r.NameKey, r.Revision, r.MainCommitID, []byte(rb), scope, q.Domain.Key, r.ID); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE core_branches SET head_commit_id=?,revision=?,body=? WHERE scope=? AND domain=? AND id=?", b.HeadCommitID, b.Revision, []byte(bb), scope, q.Domain.Key, b.ID); err != nil {
		return out, err
	}
	if err = configurationInsertSnapshot(ctx, tx, scope, q.Domain.Key, c, q.Snapshot); err != nil {
		return out, err
	}
	if err = configurationChange(ctx, tx, scope, q.Domain.Key, r.ID, c.ID, model.ResourceCommitOperation, q.Mutation, q.Snapshot.Change, "", ""); err != nil {
		return out, err
	}
	out = model.ConfigurationMutationResult{Found: true, Key: q.Mutation.Key, Domain: q.Domain.Key, Operation: model.ResourceCommitOperation, IntentDigest: q.Mutation.IntentDigest, PlanDigest: plan, Result: model.ConfigurationPublished{Disposition: "committed", Head: model.ConfigurationHead{Resource: r, Branch: b, Commit: c}}}
	err = configurationFinishReceipt(ctx, tx, scope, out, q.Snapshot, control, mainBytes)
	return out, err
}
func configurationInsertSnapshot(ctx context.Context, tx *sql.Tx, scope, domain string, c model.ConfigurationCommit, s model.PreparedConfigurationSnapshot) error {
	cb, e := encode(c)
	if e != nil {
		return e
	}
	size := len(cb) + len(s.Payload) + len(s.Manifest)
	bodies := make([]json.RawMessage, len(s.References))
	for i, ref := range s.References {
		b, e := encode(ref)
		if e != nil {
			return e
		}
		bodies[i] = b
		size += len(b)
	}
	if e = reserve(ctx, tx, scope, int64(1+len(s.References)), int64(size)); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO core_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?)", scope, domain, c.ID, c.ResourceID, c.BranchID, c.Version, c.SourceCommitID, c.ContentDigest, s.Payload, s.Manifest, []byte(cb)); e != nil {
		return e
	}
	stmt, e := tx.PrepareContext(ctx, "INSERT INTO core_references VALUES(?,?,?,?,?,?,?,?)")
	if e != nil {
		return e
	}
	defer stmt.Close()
	for i, ref := range s.References {
		if _, e = stmt.ExecContext(ctx, scope, domain, c.ID, ref.Slot, ref.TargetDomain, ref.TargetResourceID, ref.TargetCommitID, []byte(bodies[i])); e != nil {
			return e
		}
	}
	return nil
}
func configurationResourcePath(ctx context.Context, tx *sql.Tx, scope, domain, namespace, name string) (string, error) {
	parts := []string{name}
	seen := map[string]bool{}
	for namespace != "" {
		if seen[namespace] || len(seen) >= model.MaxConfigurationNamespaceDepth {
			return "", failure("data_loss", "namespace path cycle/depth bound exceeded")
		}
		seen[namespace] = true
		var parent, segment string
		e := tx.QueryRowContext(ctx, "SELECT coalesce(parent_id,''),name FROM core_namespaces WHERE scope=? AND domain=? AND id=? AND archived=0", scope, domain, namespace).Scan(&parent, &segment)
		if e != nil {
			return "", e
		}
		if model.ValidateConfigurationName(model.ConfigurationNamespaceName, segment) != nil {
			return "", failure("data_loss", "invalid namespace path segment")
		}
		parts = append(parts, segment)
		namespace = parent
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return "/" + strings.Join(parts, "/"), nil
}
func configurationChange(ctx context.Context, tx *sql.Tx, scope, domain, resource, commit, op string, m model.ConfigurationMutation, c model.ConfigurationChange, beforePath, afterPath string) error {
	body, e := encode(model.ConfigurationChangeRecord{Operation: op, ResourceID: resource, CommitID: commit, Mutation: m, Change: c, BeforePath: beforePath, AfterPath: afterPath})
	if e != nil {
		return e
	}
	if len(body) > model.MaxConfigurationDecodedBytes {
		return failure("resource_exhausted", "change aggregate exceeds budget")
	}
	if e = reserve(ctx, tx, scope, 1, int64(len(body))); e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO core_changes VALUES(?,?,?,?,?)", scope, c.ID, domain, resource, []byte(body))
	return e
}
