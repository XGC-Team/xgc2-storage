package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ImportConfiguration borrows the storage owner's creation transaction. It
// cannot merge, replay, overwrite or open a database. Every immutable value is
// checked by the existing shared digest/manifest and native snapshot reader.
func ImportConfiguration(ctx context.Context, tx *sql.Tx, scope string, in model.ConfigurationImport) error {
	if tx == nil || scope == "" || in.Format != "xgc2-configuration-import-v1" || !canonicalSessionPin(in.SourceSHA256) {
		return failure("invalid_argument", "explicit offline configuration import required")
	}
	return atomicData(ctx, tx, func() error {
		var existing int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM core_namespaces WHERE scope=?)+(SELECT count(*) FROM core_resources WHERE scope=?)+(SELECT count(*) FROM core_branches WHERE scope=?)+(SELECT count(*) FROM core_snapshots WHERE scope=?)+(SELECT count(*) FROM core_changes WHERE scope=?)`, scope, scope, scope, scope, scope).Scan(&existing); err != nil {
			return err
		}
		if existing != 0 {
			return failure("failed_precondition", "configuration import requires an empty target scope")
		}
		domains := map[string]model.ConfigurationDomainGuard{}
		for _, d := range in.Domains {
			g := model.ConfigurationDomainGuard{Key: d.Key, SchemaIdentity: d.SchemaIdentity, SchemaVersion: d.SchemaVersion, RegistryDigest: d.RegistryDigest}
			if _, err := configurationDomain(ctx, tx, g); err != nil {
				return err
			}
			domains[d.Key] = g
		}
		checkDomain := func(d string) error {
			if _, ok := domains[d]; !ok {
				return failure("invalid_argument", "import row has no declared domain")
			}
			return nil
		}
		pending := append([]model.ConfigurationImportNamespace(nil), in.Namespaces...)
		installed := map[string]bool{}
		for len(pending) > 0 {
			next := pending[:0]
			progress := false
			for _, v := range pending {
				if err := checkDomain(v.Domain); err != nil {
					return err
				}
				n := v.Row
				if n.ParentID != "" && !installed[v.Domain+"\x00"+n.ParentID] {
					next = append(next, v)
					continue
				}
				if model.ValidateConfigurationIdentifier(n.ID) != nil || !positiveRevision(n.Revision) || model.ValidateConfigurationName(model.ConfigurationNamespaceName, n.Name) != nil || model.ValidateConfigurationName(model.ConfigurationNamespaceName, n.NameKey) != nil {
					return failure("invalid_argument", "invalid imported namespace")
				}
				body, err := encode(n)
				if err != nil {
					return err
				}
				if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
					return err
				}
				if _, err = tx.ExecContext(ctx, "INSERT INTO core_namespaces VALUES(?,?,?,?,?,?,?,?,?)", scope, v.Domain, n.ID, configurationNullable(n.ParentID), n.Name, n.NameKey, n.Revision, n.ArchivedAt != "", body); err != nil {
					return err
				}
				installed[v.Domain+"\x00"+n.ID] = true
				progress = true
			}
			if !progress {
				return failure("invalid_argument", "import namespace parent is missing or cyclic")
			}
			pending = next
		}
		for _, v := range in.Resources {
			if err := checkDomain(v.Domain); err != nil {
				return err
			}
			r := v.Row
			if model.ValidateConfigurationIdentifier(r.ID) != nil || !positiveRevision(r.Revision) || !positiveRevision(r.NextVersion) || !textKey(r.MainCommitID) {
				return failure("invalid_argument", "invalid imported resource identity")
			}
			if err := configurationName(r.Name, r.NameKey); err != nil {
				return err
			}
			body, err := encode(r)
			if err != nil {
				return err
			}
			if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO core_resources VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", scope, v.Domain, r.ID, configurationNullable(r.NamespaceID), r.Name, r.NameKey, r.Revision, r.MainCommitID, r.ArchivedAt != "", r.OriginResourceID, r.OriginCommitID, body); err != nil {
				return err
			}
		}
		for _, v := range in.Branches {
			if err := checkDomain(v.Domain); err != nil {
				return err
			}
			b := v.Row
			if !textKey(b.ID) || !textKey(b.ResourceID) || !textKey(b.HeadCommitID) || !positiveRevision(b.Revision) || model.ValidateConfigurationName(model.ConfigurationBranchName, b.Name) != nil || model.ValidateConfigurationName(model.ConfigurationBranchName, b.NameKey) != nil {
				return failure("invalid_argument", "invalid imported branch")
			}
			body, err := encode(b)
			if err != nil {
				return err
			}
			if err = reserve(ctx, tx, scope, 1, int64(len(body))); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", scope, v.Domain, b.ID, b.ResourceID, b.NameKey, b.HeadCommitID, b.Revision, body); err != nil {
				return err
			}
		}
		for _, v := range in.Snapshots {
			if err := checkDomain(v.Domain); err != nil {
				return err
			}
			c := v.Commit
			if !textKey(c.ID) || !positiveRevision(c.Version) || !positiveRevision(c.BranchRevision) || c.SchemaVersion < 1 {
				return failure("invalid_argument", "invalid imported immutable identity")
			}
			manifest, err := model.DecodeConfigurationManifest(v.Manifest)
			if err != nil {
				return err
			}
			if manifest.RootDigest != c.RootDigest {
				return failure("data_loss", "legacy manifest root disagrees")
			}
			pin, err := model.ConfigurationContentDigest(v.Payload, v.Manifest, v.References)
			if err != nil {
				return err
			}
			if pin != c.ContentDigest {
				return failure("data_loss", "imported frozen content digest disagrees")
			}
			if _, err = configurationIdentity(v.Payload); err != nil {
				return err
			}
			if err = configurationInsertSnapshot(ctx, tx, scope, v.Domain, c, model.PreparedConfigurationSnapshot{Payload: v.Payload, Manifest: v.Manifest, References: v.References}); err != nil {
				return err
			}
		}
		for _, v := range in.Changes {
			if err := checkDomain(v.Domain); err != nil {
				return err
			}
			var record model.ConfigurationChangeRecord
			if !textKey(v.ID) || !json.Valid(v.Body) || json.Unmarshal(v.Body, &record) != nil || record.Change.ID != v.ID || len(v.Body) > model.MaxConfigurationDecodedBytes {
				return failure("invalid_argument", "invalid imported change")
			}
			if err := reserve(ctx, tx, scope, 1, int64(len(v.Body))); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "INSERT INTO core_changes VALUES(?,?,?,?,?)", scope, v.ID, v.Domain, v.EntityID, []byte(v.Body)); err != nil {
				return err
			}
		}
		for _, v := range in.Snapshots {
			got, err := configurationRead(ctx, tx, scope, model.ConfigurationResourceRead{Domain: domains[v.Domain], ResourceID: v.Commit.ResourceID, CommitID: v.Commit.ID, IncludeArchived: true})
			if err != nil {
				return fmt.Errorf("offline snapshot %s: %w", v.Commit.ID, err)
			}
			if got.Head.Commit != v.Commit {
				return failure("data_loss", "imported immutable metadata did not round trip")
			}
		}
		return nil
	})
}
