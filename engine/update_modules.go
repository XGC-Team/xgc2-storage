package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"

	"github.com/XGC-Team/xgc2-storage/api"
)

type ModuleUpdateReceipt struct {
	DatabaseID      string `json:"database_id"`
	OldManifestHash string `json:"old_manifest_hash"`
	ManifestHash    string `json:"manifest_hash"`
}

// UpdateModules explicitly accepts new implementations of the same deployed
// module contracts. It requires the offline database owner grant and never
// initializes modules, deploys declarations, or changes application data.
func UpdateModules(ctx context.Context, path string, old, next api.Manifest, compiled []DataModule) (out ModuleUpdateReceipt, err error) {
	defer func() { err = classify(err) }()
	for _, manifest := range []api.Manifest{old, next} {
		if err = ValidateManifest(manifest); err != nil {
			return out, err
		}
	}
	// Copies keep the exact input identities intact while comparing every field
	// other than the implementation digests, including module order and bounds.
	shape := func(manifest api.Manifest) api.Manifest {
		raw, _ := json.Marshal(manifest)
		var copy api.Manifest
		_ = json.Unmarshal(raw, &copy)
		for i := range copy.Namespaces {
			for j := range copy.Namespaces[i].Modules {
				copy.Namespaces[i].Modules[j].Digest = ""
			}
		}
		return copy
	}
	if hash(shape(old)) != hash(shape(next)) {
		return out, fail("failed_precondition", "module update may only change existing implementation digests")
	}
	registered := map[string]api.Module{}
	for _, module := range compiled {
		if _, exists := registered[module.Spec.ID]; exists {
			return out, fail("invalid_argument", "unique compiled modules required")
		}
		registered[module.Spec.ID] = module.Spec
	}
	for _, namespace := range next.Namespaces {
		for _, module := range namespace.Modules {
			if spec, ok := registered[module.ID]; !ok || hash(spec) != hash(module) {
				return out, fail("failed_precondition", "new module differs from compiled schema/operations")
			}
		}
	}
	o, err := acquire(path, false)
	if err != nil {
		return out, err
	}
	defer o.close()
	dsn := (&url.URL{Scheme: "file", Path: o.path(), RawQuery: "mode=rw&_pragma=busy_timeout(0)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return out, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var version string
	if err = db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return out, err
	}
	if version != "3.51.3" {
		return out, fail("failed_precondition", "pinned SQLite 3.51.3 engine required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var format, current string
	if err = tx.QueryRowContext(ctx, "SELECT format,database_id,manifest_hash FROM storage_meta WHERE id=1").Scan(&format, &out.DatabaseID, &current); err != nil {
		return out, err
	}
	if format != "storage-v1" {
		return out, fail("failed_precondition", "not a storage-v1 database")
	}
	out.OldManifestHash, out.ManifestHash = hash(old), hash(next)
	if current != out.OldManifestHash {
		return out, fail("conflict", "deployed manifest differs from exact old manifest")
	}
	result, err := tx.ExecContext(ctx, "UPDATE storage_meta SET manifest_hash=? WHERE id=1 AND format=? AND database_id=? AND manifest_hash=?", out.ManifestHash, format, out.DatabaseID, out.OldManifestHash)
	if err != nil {
		return out, err
	}
	if changed, e := result.RowsAffected(); e != nil {
		return out, e
	} else if changed != 1 {
		return out, fail("conflict", "deployed manifest changed during module update")
	}
	err = tx.Commit()
	return out, err
}
