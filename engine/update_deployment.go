package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"

	"github.com/XGC-Team/xgc2-storage/api"
)

type DeploymentUpdateReceipt struct {
	DatabaseID      string `json:"database_id"`
	OldManifestHash string `json:"old_manifest_hash"`
	ManifestHash    string `json:"manifest_hash"`
}

// UpdateDeployment is an explicit offline owner operation. Existing contracts
// remain exact; only module implementation digests and newly declared empty
// collections may change. It neither copies data nor initializes a schema.
func UpdateDeployment(ctx context.Context, path string, old, next api.Manifest, compiled []DataModule) (out DeploymentUpdateReceipt, err error) {
	defer func() { err = classify(err) }()
	for _, manifest := range []api.Manifest{old, next} {
		if err = ValidateManifest(manifest); err != nil {
			return out, err
		}
	}
	// Compare the full deployment after removing only the allowed additions.
	// Existing collection identity, indexes and budgets are never changed.
	comparable := func(manifest api.Manifest) api.Manifest {
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
	oldShape, nextShape := comparable(old), comparable(next)
	if len(oldShape.Namespaces) != len(nextShape.Namespaces) {
		return out, fail("failed_precondition", "deployment update must preserve namespace contracts")
	}
	for i := range oldShape.Namespaces {
		prior, candidate := &oldShape.Namespaces[i], &nextShape.Namespaces[i]
		if prior.ID != candidate.ID {
			return out, fail("failed_precondition", "deployment update must preserve namespace identity")
		}
		declared := make(map[string]api.Collection, len(candidate.Collections))
		for _, collection := range candidate.Collections {
			declared[collection.ID] = collection
		}
		retained := make([]api.Collection, 0, len(prior.Collections))
		for _, collection := range prior.Collections {
			current, ok := declared[collection.ID]
			if !ok || hash(current) != hash(collection) {
				return out, fail("failed_precondition", "deployment update cannot alter or remove an existing collection")
			}
			retained = append(retained, current)
		}
		candidate.Collections = retained
	}
	if hash(oldShape) != hash(nextShape) {
		return out, fail("failed_precondition", "deployment update may only add collections or change compiled implementation digests")
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
		return out, fail("conflict", "deployed manifest changed during owner update")
	}
	err = tx.Commit()
	return out, err
}
