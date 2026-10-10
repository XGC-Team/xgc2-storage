package coredata

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Store is the typed Core data API for one scope. Each method is one owner
// transaction: it either commits all of its effects or none. Writes that
// Core must not lose on a crash are Durable; the others say so.
type Store struct {
	db    *engine.Store
	scope string
}

// New binds the Core data module to a scope whose namespace names it.
func New(db *engine.Store, scope api.Scope) (*Store, error) {
	if db == nil {
		return nil, failure("invalid_argument", "storage owner required")
	}
	if err := db.CheckScope(scope); err != nil {
		return nil, err
	}
	if !db.HasModule(scope, model.Module) {
		return nil, failure("invalid_argument", "namespace does not use the Core data module")
	}
	return &Store{db: db, scope: engine.ScopeID(scope)}, nil
}

func write[T any](ctx context.Context, s *Store, d engine.Durability, fn func(context.Context, *sql.Tx, string) (T, error)) (out T, err error) {
	err = s.db.Write(ctx, d, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = fn(ctx, tx, s.scope)
		return e
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

func read[T any](ctx context.Context, s *Store, fn func(context.Context, *sql.Tx, string) (T, error)) (out T, err error) {
	err = s.db.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = fn(ctx, tx, s.scope)
		return e
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Configuration writes are Durable and idempotent by their product mutation
// key: repeating an intent returns the stored result with Replayed set.

func (s *Store) CreateResource(ctx context.Context, q model.ConfigurationResourceCreate) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationCreate(ctx, tx, scope, q)
	})
}

func (s *Store) CommitResource(ctx context.Context, q model.ConfigurationResourceCommit) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationCommit(ctx, tx, scope, q)
	})
}

func (s *Store) CreateBranch(ctx context.Context, q model.ConfigurationBranchCreate) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationBranchCreate(ctx, tx, scope, q)
	})
}

func (s *Store) ArchiveBranch(ctx context.Context, q model.ConfigurationBranchArchive) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationBranchArchive(ctx, tx, scope, q)
	})
}

func (s *Store) SetResourceState(ctx context.Context, q model.ConfigurationResourceState) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationResourceState(ctx, tx, scope, q)
	})
}

func (s *Store) UpdateResourceMetadata(ctx context.Context, q model.ConfigurationResourceMetadata) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationResourceMetadata(ctx, tx, scope, q)
	})
}

func (s *Store) CreateNamespace(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationNamespaceResult, error) {
		return configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceCreateOperation, q)
	})
}

func (s *Store) UpdateNamespace(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationNamespaceResult, error) {
		return configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceUpdateOperation, q)
	})
}

func (s *Store) SetNamespaceState(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationNamespaceResult, error) {
		return configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceStateOperation, q)
	})
}

func (s *Store) CloneNamespace(ctx context.Context, q model.ConfigurationNamespaceClone) (model.ConfigurationNamespaceResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationNamespaceResult, error) {
		return configurationClone(ctx, tx, scope, q)
	})
}

// Reads run on a reader connection in one snapshot.

// ReadResource returns the immutable snapshot selected by resource and branch
// or commit, together with the current main visibility pin.
func (s *Store) ReadResource(ctx context.Context, q model.ConfigurationResourceRead) (model.ConfigurationResourceSnapshot, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationResourceSnapshot, error) {
		return configurationRead(ctx, tx, scope, q)
	})
}

// Receipt looks up the stored result of a product mutation without applying it.
func (s *Store) Receipt(ctx context.Context, q model.ConfigurationReceipt) (model.ConfigurationMutationResult, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationReceipt(ctx, tx, scope, q)
	})
}

func (s *Store) CloneReceipt(ctx context.Context, q model.ConfigurationNamespaceCloneReceipt) (model.ConfigurationNamespaceResult, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationNamespaceResult, error) {
		return configurationCloneReceipt(ctx, tx, scope, q)
	})
}

func (s *Store) IncomingReferences(ctx context.Context, q model.ConfigurationIncomingRead) ([]model.ConfigurationIncomingReference, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]model.ConfigurationIncomingReference, error) {
		return configurationIncomingRead(ctx, tx, scope, q)
	})
}

// NamespaceTree returns the complete live subtree under one namespace with the
// current main snapshot of every resource, verified against its content digest.
func (s *Store) NamespaceTree(ctx context.Context, q model.NamespaceRead) (model.NamespaceTree, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.NamespaceTree, error) {
		return namespaceSnapshot(ctx, tx, scope, q)
	})
}

func catalog[T any](ctx context.Context, s *Store, operation string, q model.ConfigurationCatalogRead) ([]T, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) ([]T, error) {
		rows, err := configurationCatalog(ctx, tx, scope, operation, q)
		if err != nil {
			return nil, err
		}
		out := make([]T, 0, len(rows))
		for _, raw := range rows {
			var v T
			if json.Unmarshal(raw, &v) != nil {
				return nil, failure("data_loss", "invalid catalog metadata")
			}
			out = append(out, v)
		}
		return out, nil
	})
}

func (s *Store) Namespaces(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationNamespace, error) {
	return catalog[model.ConfigurationNamespace](ctx, s, model.ConfigurationNamespacesOperation, q)
}

func (s *Store) Resources(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationResource, error) {
	return catalog[model.ConfigurationResource](ctx, s, model.ConfigurationResourcesOperation, q)
}

func (s *Store) Branches(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationBranch, error) {
	return catalog[model.ConfigurationBranch](ctx, s, model.ConfigurationBranchesOperation, q)
}

func (s *Store) Commits(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationCommit, error) {
	return catalog[model.ConfigurationCommit](ctx, s, model.ConfigurationCommitsOperation, q)
}

func (s *Store) Changes(ctx context.Context, q model.ConfigurationCatalogRead) ([]model.ConfigurationChangeRecord, error) {
	return catalog[model.ConfigurationChangeRecord](ctx, s, model.ConfigurationChangesOperation, q)
}
