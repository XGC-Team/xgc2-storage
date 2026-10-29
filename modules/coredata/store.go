package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Store is the typed Core data API for one scope. Each method is one owner
// transaction: it either commits all of its effects or none. Writes that
// Core must not lose on a crash are Durable; the others say so.
type Store struct {
	db                *engine.Store
	scope             string
	agentLinkIdentity model.AgentLinkIdentity
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
	identity := model.AgentLinkIdentity{DatabaseID: db.DatabaseID(), Scope: scope, ModuleSchemaVersion: SchemaVersion}
	if identity.DatabaseID == "" {
		return nil, failure("failed_precondition", "storage owner identity unavailable")
	}
	return &Store{db: db, scope: engine.ScopeID(scope), agentLinkIdentity: identity}, nil
}

var errConfigurationReplay = errors.New("configuration receipt already committed")

func configurationReplay[T any](out T, err error) (T, error) {
	if err != nil {
		return out, err
	}
	switch result := any(out).(type) {
	case model.ConfigurationMutationResult:
		if result.Replayed {
			return out, errConfigurationReplay
		}
	case model.ConfigurationNamespaceResult:
		if result.Replayed {
			return out, errConfigurationReplay
		}
	}
	return out, nil
}

func write[T any](ctx context.Context, s *Store, d engine.Durability, fn func(context.Context, *sql.Tx, string) (T, error)) (T, error) {
	var out T
	err := s.db.Write(ctx, d, func(ctx context.Context, tx *sql.Tx) error {
		var e error
		out, e = fn(ctx, tx, s.scope)
		return e
	})
	if err != nil {
		if errors.Is(err, errConfigurationReplay) && engine.DefinitelyNotCommitted(err) {
			return out, nil
		}
		var zero T
		return zero, err
	}
	return out, nil
}

func writeInput[T any](ctx context.Context, s *Store, d engine.Durability, fn func(context.Context, *sql.Tx, string, *engine.WriteInput) (T, error)) (T, error) {
	var out T
	err := s.db.WriteWithInput(ctx, d, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) error {
		var e error
		out, e = fn(ctx, tx, s.scope, input)
		return e
	})
	if err != nil {
		if errors.Is(err, errConfigurationReplay) && engine.DefinitelyNotCommitted(err) {
			return out, nil
		}
		var zero T
		return zero, err
	}
	return out, nil
}

func read[T any](ctx context.Context, s *Store, fn func(context.Context, *sql.Tx, string) (T, error), prepare ...func()) (T, error) {
	var out T
	var freeze func()
	if len(prepare) != 0 {
		freeze = prepare[0]
	}
	err := s.db.ReadPrepared(ctx, freeze, func(ctx context.Context, tx *sql.Tx) error {
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
	if err := canceledWrite(ctx); err != nil {
		return model.ConfigurationMutationResult{}, err
	}
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationCreate(ctx, tx, scope, q, func(q *model.ConfigurationResourceCreate) error {
			return input.Own(ctx, func() { *q = ownedResourceCreate(*q) })
		}))
	})
}

func (s *Store) CommitResource(ctx context.Context, q model.ConfigurationResourceCommit) (model.ConfigurationMutationResult, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.ConfigurationMutationResult{}, err
	}
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationCommit(ctx, tx, scope, q, func(q *model.ConfigurationResourceCommit) error {
			return input.Own(ctx, func() { *q = ownedResourceCommit(*q) })
		}))
	})
}

func (s *Store) CreateBranch(ctx context.Context, q model.ConfigurationBranchCreate) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationBranchCreate(ctx, tx, scope, q))
	})
}

func (s *Store) ArchiveBranch(ctx context.Context, q model.ConfigurationBranchArchive) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationBranchArchive(ctx, tx, scope, q))
	})
}

func (s *Store) SetResourceState(ctx context.Context, q model.ConfigurationResourceState) (model.ConfigurationMutationResult, error) {
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationResourceState(ctx, tx, scope, q))
	})
}

func (s *Store) UpdateResourceMetadata(ctx context.Context, q model.ConfigurationResourceMetadata) (model.ConfigurationMutationResult, error) {
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationMutationResult, error) {
		return configurationReplay(configurationResourceMetadata(ctx, tx, scope, q, func(q *model.ConfigurationResourceMetadata) error {
			return input.Own(ctx, func() { q.MoveTo = ownedPointer(q.MoveTo) })
		}))
	})
}

func (s *Store) CreateNamespace(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationNamespaceResult, error) {
		return configurationReplay(configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceCreateOperation, q, func(q *model.ConfigurationNamespaceWrite) error {
			return input.Own(ctx, func() { *q = ownedNamespaceWrite(*q) })
		}))
	})
}

func (s *Store) UpdateNamespace(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationNamespaceResult, error) {
		return configurationReplay(configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceUpdateOperation, q, func(q *model.ConfigurationNamespaceWrite) error {
			return input.Own(ctx, func() { *q = ownedNamespaceWrite(*q) })
		}))
	})
}

func (s *Store) SetNamespaceState(ctx context.Context, q model.ConfigurationNamespaceWrite) (model.ConfigurationNamespaceResult, error) {
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationNamespaceResult, error) {
		return configurationReplay(configurationNamespaceWrite(ctx, tx, scope, model.ConfigurationNamespaceStateOperation, q, func(q *model.ConfigurationNamespaceWrite) error {
			return input.Own(ctx, func() { *q = ownedNamespaceWrite(*q) })
		}))
	})
}

func (s *Store) CloneNamespace(ctx context.Context, q model.ConfigurationNamespaceClone) (model.ConfigurationNamespaceResult, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.ConfigurationNamespaceResult{}, err
	}
	return writeInput(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string, input *engine.WriteInput) (model.ConfigurationNamespaceResult, error) {
		return configurationReplay(configurationClone(ctx, tx, scope, q, func(q *model.ConfigurationNamespaceClone) error {
			return input.Own(ctx, func() { *q = ownedNamespaceClone(*q) })
		}))
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
	}, func() { q.Operations = append([]string(nil), q.Operations...) })
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
	}, func() { q = ownedCatalogRead(q) })
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
