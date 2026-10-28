package engine

import (
	"context"
	"database/sql"
	"sort"
)

// Module is compiled data code that owns relational tables in the same
// database, file and transactions as the document store. Explicit creation
// installs the current schema. Reopening requires the exact current stored
// version. The code identity of a module is the binary; no digest is stored.
type Module struct {
	ID string
	// Version is the current schema version (at least 1).
	Version int
	// Install creates the current schema in a database that has none.
	Install func(ctx context.Context, tx *sql.Tx) error
}

func (m Module) validate() error {
	if !identifier(m.ID) || m.Version < 1 || m.Install == nil {
		return fail("invalid_argument", "data module needs an identifier, a version and an installer")
	}
	return nil
}

func sortedModuleIDs(modules map[string]Module, used map[string]bool) []string {
	var ids []string
	for id := range modules {
		if used[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
