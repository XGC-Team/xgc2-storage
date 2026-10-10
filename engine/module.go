package engine

import (
	"context"
	"database/sql"
	"sort"
)

// Module is compiled data code that owns relational tables in the same
// database, file and transactions as the document store. The owner installs a
// module the first time a manifest namespace names it, and migrates an older
// stored schema forward in one transaction before it serves a call. A newer
// stored schema is refused, so an old binary never writes a layout it does not
// understand. The code identity of a module is the binary; no digest is stored.
type Module struct {
	ID string
	// Version is the current schema version (at least 1).
	Version int
	// Install creates the current schema in a database that has none.
	Install func(ctx context.Context, tx *sql.Tx) error
	// Migrations upgrade one version each; From is the version they leave.
	// They must cover every stored version the binary still supports, without gaps.
	Migrations []Migration
	// Legacy reports the schema version of an installation that predates the
	// version table (0 when the module is absent). It lets the first versioned
	// owner adopt a database written by an earlier release.
	Legacy func(ctx context.Context, q Queryer) (int, error)
}

// Migration moves a module from version From to From+1 inside the migration
// transaction. It must only use the transaction it is given.
type Migration struct {
	From  int
	Apply func(ctx context.Context, tx *sql.Tx) error
}

// Queryer is the read access a legacy probe needs.
type Queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (m Module) validate() error {
	if !identifier(m.ID) || m.Version < 1 || m.Install == nil {
		return fail("invalid_argument", "data module needs an identifier, a version and an installer")
	}
	seen := map[int]bool{}
	for _, step := range m.Migrations {
		if step.From < 1 || step.From >= m.Version || step.Apply == nil || seen[step.From] {
			return fail("invalid_argument", "data module "+m.ID+" has an invalid migration")
		}
		seen[step.From] = true
	}
	if len(seen) > 0 {
		for v := m.oldest(); v < m.Version; v++ {
			if !seen[v] {
				return fail("invalid_argument", "data module "+m.ID+" migrations have a gap")
			}
		}
	}
	return nil
}

func (m Module) oldest() int {
	oldest := m.Version
	for _, step := range m.Migrations {
		oldest = min(oldest, step.From)
	}
	return oldest
}

// path returns the migrations from a stored version to the current one.
func (m Module) path(from int) ([]Migration, error) {
	var out []Migration
	for v := from; v < m.Version; v++ {
		found := false
		for _, step := range m.Migrations {
			if step.From == v {
				out = append(out, step)
				found = true
			}
		}
		if !found {
			return nil, fail("failed_precondition", "data module "+m.ID+" cannot migrate from an unsupported older schema version")
		}
	}
	return out, nil
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
