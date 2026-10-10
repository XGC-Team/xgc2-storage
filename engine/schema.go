package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"golang.org/x/sys/unix"
)

// engineVersion is the schema version of the engine's own tables. Version 1
// carried a manifest hash, Named receipts and lifetime-counted usage.
const engineVersion = 2

const ddl = `
CREATE TABLE storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),database_id TEXT NOT NULL);
CREATE TABLE schema_versions(module TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version>0));
CREATE TABLE scopes(scope TEXT PRIMARY KEY,namespace TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0),receipts INTEGER NOT NULL DEFAULT 0 CHECK(receipts>=0));
CREATE INDEX scopes_namespace ON scopes(namespace);
CREATE TABLE usage(scope TEXT NOT NULL,collection TEXT NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,tombstones INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,collection));
CREATE TABLE records(scope TEXT NOT NULL,collection TEXT NOT NULL,key TEXT NOT NULL,version INTEGER NOT NULL,deleted INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(scope,collection,key));
CREATE INDEX records_tombstones ON records(scope,collection,version) WHERE deleted=1;
CREATE TABLE lookups(scope TEXT NOT NULL,collection TEXT NOT NULL,index_name TEXT NOT NULL,index_value TEXT NOT NULL,key TEXT NOT NULL,unique_value TEXT,PRIMARY KEY(scope,collection,index_name,index_value,key),UNIQUE(scope,collection,index_name,unique_value));
CREATE TABLE receipts(scope TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,expires INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,request_id));
CREATE INDEX receipts_expiry ON receipts(expires);
CREATE TABLE collection_indexes(namespace TEXT NOT NULL,collection TEXT NOT NULL,signature TEXT NOT NULL,PRIMARY KEY(namespace,collection));
`

// usedModules is the set of module identifiers the manifest's namespaces name.
func (s *Store) usedModules() map[string]bool {
	used := map[string]bool{}
	for _, n := range s.config.Manifest.Namespaces {
		for _, id := range n.Modules {
			used[id] = true
		}
	}
	return used
}

// namespacesOf lists the manifest namespaces that name a module.
func (s *Store) namespacesOf(module string) []string {
	var out []string
	for _, n := range s.config.Manifest.Namespaces {
		for _, id := range n.Modules {
			if id == module {
				out = append(out, n.ID)
			}
		}
	}
	return out
}

func setVersion(ctx context.Context, tx *sql.Tx, module string, version int) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO schema_versions VALUES(?,?) ON CONFLICT(module) DO UPDATE SET version=excluded.version", module, version)
	return err
}

// create initializes an absent database: engine tables, every named module and
// the collection index signatures, in one transaction.
func (s *Store) create(ctx context.Context) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, ddl); err != nil {
		return err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return err
	}
	s.dbid = hex.EncodeToString(id[:])
	if _, err = tx.ExecContext(ctx, "INSERT INTO storage_meta VALUES(1,?)", s.dbid); err != nil {
		return err
	}
	if err = setVersion(ctx, tx, "engine", engineVersion); err != nil {
		return err
	}
	for _, id := range sortedModuleIDs(s.modules, s.usedModules()) {
		m := s.modules[id]
		if err = m.Install(ctx, tx); err != nil {
			return err
		}
		if err = setVersion(ctx, tx, id, m.Version); err != nil {
			return err
		}
	}
	for _, n := range s.config.Manifest.Namespaces {
		for _, c := range n.Collections {
			if _, err = tx.ExecContext(ctx, "INSERT INTO collection_indexes VALUES(?,?,?)", n.ID, c.ID, indexSignature(c)); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

type migrationStep struct {
	module string
	from   int
	to     int
	apply  func(context.Context, *sql.Tx, []string) error
}

func hasTable(ctx context.Context, q Queryer, name string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&n)
	return n > 0, err
}

// storedVersions reads the schema version of every installed component. A
// database written before versions existed counts as engine version 1 and
// each module reports its own legacy installation.
func (s *Store) storedVersions(ctx context.Context) (map[string]int, error) {
	versions := map[string]int{}
	ok, err := hasTable(ctx, s.writer, "storage_meta")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fail("failed_precondition", "not a storage database; initialize it explicitly with create")
	}
	if ok, err = hasTable(ctx, s.writer, "schema_versions"); err != nil {
		return nil, err
	}
	if !ok {
		var format string
		if err = s.writer.QueryRowContext(ctx, "SELECT format,database_id FROM storage_meta WHERE id=1").Scan(&format, &s.dbid); err != nil || format != "storage-v1" {
			return nil, fail("failed_precondition", "not a storage-v1 or newer database")
		}
		versions["engine"] = 1
		for id := range s.usedModules() {
			m := s.modules[id]
			if m.Legacy == nil {
				continue
			}
			v, err := m.Legacy(ctx, s.writer)
			if err != nil {
				return nil, err
			}
			if v > 0 {
				versions[id] = v
			}
		}
		return versions, nil
	}
	if err = s.writer.QueryRowContext(ctx, "SELECT database_id FROM storage_meta WHERE id=1").Scan(&s.dbid); err != nil {
		return nil, err
	}
	rows, err := s.writer.QueryContext(ctx, "SELECT module,version FROM schema_versions")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var module string
		var version int
		if err = rows.Scan(&module, &version); err != nil {
			return nil, err
		}
		versions[module] = version
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if versions["engine"] == 0 {
		return nil, fail("failed_precondition", "storage schema version table has no engine row")
	}
	return versions, nil
}

// upgrade opens an existing database. The same schema version opens whatever
// the code, an older one is migrated after a backup, a newer one is refused.
func (s *Store) upgrade(ctx context.Context) error {
	versions, err := s.storedVersions(ctx)
	if err != nil {
		return err
	}
	var plan []migrationStep
	if v := versions["engine"]; v > engineVersion {
		return fail("failed_precondition", fmt.Sprintf("database engine schema is version %d, newer than this release supports (%d); upgrade the software", v, engineVersion))
	} else if v < engineVersion {
		for ; v < engineVersion; v++ {
			plan = append(plan, migrationStep{module: "engine", from: v, to: v + 1, apply: engineMigrations[v]})
		}
	}
	for _, id := range sortedModuleIDs(s.modules, s.usedModules()) {
		m := s.modules[id]
		stored := versions[id]
		switch {
		case stored > m.Version:
			return fail("failed_precondition", fmt.Sprintf("database schema of module %s is version %d, newer than this release supports (%d); upgrade the software", id, stored, m.Version))
		case stored == 0:
			install := m.Install
			plan = append(plan, migrationStep{module: id, from: 0, to: m.Version, apply: func(ctx context.Context, tx *sql.Tx, _ []string) error { return install(ctx, tx) }})
		case stored < m.Version:
			path, err := m.path(stored)
			if err != nil {
				return err
			}
			for _, step := range path {
				plan = append(plan, migrationStep{module: id, from: step.From, to: step.From + 1, apply: step.Apply})
			}
		}
	}
	if len(plan) > 0 {
		if s.config.NoMigrate {
			return fail("failed_precondition", "database schema is older than this release; open it with the owning application to migrate it")
		}
		if err = s.backupBeforeMigration(ctx, plan); err != nil {
			return err
		}
		tx, err := s.writer.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, step := range plan {
			if err = step.apply(ctx, tx, s.namespacesOf(step.module)); err != nil {
				return fmt.Errorf("migrating %s from version %d: %w", step.module, step.from, err)
			}
			if err = setVersion(ctx, tx, step.module, step.to); err != nil {
				return err
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return s.syncIndexes(ctx)
}

// backupBeforeMigration keeps a consistent private copy next to the database
// so a failed or unwanted upgrade can be rolled back by restoring the file.
func (s *Store) backupBeforeMigration(ctx context.Context, plan []migrationStep) error {
	name := fmt.Sprintf("%s.before-%s-v%d.%d.db", s.owner.name, plan[0].module, plan[0].from, time.Now().Unix())
	if _, err := s.writer.ExecContext(ctx, "VACUUM INTO ?", fmt.Sprintf("/proc/self/fd/%d/%s", s.owner.parent, name)); err != nil {
		return err
	}
	fd, err := unix.Openat(s.owner.parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err = unix.Fchmod(fd, 0600); err != nil {
		return err
	}
	if err = unix.Fsync(fd); err != nil {
		return err
	}
	return unix.Fsync(s.owner.parent)
}

// engineMigrations[v] upgrades the engine's own tables from version v to v+1.
var engineMigrations = map[int]func(context.Context, *sql.Tx, []string) error{1: migrateEngine1}

// migrateEngine1 upgrades the engine tables from version 1 to 2: the manifest hash
// and Named results disappear, receipts get incremental counters and usage
// counts live records and bytes instead of every key ever written.
func migrateEngine1(ctx context.Context, tx *sql.Tx, _ []string) error {
	for _, statement := range []string{
		"ALTER TABLE storage_meta DROP COLUMN format",
		"ALTER TABLE storage_meta DROP COLUMN manifest_hash",
		"CREATE TABLE schema_versions(module TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version>0))",
		// Named receipts belonged to in-process data calls, which keep none.
		"DELETE FROM receipts WHERE EXISTS (SELECT 1 FROM named_results n WHERE n.scope=receipts.scope AND n.request_id=receipts.request_id)",
		"DROP TABLE named_results",
		"ALTER TABLE scopes ADD COLUMN receipts INTEGER NOT NULL DEFAULT 0 CHECK(receipts>=0)",
		"UPDATE scopes SET receipts=(SELECT count(*) FROM receipts r WHERE r.scope=scopes.scope)",
		"ALTER TABLE usage ADD COLUMN tombstones INTEGER NOT NULL DEFAULT 0",
		`UPDATE usage SET
 records=(SELECT count(*) FROM records r WHERE r.scope=usage.scope AND r.collection=usage.collection AND r.deleted=0),
 bytes=(SELECT coalesce(sum(length(r.data)+length(CAST(r.key AS BLOB))),0) FROM records r WHERE r.scope=usage.scope AND r.collection=usage.collection AND r.deleted=0),
 tombstones=(SELECT count(*) FROM records r WHERE r.scope=usage.scope AND r.collection=usage.collection AND r.deleted=1)`,
		"CREATE INDEX records_tombstones ON records(scope,collection,version) WHERE deleted=1",
		"CREATE TABLE collection_indexes(namespace TEXT NOT NULL,collection TEXT NOT NULL,signature TEXT NOT NULL,PRIMARY KEY(namespace,collection))",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return setVersion(ctx, tx, "engine", engineVersion)
}

// Retire deletes the data of collections that a release no longer declares,
// inside a module migration transaction. Without collections it removes the
// whole namespace, its scopes and its receipts. Live quota counters follow.
func Retire(ctx context.Context, tx *sql.Tx, namespace string, collections ...string) error {
	if namespace == "" {
		return fail("invalid_argument", "namespace required")
	}
	const scopes = "scope IN (SELECT scope FROM scopes WHERE namespace=?)"
	if len(collections) == 0 {
		for _, statement := range []string{
			"DELETE FROM lookups WHERE " + scopes, "DELETE FROM records WHERE " + scopes, "DELETE FROM usage WHERE " + scopes,
			"DELETE FROM receipts WHERE " + scopes, "DELETE FROM collection_indexes WHERE namespace=?", "DELETE FROM scopes WHERE namespace=?",
		} {
			if _, err := tx.ExecContext(ctx, statement, namespace); err != nil {
				return err
			}
		}
		return nil
	}
	for _, c := range collections {
		for _, statement := range []string{
			"DELETE FROM lookups WHERE collection=? AND " + scopes, "DELETE FROM records WHERE collection=? AND " + scopes,
			"DELETE FROM usage WHERE collection=? AND " + scopes, "DELETE FROM collection_indexes WHERE collection=? AND namespace=?",
		} {
			if _, err := tx.ExecContext(ctx, statement, c, namespace); err != nil {
				return err
			}
		}
	}
	return nil
}

func indexSignature(c api.Collection) string {
	return hash(c.Indexes)
}

// syncIndexes makes the stored lookup rows match the manifest's index
// declarations. A manifest may change between releases; an index that changed
// is rebuilt from the records in one transaction, and a unique index that the
// stored data violates fails the open with the database untouched.
func (s *Store) syncIndexes(ctx context.Context) error {
	stored := map[[2]string]string{}
	rows, err := s.writer.QueryContext(ctx, "SELECT namespace,collection,signature FROM collection_indexes")
	if err != nil {
		return err
	}
	for rows.Next() {
		var namespace, collection, signature string
		if err = rows.Scan(&namespace, &collection, &signature); err != nil {
			rows.Close()
			return err
		}
		stored[[2]string{namespace, collection}] = signature
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	declared := map[[2]string]api.Collection{}
	for _, n := range s.config.Manifest.Namespaces {
		for _, c := range n.Collections {
			declared[[2]string{n.ID, c.ID}] = c
		}
	}
	var stale, changed [][2]string
	for k := range stored {
		if _, ok := declared[k]; !ok {
			stale = append(stale, k)
		}
	}
	for k, c := range declared {
		if stored[k] != indexSignature(c) {
			changed = append(changed, k)
		}
	}
	if len(stale) == 0 && len(changed) == 0 {
		return nil
	}
	sort.Slice(changed, func(i, j int) bool {
		return changed[i][0]+"\x00"+changed[i][1] < changed[j][0]+"\x00"+changed[j][1]
	})
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, k := range stale {
		if _, err = tx.ExecContext(ctx, "DELETE FROM collection_indexes WHERE namespace=? AND collection=?", k[0], k[1]); err != nil {
			return err
		}
	}
	for _, k := range changed {
		c := declared[k]
		if err = rebuildLookups(ctx, tx, k[0], c); err != nil {
			return fmt.Errorf("rebuilding indexes of %s/%s: %w", k[0], k[1], classify(err))
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO collection_indexes VALUES(?,?,?) ON CONFLICT(namespace,collection) DO UPDATE SET signature=excluded.signature", k[0], k[1], indexSignature(c)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// rebuildLookups recomputes the lookup rows of one collection in every scope of
// its namespace from the live records.
func rebuildLookups(ctx context.Context, tx *sql.Tx, namespace string, c api.Collection) error {
	rows, err := tx.QueryContext(ctx, "SELECT scope FROM scopes WHERE namespace=?", namespace)
	if err != nil {
		return err
	}
	var scopes []string
	for rows.Next() {
		var scope string
		if err = rows.Scan(&scope); err != nil {
			rows.Close()
			return err
		}
		scopes = append(scopes, scope)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	type record struct {
		key  string
		data []byte
	}
	for _, scope := range scopes {
		if _, err = tx.ExecContext(ctx, "DELETE FROM lookups WHERE scope=? AND collection=?", scope, c.ID); err != nil {
			return err
		}
		if len(c.Indexes) == 0 {
			continue
		}
		after := ""
		for {
			page, err := tx.QueryContext(ctx, "SELECT key,data FROM records WHERE scope=? AND collection=? AND deleted=0 AND key>? ORDER BY key LIMIT 500", scope, c.ID, after)
			if err != nil {
				return err
			}
			var batch []record
			for page.Next() {
				var r record
				if err = page.Scan(&r.key, &r.data); err != nil {
					page.Close()
					return err
				}
				batch = append(batch, r)
			}
			if err = errors.Join(page.Err(), page.Close()); err != nil {
				return err
			}
			for _, r := range batch {
				_, document, err := canonicalObject(r.data)
				if err != nil {
					return err
				}
				for _, idx := range c.Indexes {
					tuple, nullable, err := indexTuple(document, idx)
					if err != nil {
						return err
					}
					var unique any
					if idx.Unique && !nullable {
						unique = tuple
					}
					if _, err = tx.ExecContext(ctx, "INSERT INTO lookups VALUES(?,?,?,?,?,?)", scope, c.ID, idx.ID, tuple, r.key, unique); err != nil {
						return err
					}
				}
			}
			if len(batch) < 500 {
				break
			}
			after = batch[len(batch)-1].key
		}
	}
	return nil
}
