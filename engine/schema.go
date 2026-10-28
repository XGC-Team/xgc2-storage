package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/XGC-Team/xgc2-storage/api"
)

// engineVersion identifies the current engine tables and required indexes.
// Older engine epochs are refused without migration or index rebuilding.
const engineVersion = 3

const ddl = `
CREATE TABLE storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),database_id TEXT NOT NULL);
CREATE TABLE schema_versions(module TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version>0));
CREATE TABLE scopes(scope TEXT PRIMARY KEY,namespace TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0),receipts INTEGER NOT NULL DEFAULT 0 CHECK(receipts>=0));
CREATE INDEX scopes_namespace ON scopes(namespace);
CREATE TABLE usage(scope TEXT NOT NULL,collection TEXT NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,tombstones INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,collection));
CREATE TABLE records(scope TEXT NOT NULL,collection TEXT NOT NULL,key TEXT NOT NULL,version INTEGER NOT NULL,deleted INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(scope,collection,key));
CREATE INDEX records_tombstones ON records(scope,collection,version) WHERE deleted=1;
CREATE TABLE lookups(scope TEXT NOT NULL,collection TEXT NOT NULL,index_name TEXT NOT NULL,index_value TEXT NOT NULL,key TEXT NOT NULL,unique_value TEXT,PRIMARY KEY(scope,collection,index_name,index_value,key),UNIQUE(scope,collection,index_name,unique_value));
CREATE INDEX lookups_by_key ON lookups(scope,collection,key);
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

func hasTable(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, name string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&n)
	return n > 0, err
}

// storedVersions reads current explicit component markers. An absent marker
// is refused without probing historic layouts or invoking legacy decoders.
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
		return nil, fail("failed_precondition", "current engine schema version markers are required")
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

// admitCurrentSchema requires every named module's current persisted marker.
// Refusal precedes index synchronization and never installs or converts data.
func (s *Store) admitCurrentSchema(ctx context.Context) error {
	versions, err := s.storedVersions(ctx)
	if err != nil {
		return err
	}
	if v := versions["engine"]; v > engineVersion {
		return fail("failed_precondition", fmt.Sprintf("database engine schema is version %d, newer than this release supports (%d); upgrade the software", v, engineVersion))
	} else if v < engineVersion {
		return fail("failed_precondition", fmt.Sprintf("database engine schema is version %d; this release requires current version %d", v, engineVersion))
	}
	for _, id := range sortedModuleIDs(s.modules, s.usedModules()) {
		m := s.modules[id]
		stored := versions[id]
		switch {
		case stored > m.Version:
			return fail("failed_precondition", fmt.Sprintf("database schema of module %s is version %d, newer than this release supports (%d); upgrade the software", id, stored, m.Version))
		case stored == 0:
			return fail("failed_precondition", fmt.Sprintf("database schema of module %s has no current version marker", id))
		case stored < m.Version:
			return fail("failed_precondition", fmt.Sprintf("database schema of module %s is version %d; this release requires current version %d", id, stored, m.Version))
		}
	}
	return s.syncIndexes(ctx)
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
