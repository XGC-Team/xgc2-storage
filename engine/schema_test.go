package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

// toy is a minimal data module with a configurable schema version.
func toy(version int, migrations ...Migration) Module {
	return Module{ID: "toy", Version: version, Migrations: migrations, Install: func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE toy_items(id INTEGER PRIMARY KEY,name TEXT NOT NULL)")
		if err == nil && version > 1 {
			_, err = tx.ExecContext(ctx, "ALTER TABLE toy_items ADD COLUMN size INTEGER NOT NULL DEFAULT 0")
		}
		return err
	}}
}

func toyManifest() api.Manifest {
	m := manifest()
	m.Namespaces[0].Modules = []string{"toy"}
	return m
}

func toyConfig(t *testing.T, create bool, path string, module Module) Config {
	if path == "" {
		path = filepath.Join(t.TempDir(), "toy.db")
		os.Chmod(filepath.Dir(path), 0700)
	}
	return Config{Path: path, Create: create, Manifest: toyManifest(), Modules: []Module{module}}
}

func exec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if err := s.Write(budget(t), Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func versionOf(t *testing.T, s *Store, module string) (v int64) {
	t.Helper()
	return countRows(t, s, "SELECT version FROM schema_versions WHERE module=?", module)
}

func TestSameSchemaVersionOpensWithDifferentCode(t *testing.T) {
	ctx := budget(t)
	config := toyConfig(t, true, "", toy(1))
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, s, "INSERT INTO toy_items(name) VALUES('kept')")
	s.Close()
	// Different compiled code (here: another installer) is not a different schema.
	other := toy(1)
	other.Install = func(context.Context, *sql.Tx) error { return errors.New("different code") }
	config.Create, config.Modules = false, []Module{other}
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatalf("same schema version refused: %v", err)
	}
	defer s.Close()
	if n := countRows(t, s, "SELECT count(*) FROM toy_items"); n != 1 || versionOf(t, s, "toy") != 1 || versionOf(t, s, "engine") != engineVersion {
		t.Fatalf("data or versions changed: %d", n)
	}
	if matches, _ := filepath.Glob(config.Path + ".before-*"); len(matches) != 0 {
		t.Fatalf("an unchanged schema took a migration backup: %v", matches)
	}
}

func TestNewerSchemaIsRefusedWithAClearError(t *testing.T) {
	for _, module := range []string{"toy", "engine"} {
		t.Run(module, func(t *testing.T) {
			ctx := budget(t)
			config := toyConfig(t, true, "", toy(1))
			s, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			exec(t, s, "UPDATE schema_versions SET version=99 WHERE module=?", module)
			s.Close()
			config.Create = false
			_, err = Open(ctx, config)
			if code(err) != "failed_precondition" || !strings.Contains(err.Error(), "newer") || !strings.Contains(err.Error(), module) {
				t.Fatalf("newer %s schema: %v", module, err)
			}
			// The refusal released the owner lock and wrote nothing.
			exec2, err := sql.Open("sqlite", "file:"+config.Path+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer exec2.Close()
			var version int
			if err = exec2.QueryRow("SELECT version FROM schema_versions WHERE module=?", module).Scan(&version); err != nil || version != 99 {
				t.Fatalf("refused open changed the stored version: %d %v", version, err)
			}
		})
	}
}

func TestOlderSchemaMigratesInOneTransactionAfterABackup(t *testing.T) {
	ctx := budget(t)
	config := toyConfig(t, true, "", toy(1))
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, s, "INSERT INTO toy_items(name) VALUES('one'),('two')")
	s.Close()

	steps := 0
	migrate := Migration{From: 1, Apply: func(ctx context.Context, tx *sql.Tx, _ []string) error {
		steps++
		if _, err := tx.ExecContext(ctx, "ALTER TABLE toy_items ADD COLUMN size INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "UPDATE toy_items SET size=length(name)")
		return err
	}}
	config.Create, config.Modules = false, []Module{toy(2, migrate)}
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if steps != 1 || versionOf(t, s, "toy") != 2 || countRows(t, s, "SELECT sum(size) FROM toy_items") != 6 {
		t.Fatalf("migration did not apply once: steps=%d", steps)
	}
	s.Close()
	backups, _ := filepath.Glob(config.Path + ".before-*")
	if len(backups) != 1 {
		t.Fatalf("expected one pre-migration backup, got %v", backups)
	}
	info, err := os.Stat(backups[0])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("backup must be private: %v %v", info, err)
	}
	// The backup is the old schema, restorable by copying the file back.
	old, err := sql.Open("sqlite", "file:"+backups[0]+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	var version, rows int
	if err = old.QueryRow("SELECT version FROM schema_versions WHERE module='toy'").Scan(&version); err != nil || version != 1 {
		t.Fatalf("backup is not the old schema: %d %v", version, err)
	}
	if err = old.QueryRow("SELECT count(*) FROM toy_items").Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("backup lost data: %d %v", rows, err)
	}
}

func TestFailedMigrationLeavesTheDatabaseUntouched(t *testing.T) {
	ctx := budget(t)
	config := toyConfig(t, true, "", toy(1))
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, s, "INSERT INTO toy_items(name) VALUES('one')")
	s.Close()
	broken := Migration{From: 1, Apply: func(ctx context.Context, tx *sql.Tx, _ []string) error {
		if _, err := tx.ExecContext(ctx, "DROP TABLE toy_items"); err != nil {
			return err
		}
		return errors.New("late failure")
	}}
	config.Create, config.Modules = false, []Module{toy(2, broken)}
	if _, err = Open(ctx, config); err == nil || !strings.Contains(err.Error(), "late failure") {
		t.Fatalf("broken migration: %v", err)
	}
	// The owner lock was released and the old schema still opens with its data.
	config.Modules = []Module{toy(1)}
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatalf("database damaged by a failed migration: %v", err)
	}
	defer s.Close()
	if countRows(t, s, "SELECT count(*) FROM toy_items") != 1 || versionOf(t, s, "toy") != 1 {
		t.Fatal("failed migration was partially applied")
	}
}

func TestNoMigrateRefusesAnOlderSchema(t *testing.T) {
	ctx := budget(t)
	config := toyConfig(t, true, "", toy(1))
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	config.Create, config.NoMigrate = false, true
	config.Modules = []Module{toy(2, Migration{From: 1, Apply: func(context.Context, *sql.Tx, []string) error { return nil }})}
	if _, err = Open(ctx, config); code(err) != "failed_precondition" || !strings.Contains(err.Error(), "older") {
		t.Fatalf("read-only administration migrated or failed unclearly: %v", err)
	}
	// An unchanged schema still opens for administration.
	config.Modules = []Module{toy(1)}
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
}

func TestAModuleAddedToAnExistingDatabaseIsInstalled(t *testing.T) {
	ctx := budget(t)
	plain := Config{Path: filepath.Join(t.TempDir(), "plain.db"), Create: true, Manifest: manifest()}
	os.Chmod(filepath.Dir(plain.Path), 0700)
	s, err := Open(ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	plain.Create, plain.Manifest, plain.Modules = false, toyManifest(), []Module{toy(1)}
	s, err = Open(ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	exec(t, s, "INSERT INTO toy_items(name) VALUES('x')")
	if versionOf(t, s, "toy") != 1 {
		t.Fatal("module version not recorded")
	}
	// A namespace naming a module this binary does not contain is refused.
	s.Close()
	plain.Modules = nil
	if _, err = Open(ctx, plain); code(err) != "failed_precondition" {
		t.Fatalf("missing compiled module: %v", err)
	}
}

func TestModuleMigrationsMustNotHaveGaps(t *testing.T) {
	skip := toy(3, Migration{From: 2, Apply: func(context.Context, *sql.Tx, []string) error { return nil }})
	if err := skip.validate(); err != nil {
		t.Fatalf("a module may drop support for older versions: %v", err)
	}
	gap := toy(4, Migration{From: 1, Apply: func(context.Context, *sql.Tx, []string) error { return nil }}, Migration{From: 3, Apply: func(context.Context, *sql.Tx, []string) error { return nil }})
	if err := gap.validate(); err == nil {
		t.Fatal("gap in the migration path accepted")
	}
	if _, err := skip.path(1); err == nil {
		t.Fatal("unsupported older version had a path")
	}
}

// firstSchemaDDL is the first engine schema, frozen here as the migration source.
const firstSchemaDDL = `
CREATE TABLE storage_meta(id INTEGER PRIMARY KEY CHECK(id=1),format TEXT NOT NULL,database_id TEXT NOT NULL,manifest_hash TEXT NOT NULL);
CREATE TABLE scopes(scope TEXT PRIMARY KEY,namespace TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0));
CREATE INDEX scopes_namespace ON scopes(namespace);
CREATE TABLE usage(scope TEXT NOT NULL,collection TEXT NOT NULL,records INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(scope,collection));
CREATE TABLE records(scope TEXT NOT NULL,collection TEXT NOT NULL,key TEXT NOT NULL,version INTEGER NOT NULL,deleted INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(scope,collection,key));
CREATE TABLE lookups(scope TEXT NOT NULL,collection TEXT NOT NULL,index_name TEXT NOT NULL,index_value TEXT NOT NULL,key TEXT NOT NULL,unique_value TEXT,PRIMARY KEY(scope,collection,index_name,index_value,key),UNIQUE(scope,collection,index_name,unique_value));
CREATE TABLE receipts(scope TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,expires INTEGER NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,request_id));
CREATE INDEX receipts_expiry ON receipts(expires);
CREATE TABLE named_results(scope TEXT NOT NULL,request_id TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope,request_id),FOREIGN KEY(scope,request_id) REFERENCES receipts(scope,request_id) ON DELETE CASCADE);
`

func TestFirstEngineSchemaMigratesInPlace(t *testing.T) {
	ctx := budget(t)
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	path := filepath.Join(dir, "first.db")
	scope := ScopeID(testScope)
	raw, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		firstSchemaDDL,
		"INSERT INTO storage_meta VALUES(1,'storage-v1','00112233445566778899aabbccddeeff','0000')",
		"INSERT INTO scopes VALUES('" + escapeSQL(scope) + "','test',9)",
		// A live record, a tombstone and an event record.
		"INSERT INTO records VALUES('" + escapeSQL(scope) + "','state','a',3,0,CAST('{\"name\":\"a\"}' AS BLOB))",
		"INSERT INTO records VALUES('" + escapeSQL(scope) + "','state','b',4,1,CAST('{}' AS BLOB))",
		"INSERT INTO records VALUES('" + escapeSQL(scope) + "','events','e1',5,0,CAST('{\"n\":1}' AS BLOB))",
		"INSERT INTO lookups VALUES('" + escapeSQL(scope) + "','state','unique','[\"a\"]','a','[\"a\"]')",
		// Version 1 counted every key ever written, tombstones included.
		"INSERT INTO usage VALUES('" + escapeSQL(scope) + "','state',7,999)",
		"INSERT INTO usage VALUES('" + escapeSQL(scope) + "','events',1,9)",
		// One document receipt and one Named receipt with its result.
		"INSERT INTO receipts VALUES('" + escapeSQL(scope) + "','doc','d1',4102444800,'{\"request_id\":\"doc\"}')",
		"INSERT INTO receipts VALUES('" + escapeSQL(scope) + "','named','d2',4102444800,'{\"request_id\":\"named\"}')",
		"INSERT INTO named_results VALUES('" + escapeSQL(scope) + "','named','{}')",
	} {
		if _, err = raw.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement[:min(60, len(statement))], err)
		}
	}
	raw.Close()
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}

	config := Config{Path: path, Manifest: manifest()}
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if backups, _ := filepath.Glob(path + ".before-engine-v1.*"); len(backups) != 1 {
		t.Fatalf("the upgrade took no backup: %v", backups)
	}
	stats, _ := s.Stats()
	if stats.DatabaseID != "00112233445566778899aabbccddeeff" || versionOf(t, s, "engine") != engineVersion {
		t.Fatalf("identity or version: %+v", stats)
	}
	for _, table := range []string{"named_results"} {
		if n := countRows(t, s, "SELECT count(*) FROM sqlite_master WHERE name=?", table); n != 0 {
			t.Fatalf("%s survived the migration", table)
		}
	}
	// Named receipts are gone, the document receipt and its counter remain.
	if countRows(t, s, "SELECT count(*) FROM receipts") != 1 || receiptCount(t, s) != 1 {
		t.Fatal("receipt cleanup or counter wrong")
	}
	if _, err = s.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "doc"}); err != nil {
		t.Fatalf("document receipt lost: %v", err)
	}
	// Usage now counts live data and keeps tombstones separately.
	records, bytes, tombstones := usageOf(t, s, "state")
	if records != 1 || tombstones != 1 || bytes != int64(len(`{"name":"a"}`)+1) {
		t.Fatalf("state usage was not recomputed: %d %d %d", records, bytes, tombstones)
	}
	// Data is readable under the existing token and the tombstone keeps its version.
	read, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Keys: []string{"a", "b"}, IncludeDeleted: false}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(read.Results[0].Records[0].Data) != `{"name":"a"}` || !read.Results[0].Records[1].Deleted || read.Results[0].Records[1].Version != "4" || read.Token.Revision != "9" {
		t.Fatalf("migrated documents: %+v", read)
	}
	// New writes work, and the lookups were rebuilt for the current manifest.
	if _, err = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "after", Mutations: []api.Mutation{mutation("state", "c", "0", `{"name":"c"}`)}}); err != nil {
		t.Fatal(err)
	}
	indexed, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{json.RawMessage(`"a"`)}}}})
	if err != nil || len(indexed.Results[0].Records) != 1 {
		t.Fatalf("lookup rows not usable after migration: %+v %v", indexed, err)
	}
	if err = s.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
}

func escapeSQL(v string) string { return strings.ReplaceAll(v, "'", "''") }

func TestChangedIndexesAreRebuiltFromTheRecords(t *testing.T) {
	ctx := budget(t)
	m := manifest()
	m.Namespaces[0].Collections[1].Indexes = nil
	config := Config{Path: filepath.Join(t.TempDir(), "idx.db"), Create: true, Manifest: m}
	os.Chmod(filepath.Dir(config.Path), 0700)
	s, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	read, _ := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Limit: 1}}})
	if _, err = s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, Mutations: []api.Mutation{mutation("events", "1", "0", `{"kind":"a","n":1}`), mutation("events", "2", "0", `{"kind":"b","n":2}`), mutation("events", "3", "0", `{"kind":"a","n":3}`)}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// A later release declares an index on a collection that has data.
	m.Namespaces[0].Collections[1].Indexes = []api.Index{{ID: "kind", Fields: []string{"kind"}}}
	config.Create, config.Manifest = false, m
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	found, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Index: "kind", Equal: []json.RawMessage{json.RawMessage(`"a"`)}}}})
	if err != nil || len(found.Results[0].Records) != 2 {
		t.Fatalf("new index not built: %+v %v", found, err)
	}
	s.Close()
	// A unique index the stored data violates fails the open and changes nothing.
	m.Namespaces[0].Collections[1].Indexes = []api.Index{{ID: "kind", Fields: []string{"kind"}, Unique: true}}
	config.Manifest = m
	if _, err = Open(ctx, config); err == nil || !strings.Contains(err.Error(), "events") {
		t.Fatalf("violated unique index admitted: %v", err)
	}
	m.Namespaces[0].Collections[1].Indexes = []api.Index{{ID: "kind", Fields: []string{"kind"}}}
	config.Manifest = m
	s, err = Open(ctx, config)
	if err != nil {
		t.Fatalf("failed rebuild damaged the database: %v", err)
	}
	found, err = s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "events", Index: "kind", Equal: []json.RawMessage{json.RawMessage(`"a"`)}}}})
	s.Close()
	if err != nil || len(found.Results[0].Records) != 2 {
		t.Fatalf("index lost after the failed attempt: %+v %v", found, err)
	}
}

func TestRetireDropsACollectionAndItsQuota(t *testing.T) {
	s, _, ctx := setup(t)
	read := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"a"}`), mutation("events", "e", "0", `{"n":1}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error {
		return Retire(ctx, tx, "test", "events")
	}); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM records WHERE collection='events'") != 0 || countRows(t, s, "SELECT count(*) FROM usage WHERE collection='events'") != 0 {
		t.Fatal("retired collection left rows")
	}
	if countRows(t, s, "SELECT count(*) FROM records WHERE collection='state'") != 1 || countRows(t, s, "SELECT count(*) FROM lookups WHERE collection='state'") != 1 {
		t.Fatal("retire touched another collection")
	}
	if err := s.Write(ctx, Durable, func(ctx context.Context, tx *sql.Tx) error { return Retire(ctx, tx, "test") }); err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "SELECT count(*) FROM records")+countRows(t, s, "SELECT count(*) FROM scopes") != 0 {
		t.Fatal("retiring a namespace left rows")
	}
}
