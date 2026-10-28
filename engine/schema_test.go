package engine

import (
	"bytes"
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
func toy(version int) Module {
	return Module{ID: "toy", Version: version, Install: func(ctx context.Context, tx *sql.Tx) error {
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

func TestNoncurrentModuleVersionIsRefusedWithoutChangingFacts(t *testing.T) {
	for _, mode := range []string{"missing", "older", "newer"} {
		t.Run(mode, func(t *testing.T) {
			ctx := budget(t)
			config := toyConfig(t, true, "", toy(2))
			s, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			exec(t, s, "INSERT INTO toy_items(name) VALUES('kept')")
			switch mode {
			case "missing":
				exec(t, s, "DELETE FROM schema_versions WHERE module='toy'")
			case "older":
				exec(t, s, "UPDATE schema_versions SET version=1 WHERE module='toy'")
			case "newer":
				exec(t, s, "UPDATE schema_versions SET version=3 WHERE module='toy'")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(config.Path)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			module := toy(2)
			module.Install = func(context.Context, *sql.Tx) error { called = true; return errors.New("installer called on reopen") }
			config.Create, config.Modules = false, []Module{module}
			opened, err := Open(ctx, config)
			if opened != nil {
				opened.Close()
				t.Fatal("noncurrent module admitted")
			}
			if code(err) != "failed_precondition" || !strings.Contains(err.Error(), "toy") || called {
				t.Fatalf("%s admission=%v installer=%v", mode, err, called)
			}
			after, err := os.ReadFile(config.Path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("%s refusal changed accepted facts: %v", mode, err)
			}
			if backups, _ := filepath.Glob(config.Path + ".before-*"); len(backups) != 0 {
				t.Fatalf("%s refusal created backup: %v", mode, backups)
			}
		})
	}
}

func TestAddingAModuleOnReopenRequiresExplicitCurrentCreation(t *testing.T) {
	ctx := budget(t)
	plain := Config{Path: filepath.Join(t.TempDir(), "plain.db"), Create: true, Manifest: manifest()}
	os.Chmod(filepath.Dir(plain.Path), 0700)
	s, err := Open(ctx, plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(plain.Path)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	module := toy(1)
	module.Install = func(context.Context, *sql.Tx) error { called = true; return errors.New("installer called on reopen") }
	plain.Create, plain.Manifest, plain.Modules = false, toyManifest(), []Module{module}
	opened, err := Open(ctx, plain)
	if opened != nil {
		opened.Close()
		t.Fatal("missing module marker was admitted")
	}
	if code(err) != "failed_precondition" || called {
		t.Fatalf("missing module admission=%v installer=%v", err, called)
	}
	after, err := os.ReadFile(plain.Path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused module changed database: %v", err)
	}
	if backups, _ := filepath.Glob(plain.Path + ".before-*"); len(backups) != 0 {
		t.Fatalf("refusal created backup: %v", backups)
	}
	// A namespace cannot name a module absent from the compiled owner.
	plain.Modules = nil
	opened, err = Open(ctx, plain)
	if opened != nil {
		opened.Close()
		t.Fatal("missing compiled module was admitted")
	}
	if code(err) != "failed_precondition" || !strings.Contains(err.Error(), "not compiled") {
		t.Fatalf("missing compiled module: %v", err)
	}
}

// This old engine schema is an isolated refusal fixture, never a decoder or migration source.
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

func TestFirstEngineSchemaIsRefusedWithoutMutation(t *testing.T) {
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
	if opened, err := Open(ctx, config); code(err) != "failed_precondition" || opened != nil {
		t.Fatalf("retired engine schema was not refused: %v", err)
	}
	if backups, _ := filepath.Glob(path + ".before-*"); len(backups) != 0 {
		t.Fatalf("engine refusal started implicit migration work: %v", backups)
	}
	raw, err = sql.Open("sqlite", "file:"+path+"?_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var data string
	if err := raw.QueryRowContext(ctx, "SELECT data FROM records WHERE collection='state' AND key='a'").Scan(&data); err != nil || data != `{"name":"a"}` {
		t.Fatalf("engine refusal changed the accepted record: %q %v", data, err)
	}
	var named int
	if err := raw.QueryRowContext(ctx, "SELECT count(*) FROM named_results").Scan(&named); err != nil || named != 1 {
		t.Fatalf("engine refusal changed historic tables: %d %v", named, err)
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
