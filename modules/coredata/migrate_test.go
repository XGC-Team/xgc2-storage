package coredata_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// testdata/production/storage.db was written by the previous release of this
// module (commit 93cf0e8): two configuration domains, folders, resources
// with a second commit, a branch, a tracking reference, an archived and a
// protected resource, Core credentials/audit documents (two audit rows
// deleted), workflow-engine documents in 25 collections, a command with its
// events, a sealed child-run group, panel state, Lichtblick documents and an
// Agent-runtime conversation, with Batch and Named receipts.

var fixtureScope = api.Scope{Namespace: "core", User: "operator", Workspace: "station"}

func fixtureCollection(id string, indexes ...api.Index) api.Collection {
	return api.Collection{ID: id, MaxRecordBytes: 1 << 20, MaxRecords: 10000, MaxBytes: 64 << 20, Indexes: indexes, Retention: "fixture", Recovery: "fixture"}
}

// keptManifest declares what Core keeps: no workflow collection, no panel state.
func keptManifest() api.Manifest {
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{
		{ID: "core", Owner: "xgc2-core", Schema: model.Schema, MaxScopes: 8, MaxReceipts: 10000, ReceiptTTLSeconds: 604800, Modules: []string{model.Module},
			Collections: []api.Collection{fixtureCollection("access_control"), fixtureCollection("access_sessions", api.Index{ID: "token", Fields: []string{"tokenHash"}, Unique: true}),
				fixtureCollection("terminal_hosts"), fixtureCollection("operator_audit", api.Index{ID: "actor", Fields: []string{"actor"}}),
				fixtureCollection("agent-links", api.Index{ID: "state", Fields: []string{"state"}}), fixtureCollection("core_nodes"), fixtureCollection("core_heartbeats"), fixtureCollection("agent_decision_policies")}},
		{ID: "lichtblick", Owner: "xgc2-lichtblick", Schema: "lichtblick.persistence.v1", MaxScopes: 8, MaxReceipts: 1000, ReceiptTTLSeconds: 604800,
			Collections: []api.Collection{fixtureCollection("documents", api.Index{ID: "family", Fields: []string{"family"}}), fixtureCollection("extensions")}},
		{ID: "agent-runtime", Owner: "agent-runtime", Schema: "agent-runtime.v1", MaxScopes: 8, MaxReceipts: 1000, ReceiptTTLSeconds: 604800,
			Collections: []api.Collection{fixtureCollection("sessions"), fixtureCollection("events"), fixtureCollection("prompts"), fixtureCollection("settings")}},
	}}
}

func copyFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	in, err := os.Open("testdata/production/storage.db")
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	path := filepath.Join(dir, "storage.db")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err = out.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// digest hashes every row of a query in order.
func digest(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	h := sha256.New()
	n := 0
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%q\n", values)
		n++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%d:%s", n, hex.EncodeToString(h.Sum(nil)))
}

var configTables = []string{"core_namespaces", "core_resources", "core_branches", "core_snapshots", "core_references", "core_changes", "core_configuration_domains", "core_configuration_receipts", "core_catalog_receipts"}

const keptDocuments = "SELECT scope,collection,key,version,deleted,data FROM records WHERE collection IN ('access_control','access_sessions','terminal_hosts','operator_audit','agent-links','documents','extensions','prompts','settings','events') OR (collection='sessions' AND scope LIKE '%agent-runtime%') ORDER BY scope,collection,key"

func rawView(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestProductionDatabaseMigratesKeepingConfigurationAndDroppingExecutionData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	path := copyFixture(t)
	before := rawView(t, path)
	want := map[string]string{}
	for _, table := range configTables {
		want[table] = digest(t, before, "SELECT * FROM "+table+" ORDER BY rowid")
	}
	keptBefore := digest(t, before, keptDocuments)
	var databaseID string
	if err := before.QueryRow("SELECT database_id FROM storage_meta").Scan(&databaseID); err != nil {
		t.Fatal(err)
	}
	var retiredBefore int
	before.QueryRow("SELECT count(*) FROM records WHERE collection IN ('runs','invocations','trigger_events','job_runs','process_instances','run-configurations','mcp_connections') OR scope LIKE '%core-panel-state%'").Scan(&retiredBefore)
	if retiredBefore < 10 {
		t.Fatalf("fixture lost its execution data: %d", retiredBefore)
	}
	before.Close()

	// A read-only administration open refuses to migrate and changes nothing.
	config := engine.Config{Path: path, Manifest: keptManifest(), Modules: []engine.Module{coredata.Module()}, MaxDBBytes: 256 << 20, NoMigrate: true}
	if _, err := engine.Open(ctx, config); err == nil || !strings.Contains(err.Error(), "older") {
		t.Fatalf("administration must not migrate: %v", err)
	}
	config.NoMigrate = false
	db, err := engine.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stats, _ := db.Stats()
	if stats.DatabaseID != databaseID {
		t.Fatal("migration changed the database identity")
	}
	backups, _ := filepath.Glob(path + ".before-engine-v1.*")
	if len(backups) != 1 {
		t.Fatalf("expected one pre-migration backup: %v", backups)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	after := rawView(t, path)
	for _, table := range configTables {
		if got := digest(t, after, "SELECT * FROM "+table+" ORDER BY rowid"); got != want[table] {
			t.Errorf("configuration table %s changed: %s != %s", table, got, want[table])
		}
	}
	if got := digest(t, after, keptDocuments); got != keptBefore {
		t.Errorf("kept documents changed")
	}
	var retiredAfter int
	after.QueryRow("SELECT count(*) FROM records WHERE collection IN ('runs','invocations','trigger_events','job_runs','process_instances','run-configurations','workflow-bundles','mcp_connections','definitions','sessions') AND scope LIKE '%\"core\"%'").Scan(&retiredAfter)
	var panelScopes int
	after.QueryRow("SELECT count(*) FROM scopes WHERE namespace='core-panel-state'").Scan(&panelScopes)
	if retiredAfter != 0 || panelScopes != 0 {
		t.Errorf("execution data survived: %d records, %d panel scopes", retiredAfter, panelScopes)
	}
	for _, name := range []string{"core_groups", "core_group_members", "core_commands", "core_execution_events", "core_event_offsets", "core_event_sequences", "core_execution_identity", "core_group_events", "core_child_links", "named_results"} {
		var n int
		after.QueryRow("SELECT count(*) FROM sqlite_master WHERE name=?", name).Scan(&n)
		if n != 0 {
			t.Errorf("%s survived the migration", name)
		}
	}
	for _, table := range []string{"runs", "sessions", "recordings"} {
		var n int
		if err := after.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Errorf("%s: %d %v", table, n, err)
		}
	}
	var engineVersion, moduleVersion int
	after.QueryRow("SELECT version FROM schema_versions WHERE module='engine'").Scan(&engineVersion)
	after.QueryRow("SELECT version FROM schema_versions WHERE module='coredata'").Scan(&moduleVersion)
	if engineVersion != 2 || moduleVersion != coredata.SchemaVersion {
		t.Errorf("versions: engine=%d coredata=%d", engineVersion, moduleVersion)
	}
	// The live total is what the remaining rows occupy, not what version 1 had charged.
	var rows, bytes, wantRows, wantBytes int64
	after.QueryRow("SELECT rows,bytes FROM core_data_usage").Scan(&rows, &bytes)
	after.QueryRow(`SELECT (SELECT count(*) FROM core_namespaces)+(SELECT count(*) FROM core_resources)+(SELECT count(*) FROM core_branches)+(SELECT count(*) FROM core_snapshots)+(SELECT count(*) FROM core_references)+(SELECT count(*) FROM core_changes)+(SELECT count(*) FROM core_configuration_receipts)+(SELECT count(*) FROM core_catalog_receipts)`).Scan(&wantRows)
	after.QueryRow(`SELECT (SELECT sum(length(body)) FROM core_namespaces)+(SELECT sum(length(body)) FROM core_resources)+(SELECT sum(length(body)) FROM core_branches)+(SELECT sum(length(body)+length(payload)+length(manifest)) FROM core_snapshots)+(SELECT sum(length(body)) FROM core_references)+(SELECT sum(length(body)) FROM core_changes)+(SELECT sum(length(body)) FROM core_configuration_receipts)+(SELECT sum(length(body)) FROM core_catalog_receipts)`).Scan(&wantBytes)
	if rows != wantRows || bytes != wantBytes || rows == 0 {
		t.Errorf("core_data_usage %d/%d, want %d/%d", rows, bytes, wantRows, wantBytes)
	}
	after.Close()

	// Reopening is an ordinary open: nothing is migrated or backed up again.
	db, err = engine.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if again, _ := filepath.Glob(path + ".before-*"); len(again) != 1 {
		t.Fatalf("a second open took another backup: %v", again)
	}
	core, err := coredata.New(db, fixtureScope)
	if err != nil {
		t.Fatal(err)
	}
	automation := model.ConfigurationDomainGuard{Key: "automation", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	robot := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "catalog-v1", SchemaVersion: 2, RegistryDigest: strings.Repeat("b", 64)}
	if err = coredata.DeclareConfigurationDomains(ctx, db, []model.ConfigurationDomainDeclaration{
		{Key: "automation", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: automation.RegistryDigest, MainVisibility: true, AllowSystemProvisioning: true, Capabilities: []string{"read", "write"}},
		{Key: "robot", SchemaIdentity: "catalog-v1", SchemaVersion: 2, RegistryDigest: robot.RegistryDigest, Capabilities: []string{"read"}},
	}); err != nil {
		t.Fatalf("the stored domain catalog rejects Core's declaration: %v", err)
	}

	// The configuration model works on the migrated data.
	main, err := core.ReadResource(ctx, model.ConfigurationResourceRead{Domain: automation, ResourceID: "res-one", Branch: "main"})
	if err != nil || main.Head.Commit.Version != "2" || main.Head.Commit.ID != "res-one-edit-commit" || main.Head.Resource.NamespaceID != "folder-b" {
		t.Fatalf("main head: %+v %v", main.Head, err)
	}
	first, err := core.ReadResource(ctx, model.ConfigurationResourceRead{Domain: automation, ResourceID: "res-one", CommitID: "res-one-v1"})
	if err != nil || first.Head.Commit.Version != "1" {
		t.Fatalf("immutable first commit: %+v %v", first.Head, err)
	}
	commits, err := core.Commits(ctx, model.ConfigurationCatalogRead{Domain: automation, ID: "res-one"})
	if err != nil || len(commits) != 2 {
		t.Fatalf("commit history: %v %v", commits, err)
	}
	branches, err := core.Branches(ctx, model.ConfigurationCatalogRead{Domain: automation, ID: "res-one"})
	if err != nil || len(branches) != 2 {
		t.Fatalf("branches: %v %v", branches, err)
	}
	incoming, err := core.IncomingReferences(ctx, model.ConfigurationIncomingRead{Domain: automation, ResourceID: "res-one"})
	if err != nil || len(incoming) != 1 || incoming[0].SourceResourceID != "res-two" {
		t.Fatalf("incoming references: %v %v", incoming, err)
	}
	archived, err := core.ReadResource(ctx, model.ConfigurationResourceRead{Domain: robot, ResourceID: "robot-one", Branch: "main", IncludeArchived: true})
	if err != nil || archived.Head.Resource.ArchivedAt == "" {
		t.Fatalf("archived resource: %+v %v", archived.Head.Resource, err)
	}
	protected, err := core.ReadResource(ctx, model.ConfigurationResourceRead{Domain: automation, ResourceID: "res-three", Branch: "main"})
	if err != nil || !protected.Head.Resource.System {
		t.Fatalf("protected resource: %+v %v", protected.Head.Resource, err)
	}
	// A product mutation made before the migration still replays.
	replay, err := core.Receipt(ctx, model.ConfigurationReceipt{Domain: automation, Key: "create-res-one", IntentDigest: strings.Repeat("a", 64), Operations: []string{model.ResourceCreateOperation}})
	if err != nil || !replay.Found || !replay.Replayed {
		t.Fatalf("pre-migration product receipt: %+v %v", replay, err)
	}
	// New data lands in the new tables and counts against the live quota.
	run, created, err := core.CreateRun(ctx, model.NewRun{ID: "run-after", TargetID: "local", WorkflowResourceID: "res-one", WorkflowCommitID: main.Head.Commit.ID, DefinitionDigest: main.Head.Commit.ContentDigest, ActionID: "run", IdempotencyKey: "key", Inputs: []byte(`{"a":1}`), Trigger: []byte(`{"kind":"manual"}`)})
	if err != nil || !created || run.Status != model.RunQueued {
		t.Fatalf("run after migration: %+v %v", run, err)
	}

	// Documents survive with their versions, tombstones and rebuilt indexes.
	lichtblick := api.Scope{Namespace: "lichtblick", User: "operator", Workspace: "station"}
	layouts, err := db.Snapshot(ctx, api.SnapshotRequest{Scope: lichtblick, Queries: []api.Query{{Collection: "documents", Index: "family", Equal: []jsonRaw{jsonRaw(`"layouts"`)}}, {Collection: "documents", Keys: []string{"layouts:local/old"}}}})
	if err != nil || len(layouts.Results[0].Records) != 1 || !layouts.Results[1].Records[0].Deleted || layouts.Results[1].Records[0].Version != "2" {
		t.Fatalf("lichtblick documents: %+v %v", layouts, err)
	}
	if _, err = db.Receipt(ctx, api.ReceiptRequest{Scope: lichtblick, RequestID: "lb-1"}); err != nil {
		t.Fatalf("document receipt lost: %v", err)
	}
	conversation, err := db.Snapshot(ctx, api.SnapshotRequest{Scope: api.Scope{Namespace: "agent-runtime", User: "operator", Workspace: "station"}, Queries: []api.Query{{Collection: "sessions", Keys: []string{"conversation-1"}}}})
	if err != nil || conversation.Results[0].Records[0].Missing {
		t.Fatalf("a same-named collection of another namespace was retired: %+v %v", conversation, err)
	}
	credentials, err := db.Snapshot(ctx, api.SnapshotRequest{Scope: fixtureScope, Queries: []api.Query{{Collection: "access_sessions", Keys: []string{"s1"}}, {Collection: "operator_audit", Index: "actor", Equal: []jsonRaw{jsonRaw(`"alice"`)}}}})
	if err != nil || credentials.Results[0].Records[0].Missing || len(credentials.Results[1].Records) != 4 {
		t.Fatalf("core documents: %+v %v", credentials, err)
	}
	read, _ := db.Snapshot(ctx, api.SnapshotRequest{Scope: fixtureScope, Queries: []api.Query{{Collection: "access_sessions", Keys: []string{"s3"}}}})
	_, err = db.Batch(ctx, api.BatchRequest{Scope: fixtureScope, Expected: read.Token, Mutations: []api.Mutation{{Collection: "access_sessions", Key: "s3", ExpectedVersion: "0", Data: []byte(`{"tokenHash":"hash-1"}`)}}})
	if code(err) != "conflict" {
		t.Fatalf("the rebuilt unique index lets a duplicate in: %v", err)
	}
	if err = db.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
}

type jsonRaw = json.RawMessage

func code(err error) string { return errCode(err) }
