package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSelectedCatalogTracksCurrentMainAndRejectsDamagedProjection(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	create := configurationCreateForTest(t, domain, "camera", nil)
	create.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"camera:left"}`)
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, create)
	read := func(key string, archived, ordinary bool) []model.ConfigurationResourceHeader {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		rows, err := configurationHeaderPage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, IncludeArchived: archived, Limit: 10}, key, ordinary)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	if rows := read("camera:left", false, false); len(rows) != 1 || rows[0].Head.Commit.ID != create.CommitID {
		t.Fatalf("created=%+v", rows)
	}
	commit := configurationCommitForTest(t, domain, created, "camera-change", "b", true, nil, create.Snapshot.Manifest)
	commit.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"camera:right"}`)
	changed := configurationWrite(t, db, ctx, model.ResourceCommitOperation, commit)
	if len(read("camera:left", false, false)) != 0 {
		t.Fatal("previous main selector still returned")
	}
	if rows := read("camera:right", false, false); len(rows) != 1 || rows[0].Head.Commit.ID != commit.CommitID {
		t.Fatalf("changed=%+v", rows)
	}
	guard := model.ConfigurationBranchGuard{ID: changed.Result.Head.Branch.ID, ExpectedRevision: changed.Result.Head.Branch.Revision, CommitID: changed.Result.Head.Commit.ID, ContentDigest: changed.Result.Head.Commit.ContentDigest}
	state := model.ConfigurationResourceState{Domain: domain, Mutation: model.ConfigurationMutation{Key: "selected-archive", IntentDigest: strings.Repeat("c", 64), Actor: "tester"}, ResourceID: "camera", ExpectedRevision: changed.Result.Head.Resource.Revision, Main: guard, Archived: true}
	archived := configurationWrite(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	if len(read("camera:right", false, false)) != 0 || len(read("camera:right", true, false)) != 1 {
		t.Fatal("archive selection disagrees")
	}
	state.Mutation.Key = "selected-restore"
	state.Mutation.IntentDigest = strings.Repeat("d", 64)
	state.ExpectedRevision = archived.Result.Head.Resource.Revision
	state.Archived = false
	configurationWrite(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	if len(read("camera:right", false, false)) != 1 {
		t.Fatal("restore lost current selector")
	}
	// Damage only the resource metadata key: the selected row is still found,
	// but its current immutable summary must independently agree.
	execSQL(t, db, ctx, `UPDATE core_snapshots SET catalog_summary_json=? WHERE id=?`, []byte(`{"_catalogSelector":"camera:other"}`), commit.CommitID)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := configurationHeaderPage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 1}, "camera:right", false)
	tx.Rollback()
	configurationCode(t, err, "data_loss")
	if rows != nil {
		t.Fatal("damaged page returned a prefix")
	}
}

func TestSelectedCatalogUsesIndexAndFixedBytesWithUnrelatedResourcesAndHistory(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "selected.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err == nil {
		err = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: domain.RegistryDigest, MainVisibility: true}})
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	create := configurationCreateForTest(t, domain, "chosen", nil)
	create.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"camera:chosen"}`)
	create.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 768<<10) + `"}}`)
	current := configurationWrite(t, db, ctx, model.ResourceCreateOperation, create)
	previous := create.Snapshot.Manifest
	for i := 0; i < 64; i++ {
		tag := string("0123456789abcdef"[i%16])
		q := configurationCommitForTest(t, domain, current, fmt.Sprintf("chosen-%d", i), tag, true, nil, previous)
		q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"camera:chosen"}`)
		current = configurationWrite(t, db, ctx, model.ResourceCommitOperation, q)
		previous = q.Snapshot.Manifest
	}
	read := func() (int, string) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationHeaderPage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 8}, "camera:chosen", false)
		tx.Rollback()
		if err != nil || len(rows) != 1 || rows[0].Head.Commit.ID != current.Result.Head.Commit.ID || len(rec.statements) != 2 {
			t.Fatalf("selected rows=%+v statements=%d err=%v", rows, len(rec.statements), err)
		}
		query := rec.statements[1]
		for _, forbidden := range []string{"s.payload", "s.manifest", "core_references"} {
			if strings.Contains(strings.ToLower(query), forbidden) {
				t.Fatalf("selected query read %s", forbidden)
			}
		}
		return rec.bytes, query
	}
	baseline, query := read()
	for i := 0; i < 256; i++ {
		q := configurationCreateForTest(t, domain, fmt.Sprintf("other-%03d", i), nil)
		q.Snapshot.CatalogSummary, _ = json.Marshal(map[string]string{"_catalogSelector": fmt.Sprintf("camera:other-%03d", i)})
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	got, _ := read()
	if got != baseline {
		t.Fatalf("unrelated resources changed driver bytes: %d vs %d", baseline, got)
	}
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, testScope, domain.Key, "camera:chosen", 8, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "; ")
	}
	if !strings.Contains(plan.String(), "core_resources_catalog_selector_live (") || strings.Contains(plan.String(), "SCAN r") || strings.Contains(plan.String(), "USE TEMP B-TREE") {
		t.Fatalf("selected plan is not indexed: %s", plan.String())
	}
}

func TestSelectedCatalogEachScopeUsesTheMatchingIndex(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "plans.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err == nil {
		err = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: domain.RegistryDigest}})
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	root, namespace := "", "specific"
	for _, tc := range []struct {
		name               string
		parent             *string
		archived, ordinary bool
		index              string
	}{
		{"all-live", nil, false, false, "core_resources_catalog_selector_live"},
		{"all-with-archived", nil, true, false, "core_resources_catalog_selector"},
		{"root-live", &root, false, false, "core_resources_catalog_selector_namespace_live"},
		{"namespace-with-archived", &namespace, true, false, "core_resources_catalog_selector_namespace"},
		{"ordinary-all", nil, false, true, "core_resources_catalog_selector_ordinary"},
		{"ordinary-root", &root, false, true, "core_resources_catalog_selector_namespace_ordinary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := model.ConfigurationCatalogRead{Domain: domain, ParentID: tc.parent, IncludeArchived: tc.archived, Limit: 4}
			_, args, _, err := configurationResourceCatalogSelector(testScope, q)
			if err != nil {
				t.Fatal(err)
			}
			tail := append([]any(nil), args[len(args)-2:]...)
			args = append(args[:len(args)-2], "scene")
			args = append(args, tail...)
			rec.statements = nil
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			_, err = configurationHeaderPage(ctx, tx, testScope, q, "scene", tc.ordinary)
			tx.Rollback()
			if err != nil {
				t.Fatal(err)
			}
			query := rec.statements[len(rec.statements)-1]
			rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan strings.Builder
			for rows.Next() {
				var a, b, c int
				var detail string
				if err := rows.Scan(&a, &b, &c, &detail); err != nil {
					t.Fatal(err)
				}
				plan.WriteString(detail + "; ")
			}
			if !strings.Contains(plan.String(), tc.index+" (") || strings.Contains(plan.String(), "SCAN r") || strings.Contains(plan.String(), "USE TEMP B-TREE") {
				t.Fatalf("wrong plan: %s", plan.String())
			}
		})
	}
}

func TestSelectedCatalogOrdinaryExcludesSystemCloneAndArchive(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	for _, id := range []string{"ordinary", "system", "clone", "archived"} {
		q := configurationCreateForTest(t, domain, id, nil)
		q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/world"}`)
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	// Fixture-only metadata changes model valid current states; the actual
	// owner updates these columns and index expressions atomically.
	execSQL(t, db, ctx, `UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.system',json('true')) WHERE id='system'`)
	execSQL(t, db, ctx, `UPDATE core_resources SET origin_resource_id='source',origin_commit_id='source-v1',body=json_set(CAST(body AS TEXT),'$.origin_resource_id','source','$.origin_commit_id','source-v1') WHERE id='clone'`)
	execSQL(t, db, ctx, `UPDATE core_resources SET archived=1,body=json_set(CAST(body AS TEXT),'$.archived_at','2026-01-01T00:00:00Z') WHERE id='archived'`)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := configurationHeaderPage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 10}, "scene", true)
	tx.Rollback()
	if err != nil || len(rows) != 1 || rows[0].Head.Resource.ID != "ordinary" {
		t.Fatalf("ordinary rows=%+v err=%v", rows, err)
	}
}

func TestSelectedCatalogRejectsInvalidProjectionAtomically(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	for i, bad := range []string{`{"_catalogSelector":42}`, `{"_catalogSelector":null}`, `{"_catalogSelector":" bad "}`, `{"_catalogSelector":"` + strings.Repeat("x", 256) + `"}`} {
		q := configurationCreateForTest(t, domain, fmt.Sprintf("bad-%d", i), nil)
		q.Snapshot.CatalogSummary = []byte(bad)
		before := configurationCounts(t, db, ctx)
		_, err := run(t, db, ctx, model.ResourceCreateOperation, q)
		configurationCode(t, err, "invalid_argument")
		if after := configurationCounts(t, db, ctx); after != before {
			t.Fatalf("invalid projection changed owner state: %s -> %s", before, after)
		}
	}
}
