package coredata

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestChoiceProjectionReadsOnlySelectedFacts(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "choices.db") + "?_pragma=foreign_keys(1)", rec: rec})
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
	q := configurationCreateForTest(t, domain, "chosen", nil)
	q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/chosen"}`)
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	read := func() (int, string) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		choices, err := configurationChoicePage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 10}, "scene", true)
		tx.Rollback()
		if err != nil || len(choices) != 1 || choices[0].ResourceID != q.ResourceID || choices[0].CommitID != q.CommitID || len(rec.statements) != 2 {
			t.Fatalf("choices=%+v SQL calls=%d err=%v", choices, len(rec.statements), err)
		}
		query := strings.ToLower(rec.statements[1])
		for _, forbidden := range []string{"r.origin_resource_id", "r.origin_commit_id", "s.source_commit_id", "$.origin_resource_id", "$.origin_commit_id", "$.source_commit_id", "$.created_from_commit_id", "s.payload", "s.manifest", "core_references"} {
			if strings.Contains(query, forbidden) {
				t.Fatalf("choice query materializes %s", forbidden)
			}
		}
		return rec.bytes, rec.statements[1]
	}
	before, statement := read()
	// The reusable scanner reports actual selected row bytes at the SQLite
	// driver boundary, independently of the domain declaration read.
	rec.statements, rec.bytes = nil, 0
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, statement, testScope, domain.Key, "scene", 10, 0)
	if err != nil || !rows.Next() {
		t.Fatalf("choice scanner query: %v", err)
	}
	item, selectedBytes, err := scanConfigurationCatalogChoice(rows, "scene")
	if err != nil || item.ResourceID != q.ResourceID || selectedBytes != rec.bytes {
		t.Fatalf("choice scanner bytes=%d driver bytes=%d item=%+v err=%v", selectedBytes, rec.bytes, item, err)
	}
	rows.Close()
	tx.Rollback()
	rec.statements, rec.bytes = nil, 0
	tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	headers, err := configurationHeaderPage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 10}, "scene", true)
	tx.Rollback()
	if err != nil || len(headers) != 1 {
		t.Fatalf("full header=%+v err=%v", headers, err)
	}
	headerBytes := rec.bytes
	if before >= headerBytes {
		t.Fatalf("choice driver bytes %d did not reduce full header bytes %d", before, headerBytes)
	}
	for i := 0; i < 16; i++ {
		other := configurationCreateForTest(t, domain, fmt.Sprintf("other-%02d", i), nil)
		other.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"camera:other"}`)
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, other)
	}
	after, _ := read()
	if before != after {
		t.Fatalf("unrelated choices changed driver bytes: %d/%d", before, after)
	}
	rows, err = db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+statement, testScope, domain.Key, "scene", 10, 0)
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "core_resources_catalog_selector_ordinary (") || strings.Contains(plan.String(), "SCAN r") || strings.Contains(plan.String(), "USE TEMP B-TREE") {
		t.Fatalf("choice plan is not the exact ordinary selector index: %s", plan.String())
	}
	t.Logf("same selected row: full header/choice driver bytes %d/%d; choice bytes with 1/17 total resources %d/%d; query excludes lineage and frozen bodies", headerBytes, before, before, after)
}

func TestChoiceProjectionRejectsDamagedCurrentFacts(t *testing.T) {
	for _, damage := range []string{
		`UPDATE core_resources SET name='different' WHERE id='chosen'`,
		`UPDATE core_resources SET main_commit_id='missing' WHERE id='chosen'`,
		`UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.archived_at','2026-01-01T00:00:00Z') WHERE id='chosen'`,
		`UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.system',0) WHERE id='chosen'`,
		`UPDATE core_snapshots SET body=json_set(CAST(body AS TEXT),'$.version',1) WHERE id='chosen-v1'`,
		`UPDATE core_snapshots SET catalog_summary_json='{"_catalogSelector":"other"}' WHERE id='chosen-v1'`,
		`UPDATE core_snapshots SET identity_json='{"active":false}' WHERE id='chosen-v1'`,
	} {
		t.Run(damage, func(t *testing.T) {
			db, ctx, domain := configurationFixture(t)
			q := configurationCreateForTest(t, domain, "chosen", nil)
			q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene"}`)
			configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
			execSQL(t, db, ctx, damage)
			tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := configurationChoicePage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 1}, "scene", true)
			tx.Rollback()
			configurationCode(t, err, "data_loss")
			if rows != nil {
				t.Fatal("damaged choice returned a partial result")
			}
		})
	}
	t.Run("oversized current metadata", func(t *testing.T) {
		db, ctx, domain := configurationFixture(t)
		q := configurationCreateForTest(t, domain, "chosen", nil)
		q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene"}`)
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
		execSQL(t, db, ctx, `UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.padding',?) WHERE id='chosen'`, strings.Repeat("x", 262145))
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationChoicePage(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 1}, "scene", true)
		tx.Rollback()
		configurationCode(t, err, "data_loss")
		if rows != nil {
			t.Fatal("oversized metadata returned a partial result")
		}
	})
}
