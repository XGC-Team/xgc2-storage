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

func TestCommitCatalogPagesAndDoesNotReadFrozenBodies(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "history.db") + "?_pragma=foreign_keys(1)", rec: rec})
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
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	create := configurationCreateForTest(t, domain, "history", nil)
	create.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"large":"` + strings.Repeat("x", 768<<10) + `"}}`)
	previous := configurationWrite(t, db, ctx, model.ResourceCreateOperation, create)
	for i := 0; i < 4; i++ {
		commit := configurationCommitForTest(t, domain, previous, fmt.Sprintf("revision-%d", i), "a", true, nil, create.Snapshot.Manifest)
		commit.AppendUnchangedSnapshot = true
		commit.Snapshot.Payload = create.Snapshot.Payload
		previous = configurationWrite(t, db, ctx, model.ResourceCommitOperation, commit)
	}
	read := func(limit, offset int) (model.ConfigurationCommitCatalog, error) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		out, err := configurationCommitCatalog(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, ID: "history", Limit: limit, Offset: offset})
		tx.Rollback()
		for _, statement := range rec.statements {
			query := strings.ToLower(statement)
			for _, forbidden := range []string{"s.payload", "s.manifest", "core_references", "json_extract(cast(s.payload"} {
				if strings.Contains(query, forbidden) {
					t.Fatalf("history selected a frozen body: %s", query)
				}
			}
		}
		if rec.bytes > 16<<10 {
			t.Fatalf("history selected %d bytes for a two-row page", rec.bytes)
		}
		return out, err
	}
	page, err := read(2, 1)
	if err != nil || len(page.Items) != 2 || page.Items[0].Commit.Version != "4" || page.Items[1].Commit.Version != "3" || page.CurrentMain.Branch.CommitID != previous.Result.Head.Commit.ID {
		t.Fatalf("unexpected history page: %+v / %v", page, err)
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte(`corrupt`), page.Items[1].Commit.ID)
	if page, err = read(2, 1); err != nil || len(page.Items) != 2 {
		t.Fatalf("unused payload broke history: %+v %v", page, err)
	}
	_, err = run(t, db, ctx, resourceSnapshotOperation, model.ConfigurationResourceRead{Domain: domain, ResourceID: "history", CommitID: page.Items[1].Commit.ID})
	configurationCode(t, err, "data_loss")
	plan, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+configurationCommitCatalogSQL, testScope, domain.Key, "history", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	var details strings.Builder
	for plan.Next() {
		var id, parent, unused int
		var detail string
		if err = plan.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details.WriteString(detail)
	}
	plan.Close()
	if !strings.Contains(details.String(), "sqlite_autoindex_core_snapshots_2") || strings.Contains(details.String(), "TEMP B-TREE") || strings.Contains(details.String(), "SCAN ") {
		t.Fatalf("history did not use its resource/version index: %s", details.String())
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET catalog_summary_json=? WHERE id=?`, []byte(`{"changed":true}`), page.Items[1].Commit.ID)
	if failed, err := read(2, 1); err == nil || failed.Items != nil {
		t.Fatalf("damaged summary leaked a page: %+v %v", failed, err)
	}
}
