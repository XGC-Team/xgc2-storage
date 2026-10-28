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

func TestCommitIdentityBatchUsesOneScopedIndexedReadAndFailsClosed(t *testing.T) {
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
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		q := configurationCreateForTest(t, domain, id, nil)
		q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 512<<10) + `"}}`)
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	read := func(scope string, ids []string) []model.ConfigurationCommitIdentityState {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			t.Fatal(e)
		}
		items, e := configurationCommitIdentities(ctx, tx, scope, domain, ids)
		tx.Rollback()
		if e != nil {
			t.Fatal(e)
		}
		if len(rec.statements) != 2 || rec.bytes > 16384 {
			t.Fatalf("batch read %d statements/%d bytes", len(rec.statements), rec.bytes)
		}
		for _, q := range rec.statements {
			for _, forbidden := range []string{"s.payload", "x.payload", "s.manifest", "x.manifest", "core_references"} {
				if strings.Contains(strings.ToLower(q), forbidden) {
					t.Fatalf("visibility read %s", forbidden)
				}
			}
		}
		return items
	}
	ids := []string{"first-v1", "second-v1", "missing"}
	before := read(testScope, ids)
	if len(before) != 3 || !before[0].Valid || !before[1].Valid || before[2].Valid {
		t.Fatalf("identity states %+v", before)
	}
	baseline := rec.bytes
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("other-%02d", i)
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, domain, id, nil))
	}
	if after := read(testScope, ids); rec.bytes != baseline || !after[0].Valid || !after[1].Valid {
		t.Fatalf("unrelated rows changed selected work: %d vs %d", rec.bytes, baseline)
	}
	for _, item := range read("other-scope", ids) {
		if item.Valid {
			t.Fatal("scope escaped")
		}
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte("corrupt"), "first-v1")
	if got := read(testScope, ids); !got[0].Valid {
		t.Fatal("visibility read unused payload")
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET identity_json=? WHERE id=?`, []byte(`{"active":false}`), "second-v1")
	if got := read(testScope, ids); !got[0].Valid || got[1].Valid {
		t.Fatalf("corrupt projection not isolated %+v", got)
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET identity_json=? WHERE id=?`, []byte(strings.Repeat("z", 3<<20)), "second-v1")
	if got := read(testScope, ids); !got[0].Valid || got[1].Valid {
		t.Fatalf("oversized identity was not failed closed: %+v", got)
	}
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+configurationCommitIdentitiesSQL, `["first-v1"]`, testScope, domain.Key)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var a, b, c int
		var detail string
		if err = rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "; ")
	}
	if !strings.Contains(plan.String(), "sqlite_autoindex_core_snapshots_1") || strings.Contains(plan.String(), "SCAN x ") {
		t.Fatalf("nonindexed identity batch: %s", plan.String())
	}
}
