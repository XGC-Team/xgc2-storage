package coredata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"modernc.org/sqlite"
)

type headerSQLRecorder struct {
	statements []string
	bytes      int
}

type headerCountingConnector struct {
	dsn string
	rec *headerSQLRecorder
}

func (c headerCountingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := (&sqlite.Driver{}).Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return headerCountingConnection{Conn: conn, rec: c.rec}, nil
}
func (c headerCountingConnector) Driver() driver.Driver { return &sqlite.Driver{} }

type headerCountingConnection struct {
	driver.Conn
	rec *headerSQLRecorder
}

func (c headerCountingConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (c headerCountingConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c headerCountingConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	c.rec.statements = append(c.rec.statements, query)
	return &headerCountingRows{Rows: rows, rec: c.rec}, nil
}

type headerCountingRows struct {
	driver.Rows
	rec *headerSQLRecorder
}

func (r *headerCountingRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err != nil {
		return err
	}
	for _, value := range dest {
		switch v := value.(type) {
		case []byte:
			r.rec.bytes += len(v)
		case string:
			r.rec.bytes += len(v)
		case int64, float64:
			r.rec.bytes += 8
		}
	}
	return nil
}

func TestConfigurationHeadersSQLReadDoesNotSelectFrozenBodies(t *testing.T) {
	rec := &headerSQLRecorder{}
	dsn := "file:" + filepath.Join(t.TempDir(), "headers.db") + "?_pragma=foreign_keys(1)"
	db := sql.OpenDB(headerCountingConnector{dsn: dsn, rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := t.Context()
	g := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err == nil {
		err = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: g.SchemaIdentity, SchemaVersion: 1, RegistryDigest: g.RegistryDigest, MainVisibility: true}})
	}
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, total := range []int{1, 16} {
		for i := range total {
			id := fmt.Sprintf("resource-%02d-%02d", total, i)
			q := configurationCreateForTest(t, g, id, nil)
			q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 768<<10) + `"}}`)
			q.Snapshot.References = []model.ConfigurationReference{{Slot: "self", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: id, TargetBranch: "main"}}
			configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
		}
		rec.statements, rec.bytes = nil, 0
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationResourceHeaders(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: g})
		tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) < total || len(rec.statements) != 2 {
			t.Fatalf("%d rows: %d SQL reads, want domain guard plus one page", len(rows), len(rec.statements))
		}
		if rec.bytes > len(rows)*4096+1024 {
			t.Fatalf("%d selected bytes for %d headers, despite large frozen bodies", rec.bytes, len(rows))
		}
		query := strings.ToLower(rec.statements[1])
		for _, forbidden := range []string{"s.payload", "s.manifest", "core_references", "json_extract(cast(s."} {
			if strings.Contains(query, forbidden) {
				t.Fatalf("header query selected frozen data: %s", query)
			}
		}
	}
}

func TestConfigurationHeadersUseIndexedPages(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	root, selected := "", "selected"
	for _, c := range []struct {
		name     string
		parent   *string
		archived bool
		index    string
	}{
		{"all live", nil, false, "core_resources_live_catalog"},
		{"all archived", nil, true, "sqlite_autoindex_core_resources_1"},
		{"root live", &root, false, "core_resources_namespace"},
		{"namespace live", &selected, false, "core_resources_namespace"},
		{"root archived", &root, true, "core_resources_namespace_all"},
		{"namespace archived", &selected, true, "core_resources_namespace_all"},
	} {
		t.Run(c.name, func(t *testing.T) {
			q := model.ConfigurationCatalogRead{Domain: domain, ParentID: c.parent, IncludeArchived: c.archived}
			predicate, args, _, err := configurationResourceCatalogSelector(testScope, q)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+configurationResourceHeadersQuery(q, predicate), args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan strings.Builder
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				plan.WriteString(detail + "; ")
			}
			if err := rows.Err(); err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if !strings.Contains(plan.String(), c.index+" (") || strings.Contains(plan.String(), "SCAN ") || strings.Contains(plan.String(), "TEMP B-TREE") {
				t.Fatalf("header page is not bounded by its index: %s", plan.String())
			}
		})
	}
}

func TestConfigurationHeadersRejectRowAndByteOverflowWithoutPrefix(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	q := configurationCreateForTest(t, domain, "template", nil)
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	const ids = `WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<4096) `
	execSQL(t, db, ctx, ids+`INSERT INTO core_resources SELECT scope,domain,printf('r-%04d',n),NULL,printf('r-%04d',n),printf('r-%04d',n),revision,printf('c-%04d',n),archived,origin_resource_id,origin_commit_id,json_set(CAST(body AS TEXT),'$.id',printf('r-%04d',n),'$.name',printf('r-%04d',n),'$.name_key',printf('r-%04d',n),'$.main_commit_id',printf('c-%04d',n)) FROM core_resources CROSS JOIN ids WHERE id='template'`)
	execSQL(t, db, ctx, ids+`INSERT INTO core_branches SELECT scope,domain,printf('b-%04d',n),printf('r-%04d',n),name_key,printf('c-%04d',n),revision,json_set(CAST(body AS TEXT),'$.id',printf('b-%04d',n),'$.resource_id',printf('r-%04d',n),'$.head_commit_id',printf('c-%04d',n)) FROM core_branches CROSS JOIN ids WHERE id='template-main'`)
	execSQL(t, db, ctx, ids+`INSERT INTO core_snapshots SELECT scope,domain,printf('c-%04d',n),printf('r-%04d',n),printf('b-%04d',n),version,source_commit_id,content_digest,payload,manifest,json_set(CAST(body AS TEXT),'$.id',printf('c-%04d',n),'$.resource_id',printf('r-%04d',n),'$.branch_id',printf('b-%04d',n)) FROM core_snapshots CROSS JOIN ids WHERE id='template-v1'`)
	for _, q := range []model.ConfigurationCatalogRead{{Domain: domain}, {Domain: domain, Limit: -1}, {Domain: domain, Limit: 4097}, {Domain: domain, Offset: -1}} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationResourceHeaders(ctx, tx, testScope, q)
		tx.Rollback()
		code := "invalid_argument"
		if q.Limit == 0 && q.Offset == 0 {
			code = "resource_exhausted"
		}
		configurationCode(t, err, code)
		if rows != nil {
			t.Fatal("failed page leaked a prefix")
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := configurationResourceHeaders(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 2, Offset: 4095})
	tx.Rollback()
	if err != nil || len(rows) != 2 || rows[1].Resource.ID != "template" {
		t.Fatalf("last page changed membership: %d / %v", len(rows), err)
	}
	// Corrupt unused metadata with enormous unknown fields to verify the byte
	// bound independently of the row bound and without returning earlier rows.
	padding := strings.Repeat("x", 6<<20)
	if _, err := db.ExecContext(ctx, `UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.padding',?) WHERE id IN ('r-0001','r-0002','r-0003')`, padding); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = configurationResourceHeaders(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 3})
	tx.Rollback()
	configurationCode(t, err, "resource_exhausted")
	if rows != nil {
		t.Fatal("byte overflow leaked a prefix")
	}
}
