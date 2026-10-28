package coredata

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"modernc.org/sqlite"
)

// This wraps the real SQLite connection, counting statements at its driver
// boundary. It does not replace domain state, storage, rows, or transaction
// semantics; an N+1 hidden inside one admission still fails the count.
type configurationCountingConnector struct {
	queries *atomic.Int64
	dsn     string
}

func (c configurationCountingConnector) Connect(context.Context) (driver.Conn, error) {
	connection, err := (&sqlite.Driver{}).Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return configurationCountingConnection{Conn: connection, queries: c.queries}, nil
}
func (c configurationCountingConnector) Driver() driver.Driver { return &sqlite.Driver{} }

type configurationCountingConnection struct {
	driver.Conn
	queries *atomic.Int64
}

func (c configurationCountingConnection) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func (c configurationCountingConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queries.Add(1)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c configurationCountingConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestConfigurationCatalogSQLWorkDoesNotMultiply(t *testing.T) {
	var queries atomic.Int64
	dsn := "file:" + filepath.Join(t.TempDir(), "count.db") + "?_pragma=foreign_keys(1)"
	db := sql.OpenDB(configurationCountingConnector{queries: &queries, dsn: dsn})
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
	for _, total := range []int{1, 37} {
		for i := range total {
			id := fmt.Sprintf("resource-%d-%02d", total, i)
			q := configurationCreateForTest(t, g, id, nil)
			q.Snapshot.References = []model.ConfigurationReference{{Slot: "self", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: id, TargetBranch: "main"}}
			configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
		}
		queries.Store(0)
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationResourceCatalog(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: g})
		tx.Rollback()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) < total || queries.Load() != 4 {
			t.Fatalf("%d resources used %d SQL reads for %d rows; want domain + joined page + selected references and associations", total, queries.Load(), len(rows))
		}
	}
}

func TestConfigurationCatalogSelectorsUseOwnedIndexes(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	root, selected := "", "selected"
	for _, c := range []struct {
		name     string
		parent   *string
		archived bool
		index    string
	}{
		{"all live", nil, false, "core_resources_live_catalog"},
		{"all with archived", nil, true, "sqlite_autoindex_core_resources_1"},
		{"root live", &root, false, "core_resources_namespace"},
		{"selected live", &selected, false, "core_resources_namespace"},
		{"root with archived", &root, true, "core_resources_namespace_all"},
		{"selected with archived", &selected, true, "core_resources_namespace_all"},
	} {
		t.Run(c.name, func(t *testing.T) {
			selector := model.ConfigurationCatalogRead{Domain: domain, ParentID: c.parent, IncludeArchived: c.archived}
			predicate, args, _, err := configurationResourceCatalogSelector(testScope, selector)
			if err != nil {
				t.Fatal(err)
			}
			referenceArgs := append([]any{testScope, domain.Key}, args...)
			referenceArgs = append(referenceArgs, MaxReferences+1)
			for _, query := range []struct {
				sql   string
				args  []any
				index string
			}{
				{configurationResourceCatalogQuery(selector, predicate), args, c.index},
				{configurationResourceCatalogReferencesQuery(selector, predicate), referenceArgs, "sqlite_autoindex_core_references_1"},
			} {
				rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query.sql, query.args...)
				if err != nil {
					t.Fatal(err)
				}
				var plan strings.Builder
				for rows.Next() {
					var id, parent, unused int
					var detail string
					if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
						t.Fatal(err)
					}
					plan.WriteString(detail + "; ")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				if !strings.Contains(plan.String(), query.index+" (") || !strings.Contains(plan.String(), c.index+" (") || strings.Contains(plan.String(), "SCAN ") || strings.Contains(plan.String(), "TEMP B-TREE") {
					t.Fatalf("catalog lost its bounded indexed page: %s", plan.String())
				}
			}
		})
	}
}

func TestConfigurationCatalogBoundAndSelectorValidation(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	q := configurationCreateForTest(t, domain, "template", nil)
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	// Seed valid relational/frozen fixture rows in three bounded statements,
	// rather than performing thousands of unrelated durable writes.
	const ids = `WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<4096) `
	execSQL(t, db, ctx, ids+`INSERT INTO core_resources(scope,domain,id,namespace_id,name,name_key,revision,main_commit_id,archived,origin_resource_id,origin_commit_id,body) SELECT scope,domain,printf('r-%04d',n),NULL,printf('r-%04d',n),printf('r-%04d',n),revision,printf('c-%04d',n),archived,origin_resource_id,origin_commit_id,json_set(CAST(body AS TEXT),'$.id',printf('r-%04d',n),'$.name',printf('r-%04d',n),'$.name_key',printf('r-%04d',n),'$.main_commit_id',printf('c-%04d',n)) FROM core_resources CROSS JOIN ids WHERE id='template'`)
	execSQL(t, db, ctx, ids+`INSERT INTO core_branches SELECT scope,domain,printf('b-%04d',n),printf('r-%04d',n),name_key,printf('c-%04d',n),revision,json_set(CAST(body AS TEXT),'$.id',printf('b-%04d',n),'$.resource_id',printf('r-%04d',n),'$.head_commit_id',printf('c-%04d',n)) FROM core_branches CROSS JOIN ids WHERE id='template-main'`)
	execSQL(t, db, ctx, ids+`INSERT INTO core_snapshots SELECT scope,domain,printf('c-%04d',n),printf('r-%04d',n),printf('b-%04d',n),version,source_commit_id,content_digest,payload,manifest,identity_json,catalog_summary_json,json_set(CAST(body AS TEXT),'$.id',printf('c-%04d',n),'$.resource_id',printf('r-%04d',n),'$.branch_id',printf('b-%04d',n)) FROM core_snapshots CROSS JOIN ids WHERE id='template-v1'`)
	for _, query := range []model.ConfigurationCatalogRead{
		{Domain: domain}, {Domain: domain, Limit: -1}, {Domain: domain, Limit: 4097}, {Domain: domain, Offset: -1}, {Domain: domain, ID: " bad"},
	} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := configurationResourceCatalog(ctx, tx, testScope, query)
		tx.Rollback()
		code := "invalid_argument"
		if query.Limit == 0 && query.Offset == 0 && query.ID == "" {
			code = "resource_exhausted"
		}
		configurationCode(t, err, code)
		if rows != nil {
			t.Fatal("failed catalog returned a successful prefix")
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := configurationResourceCatalog(ctx, tx, testScope, model.ConfigurationCatalogRead{Domain: domain, Limit: 2, Offset: 4095})
	tx.Rollback()
	if err != nil || len(rows) != 2 || rows[1].Head.Resource.ID != "template" {
		t.Fatalf("explicit last page changed results: %d / %v", len(rows), err)
	}
}

func TestConfigurationCatalogResponseAccountingMatchesEncodedSnapshots(t *testing.T) {
	random := rand.New(rand.NewPCG(23, 89))
	for range 257 {
		snapshot := model.ConfigurationResourceSnapshot{
			Payload: make([]byte, random.IntN(1024)), Manifest: make([]byte, random.IntN(1024)),
			Identity: make([]byte, random.IntN(257)), CatalogSummary: make([]byte, random.IntN(257)),
			CurrentMain: model.ConfigurationVisibilityPin{Identity: make([]byte, random.IntN(257))},
			References:  []model.ConfigurationReference{}, Associations: []model.ConfigurationAgentAssociation{},
		}
		snapshot.Head.Resource.Name = "A <readable> resource & one frozen main"
		count, err := configurationSnapshotResponseBytes(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := encode(snapshot)
		if err != nil || count != len(encoded) {
			t.Fatalf("allocation-free aggregate accounting drifted: got%d want%d err=%v", count, len(encoded), err)
		}
	}
}
