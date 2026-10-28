package coredata

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestBranchCatalogSharesImmutableHeadsWithoutReadingPayload(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	create := configurationCreateForTest(t, domain, "resource", nil)
	create.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 512<<10) + `"}}`)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, create)
	branch := first.Result.Head.Branch
	branch.ID = "review"
	branch.Name = "Review"
	branch.NameKey = "review"
	branch.CreatedFromCommitID = first.Result.Head.Commit.ID
	body, err := json.Marshal(branch)
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, ctx, `INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)`, testScope, domain.Key, branch.ID, branch.ResourceID, branch.NameKey, branch.HeadCommitID, branch.Revision, body)
	read := func(q model.ConfigurationCatalogRead) (model.ConfigurationBranchCatalog, error) {
		t.Helper()
		tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		return configurationBranchCatalog(ctx, tx, testScope, q)
	}
	q := model.ConfigurationCatalogRead{Domain: domain, ID: "resource"}
	page, err := read(q)
	if err != nil || len(page.Items) != 2 || page.Items[0].Branch.NameKey != "main" || page.Items[1].Branch.NameKey != "review" || page.Items[1].Commit.ID != first.Result.Head.Commit.ID {
		t.Fatalf("shared branch head: %+v %v", page, err)
	}
	q.Limit = 1
	q.Offset = 1
	page, err = read(q)
	if err != nil || len(page.Items) != 1 || page.Items[0].Branch.ID != "review" {
		t.Fatalf("branch page: %+v %v", page, err)
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte("corrupt"), create.CommitID)
	if _, err = read(q); err != nil {
		t.Fatalf("branch list opened frozen body: %v", err)
	}
	_, err = run(t, db, ctx, resourceSnapshotOperation, model.ConfigurationResourceRead{Domain: domain, ResourceID: "resource", Branch: "main"})
	configurationCode(t, err, "data_loss")
	for _, archived := range []bool{false, true} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+configurationBranchCatalogQuery(archived), testScope, domain.Key, "resource", 2, 0)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var a, b, c int
			var detail string
			if err = rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "; ")
		}
		rows.Close()
		if strings.Contains(plan.String(), "SCAN b") || !strings.Contains(plan.String(), "resource_id") {
			t.Fatalf("branch page did not use resource index: %s", plan.String())
		}
	}
	execSQL(t, db, ctx, `UPDATE core_snapshots SET catalog_summary_json=? WHERE id=?`, []byte(`{"broken":true}`), create.CommitID)
	if failed, err := read(q); err == nil || failed.Items != nil {
		t.Fatalf("corrupt header leaked branch page: %+v %v", failed, err)
	}
}
