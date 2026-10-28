package coredata

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSystemResourceUsesUniqueIndex(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	execSQL(t, db, ctx, `UPDATE core_configuration_domains SET system_provisioning=1 WHERE domain='test'`)
	q := configurationCreateForTest(t, domain, "custom-id", nil)
	q.System = true
	q.SystemKey = "xgc.system.robot-runtime"
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+configurationSystemResourceQuery, testScope, domain.Key, q.SystemKey)
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
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "core_resource_system_keys (") || strings.Contains(plan.String(), "SCAN ") {
		t.Fatalf("system key did not use its unique index: %s", plan.String())
	}
	read := func(key string) (model.ConfigurationResource, error) {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return configurationSystemResource(ctx, tx, testScope, domain, key)
	}
	found, err := read(q.SystemKey)
	if err != nil || found.ID != q.ResourceID || found.SystemKey != q.SystemKey {
		t.Fatalf("wrong system resource: %+v / %v", found, err)
	}
	for _, key := range []string{"XGC.system.robot-runtime", "not-provisioned"} {
		_, err := read(key)
		configurationCode(t, err, "not_found")
	}
	for _, key := range []string{"", " padded "} {
		_, err := read(key)
		configurationCode(t, err, "invalid_argument")
	}
}
