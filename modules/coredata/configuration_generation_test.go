package coredata

import (
	"database/sql"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestConfigurationGenerationSharesCommitAndRollback(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	read := func(scope string) int64 {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		v, err := configurationGeneration(ctx, tx, scope)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if read(testScope) != 0 || read("another") != 0 {
		t.Fatal("fresh scope generation must be zero")
	}
	q := configurationCreateForTest(t, domain, "asset", nil)
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	if read(testScope) != 1 {
		t.Fatal("create did not atomically advance generation")
	}
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	if read(testScope) != 1 {
		t.Fatal("receipt replay advanced generation")
	}
	noop := configurationCommitForTest(t, domain, created, "noop", "a", true, nil, q.Snapshot.Manifest)
	configurationWrite(t, db, ctx, model.ResourceCommitOperation, noop)
	if read(testScope) != 1 {
		t.Fatal("unchanged root advanced generation")
	}
	bad := configurationCommitForTest(t, domain, created, "bad", "b", true, nil, q.Snapshot.Manifest)
	bad.Branch.ExpectedRevision = "9"
	_, err := run(t, db, ctx, model.ResourceCommitOperation, bad)
	configurationCode(t, err, "conflict")
	if read(testScope) != 1 {
		t.Fatal("failed transaction advanced generation")
	}
	good := configurationCommitForTest(t, domain, created, "good", "b", true, nil, q.Snapshot.Manifest)
	configurationWrite(t, db, ctx, model.ResourceCommitOperation, good)
	if read(testScope) != 2 || read("another") != 0 {
		t.Fatal("scope generation did not advance exactly once")
	}
	// At integer exhaustion the configuration, audit and quota all roll back.
	execSQL(t, db, ctx, `UPDATE core_data_usage SET configuration_generation=9223372036854775807 WHERE scope=?`, testScope)
	before := configurationCounts(t, db, ctx)
	_, err = run(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, domain, "overflow", nil))
	configurationCode(t, err, "resource_exhausted")
	if configurationCounts(t, db, ctx) != before || read(testScope) != 9223372036854775807 {
		t.Fatal("generation exhaustion escaped atomic rollback")
	}
}
