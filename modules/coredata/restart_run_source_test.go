package coredata_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/XGC-Team/xgc2-storage/engine"
)

func TestRestartRunSourceDoesNotReadRecordedGraphOrInputs(t *testing.T) {
	f := openStore(t)
	q := newRun("source")
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE runs SET inputs_json=x'01',nodes_json=x'02',result_json=x'03',cleanup_errors_json=x'04' WHERE id='source'")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	source, err := f.core.GetRunRestartSource(f.ctx, "source")
	if err != nil || source.ID != "source" || string(source.Trigger) != string(q.Trigger) || len(source.Inputs)+len(source.Nodes)+len(source.Result)+len(source.CleanupErrors) != 0 {
		t.Fatalf("source: %+v %v", source, err)
	}
	if _, err := f.core.GetRun(f.ctx, "source"); err == nil {
		t.Fatal("fixture did not corrupt ignored execution payloads")
	}
}
