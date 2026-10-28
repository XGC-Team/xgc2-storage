package coredata_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRestartReceiptReadsOnlyAdmissionProofAndObservation(t *testing.T) {
	f := openStore(t)
	q := newRun("receipt")
	q.PredecessorRunID = "predecessor"
	q.Inputs = []byte(`{"restartRequestVerifier":"proof"}`)
	q.Trigger = []byte(`{"kind":"session-action","instanceId":"one","actionId":"run"}`)
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunStopped, Error: []byte(`{"message":"closed"}`), Result: []byte(`{"value":42}`), Nodes: []byte(`[{"id":"one"}]`)}); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE runs SET error_json=x'01',nodes_json=x'02',result_json=x'03',cleanup_errors_json=x'04' WHERE id=?", q.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err := f.core.GetRunRestartReceipt(f.ctx, q.ID)
	if err != nil || got.ID != q.ID || got.PredecessorRunID != q.PredecessorRunID || string(got.Inputs) != string(q.Inputs) || string(got.Trigger) != string(q.Trigger) || string(got.Error) != `{"message":"closed"}` || len(got.Nodes)+len(got.Result)+len(got.CleanupErrors) != 0 || !got.HasResult {
		t.Fatalf("receipt: %+v %v", got, err)
	}
	if _, err := f.core.GetRun(f.ctx, q.ID); err == nil {
		t.Fatal("fixture did not corrupt ignored execution payloads")
	}
}
