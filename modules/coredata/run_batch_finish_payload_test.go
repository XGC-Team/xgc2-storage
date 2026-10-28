package coredata_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A changed batch finish returns no Run. Unrelated stored payload corruption
// must therefore not force an otherwise unnecessary detail read; operations
// that actually return or compare that detail still report the corruption.
func TestBatchFinishDoesNotReadExistingRunPayload(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"batch", "direct"} {
		must3(f.core.CreateRun(f.ctx, newRun(id)))
		f.exec(t, "UPDATE runs SET inputs_json=? WHERE id=?", []byte("not a gzip stream"), id)
	}
	finish := model.RunFinish{ID: "batch", Status: model.RunSucceeded, Result: []byte(`{"ok":true}`)}
	if _, err := f.core.ApplyRunOps(f.ctx, nil, nil, []model.RunFinish{finish}); err != nil {
		t.Fatalf("changed batch selected an unneeded existing payload: %v", err)
	}
	var status string
	var revision int
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT status,revision FROM runs WHERE id=?", finish.ID).Scan(&status, &revision)
	}); err != nil || status != string(model.RunSucceeded) || revision != 2 {
		t.Fatalf("batch finish status=%q revision=%d err=%v", status, revision, err)
	}
	if _, err := f.core.GetRun(f.ctx, finish.ID); errCode(err) != "data_loss" {
		t.Fatalf("full read did not reject corrupt payload: %v", err)
	}
	if _, err := f.core.ApplyRunOps(f.ctx, nil, nil, []model.RunFinish{finish}); errCode(err) != "data_loss" {
		t.Fatalf("replay did not validate stored outcome and payload: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "direct", Status: model.RunSucceeded}); errCode(err) != "data_loss" {
		t.Fatalf("direct result did not reject corrupt payload: %v", err)
	}
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT status,revision FROM runs WHERE id=?", "direct").Scan(&status, &revision)
	}); err != nil || status != string(model.RunQueued) || revision != 1 {
		t.Fatalf("failed direct finish was not rolled back: status=%q revision=%d err=%v", status, revision, err)
	}
}
