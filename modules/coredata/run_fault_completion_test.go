package coredata

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunFaultRevalidationKeepsJoinedPanicNoncommit(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	db, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "fault.db"), Create: true,
		Manifest: api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: Schema,
			MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}, Modules: []engine.Module{Module()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	state := &runFaultState{scope: engine.ScopeID(scope), tracksWrites: true}
	entered, release := make(chan struct{}), make(chan struct{})
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		returned <- db.WriteWithFaultProbe(caller, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
			q := model.NewRun{ID: "abandoned", TargetID: "core", WorkflowResourceID: "workflow", WorkflowCommitID: "commit",
				DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{"input":"retained"}`), Trigger: []byte(`{}`), At: time.Unix(100, 0).UTC()}
			run, created, err := createRun(ctx, tx, state.scope, q)
			if err != nil {
				close(entered)
				return err
			}
			state.created = []model.CreatedRun{{Run: run, Created: created}}
			close(entered)
			<-release
			panic("after admitted create, before COMMIT")
		}, state.probe)
	}()
	<-entered
	cancel()
	raw := <-returned
	failure, ok := runFaultError(raw, state).(*RunWriteFaultError)
	if !ok {
		close(release)
		t.Fatalf("missing retained typed attempt: %v", raw)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := failure.WaitRevalidation(canceled); !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("canceled wait claimed a fact before join: %v", err)
	}
	close(release)
	fact, err := failure.WaitRevalidation(ctx)
	if err != nil || !fact.NonCommitted || len(fact.Creates) != 1 || !fact.Creates[0].Attempted.Created || fact.Creates[0].Present || !fact.Creates[0].CurrentKnown {
		t.Fatalf("joined no-effect attempt lost its factual result: %+v %v", fact, err)
	}
	if err := db.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM runs WHERE scope=? AND id='abandoned'", state.scope).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("panicking transaction committed %d rows", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
