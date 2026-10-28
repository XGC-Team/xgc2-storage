package host

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestCoreDataAdmissionOnTheActualEmbeddedHost(t *testing.T) {
	for _, mode := range []string{"current", "older", "missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
			manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
			config := Config{Path: filepath.Join(dir, "host.db"), Create: true, Manifest: manifest}
			owner, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			store, err := owner.Core(scope)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = store.CreateRun(ctx, model.NewRun{ID: "accepted", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)})
			if err != nil {
				t.Fatal(err)
			}
			if mode != "current" {
				err = owner.store.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
					if mode == "missing" {
						_, err := tx.ExecContext(ctx, `DELETE FROM schema_versions WHERE module=?`, model.Module)
						return err
					}
					_, err := tx.ExecContext(ctx, `UPDATE schema_versions SET version=? WHERE module=?`, coredata.SchemaVersion-1, model.Module)
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = owner.Close(ctx); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(config.Path)
			if err != nil {
				t.Fatal(err)
			}
			config.Create = false
			reopened, err := Open(ctx, config)
			if mode == "current" {
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close(ctx)
				store, err := reopened.Core(scope)
				if err != nil {
					t.Fatal(err)
				}
				if run, err := store.GetRun(ctx, "accepted"); err != nil || run.ID != "accepted" {
					t.Fatalf("current owner lost accepted record: %+v %v", run, err)
				}
				return
			}
			if reopened != nil {
				reopened.Close(ctx)
				t.Fatal("non-current owner admitted")
			}
			var apiErr *api.Error
			if !errors.As(err, &apiErr) || apiErr.Code != "failed_precondition" {
				t.Fatalf("non-current host error=%v", err)
			}
			after, err := os.ReadFile(config.Path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused host changed stored facts: %v", err)
			}
			backups, err := filepath.Glob(config.Path + ".before-*")
			if err != nil || len(backups) != 0 {
				t.Fatalf("refusal initiated migration: backups=%v err=%v", backups, err)
			}
		})
	}
}
