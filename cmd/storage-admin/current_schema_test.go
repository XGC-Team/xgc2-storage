package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"github.com/XGC-Team/xgc2-storage/registry"
)

func TestAdminAdmitsCurrentAndRefusesNoncurrentModuleWithoutMutation(t *testing.T) {
	for _, mode := range []string{"current", "older", "missing"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
			manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
			path := filepath.Join(dir, "owner.db")
			db, err := engine.Open(ctx, engine.Config{Path: path, Create: true, Manifest: manifest, Modules: registry.Compiled()})
			if err != nil {
				t.Fatal(err)
			}
			store, err := coredata.New(db, scope)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = store.CreateRun(ctx, model.NewRun{ID: "kept", TargetID: "core", WorkflowResourceID: "wf", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: []byte(`{}`), Trigger: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if mode != "current" {
				err = db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
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
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(dir, "manifest.json")
			if err = os.WriteFile(manifestPath, body, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			original := os.Args
			os.Args = []string{"storage-admin", "check", "-db", path, "-manifest", manifestPath}
			defer func() { os.Args = original }()
			err = run()
			if mode == "current" {
				if err != nil {
					t.Fatalf("current admin: %v", err)
				}
				return
			}
			var apiErr *api.Error
			if !errors.As(err, &apiErr) || apiErr.Code != "failed_precondition" {
				t.Fatalf("%s admin accepted or hid error: %v", mode, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("%s refusal changed database: %v", mode, err)
			}
			backups, err := filepath.Glob(path + ".before-*")
			if err != nil || len(backups) != 0 {
				t.Fatalf("%s refusal created backup: %v %v", mode, backups, err)
			}
		})
	}
}
