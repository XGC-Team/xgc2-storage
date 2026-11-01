package main

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"github.com/XGC-Team/xgc2-storage/registry"
)

func TestAdminExplicitUpgradeUsesExistingOwnerAndBackup(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.Open("../../modules/coredata/testdata/coredata13-real.db.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	in, err := gzip.NewReader(fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	path := filepath.Join(dir, "owner.db")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600,
		Modules: []string{model.Module}, Collections: []api.Collection{{ID: "marker", MaxRecordBytes: 4096, MaxRecords: 100, MaxBytes: 1 << 20, Retention: "test point", Recovery: "test backup"}}}}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(dir, "backups")
	if err := os.Mkdir(backupDir, 0700); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(backupDir, "before.db")
	original := os.Args
	os.Args = []string{"storage-admin", "upgrade-coredata-13-to-14", "-db", path, "-manifest", manifestPath, "-destination", backup}
	defer func() { os.Args = original }()
	if err := run(); err != nil {
		t.Fatal(err)
	}
	db, err := engine.Open(t.Context(), engine.Config{Path: path, Manifest: manifest, Modules: registry.Compiled()})
	if err != nil {
		t.Fatalf("normal reopen after CLI upgrade: %v", err)
	}
	defer db.Close()
	store, err := coredata.New(db, api.Scope{Namespace: "core", User: "operator", Workspace: "station"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetRun(t.Context(), "kept-run"); err != nil || got.ID != "kept-run" {
		t.Fatalf("accepted run lost: %+v %v", got, err)
	}
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("pre-upgrade backup missing: %v", err)
	}
}
