package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
)

func TestConsistentBackupRestoreNoOverwrite(t *testing.T) {
	s, _, ctx := setup(t)
	read := snapshot(t, s, ctx, "a")
	commit, e := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: read.Token, RequestID: "backup-source", Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"original"}`)}})
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	if e = os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	destination := filepath.Join(dir, "snapshot.db")
	receipt, e := s.Backup(ctx, destination)
	if e != nil {
		t.Fatal(e)
	}
	if receipt.DatabaseID != commit.Token.DatabaseID {
		t.Fatal("wrong identity")
	}
	if _, e = s.Backup(ctx, filepath.Join(dir, "second.db")); e != nil {
		t.Fatalf("second independent backup: %v", e)
	}
	if _, e = s.Backup(ctx, destination); e == nil {
		t.Fatal("backup overwrote existing output")
	}
	restored, e := Open(ctx, Config{Path: destination, Manifest: manifest()})
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	saved := snapshot(t, restored, ctx, "a")
	if string(saved.Results[0].Records[0].Data) != `{"name":"original"}` {
		t.Fatal("lost committed WAL data")
	}
	if e = restored.Integrity(ctx); e != nil {
		t.Fatal(e)
	}
	original, e := restored.Receipt(ctx, api.ReceiptRequest{Scope: testScope, RequestID: "backup-source"})
	if e != nil || original.Digest != commit.Digest {
		t.Fatalf("backup receipt %v %v", original, e)
	}
}
