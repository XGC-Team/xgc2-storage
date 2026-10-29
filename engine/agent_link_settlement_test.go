package engine_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAgentLinkReplayDoesNotHideFailedRollback(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	manifest := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600, Modules: []string{model.Module}}}}
	db, err := engine.Open(t.Context(), engine.Config{Path: filepath.Join(dir, "storage.db"), Create: true, Manifest: manifest, Modules: []engine.Module{coredata.Module()}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := coredata.New(db, scope)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.ListAgentLinks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	record := model.AgentLinkRecord{AgentID: "agent", Enrollment: "known", AgentLinkFacts: model.AgentLinkFacts{CapabilityManifest: []byte(`{}`)}}
	q := model.AgentLinkWrite{Expected: model.AgentLinkGuard{Identity: initial.Identity, AgentID: record.AgentID, Version: "0"}, Record: record}
	written, err := store.WriteAgentLink(t.Context(), q)
	if err != nil || !written.Changed {
		t.Fatalf("initial write: %+v %v", written, err)
	}
	q.Expected = written.Stored.Guard
	injected := engine.InjectRollbackFailureForModuleTest(t, db)
	out, err := store.WriteAgentLink(t.Context(), q)
	var disposition *engine.WriteError
	if !errors.As(err, &disposition) || disposition.Disposition != engine.WriteUnknown || !disposition.Settled || !errors.Is(err, injected) || out.Changed {
		t.Fatalf("replay hid failed rollback: %+v disposition=%+v error=%v", out, disposition, err)
	}
	replay, err := store.WriteAgentLink(t.Context(), q)
	if err != nil || replay.Changed || replay.Stored.Guard != written.Stored.Guard {
		t.Fatalf("settled normal replay: %+v %v", replay, err)
	}
}
