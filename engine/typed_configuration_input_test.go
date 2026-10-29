package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationInputFixture(t *testing.T, db *engine.Store) (model.ConfigurationDomainGuard, model.ConfigurationResourceCreate) {
	t.Helper()
	domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	if err := coredata.DeclareConfigurationDomains(t.Context(), db, []model.ConfigurationDomainDeclaration{{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: domain.SchemaVersion, RegistryDigest: domain.RegistryDigest, MainVisibility: true}}); err != nil {
		t.Fatal(err)
	}
	manifest := model.ConfigurationManifest{Nodes: []model.ConfigurationManifestNode{{ID: "root", Kind: "root"}, {ID: "x", ParentID: "root", Name: "X", Kind: "document", DocumentKind: "test.data.v1", PayloadDigest: strings.Repeat("a", 64)}}}
	validated, err := model.ValidateConfigurationManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	changes, summary, err := model.DiffConfigurationManifests(model.ValidatedConfigurationManifest{}, validated)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(model.ConfigurationManifest{Nodes: validated.Nodes})
	if err != nil {
		t.Fatal(err)
	}
	q := model.ConfigurationResourceCreate{Domain: domain, Mutation: model.ConfigurationMutation{Key: "create-resource", IntentDigest: strings.Repeat("a", 64), Actor: "tester"}, ResourceID: "resource", BranchID: "resource-main", CommitID: "resource-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}, Name: "resource", NameKey: "resource", Snapshot: model.PreparedConfigurationSnapshot{RootDigest: validated.RootDigest, Payload: []byte(`{"identity":{"active":true},"body":{"value":"original"}}`), Manifest: body, Identity: []byte(`{"active":true}`), CatalogSummary: []byte(`{"label":"fixture"}`), Associations: []model.ConfigurationAgentAssociation{}, Change: model.ConfigurationChange{ID: "change", Summary: summary, Nodes: changes}}}
	return domain, q
}

func TestConfigurationReceiptStageKeepsBorrowedInputSafeAfterDeadline(t *testing.T) {
	for _, exists := range []bool{false, true} {
		name := "new"
		if exists {
			name = "replay"
		}
		t.Run(name, func(t *testing.T) {
			db, store, _, _, _ := remoteSettlementFixture(t)
			domain, q := configurationInputFixture(t, db)
			if exists {
				if _, err := store.CreateResource(t.Context(), q); err != nil {
					t.Fatal(err)
				}
			}
			entered, release := engine.GateCallbackSQLForModuleTest(t, db, "SELECT intent_digest,operation,body FROM core_configuration_receipts", "row")
			defer release()
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			type answer struct {
				result model.ConfigurationMutationResult
				err    error
			}
			done := make(chan answer, 1)
			go func() { result, err := store.CreateResource(ctx, q); done <- answer{result, err} }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real receipt row did not enter")
			}
			got := <-done
			var outcome *engine.WriteError
			if !errors.As(got.err, &outcome) || !outcome.CallbackPending || got.result.Found {
				t.Fatalf("held receipt exposed an outcome: %+v %v", got.result, got.err)
			}
			for i := range q.Snapshot.Payload {
				q.Snapshot.Payload[i] = 'x'
			}
			for i := range q.Snapshot.Manifest {
				q.Snapshot.Manifest[i] = 'x'
			}
			release()
			if err := outcome.WaitSettlement(t.Context()); err != nil {
				t.Fatal(err)
			}
			current, err := store.ReadResource(t.Context(), model.ConfigurationResourceRead{Domain: domain, ResourceID: "resource", Branch: "main"})
			if exists {
				if err != nil || current.Head.Resource.ID != "resource" {
					t.Fatalf("existing receipt source: %+v %v", current, err)
				}
			} else {
				var domainErr *api.Error
				if !errors.As(err, &domainErr) || domainErr.Code != "not_found" {
					t.Fatalf("canceled create must remain absent: %v", err)
				}
			}
			stats, err := db.Stats()
			if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
				t.Fatalf("owner did not return to idle: %+v %v", stats, err)
			}
		})
	}
}

func TestConfigurationReplayDoesNotHideFailedRollback(t *testing.T) {
	db, store, _, _, _ := remoteSettlementFixture(t)
	_, q := configurationInputFixture(t, db)
	if _, err := store.CreateResource(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	injected := engine.InjectRollbackFailureForModuleTest(t, db)
	result, err := store.CreateResource(t.Context(), q)
	var outcome *engine.WriteError
	if !errors.Is(err, injected) || !errors.As(err, &outcome) || outcome.Disposition != engine.WriteUnknown || result.Found || result.Replayed {
		t.Fatalf("failed rollback became a replay success: %+v %v", result, err)
	}
	replay, err := store.CreateResource(t.Context(), q)
	if err != nil || !replay.Found || !replay.Replayed {
		t.Fatalf("joined owner failed exact receipt retry: %+v %v", replay, err)
	}
}

func TestConfigurationReplayFailedRollbackRetainsOwnerUntilClose(t *testing.T) {
	db, store, cfg, scope, _ := remoteSettlementFixture(t)
	_, q := configurationInputFixture(t, db)
	if _, err := store.CreateResource(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	injected := engine.InjectUnjoinedRollbackForModuleTest(t, db)
	result, err := store.CreateResource(t.Context(), q)
	var outcome *engine.WriteError
	if !errors.Is(err, injected) || !errors.As(err, &outcome) || outcome.Disposition != engine.WriteUnknown || outcome.Settled || result.Found {
		t.Fatalf("unjoined rollback exposed a replay: %+v %v", result, err)
	}
	wait, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	if err := outcome.WaitSettlement(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined writer reported settlement: %v", err)
	}
	cancel()
	if err := db.Close(); err != nil {
		t.Fatalf("explicit physical join: %v", err)
	}
	if err := outcome.WaitSettlement(t.Context()); err != nil {
		t.Fatalf("original rollback did not observe join: %v", err)
	}
	cfg.Create = false
	reopened, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, err := coredata.New(reopened, scope)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := current.CreateResource(t.Context(), q)
	if err != nil || !replay.Found || !replay.Replayed {
		t.Fatalf("receipt after physical reopen: %+v %v", replay, err)
	}
}
