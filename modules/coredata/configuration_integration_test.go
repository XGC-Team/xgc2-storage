package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type configNamedFixture struct {
	ctx    context.Context
	store  *engine.Store
	config engine.Config
	scope  api.Scope
	token  api.Token
	domain model.ConfigurationDomainGuard
}

func openConfigurationNamed(t *testing.T, dev, late bool) *configNamedFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	g := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	init := func(ctx context.Context, tx *sql.Tx) error {
		if e := coredata.Initialize(ctx, tx); e != nil {
			return e
		}
		if e := coredata.DeclareConfigurationDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: g.SchemaIdentity, SchemaVersion: 1, RegistryDigest: g.RegistryDigest, MainVisibility: true}}); e != nil {
			return e
		}
		if late {
			_, e := tx.ExecContext(ctx, `CREATE TRIGGER late_named_result BEFORE INSERT ON named_results WHEN NEW.request_id='late-transport' BEGIN SELECT RAISE(ABORT,'late outer result fault'); END`)
			return e
		}
		return nil
	}
	execute := func(ctx context.Context, tx *sql.Tx, scope, op string, raw json.RawMessage) (json.RawMessage, error) {
		result, e := coredata.Execute(ctx, tx, scope, op, raw)
		if e != nil {
			return nil, e
		}
		// Fixture-only branch seed models the later explicit branch-create authority;
		// it is not a production named operation or consumer fallback.
		if dev && op == model.ResourceCreateOperation {
			var out model.ConfigurationMutationResult
			if e = json.Unmarshal(result, &out); e != nil {
				return nil, e
			}
			if !out.Replayed && out.Result.Head.Resource.ID == "resource" {
				b := out.Result.Head.Branch
				b.ID = "resource-dev"
				b.Name = "dev"
				b.NameKey = "dev"
				b.CreatedFromCommitID = b.HeadCommitID
				body, _ := json.Marshal(b)
				_, e = tx.ExecContext(ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", scope, g.Key, b.ID, b.ResourceID, b.NameKey, b.HeadCommitID, b.Revision, body)
				if e != nil {
					return nil, e
				}
			}
		}
		return result, nil
	}
	spec := coredata.Spec()
	n := api.Namespace{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 1, Modules: []api.Module{spec}, Collections: []api.Collection{{ID: "marker", MaxRecordBytes: 4096, MaxRecords: 100, MaxBytes: 1 << 20, Retention: "test point", Recovery: "test backup"}}}
	cfg := engine.Config{Path: filepath.Join(dir, "store.db"), Create: true, Manifest: api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{n}}, Modules: []engine.DataModule{{Spec: spec, Initialize: init, Execute: execute}}}
	store, e := engine.Open(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	f := &configNamedFixture{ctx: ctx, store: store, config: cfg, scope: api.Scope{Namespace: "core", User: "operator", Workspace: "station"}, domain: g}
	t.Cleanup(func() { f.store.Close() })
	snap, e := store.Snapshot(ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "marker", Keys: []string{"token"}}}})
	if e != nil {
		t.Fatal(e)
	}
	f.token = snap.Token
	return f
}
func (f *configNamedFixture) named(op, id string, q any) (api.NamedResponse, error) {
	return f.store.Named(f.ctx, api.NamedRequest{Scope: f.scope, DatabaseID: f.token.DatabaseID, Schema: coredata.Schema, Module: coredata.Spec().ID, Operation: op, RequestID: id, Payload: sessionWire(q)})
}
func configPrepared(t *testing.T, id, tag string, before []byte) model.PreparedConfigurationSnapshot {
	t.Helper()
	v, e := model.ValidateConfigurationManifest(model.ConfigurationManifest{Nodes: []model.ConfigurationManifestNode{{ID: "root", Kind: "root"}, {ID: "x", ParentID: "root", Name: "X", Kind: "document", DocumentKind: "test.data.v1", PayloadDigest: strings.Repeat(tag, 64)}}})
	if e != nil {
		t.Fatal(e)
	}
	var old model.ValidatedConfigurationManifest
	if before != nil {
		old, e = model.DecodeConfigurationManifest(before)
		if e != nil {
			t.Fatal(e)
		}
	}
	d, summary, e := model.DiffConfigurationManifests(old, v)
	if e != nil {
		t.Fatal(e)
	}
	manifest, _ := json.Marshal(model.ConfigurationManifest{Nodes: v.Nodes})
	return model.PreparedConfigurationSnapshot{RootDigest: v.RootDigest, Payload: []byte(`{ "identity":{"active":true},"body":{"value":"` + tag + `"} }`), Manifest: manifest, Change: model.ConfigurationChange{ID: id, Summary: summary, Nodes: d}}
}
func configCreate(t *testing.T, f *configNamedFixture, id string) model.ConfigurationResourceCreate {
	return model.ConfigurationResourceCreate{Domain: f.domain, Mutation: model.ConfigurationMutation{Key: "create-" + id, IntentDigest: strings.Repeat("a", 64), Actor: "tester"}, ResourceID: id, BranchID: id + "-main", CommitID: id + "-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}, Name: id, NameKey: id, Snapshot: configPrepared(t, id+"-change", "a", nil)}
}
func configResult(t *testing.T, r api.NamedResponse, e error) model.ConfigurationMutationResult {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
	if r.Receipt == nil {
		t.Fatal("write returned no commit receipt")
	}
	var out model.ConfigurationMutationResult
	if e = json.Unmarshal(r.Result, &out); e != nil {
		t.Fatal(e)
	}
	return out
}
func (f *configNamedFixture) read(t *testing.T, resource, branch, commit string) model.ConfigurationResourceSnapshot {
	t.Helper()
	r, e := f.named(model.ResourceSnapshotOperation, "read", model.ConfigurationResourceRead{Domain: f.domain, ResourceID: resource, Branch: branch, CommitID: commit})
	if e != nil {
		t.Fatal(e)
	}
	if r.Receipt != nil {
		t.Fatal("snapshot minted receipt")
	}
	var out model.ConfigurationResourceSnapshot
	if e = json.Unmarshal(r.Result, &out); e != nil {
		t.Fatal(e)
	}
	return out
}
func configCommit(t *testing.T, f *configNamedFixture, s model.ConfigurationResourceSnapshot, key, tag string) model.ConfigurationResourceCommit {
	h := s.Head
	return model.ConfigurationResourceCommit{Domain: f.domain, Mutation: model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("b", 64), Actor: "tester"}, ResourceID: h.Resource.ID, Branch: model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}, Main: s.CurrentMain.Branch, CommitID: key + "-commit", Name: h.Resource.Name, NameKey: h.Resource.NameKey, Snapshot: configPrepared(t, key+"-change", tag, s.Manifest)}
}

func TestConfigurationNamedConcurrentProductReceiptAndRestartTTL(t *testing.T) {
	f := openConfigurationNamed(t, false, false)
	a := configCreate(t, f, "resource")
	b := configCreate(t, f, "loser")
	b.Mutation = a.Mutation
	requests := []model.ConfigurationResourceCreate{a, b}
	results := make([]api.NamedResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.named(model.ResourceCreateOperation, fmt.Sprintf("attempt-%d", i), requests[i])
		}(i)
	}
	wg.Wait()
	one := configResult(t, results[0], errs[0])
	two := configResult(t, results[1], errs[1])
	if one.PlanDigest != two.PlanDigest || one.Result.Head != two.Result.Head || one.Replayed == two.Replayed {
		t.Fatal("concurrent miss did not preserve first plan/head")
	}
	winner := one.Result.Head.Resource.ID
	loser := "resource"
	if winner == loser {
		loser = "loser"
	}
	_, e := f.named(model.ResourceSnapshotOperation, "loser-read", model.ConfigurationResourceRead{Domain: f.domain, ResourceID: loser, Branch: "main"})
	requireSessionCode(t, e, "not_found")
	// A repeated transport identity with changed full bytes is still an outer
	// conflict, even though the product intent is the same.
	changed := requests[0]
	changed.CommitID = "different-allocated-id"
	_, e = f.named(model.ResourceCreateOperation, "attempt-0", changed)
	requireSessionCode(t, e, "conflict")
	time.Sleep(2100 * time.Millisecond)
	n, e := f.store.PruneExpiredReceipts(f.ctx, time.Now(), 100)
	if e != nil || n != 2 {
		t.Fatalf("prune=%d %v", n, e)
	}
	_, e = f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: "attempt-0"})
	requireSessionCode(t, e, "not_found")
	if e = f.store.Close(); e != nil {
		t.Fatal(e)
	}
	f.config.Create = false
	f.store, e = engine.Open(f.ctx, f.config)
	if e != nil {
		t.Fatal(e)
	}
	r, e := f.named(model.ConfigurationReceiptOperation, "business-receipt", model.ConfigurationReceipt{Domain: f.domain, Key: a.Mutation.Key, IntentDigest: a.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}})
	if e != nil || r.Receipt != nil {
		t.Fatalf("durable lookup %v", e)
	}
	var receipt model.ConfigurationMutationResult
	json.Unmarshal(r.Result, &receipt)
	if !receipt.Found || !receipt.Replayed || receipt.PlanDigest != one.PlanDigest || receipt.Result.Head != one.Result.Head {
		t.Fatal("business receipt expired or changed")
	}
	f.read(t, winner, "main", "")
	if e = f.store.Integrity(f.ctx); e != nil {
		t.Fatal(e)
	}
}
func TestConfigurationNamedLocalCASBranchesAndVisibilityFence(t *testing.T) {
	f := openConfigurationNamed(t, true, false)
	create := configCreate(t, f, "resource")
	r, e := f.named(model.ResourceCreateOperation, "create", create)
	configResult(t, r, e)
	main := f.read(t, "resource", "main", "")
	dev := f.read(t, "resource", "dev", "")
	q1 := configCommit(t, f, main, "main-write", "b")
	q2 := configCommit(t, f, dev, "dev-write", "c")
	// Domains with main visibility deliberately fence a concurrent main change.
	r, e = f.named(model.ResourceCommitOperation, "main-write", q1)
	first := configResult(t, r, e)
	_, e = f.named(model.ResourceCommitOperation, "stale-dev", q2)
	requireSessionCode(t, e, "conflict")
	freshDev := f.read(t, "resource", "dev", "")
	q2 = configCommit(t, f, freshDev, "dev-write", "c")
	r, e = f.named(model.ResourceCommitOperation, "fresh-dev", q2)
	second := configResult(t, r, e)
	if first.Result.Head.Commit.Version != "2" || second.Result.Head.Commit.Version != "3" || second.Result.Head.Resource.MainCommitID != q1.CommitID || second.Result.Head.Resource.NextVersion != "4" {
		t.Fatal("branch allocation/main identity drift")
	}
	// A generic record write changes outer scope revision without invalidating
	// any exact configuration point/manifest guard.
	base := f.read(t, "resource", "main", "")
	token, e := f.store.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "marker", Keys: []string{"unrelated"}}}})
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.store.Batch(f.ctx, api.BatchRequest{Scope: f.scope, Expected: token.Token, RequestID: "unrelated-write", Mutations: []api.Mutation{{Collection: "marker", Key: "unrelated", ExpectedVersion: "0", Data: json.RawMessage(`{"value":1}`)}}})
	if e != nil {
		t.Fatal(e)
	}
	qa := configCommit(t, f, base, "same-branch-a", "d")
	qb := configCommit(t, f, base, "same-branch-b", "e")
	qs := []model.ConfigurationResourceCommit{qa, qb}
	out := make([]api.NamedResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range qs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i], errs[i] = f.named(model.ResourceCommitOperation, qs[i].Mutation.Key, qs[i])
		}(i)
	}
	wg.Wait()
	wins := 0
	for i, e := range errs {
		if e == nil {
			configResult(t, out[i], e)
			wins++
		} else {
			requireSessionCode(t, e, "conflict")
			receipt, e := f.named(model.ConfigurationReceiptOperation, "loser-receipt", model.ConfigurationReceipt{Domain: f.domain, Key: qs[i].Mutation.Key, IntentDigest: qs[i].Mutation.IntentDigest, Operations: []string{model.ResourceCommitOperation}})
			if e != nil {
				t.Fatal(e)
			}
			var missing model.ConfigurationMutationResult
			json.Unmarshal(receipt.Result, &missing)
			if missing.Found {
				t.Fatal("stale loser wrote business receipt")
			}
		}
	}
	if wins != 1 {
		t.Fatal("same-branch CAS admitted wrong winner count")
	}
}
func TestConfigurationNamedOuterResultFailureRollsBackProduct(t *testing.T) {
	f := openConfigurationNamed(t, false, true)
	q := configCreate(t, f, "resource")
	_, e := f.named(model.ResourceCreateOperation, "late-transport", q)
	if e == nil {
		t.Fatal("late outer result fault succeeded")
	}
	_, e = f.store.Receipt(f.ctx, api.ReceiptRequest{Scope: f.scope, RequestID: "late-transport"})
	requireSessionCode(t, e, "not_found")
	r, e := f.named(model.ConfigurationReceiptOperation, "read-product", model.ConfigurationReceipt{Domain: f.domain, Key: q.Mutation.Key, IntentDigest: q.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}})
	if e != nil {
		t.Fatal(e)
	}
	var product model.ConfigurationMutationResult
	json.Unmarshal(r.Result, &product)
	if product.Found {
		t.Fatal("product survived failed outer commit")
	}
	_, e = f.named(model.ResourceSnapshotOperation, "read-resource", model.ConfigurationResourceRead{Domain: f.domain, ResourceID: q.ResourceID, Branch: "main"})
	requireSessionCode(t, e, "not_found")
	r, e = f.named(model.ResourceCreateOperation, "fresh-attempt", q)
	out := configResult(t, r, e)
	if out.Result.Head.Commit.Version != "1" || out.Result.Head.Resource.NextVersion != "2" {
		t.Fatal("late fault leaked allocation")
	}
}
