package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// store is an open owner with the Core data module on the "core" namespace.
type store struct {
	ctx    context.Context
	db     *engine.Store
	config engine.Config
	scope  api.Scope
	core   *coredata.Store
	domain model.ConfigurationDomainGuard
}

func coreManifest() api.Manifest {
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "core", Schema: coredata.Schema, MaxScopes: 8, MaxReceipts: 100, ReceiptTTLSeconds: 3600,
		Modules:     []string{model.Module},
		Collections: []api.Collection{{ID: "marker", MaxRecordBytes: 4096, MaxRecords: 100, MaxBytes: 1 << 20, Retention: "test point", Recovery: "test backup"}}}}}
}

func openStore(t *testing.T) *store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &store{ctx: ctx, scope: api.Scope{Namespace: "core", User: "operator", Workspace: "station"},
		domain: model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}}
	f.config = engine.Config{Path: filepath.Join(dir, "store.db"), Create: true, Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}}
	f.open(t)
	err := coredata.DeclareConfigurationDomains(ctx, f.db, []model.ConfigurationDomainDeclaration{{Key: f.domain.Key, SchemaIdentity: f.domain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: f.domain.RegistryDigest, MainVisibility: true}})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *store) open(t *testing.T) {
	t.Helper()
	db, err := engine.Open(f.ctx, f.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	core, err := coredata.New(db, f.scope)
	if err != nil {
		t.Fatal(err)
	}
	f.db, f.core = db, core
}

// reopen restarts the owner on the same file.
func (f *store) reopen(t *testing.T) {
	t.Helper()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.config.Create = false
	f.open(t)
}

// exec runs raw SQL in one owner transaction. It only seeds or inspects the
// isolated fixture; production code has no such door.
func (f *store) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *store) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// addBranch models the later explicit branch-create authority for a fixture.
func (f *store) addBranch(t *testing.T, from model.ConfigurationBranch, name, key string) {
	t.Helper()
	b := from
	b.ID, b.Name, b.NameKey = from.ResourceID+"-"+key, name, key
	b.CreatedFromCommitID = b.HeadCommitID
	body, _ := json.Marshal(b)
	f.exec(t, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", engine.ScopeID(f.scope), f.domain.Key, b.ID, b.ResourceID, b.NameKey, b.HeadCommitID, b.Revision, body)
}

func errCode(err error) string {
	var e *api.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if got := errCode(err); got != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
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

func (f *store) create(t *testing.T, id string) model.ConfigurationResourceCreate {
	return model.ConfigurationResourceCreate{Domain: f.domain, Mutation: model.ConfigurationMutation{Key: "create-" + id, IntentDigest: strings.Repeat("a", 64), Actor: "tester"}, ResourceID: id, BranchID: id + "-main", CommitID: id + "-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}, Name: id, NameKey: id, Snapshot: configPrepared(t, id+"-change", "a", nil)}
}

func (f *store) read(t *testing.T, resource, branch, commit string) model.ConfigurationResourceSnapshot {
	t.Helper()
	out, e := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: resource, Branch: branch, CommitID: commit})
	if e != nil {
		t.Fatal(e)
	}
	return out
}

func (f *store) commit(t *testing.T, s model.ConfigurationResourceSnapshot, key, tag string) model.ConfigurationResourceCommit {
	h := s.Head
	return model.ConfigurationResourceCommit{Domain: f.domain, Mutation: model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("b", 64), Actor: "tester"}, ResourceID: h.Resource.ID, Branch: model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}, Main: s.CurrentMain.Branch, CommitID: key + "-commit", Name: h.Resource.Name, NameKey: h.Resource.NameKey, Snapshot: configPrepared(t, key+"-change", tag, s.Manifest)}
}

// must returns v or fails the test through a panic, so that it can wrap a call
// that returns a result and an error.
func must[T any](v T, e error) T {
	if e != nil {
		panic(e)
	}
	return v
}

// must3 does the same for calls that also report whether they created the result.
func must3[T any](v T, _ bool, e error) T { return must(v, e) }

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestCanonicalBranchSelector(t *testing.T) {
	f := openStore(t)
	create := f.create(t, "resource")
	first := must(f.core.CreateResource(f.ctx, create))
	display, key, e := model.NormalizeConfigurationName(model.ConfigurationBranchName, "Review")
	if e != nil {
		t.Fatal(e)
	}
	f.addBranch(t, first.Result.Head.Branch, display, key)
	selected := f.read(t, create.ResourceID, key, "")
	if selected.Head.Branch.Name != display || selected.Head.Branch.NameKey != key || selected.Head.Branch.HeadCommitID != selected.Head.Commit.ID {
		t.Fatal("canonical selector did not identify display branch")
	}
	_, e = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: create.ResourceID, Branch: display})
	requireCode(t, e, "invalid_argument")
	_, e = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: create.ResourceID, Branch: selected.Head.Branch.ID})
	requireCode(t, e, "not_found")
	result := must(f.core.CommitResource(f.ctx, f.commit(t, selected, "review-noop", "a")))
	if result.Replayed || result.Result.Disposition != "noop" || result.Result.Head.Branch != selected.Head.Branch || result.Result.Head.Commit != selected.Head.Commit {
		t.Fatal("fresh noop changed selected immutable/branch facts")
	}
}

func TestSharedHeadTrackingBranchBlocksRemoval(t *testing.T) {
	f := openStore(t)
	create := f.create(t, "resource")
	self := model.ConfigurationReference{Slot: "self", Mode: "tracking", TargetDomain: f.domain.Key, TargetResourceID: create.ResourceID, TargetBranch: "main", TargetComponentID: "x"}
	create.Snapshot.References = []model.ConfigurationReference{self}
	created := must(f.core.CreateResource(f.ctx, create))
	f.addBranch(t, created.Result.Head.Branch, "other", "other")
	main := f.read(t, create.ResourceID, "main", "")
	other := f.read(t, create.ResourceID, "other", "")
	if main.Head.Commit.ID != other.Head.Commit.ID || other.Head.Branch.CreatedFromCommitID != main.Head.Commit.ID || len(other.References) != 1 || other.References[0] != self {
		t.Fatal("fixture branch did not inherit the exact immutable tracking set")
	}
	before, e := model.DecodeConfigurationManifest(main.Manifest)
	if e != nil {
		t.Fatal(e)
	}
	manifest := model.ConfigurationManifest{Nodes: []model.ConfigurationManifestNode{{ID: "root", Kind: "root"}}}
	after, e := model.ValidateConfigurationManifest(manifest)
	if e != nil {
		t.Fatal(e)
	}
	changes, summary, e := model.DiffConfigurationManifests(before, after)
	if e != nil {
		t.Fatal(e)
	}
	manifestBytes, e := json.Marshal(manifest)
	if e != nil {
		t.Fatal(e)
	}
	h := main.Head
	q := model.ConfigurationResourceCommit{
		Domain: f.domain, Mutation: model.ConfigurationMutation{Key: "remove-x", IntentDigest: strings.Repeat("b", 64), Actor: "tester"},
		ResourceID: h.Resource.ID, Branch: model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}, Main: main.CurrentMain.Branch,
		CommitID: "remove-x-commit", Name: h.Resource.Name, NameKey: h.Resource.NameKey,
		Snapshot: model.PreparedConfigurationSnapshot{RootDigest: after.RootDigest, Payload: []byte(`{"identity":{"active":true},"body":{}}`), Manifest: manifestBytes, Change: model.ConfigurationChange{ID: "remove-x-change", Summary: summary, Nodes: changes}},
	}
	state := func() string {
		out := ""
		for _, table := range []string{"core_resources", "core_branches", "core_snapshots", "core_references", "core_changes", "core_configuration_receipts", "receipts"} {
			out += fmt.Sprint(table, "=", f.count(t, table), ";")
		}
		var rows, bytes int64
		if e := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, "SELECT coalesce(sum(rows),0),coalesce(sum(bytes),0) FROM core_data_usage").Scan(&rows, &bytes)
		}); e != nil {
			t.Fatal(e)
		}
		return out + fmt.Sprint(rows, "/", bytes)
	}
	persisted := state()
	_, e = f.core.CommitResource(f.ctx, q)
	requireCode(t, e, "failed_precondition")
	if state() != persisted {
		t.Fatal("inherited live-source rejection left rows, receipts or quota")
	}
	if f.read(t, create.ResourceID, "main", "").Head != main.Head || f.read(t, create.ResourceID, "other", "").Head != other.Head {
		t.Fatal("inherited live-source rejection changed pointers or counters")
	}
}

func TestConcurrentProductReceiptAndRestart(t *testing.T) {
	f := openStore(t)
	a := f.create(t, "resource")
	b := f.create(t, "loser")
	b.Mutation = a.Mutation
	requests := []model.ConfigurationResourceCreate{a, b}
	results := make([]model.ConfigurationMutationResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = f.core.CreateResource(f.ctx, requests[i])
		}(i)
	}
	wg.Wait()
	one, two := must(results[0], errs[0]), must(results[1], errs[1])
	if one.PlanDigest != two.PlanDigest || one.Result.Head != two.Result.Head || one.Replayed == two.Replayed {
		t.Fatal("concurrent miss did not preserve first plan/head")
	}
	winner := one.Result.Head.Resource.ID
	loser := "resource"
	if winner == loser {
		loser = "loser"
	}
	_, e := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: loser, Branch: "main"})
	requireCode(t, e, "not_found")
	if f.count(t, "receipts") != 0 {
		t.Fatal("an in-process call minted a transport receipt")
	}
	f.reopen(t)
	receipt := must(f.core.Receipt(f.ctx, model.ConfigurationReceipt{Domain: f.domain, Key: a.Mutation.Key, IntentDigest: a.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}}))
	if !receipt.Found || !receipt.Replayed || receipt.PlanDigest != one.PlanDigest || receipt.Result.Head != one.Result.Head {
		t.Fatal("business receipt changed across restart")
	}
	f.read(t, winner, "main", "")
	if e = f.db.Integrity(f.ctx); e != nil {
		t.Fatal(e)
	}
}

func TestLocalCASBranchesAndVisibilityFence(t *testing.T) {
	f := openStore(t)
	create := f.create(t, "resource")
	created := must(f.core.CreateResource(f.ctx, create))
	f.addBranch(t, created.Result.Head.Branch, "dev", "dev")
	main := f.read(t, "resource", "main", "")
	dev := f.read(t, "resource", "dev", "")
	q1 := f.commit(t, main, "main-write", "b")
	q2 := f.commit(t, dev, "dev-write", "c")
	// Domains with main visibility deliberately fence a concurrent main change.
	first := must(f.core.CommitResource(f.ctx, q1))
	_, e := f.core.CommitResource(f.ctx, q2)
	requireCode(t, e, "conflict")
	freshDev := f.read(t, "resource", "dev", "")
	q2 = f.commit(t, freshDev, "dev-write", "c")
	second := must(f.core.CommitResource(f.ctx, q2))
	if first.Result.Head.Commit.Version != "2" || second.Result.Head.Commit.Version != "3" || second.Result.Head.Resource.MainCommitID != q1.CommitID || second.Result.Head.Resource.NextVersion != "4" {
		t.Fatal("branch allocation/main identity drift")
	}
	// A document write changes the scope revision without invalidating any
	// exact configuration point/manifest guard.
	base := f.read(t, "resource", "main", "")
	token := must(f.db.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "marker", Keys: []string{"unrelated"}}}}))
	if _, e = f.db.Batch(f.ctx, api.BatchRequest{Scope: f.scope, Expected: token.Token, RequestID: "unrelated-write", Mutations: []api.Mutation{{Collection: "marker", Key: "unrelated", ExpectedVersion: "0", Data: json.RawMessage(`{"value":1}`)}}}); e != nil {
		t.Fatal(e)
	}
	qs := []model.ConfigurationResourceCommit{f.commit(t, base, "same-branch-a", "d"), f.commit(t, base, "same-branch-b", "e")}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range qs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.core.CommitResource(f.ctx, qs[i])
		}(i)
	}
	wg.Wait()
	wins := 0
	for i, e := range errs {
		if e == nil {
			wins++
			continue
		}
		requireCode(t, e, "conflict")
		missing := must(f.core.Receipt(f.ctx, model.ConfigurationReceipt{Domain: f.domain, Key: qs[i].Mutation.Key, IntentDigest: qs[i].Mutation.IntentDigest, Operations: []string{model.ResourceCommitOperation}}))
		if missing.Found {
			t.Fatal("stale loser wrote business receipt")
		}
	}
	if wins != 1 {
		t.Fatal("same-branch CAS admitted wrong winner count")
	}
}

func TestBranchAndArchiveLifecycle(t *testing.T) {
	f := openStore(t)
	must(f.core.CreateResource(f.ctx, f.create(t, "target")))
	main := f.read(t, "target", "main", "")
	meta := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("c", 64), Actor: "tester"}
	}
	fork := model.ConfigurationBranchCreate{Domain: f.domain, Mutation: meta("fork"), ResourceID: "target", ExpectedResourceRevision: main.Head.Resource.Revision, Main: main.CurrentMain.Branch, ID: "review-v1", Name: "Review", FromCommitID: main.Head.Commit.ID, FromContentDigest: main.Head.Commit.ContentDigest}
	first := must(f.core.CreateBranch(f.ctx, fork))
	if first.Result.Head.Branch.NameKey != "review" || first.Result.Head.Branch.HeadCommitID != main.Head.Commit.ID {
		t.Fatal("fork did not retain exact shared head")
	}
	losing := fork
	losing.ID = "losing-branch"
	losing.FromContentDigest = strings.Repeat("f", 64)
	replay := must(f.core.CreateBranch(f.ctx, losing))
	if !replay.Replayed || replay.PlanDigest != first.PlanDigest || replay.Result.Head.Branch.ID != fork.ID {
		t.Fatal("product replay re-evaluated mutable pins")
	}
	branch := f.read(t, "target", "review", "")
	linked := f.create(t, "linked")
	linked.Snapshot.References = []model.ConfigurationReference{{Slot: "target", Mode: "tracking", TargetDomain: f.domain.Key, TargetResourceID: "target", TargetBranch: "review", TargetComponentID: "x"}}
	must(f.core.CreateResource(f.ctx, linked))
	guard := model.ConfigurationBranchGuard{ID: branch.Head.Branch.ID, ExpectedRevision: branch.Head.Branch.Revision, CommitID: branch.Head.Commit.ID, ContentDigest: branch.Head.Commit.ContentDigest}
	archive := model.ConfigurationBranchArchive{Domain: f.domain, Mutation: meta("archive-branch"), ResourceID: "target", Main: main.CurrentMain.Branch, Branch: guard}
	_, e := f.core.ArchiveBranch(f.ctx, archive)
	requireCode(t, e, "failed_precondition")
	current := f.read(t, "linked", "main", "")
	state := model.ConfigurationResourceState{Domain: f.domain, Mutation: meta("archive-linked"), ResourceID: "linked", ExpectedRevision: current.Head.Resource.Revision, Main: current.CurrentMain.Branch, Archived: true}
	archivedLinked := must(f.core.SetResourceState(f.ctx, state))
	archived := must(f.core.ArchiveBranch(f.ctx, archive))
	if archived.Result.Head.Branch.Revision != "2" || archived.Result.Head.Branch.ArchivedAt == "" {
		t.Fatal("archive did not advance branch")
	}
	fork.ID = "review-fork"
	fork.Mutation = meta("fork-again")
	second := must(f.core.CreateBranch(f.ctx, fork))
	if second.Result.Head.Branch.ID == first.Result.Head.Branch.ID {
		t.Fatal("archived name did not become reusable")
	}
	if f.read(t, "target", "review", "").Head.Branch.ID != fork.ID {
		t.Fatal("canonical selector did not select live replacement")
	}
	targetState := model.ConfigurationResourceState{Domain: f.domain, Mutation: meta("archive-target"), ResourceID: "target", ExpectedRevision: main.Head.Resource.Revision, Main: main.CurrentMain.Branch, Archived: true}
	archivedTarget := must(f.core.SetResourceState(f.ctx, targetState))
	state.ExpectedRevision = archivedLinked.Result.Head.Resource.Revision
	state.Archived = false
	state.Mutation = meta("restore-linked")
	_, e = f.core.SetResourceState(f.ctx, state)
	requireCode(t, e, "not_found")
	targetState.ExpectedRevision = archivedTarget.Result.Head.Resource.Revision
	targetState.Archived = false
	targetState.Mutation = meta("restore-target")
	must(f.core.SetResourceState(f.ctx, targetState))
	restored := must(f.core.SetResourceState(f.ctx, state))
	if restored.Result.Head.Resource.ArchivedAt != "" || restored.Result.Head.Resource.Revision != "3" {
		t.Fatal("restoration was not atomic")
	}
}

func TestFailedOperationRollsBackEveryFact(t *testing.T) {
	f := openStore(t)
	f.exec(t, `CREATE TRIGGER reject_product_receipt BEFORE INSERT ON core_configuration_receipts WHEN NEW.mutation_key='create-resource' BEGIN SELECT RAISE(ABORT,'late receipt fault'); END`)
	q := f.create(t, "resource")
	if _, e := f.core.CreateResource(f.ctx, q); e == nil {
		t.Fatal("late fault succeeded")
	}
	for _, table := range []string{"core_resources", "core_branches", "core_snapshots", "core_changes", "core_configuration_receipts"} {
		if n := f.count(t, table); n != 0 {
			t.Fatalf("failed operation left %d rows in %s", n, table)
		}
	}
	var rows int64
	f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT coalesce(sum(rows),0) FROM core_data_usage").Scan(&rows)
	})
	if rows != 0 {
		t.Fatalf("failed operation kept %d quota rows", rows)
	}
	f.exec(t, "DROP TRIGGER reject_product_receipt")
	if out := must(f.core.CreateResource(f.ctx, q)); out.Result.Head.Commit.Version != "1" || out.Result.Head.Resource.NextVersion != "2" {
		t.Fatal("late fault leaked allocation")
	}
}

// Every exported configuration function reaches the right operation: the
// behavior itself is covered where it is implemented, this walks the typed API.
func TestEveryConfigurationFunctionIsWired(t *testing.T) {
	f := openStore(t)
	root := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
	meta := func(key, tag string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat(tag, 64), Actor: "tester"}
	}
	created := must(f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: meta("ns", "1"), ID: "source", ExpectedRevision: "0", Parent: &root, Name: "Source"}))
	renamed := must(f.core.UpdateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: meta("ns-rename", "2"), ID: "source", ExpectedRevision: created.Namespace.Revision, Name: "Origin"}))
	if renamed.Namespace.Name != "Origin" || renamed.Namespace.Revision != "2" {
		t.Fatalf("update: %+v", renamed)
	}
	spare := must(f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: meta("spare", "3"), ID: "spare", ExpectedRevision: "0", Parent: &root, Name: "Spare"}))
	archived := must(f.core.SetNamespaceState(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: meta("spare-archive", "4"), ID: "spare", ExpectedRevision: spare.Namespace.Revision, Archived: true}))
	if archived.Namespace.ArchivedAt == "" {
		t.Fatalf("state: %+v", archived)
	}

	source := f.create(t, "r")
	source.Namespace = model.ConfigurationNamespaceGuard{ID: "source", ExpectedRevision: "2"}
	head := must(f.core.CreateResource(f.ctx, source)).Result.Head
	protect := must(f.core.UpdateResourceMetadata(f.ctx, model.ConfigurationResourceMetadata{Domain: f.domain, Mutation: meta("protect", "5"), ResourceID: "r", ExpectedRevision: head.Resource.Revision,
		Main: model.ConfigurationBranchGuard{ID: head.Branch.ID, ExpectedRevision: head.Branch.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest}, Protect: true}))
	if !protect.Result.Head.Resource.System {
		t.Fatalf("metadata: %+v", protect.Result.Head.Resource)
	}
	head = protect.Result.Head

	copyOf := f.create(t, "copy-r")
	copyOf.Name, copyOf.NameKey = head.Resource.Name, head.Resource.NameKey
	copyOf.Namespace = model.ConfigurationNamespaceGuard{ID: "copy-ns", ExpectedRevision: "1"}
	copyOf.Source = &model.ConfigurationSourcePin{ResourceID: head.Resource.ID, ExpectedResourceRevision: head.Resource.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest,
		Main: model.ConfigurationBranchGuard{ID: head.Branch.ID, ExpectedRevision: head.Branch.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest}}
	clone := model.ConfigurationNamespaceClone{Domain: f.domain, Mutation: meta("clone", "6"), SourceID: "source", ExpectedRevision: renamed.Namespace.Revision, TargetParent: root, Name: "Copy",
		Namespaces: []model.ConfigurationNamespaceMapping{{SourceID: "source", ExpectedRevision: renamed.Namespace.Revision, TargetID: "copy-ns"}}, Resources: []model.ConfigurationResourceCreate{copyOf}}
	cloned := must(f.core.CloneNamespace(f.ctx, clone))
	if cloned.Namespace.ID != "copy-ns" {
		t.Fatalf("clone: %+v", cloned)
	}
	replay := must(f.core.CloneReceipt(f.ctx, model.ConfigurationNamespaceCloneReceipt{Domain: f.domain, Mutation: clone.Mutation}))
	if !replay.Replayed || replay.Namespace.ID != "copy-ns" {
		t.Fatalf("clone receipt: %+v", replay)
	}

	tree := must(f.core.NamespaceTree(f.ctx, model.NamespaceRead{Domain: f.domain.Key, ID: "copy-ns"}))
	if len(tree.Namespaces) != 1 || len(tree.Resources) != 1 || tree.Resources[0].ID != "copy-r" {
		t.Fatalf("tree: %+v", tree)
	}
	namespaces := must(f.core.Namespaces(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain}))
	resources := must(f.core.Resources(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain}))
	branches := must(f.core.Branches(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain, ID: "copy-r"}))
	commits := must(f.core.Commits(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain, ID: "copy-r"}))
	changes := must(f.core.Changes(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain, ID: "copy-r"}))
	if len(namespaces) != 2 || len(resources) != 2 || len(branches) != 1 || len(commits) != 1 || len(changes) != 1 {
		t.Fatalf("catalog: %d namespaces, %d resources, %d branches, %d commits, %d changes", len(namespaces), len(resources), len(branches), len(commits), len(changes))
	}
	if commits[0].SourceCommitID != head.Commit.ID || resources[0].Name == "" {
		t.Fatalf("catalog content: %+v %+v", commits[0], resources[0])
	}
	// A resource that tracks another shows up as its incoming reference.
	target := f.create(t, "target")
	must(f.core.CreateResource(f.ctx, target))
	linked := f.create(t, "linked")
	linked.Snapshot.References = []model.ConfigurationReference{{Slot: "target", Mode: "tracking", TargetDomain: f.domain.Key, TargetResourceID: "target", TargetBranch: "main", TargetComponentID: "x"}}
	must(f.core.CreateResource(f.ctx, linked))
	incoming := must(f.core.IncomingReferences(f.ctx, model.ConfigurationIncomingRead{Domain: f.domain, ResourceID: "target"}))
	if len(incoming) != 1 || incoming[0].SourceResourceID != "linked" {
		t.Fatalf("incoming: %+v", incoming)
	}
}
