package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func configurationFixture(t *testing.T) (*sql.DB, context.Context, model.ConfigurationDomainGuard) {
	t.Helper()
	db, ctx := fixture(t)
	g := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if e = DeclareConfigurationDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: g.SchemaIdentity, SchemaVersion: g.SchemaVersion, RegistryDigest: g.RegistryDigest, MainVisibility: true}}); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	return db, ctx, g
}
func configurationSnapshotForTest(t *testing.T, id, tag string, before model.ValidatedConfigurationManifest, component bool, refs []model.ConfigurationReference) model.PreparedConfigurationSnapshot {
	t.Helper()
	nodes := []model.ConfigurationManifestNode{{ID: "root", Kind: "root"}}
	if component {
		nodes = append(nodes, model.ConfigurationManifestNode{ID: "x", ParentID: "root", Name: "X", Kind: "document", DocumentKind: "test.data.v1", PayloadDigest: strings.Repeat(tag, 64)})
	}
	v, e := model.ValidateConfigurationManifest(model.ConfigurationManifest{Nodes: nodes})
	if e != nil {
		t.Fatal(e)
	}
	manifest, _ := json.Marshal(model.ConfigurationManifest{Nodes: v.Nodes})
	changes, summary, e := model.DiffConfigurationManifests(before, v)
	if e != nil {
		t.Fatal(e)
	}
	return model.PreparedConfigurationSnapshot{RootDigest: v.RootDigest, Payload: []byte(fmt.Sprintf("{ \"identity\": {\"active\":true}, \"body\":{\"value\":\"%s\"} }", tag)), Manifest: manifest, References: refs, Change: model.ConfigurationChange{ID: id, Summary: summary, Nodes: changes}}
}
func configurationCreateForTest(t *testing.T, g model.ConfigurationDomainGuard, id string, refs []model.ConfigurationReference) model.ConfigurationResourceCreate {
	return model.ConfigurationResourceCreate{Domain: g, Mutation: model.ConfigurationMutation{Key: "create-" + id, IntentDigest: strings.Repeat("a", 64), Actor: "tester"}, ResourceID: id, BranchID: id + "-main", CommitID: id + "-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}, Name: id, NameKey: id, Snapshot: configurationSnapshotForTest(t, id+"-change1", "a", model.ValidatedConfigurationManifest{}, true, refs)}
}
func configurationWrite(t *testing.T, db *sql.DB, ctx context.Context, op string, q any) model.ConfigurationMutationResult {
	t.Helper()
	raw, e := run(t, db, ctx, op, q)
	if e != nil {
		t.Fatal(e)
	}
	var out model.ConfigurationMutationResult
	if e = json.Unmarshal(raw, &out); e != nil {
		t.Fatal(e)
	}
	if !out.Found {
		t.Fatal("missing product receipt result")
	}
	return out
}
func configurationCommitForTest(t *testing.T, g model.ConfigurationDomainGuard, out model.ConfigurationMutationResult, key, tag string, component bool, refs []model.ConfigurationReference, before []byte) model.ConfigurationResourceCommit {
	t.Helper()
	v, e := model.DecodeConfigurationManifest(before)
	if e != nil {
		t.Fatal(e)
	}
	h := out.Result.Head
	guard := model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}
	return model.ConfigurationResourceCommit{Domain: g, Mutation: model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("b", 64), Actor: "tester"}, ResourceID: h.Resource.ID, Branch: guard, Main: guard, CommitID: key + "-commit", Name: h.Resource.Name, NameKey: h.Resource.NameKey, Snapshot: configurationSnapshotForTest(t, key+"-change", tag, v, component, refs)}
}
func configurationCode(t *testing.T, e error, code string) {
	t.Helper()
	var a *api.Error
	if !errors.As(e, &a) || a.Code != code {
		t.Fatalf("want %s got %v", code, e)
	}
}
func configurationCounts(t *testing.T, db *sql.DB, ctx context.Context) string {
	t.Helper()
	var rows, bytes int64
	if e := db.QueryRowContext(ctx, "SELECT rows,bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&rows, &bytes); e != nil && !errors.Is(e, sql.ErrNoRows) {
		t.Fatal(e)
	}
	return fmt.Sprint(count(t, db, ctx, "core_resources"), count(t, db, ctx, "core_branches"), count(t, db, ctx, "core_snapshots"), count(t, db, ctx, "core_references"), count(t, db, ctx, "core_changes"), count(t, db, ctx, "core_configuration_receipts"), rows, bytes)
}
func TestConfigurationAtomicReplayCASAndBytes(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	q := configurationCreateForTest(t, g, "resource", nil)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	wantPlan, _ := model.ConfigurationCreatePlanDigest(q)
	if first.PlanDigest != wantPlan || first.Result.Head.Resource.NextVersion != "2" {
		t.Fatal(first)
	}
	raw, e := run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: q.ResourceID, Branch: "main"})
	if e != nil {
		t.Fatal(e)
	}
	var snap model.ConfigurationResourceSnapshot
	json.Unmarshal(raw, &snap)
	if string(snap.Payload) != string(q.Snapshot.Payload) || string(snap.Manifest) != string(q.Snapshot.Manifest) || snap.CurrentMain.Branch.CommitID != q.CommitID {
		t.Fatal("frozen bytes/main pin drift")
	}
	before := configurationCounts(t, db, ctx)
	loser := q
	loser.ResourceID = "loser"
	loser.BranchID = "loser-main"
	loser.CommitID = "loser-v1"
	loser.Namespace = model.ConfigurationNamespaceGuard{ID: "absent", ExpectedRevision: "99"}
	loser.Snapshot.Payload = []byte(`invalid loser bytes`)
	replay := configurationWrite(t, db, ctx, model.ResourceCreateOperation, loser)
	if !replay.Replayed || replay.PlanDigest != first.PlanDigest || replay.Result.Head.Commit.ID != q.CommitID || before != configurationCounts(t, db, ctx) {
		t.Fatal("product receipt did not precede loser guards/bytes")
	}
	loser.Mutation.IntentDigest = strings.Repeat("c", 64)
	_, e = run(t, db, ctx, model.ResourceCreateOperation, loser)
	configurationCode(t, e, "conflict")
	commit := configurationCommitForTest(t, g, first, "write-two", "b", true, nil, q.Snapshot.Manifest)
	second := configurationWrite(t, db, ctx, model.ResourceCommitOperation, commit)
	if second.Result.Head.Commit.Version != "2" || second.Result.Head.Resource.NextVersion != "3" {
		t.Fatal(second)
	}
	stale := commit
	stale.Mutation.Key = "stale"
	stale.CommitID = "stale-commit"
	stale.Snapshot.Change.ID = "stale-change"
	before = configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, stale)
	configurationCode(t, e, "conflict")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("stale CAS drift")
	}
}
func TestConfigurationC1NoopLocationAndLateReceiptRollback(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	q := configurationCreateForTest(t, g, "resource", nil)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	no := configurationCommitForTest(t, g, first, "noop", "a", true, nil, q.Snapshot.Manifest)
	before := configurationCounts(t, db, ctx)
	bad := no
	bad.Name = "renamed"
	bad.NameKey = "renamed"
	bad.ExpectedResourceRevision = "1"
	_, e := run(t, db, ctx, model.ResourceCommitOperation, bad)
	configurationCode(t, e, "invalid_argument")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("C1 wrote state")
	}
	noop := configurationWrite(t, db, ctx, model.ResourceCommitOperation, no)
	if noop.Result.Disposition != "noop" || noop.Result.Head != first.Result.Head {
		t.Fatal("noop changed persisted metadata")
	}
	execSQL(t, db, ctx, "INSERT INTO core_namespaces VALUES(?,?,?,NULL,?,?,1,0,?)", testScope, g.Key, "destination", "Destination", "destination", []byte(`{}`))
	move := no
	move.Mutation.Key = "move"
	move.MoveTo = &model.ConfigurationNamespaceGuard{ID: "destination", ExpectedRevision: "1"}
	move.ExpectedResourceRevision = "1"
	move.Snapshot.Change.ID = "move-change"
	moved := configurationWrite(t, db, ctx, model.ResourceCommitOperation, move)
	if moved.Result.Disposition != "identity-only" || moved.Result.Head.Resource.NamespaceID != "destination" || moved.Result.Head.Resource.Revision != "2" || moved.Result.Head.Commit != first.Result.Head.Commit || moved.Result.Head.Branch != first.Result.Head.Branch {
		t.Fatal("move changed immutable bytes/version/branch")
	}
	execSQL(t, db, ctx, `CREATE TRIGGER reject_product_receipt BEFORE INSERT ON core_configuration_receipts WHEN NEW.mutation_key='late' BEGIN SELECT RAISE(ABORT,'late receipt fault'); END`)
	late := configurationCommitForTest(t, g, moved, "late", "b", true, nil, q.Snapshot.Manifest)
	before = configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, late)
	if e == nil {
		t.Fatal("late fault succeeded")
	}
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("late receipt failure leaked quota/rows")
	}
	var main string
	db.QueryRowContext(ctx, "SELECT main_commit_id FROM core_resources WHERE id=?", q.ResourceID).Scan(&main)
	if main != q.CommitID {
		t.Fatal("late failure leaked pointer")
	}
}
func TestConfigurationC2PostStateAndOtherLiveSource(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	self := model.ConfigurationReference{Slot: "self", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: "resource", TargetBranch: "main", TargetComponentID: "x"}
	q := configurationCreateForTest(t, g, "resource", []model.ConfigurationReference{self})
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	retained := configurationCommitForTest(t, g, first, "retain-self", "b", false, []model.ConfigurationReference{self}, q.Snapshot.Manifest)
	before := configurationCounts(t, db, ctx)
	_, e := run(t, db, ctx, model.ResourceCommitOperation, retained)
	configurationCode(t, e, "not_found")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("invalid new self reference committed")
	}
	deleted := retained
	deleted.Mutation.Key = "delete-self"
	deleted.CommitID = "delete-self-commit"
	deleted.Snapshot.Change.ID = "delete-self-change"
	deleted.Snapshot.References = nil
	second := configurationWrite(t, db, ctx, model.ResourceCommitOperation, deleted)
	restore := configurationCommitForTest(t, g, second, "restore-x", "c", true, nil, deleted.Snapshot.Manifest)
	third := configurationWrite(t, db, ctx, model.ResourceCommitOperation, restore)
	other := configurationCreateForTest(t, g, "other", []model.ConfigurationReference{self})
	other.Snapshot.References[0].Slot = "target"
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, other)
	remove := configurationCommitForTest(t, g, third, "blocked", "d", false, nil, restore.Snapshot.Manifest)
	before = configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, remove)
	configurationCode(t, e, "failed_precondition")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("other live source blocker bypassed")
	}
	pinned := configurationCreateForTest(t, g, "bad-pin", []model.ConfigurationReference{{Slot: "pin", Mode: "pinned", TargetDomain: g.Key, TargetResourceID: q.ResourceID, TargetBranch: "main", TargetCommitID: q.CommitID, TargetVersion: "99", TargetRootDigest: q.Snapshot.RootDigest}})
	_, e = run(t, db, ctx, model.ResourceCreateOperation, pinned)
	configurationCode(t, e, "conflict")
}

func TestConfigurationSameCommitOtherBranchStillBlocksAndBounds(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	self := model.ConfigurationReference{Slot: "self", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: "resource", TargetBranch: "main", TargetComponentID: "x"}
	q := configurationCreateForTest(t, g, "resource", []model.ConfigurationReference{self})
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	b := first.Result.Head.Branch
	b.ID = "resource-dev"
	b.Name = "dev"
	b.NameKey = "dev"
	body, _ := json.Marshal(b)
	execSQL(t, db, ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", testScope, g.Key, b.ID, b.ResourceID, b.NameKey, b.HeadCommitID, b.Revision, body)
	remove := configurationCommitForTest(t, g, first, "other-branch-block", "b", false, nil, q.Snapshot.Manifest)
	before := configurationCounts(t, db, ctx)
	_, e := run(t, db, ctx, model.ResourceCommitOperation, remove)
	configurationCode(t, e, "failed_precondition")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("shared-head different branch was excluded")
	}
	tooMany := configurationCommitForTest(t, g, first, "too-many-noop", "a", true, nil, q.Snapshot.Manifest)
	tooMany.Snapshot.References = make([]model.ConfigurationReference, model.MaxConfigurationReferences+1)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, tooMany)
	configurationCode(t, e, "resource_exhausted")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("noop bound failure wrote rows/quota")
	}
	bad := configurationCreateForTest(t, g, "bad-manifest", nil)
	bad.Snapshot.Change.Nodes = nil
	_, e = run(t, db, ctx, model.ResourceCreateOperation, bad)
	configurationCode(t, e, "invalid_argument")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("incomplete audit accepted")
	}
	bad = configurationCreateForTest(t, g, "duplicate-identity", nil)
	bad.Snapshot.Payload = []byte(`{"identity":{"active":true},"identity":{"active":false},"body":{}}`)
	_, e = run(t, db, ctx, model.ResourceCreateOperation, bad)
	configurationCode(t, e, "invalid_argument")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("ambiguous envelope accepted")
	}
}
func TestConfigurationReferenceSharedHeadOwnership(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	q := configurationCreateForTest(t, g, "target", nil)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	b := first.Result.Head.Branch
	b.ID, b.Name, b.NameKey = "target-dev", "dev", "dev"
	body, _ := json.Marshal(b)
	execSQL(t, db, ctx, "INSERT INTO core_branches VALUES(?,?,?,?,?,?,?,?)", testScope, g.Key, b.ID, b.ResourceID, b.NameKey, b.HeadCommitID, b.Revision, body)
	refs := []model.ConfigurationReference{
		{Slot: "tracking", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: q.ResourceID, TargetBranch: "dev", TargetComponentID: "x"},
		{Slot: "pinned", Mode: "pinned", TargetDomain: g.Key, TargetResourceID: q.ResourceID, TargetBranch: "dev", TargetComponentID: "x", TargetCommitID: q.CommitID, TargetVersion: "1", TargetRootDigest: q.Snapshot.RootDigest},
	}
	source := configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, g, "source", refs))
	if source.Result.Head.Commit.Version != "1" {
		t.Fatal("shared owned immutable head was not accepted")
	}
	// Both individual FKs and the snapshot body agree, but its original branch
	// belongs to another aggregate. Neither live selector nor exact pins bypass it.
	c := first.Result.Head.Commit
	c.BranchID = source.Result.Head.Branch.ID
	body, _ = json.Marshal(c)
	execSQL(t, db, ctx, "UPDATE core_snapshots SET branch_id=?,body=? WHERE scope=? AND domain=? AND id=?", c.BranchID, body, testScope, g.Key, c.ID)
	for _, ref := range refs {
		before := configurationCounts(t, db, ctx)
		bad := configurationCreateForTest(t, g, "bad-"+ref.Mode, []model.ConfigurationReference{ref})
		_, e := run(t, db, ctx, model.ResourceCreateOperation, bad)
		configurationCode(t, e, "data_loss")
		if before != configurationCounts(t, db, ctx) {
			t.Fatal("invalid reference owner left rows or quota")
		}
	}
}

func TestConfigurationTrackingAndPinnedModeSeparation(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	logical := model.ConfigurationReference{Slot: "self", Mode: "tracking", TargetDomain: g.Key, TargetResourceID: "resource", TargetBranch: "main", TargetComponentID: "x"}
	q := configurationCreateForTest(t, g, "resource", []model.ConfigurationReference{logical})
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	content, e := model.ConfigurationContentDigest(q.Snapshot.Payload, q.Snapshot.Manifest, q.Snapshot.References)
	if e != nil || content != first.Result.Head.Commit.ContentDigest {
		t.Fatal("logical references changed during resolution/hash", e)
	}
	raw, e := run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: q.ResourceID, Branch: "main"})
	if e != nil {
		t.Fatal(e)
	}
	var snapshot model.ConfigurationResourceSnapshot
	if e = json.Unmarshal(raw, &snapshot); e != nil || len(snapshot.References) != 1 || snapshot.References[0] != logical {
		t.Fatal("tracking reference was converted or filled with a resolved pin", e)
	}
	for _, fields := range []string{"all", "commit", "version", "root"} {
		ref := logical
		if fields == "all" || fields == "commit" {
			ref.TargetCommitID = q.CommitID
		}
		if fields == "all" || fields == "version" {
			ref.TargetVersion = "1"
		}
		if fields == "all" || fields == "root" {
			ref.TargetRootDigest = q.Snapshot.RootDigest
		}
		for _, kind := range []string{"create", "commit", "noop"} {
			key := "tracking-pin-" + fields + "-" + kind
			var plan any
			op := model.ResourceCommitOperation
			if kind == "create" {
				plan = configurationCreateForTest(t, g, key, []model.ConfigurationReference{ref})
				op = model.ResourceCreateOperation
			} else {
				tag := "b"
				if kind == "noop" {
					tag = "a"
				}
				plan = configurationCommitForTest(t, g, first, key, tag, true, []model.ConfigurationReference{ref}, q.Snapshot.Manifest)
			}
			before := configurationCounts(t, db, ctx)
			_, e = run(t, db, ctx, op, plan)
			configurationCode(t, e, "invalid_argument")
			if before != configurationCounts(t, db, ctx) {
				t.Fatal("invalid tracking pin left rows or quota", key)
			}
		}
		if _, e = model.ConfigurationContentDigest(q.Snapshot.Payload, q.Snapshot.Manifest, []model.ConfigurationReference{ref}); e == nil {
			t.Fatal("shared content digest accepted tracking pin", fields)
		}
	}
	pinned := logical
	pinned.Mode, pinned.TargetCommitID, pinned.TargetVersion, pinned.TargetRootDigest = "pinned", q.CommitID, "1", q.Snapshot.RootDigest
	bad := pinned
	bad.TargetVersion = "9223372036854775808"
	before := configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, g, "overflow-pin", []model.ConfigurationReference{bad}))
	configurationCode(t, e, "invalid_argument")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("overflowing pinned version left rows or quota")
	}
	// Historical pinned X remains an immutable target when this new main lacks X.
	commit := configurationCommitForTest(t, g, first, "pinned-history", "b", false, []model.ConfigurationReference{pinned}, q.Snapshot.Manifest)
	second := configurationWrite(t, db, ctx, model.ResourceCommitOperation, commit)
	if second.Result.Head.Commit.ID != commit.CommitID {
		t.Fatal("historical pin was compared to the prospective live head")
	}
}

func TestConfigurationAcceptedCatalogAndCanonicalLargeVersion(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	q := configurationCreateForTest(t, g, "resource", nil)
	q.Name = strings.Repeat("界", 160)
	q.NameKey = q.Name
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	r := first.Result.Head.Resource
	r.NextVersion = "9007199254740993"
	body, _ := json.Marshal(r)
	execSQL(t, db, ctx, "UPDATE core_resources SET body=? WHERE id=?", body, r.ID)
	next := configurationCommitForTest(t, g, first, "large-version", "b", true, nil, q.Snapshot.Manifest)
	second := configurationWrite(t, db, ctx, model.ResourceCommitOperation, next)
	if second.Result.Head.Commit.Version != "9007199254740993" || second.Result.Head.Resource.NextVersion != "9007199254740994" {
		t.Fatal("large decimal drift")
	}
	newg := g
	newg.SchemaIdentity = "catalog-v2"
	newg.RegistryDigest = strings.Repeat("b", 64)
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	e = DeclareConfigurationDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: newg.SchemaIdentity, SchemaVersion: 1, RegistryDigest: newg.RegistryDigest, MainVisibility: true}})
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	_, e = run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: r.ID, CommitID: q.CommitID})
	configurationCode(t, e, "conflict")
	raw, e := run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: newg, ResourceID: r.ID, CommitID: q.CommitID})
	if e != nil {
		t.Fatal(e)
	}
	var history model.ConfigurationResourceSnapshot
	json.Unmarshal(raw, &history)
	if history.Head.Commit.ID != q.CommitID || history.CurrentMain.Branch.CommitID != next.CommitID || history.Head.Commit.SchemaVersion != 1 {
		t.Fatal("catalog change hid history or lost current-main pin")
	}
	_, e = run(t, db, ctx, model.ResourceCreateOperation, q)
	configurationCode(t, e, "conflict")
	_, e = run(t, db, ctx, model.ConfigurationReceiptOperation, model.ConfigurationReceipt{Domain: g, Key: q.Mutation.Key, IntentDigest: q.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}})
	configurationCode(t, e, "conflict")
	q.Domain = newg
	replay := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	if !replay.Replayed || replay.PlanDigest != first.PlanDigest || replay.Result.Head != first.Result.Head {
		t.Fatal("current admitted catalog did not preserve original historical result/plan")
	}
	raw, e = run(t, db, ctx, model.ConfigurationReceiptOperation, model.ConfigurationReceipt{Domain: newg, Key: q.Mutation.Key, IntentDigest: q.Mutation.IntentDigest, Operations: []string{model.ResourceCreateOperation}})
	if e != nil {
		t.Fatal(e)
	}
	var receipt model.ConfigurationMutationResult
	if json.Unmarshal(raw, &receipt) != nil || !receipt.Replayed || receipt.PlanDigest != first.PlanDigest || receipt.Result.Head != first.Result.Head {
		t.Fatal("current admitted readonly receipt changed original facts")
	}
	r = second.Result.Head.Resource
	r.NextVersion = "9223372036854775807"
	body, _ = json.Marshal(r)
	execSQL(t, db, ctx, "UPDATE core_resources SET body=? WHERE id=?", body, r.ID)
	overflow := configurationCommitForTest(t, newg, second, "overflow", "c", true, nil, next.Snapshot.Manifest)
	before := configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, overflow)
	configurationCode(t, e, "resource_exhausted")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("overflow leaked state")
	}
}

func TestConfigurationSystemProvisioningIdentityAndCatalogPolicy(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	g.Key = "system"
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	e = DeclareConfigurationDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: g.SchemaIdentity, SchemaVersion: 1, RegistryDigest: g.RegistryDigest, MainVisibility: true, AllowSystemProvisioning: true}})
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	q := configurationCreateForTest(t, g, "system-one", nil)
	q.System = true
	q.SystemKey = strings.Repeat("界", 160)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	duplicate := configurationCreateForTest(t, g, "system-two", nil)
	duplicate.System = true
	duplicate.SystemKey = q.SystemKey
	before := configurationCounts(t, db, ctx)
	_, e = run(t, db, ctx, model.ResourceCreateOperation, duplicate)
	configurationCode(t, e, "conflict")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("duplicate stable system identity committed")
	}
	rename := configurationCommitForTest(t, g, first, "protected-rename", "b", true, nil, q.Snapshot.Manifest)
	rename.Name = "renamed"
	rename.NameKey = "renamed"
	rename.ExpectedResourceRevision = "1"
	_, e = run(t, db, ctx, model.ResourceCommitOperation, rename)
	configurationCode(t, e, "failed_precondition")
	rename.AllowSystemRename = true
	renamed := configurationWrite(t, db, ctx, model.ResourceCommitOperation, rename)
	if renamed.Result.Head.Resource.ID != q.ResourceID || renamed.Result.Head.Resource.SystemKey != q.SystemKey {
		t.Fatal("authorized rename changed stable protected identity")
	}
	tx, e = db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	e = DeclareConfigurationDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: "catalog-v2", SchemaVersion: 1, RegistryDigest: g.RegistryDigest, MainVisibility: true, AllowSystemProvisioning: false}})
	configurationCode(t, e, "conflict")
	tx.Rollback()
}

func TestConfigurationSnapshotBranchAggregateAndExactContentGuard(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	q := configurationCreateForTest(t, g, "resource", nil)
	first := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	bad := configurationCommitForTest(t, g, first, "wrong-content", "b", true, nil, q.Snapshot.Manifest)
	bad.Branch.ContentDigest = strings.Repeat("0", 64)
	before := configurationCounts(t, db, ctx)
	_, e := run(t, db, ctx, model.ResourceCommitOperation, bad)
	configurationCode(t, e, "conflict")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("correct head with wrong content pin wrote facts")
	}
	other := configurationCreateForTest(t, g, "other", nil)
	second := configurationWrite(t, db, ctx, model.ResourceCreateOperation, other)
	// Both declared FKs still hold, but the immutable snapshot points to the
	// other aggregate's branch. Its scalar row and body deliberately agree.
	c := first.Result.Head.Commit
	c.BranchID = second.Result.Head.Branch.ID
	body, _ := json.Marshal(c)
	execSQL(t, db, ctx, "UPDATE core_snapshots SET branch_id=?,body=? WHERE scope=? AND domain=? AND id=?", c.BranchID, body, testScope, g.Key, c.ID)
	before = configurationCounts(t, db, ctx)
	validGuard := configurationCommitForTest(t, g, first, "foreign-owner", "b", true, nil, q.Snapshot.Manifest)
	_, e = run(t, db, ctx, model.ResourceCommitOperation, validGuard)
	configurationCode(t, e, "data_loss")
	if before != configurationCounts(t, db, ctx) {
		t.Fatal("foreign snapshot branch owner wrote facts")
	}
	_, e = run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: q.ResourceID, Branch: "main"})
	configurationCode(t, e, "data_loss")
	// Selecting a different immutable commit still validates current-main's
	// owner while returning only its identity projection.
	otherCommit := second.Result.Head.Commit
	otherCommit.ID = "owned-history"
	otherCommit.ResourceID = first.Result.Head.Resource.ID
	otherCommit.BranchID = first.Result.Head.Branch.ID
	otherCommit.Version = "2"
	body, _ = json.Marshal(otherCommit)
	execSQL(t, db, ctx, "INSERT INTO core_snapshots VALUES(?,?,?,?,?,?,?,?,?,?,?)", testScope, g.Key, otherCommit.ID, otherCommit.ResourceID, otherCommit.BranchID, otherCommit.Version, "", otherCommit.ContentDigest, other.Snapshot.Payload, other.Snapshot.Manifest, body)
	_, e = run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: q.ResourceID, CommitID: otherCommit.ID})
	configurationCode(t, e, "data_loss")
}
