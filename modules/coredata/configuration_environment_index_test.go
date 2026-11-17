package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestObservedEnvironmentIndexKeepsSnapshotAndReopens(t *testing.T) {
	f := openStore(t)
	robotDomain := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{{Key: robotDomain.Key, SchemaIdentity: robotDomain.SchemaIdentity, SchemaVersion: robotDomain.SchemaVersion, RegistryDigest: robotDomain.RegistryDigest, MainVisibility: true}}); err != nil {
		t.Fatal(err)
	}
	robot := f.create(t, "robot-a")
	robot.Domain = robotDomain
	if _, err := f.core.CreateResource(f.ctx, robot); err != nil {
		t.Fatal(err)
	}
	q := f.create(t, "experiment")
	q.Snapshot.References = []model.ConfigurationReference{{Slot: "robot:one", Mode: "tracking", TargetDomain: "robot", TargetResourceID: "robot-a", TargetBranch: "main"}}
	if _, err := f.core.CreateResource(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	before := f.read(t, "experiment", "main", "")
	fact := model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot, AgentID: "actual-container", SlotID: "one", RobotResourceID: "robot-a"}
	ids, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", before.Head.Commit.ID, []string{"one"}, []model.ConfigurationAgentAssociation{fact})
	if err != nil || !reflect.DeepEqual(ids, []string{"actual-container"}) {
		t.Fatalf("projection: %v %v", ids, err)
	}
	read := func() model.ConfigurationAgentSourcePage {
		t.Helper()
		page, err := f.core.ReadAgentSources(f.ctx, model.ConfigurationAgentSourceRead{Domain: f.domain, AgentID: "actual-container", Limit: 4})
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	page := read()
	if len(page.Items) != 1 || !page.Items[0].EnvironmentDerived || page.Items[0].Association != fact {
		t.Fatalf("selected index: %+v", page)
	}
	after := f.read(t, "experiment", "main", "")
	if before.Head.Commit != after.Head.Commit || !reflect.DeepEqual(before.Associations, after.Associations) || !reflect.DeepEqual(before.Payload, after.Payload) {
		t.Fatal("projection rewrote immutable snapshot")
	}
	f.reopen(t)
	if page = read(); len(page.Items) != 1 || !page.Items[0].EnvironmentDerived {
		t.Fatalf("cold index: %+v", page)
	}
	commit := f.commit(t, f.read(t, "experiment", "main", ""), "description", "b")
	commit.Snapshot.References = q.Snapshot.References
	if _, err = f.core.CommitResource(f.ctx, commit); err != nil {
		t.Fatal(err)
	}
	if page = read(); len(page.Items) != 1 || page.Items[0].Header.Head.Commit.ID != commit.CommitID {
		t.Fatalf("index did not follow current head: %+v", page)
	}
	ids, err = f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commit.CommitID, []string{"one"}, nil)
	if err != nil || !reflect.DeepEqual(ids, []string{"actual-container"}) || len(read().Items) != 0 {
		t.Fatalf("selected retirement: %v %v", ids, err)
	}

	// Changing the saved carrier keeps the prior accepted container discovery
	// for the same Robot reference; neither immutable association set changes.
	declared := f.create(t, "declared")
	declared.Snapshot.References = q.Snapshot.References
	declared.Snapshot.Associations = []model.ConfigurationAgentAssociation{fact}
	if _, err = f.core.CreateResource(f.ctx, declared); err != nil {
		t.Fatal(err)
	}
	saved := f.read(t, "declared", "main", "")
	switchCarrier := f.commit(t, saved, "switch-carrier", "c")
	switchCarrier.Snapshot.References = q.Snapshot.References
	if _, err = f.core.CommitResource(f.ctx, switchCarrier); err != nil {
		t.Fatal(err)
	}
	page = read()
	if len(page.Items) != 1 || page.Items[0].Header.Head.Resource.ID != "declared" || !page.Items[0].EnvironmentDerived || page.Items[0].Association != fact {
		t.Fatalf("carrier transition lost accepted identity: %+v", page)
	}
	if old := f.read(t, "declared", "", saved.Head.Commit.ID); len(old.Associations) != 1 || old.Associations[0] != fact {
		t.Fatalf("old immutable association changed: %+v", old.Associations)
	}
	if current := f.read(t, "declared", "main", ""); len(current.Associations) != 0 {
		t.Fatalf("new immutable association changed: %+v", current.Associations)
	}
}

func TestEnvironmentIndexSelectsOnlyRequestedSlotsAndKeepsEmptyNoop(t *testing.T) {
	f := openStore(t)
	robotDomain := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{{Key: robotDomain.Key, SchemaIdentity: robotDomain.SchemaIdentity, SchemaVersion: robotDomain.SchemaVersion, RegistryDigest: robotDomain.RegistryDigest, MainVisibility: true}}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"robot-a", "robot-b"} {
		robot := f.create(t, id)
		robot.Domain = robotDomain
		if _, err := f.core.CreateResource(f.ctx, robot); err != nil {
			t.Fatal(err)
		}
	}
	q := f.create(t, "experiment")
	q.Snapshot.References = []model.ConfigurationReference{
		{Slot: "robot:one", Mode: "tracking", TargetDomain: "robot", TargetResourceID: "robot-a", TargetBranch: "main"},
		{Slot: "robot:two", Mode: "tracking", TargetDomain: "robot", TargetResourceID: "robot-b", TargetBranch: "main"},
	}
	if _, err := f.core.CreateResource(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	before := f.read(t, "experiment", "main", "")
	first := model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot, AgentID: "agent-one", SlotID: "one", RobotResourceID: "robot-a"}
	second := model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot, AgentID: "agent-two", SlotID: "two", RobotResourceID: "robot-b"}
	if _, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", before.Head.Commit.ID, []string{"one", "two"}, []model.ConfigurationAgentAssociation{first, second}); err != nil {
		t.Fatal(err)
	}
	rows := func(agent string) []model.ConfigurationAgentAssociation {
		t.Helper()
		page, err := f.core.ReadAgentSources(f.ctx, model.ConfigurationAgentSourceRead{Domain: f.domain, AgentID: agent, Limit: 8})
		if err != nil {
			t.Fatal(err)
		}
		out := make([]model.ConfigurationAgentAssociation, 0, len(page.Items))
		for _, item := range page.Items {
			out = append(out, item.Association)
		}
		return out
	}
	if got := rows("agent-one"); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{first}) {
		t.Fatalf("initial one: %+v", got)
	}
	if got := rows("agent-two"); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{second}) {
		t.Fatalf("initial two: %+v", got)
	}
	changed := model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot, AgentID: "agent-one-new", SlotID: "one", RobotResourceID: "robot-a"}
	if _, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", before.Head.Commit.ID, []string{"one"}, []model.ConfigurationAgentAssociation{changed}); err != nil {
		t.Fatal(err)
	}
	if got := rows("agent-one-new"); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{changed}) {
		t.Fatalf("selected replacement: %+v", got)
	}
	if got := rows("agent-one"); len(got) != 0 {
		t.Fatalf("replaced selected row retained: %+v", got)
	}
	if got := rows("agent-two"); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{second}) {
		t.Fatalf("unselected row changed: %+v", got)
	}
	id, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", before.Head.Commit.ID, nil, nil)
	if err != nil || id != nil {
		t.Fatalf("empty selection was not a no-op: %v %v", id, err)
	}
	if got := rows("agent-two"); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{second}) {
		t.Fatalf("empty selection changed rows: %+v", got)
	}
	if _, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", before.Head.Commit.ID+"-stale", []string{"one"}, nil); err == nil {
		t.Fatal("stale current-main generation was accepted")
	}
}

func environmentPrepared(tb testing.TB, id, tag string) model.PreparedConfigurationSnapshot {
	tb.Helper()
	v, err := model.ValidateConfigurationManifest(model.ConfigurationManifest{Nodes: []model.ConfigurationManifestNode{
		{ID: "root", Kind: "root"},
		{ID: "x", ParentID: "root", Name: "X", Kind: "document", DocumentKind: "test.data.v1", PayloadDigest: strings.Repeat(tag, 64)},
	}})
	if err != nil {
		tb.Fatal(err)
	}
	nodes, summary, err := model.DiffConfigurationManifests(model.ValidatedConfigurationManifest{}, v)
	if err != nil {
		tb.Fatal(err)
	}
	manifest, err := json.Marshal(model.ConfigurationManifest{Nodes: v.Nodes})
	if err != nil {
		tb.Fatal(err)
	}
	return model.PreparedConfigurationSnapshot{
		RootDigest: v.RootDigest,
		Payload:    []byte(`{ "identity":{"active":true},"body":{"value":"` + tag + `"} }`),
		Manifest:   manifest, Identity: []byte(`{"active":true}`), CatalogSummary: []byte(`{"label":"fixture"}`),
		Associations: []model.ConfigurationAgentAssociation{},
		Change:       model.ConfigurationChange{ID: id, Summary: summary, Nodes: nodes},
	}
}

func environmentSeed(tb testing.TB, core *coredata.Store, ctx context.Context, domain, robotDomain model.ConfigurationDomainGuard, n int) (string, []string, []model.ConfigurationAgentAssociation) {
	tb.Helper()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("robot-%03d", i)
		robot := model.ConfigurationResourceCreate{
			Domain: robotDomain, Mutation: model.ConfigurationMutation{Key: "create-" + id, IntentDigest: strings.Repeat("a", 64), Actor: "tester"},
			ResourceID: id, BranchID: id + "-main", CommitID: id + "-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"},
			Name: id, NameKey: id, Snapshot: environmentPrepared(tb, id+"-change", "a"),
		}
		if _, err := core.CreateResource(ctx, robot); err != nil {
			tb.Fatal(err)
		}
	}
	slots := make([]string, n)
	facts := make([]model.ConfigurationAgentAssociation, n)
	refs := make([]model.ConfigurationReference, n)
	for i := 0; i < n; i++ {
		slot := fmt.Sprintf("slot-%03d", i)
		robot := fmt.Sprintf("robot-%03d", i)
		slots[i] = slot
		refs[i] = model.ConfigurationReference{Slot: "robot:" + slot, Mode: "tracking", TargetDomain: "robot", TargetResourceID: robot, TargetBranch: "main"}
		facts[i] = model.ConfigurationAgentAssociation{Kind: model.AgentAssociationOnboardSlot, AgentID: fmt.Sprintf("agent-%03d", i), SlotID: slot, RobotResourceID: robot}
	}
	experiment := model.ConfigurationResourceCreate{
		Domain: domain, Mutation: model.ConfigurationMutation{Key: "create-experiment", IntentDigest: strings.Repeat("a", 64), Actor: "tester"},
		ResourceID: "experiment", BranchID: "experiment-main", CommitID: "experiment-v1", Namespace: model.ConfigurationNamespaceGuard{ExpectedRevision: "0"},
		Name: "experiment", NameKey: "experiment", Snapshot: environmentPrepared(tb, "experiment-change", "a"),
	}
	experiment.Snapshot.References = refs
	if _, err := core.CreateResource(ctx, experiment); err != nil {
		tb.Fatal(err)
	}
	snapshot, err := core.ReadResource(ctx, model.ConfigurationResourceRead{Domain: domain, ResourceID: "experiment", Branch: "main"})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := core.ReplaceEnvironmentAssociations(ctx, domain, "experiment", snapshot.Head.Commit.ID, slots, facts); err != nil {
		tb.Fatal(err)
	}
	return snapshot.Head.Commit.ID, slots, facts
}

func newSeededEnvironment(t *testing.T, n int) (*store, string, []string, []model.ConfigurationAgentAssociation) {
	t.Helper()
	f := openStore(t)
	robotDomain := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{{Key: robotDomain.Key, SchemaIdentity: robotDomain.SchemaIdentity, SchemaVersion: robotDomain.SchemaVersion, RegistryDigest: robotDomain.RegistryDigest, MainVisibility: true}}); err != nil {
		t.Fatal(err)
	}
	commitID, slots, facts := environmentSeed(t, f.core, f.ctx, f.domain, robotDomain, n)
	return f, commitID, slots, facts
}

func environmentChargedBytes(t *testing.T, f *store) int64 {
	t.Helper()
	var charged int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT bytes FROM core_data_usage WHERE scope=?`, engine.ScopeID(f.scope)).Scan(&charged)
	}); err != nil {
		t.Fatal(err)
	}
	return charged
}

func environmentAgentRows(t *testing.T, f *store, agent string) []model.ConfigurationAgentAssociation {
	t.Helper()
	page, err := f.core.ReadAgentSources(f.ctx, model.ConfigurationAgentSourceRead{Domain: f.domain, AgentID: agent, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	out := make([]model.ConfigurationAgentAssociation, 0, len(page.Items))
	for _, item := range page.Items {
		out = append(out, item.Association)
	}
	return out
}

func TestEnvironmentIndexSameFactsNoopKeepsGenerationChargeAndNotification(t *testing.T) {
	f, commitID, selected, facts := newSeededEnvironment(t, 2)
	generation, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	charged := environmentChargedBytes(t, f)
	indexRows := f.count(t, "core_configuration_agent_index")
	snapshots := f.count(t, "core_snapshots")
	changes := f.count(t, "core_changes")
	ids, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID,
		[]string{selected[1], selected[0], selected[1]}, []model.ConfigurationAgentAssociation{facts[1], facts[0]})
	if err != nil || len(ids) != 0 {
		t.Fatalf("same-facts projection ids=%v err=%v", ids, err)
	}
	after, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if afterCharge := environmentChargedBytes(t, f); afterCharge != charged {
		t.Fatalf("same-facts projection charged %d->%d", charged, afterCharge)
	}
	if after != generation || f.count(t, "core_configuration_agent_index") != indexRows || f.count(t, "core_snapshots") != snapshots || f.count(t, "core_changes") != changes {
		t.Fatalf("same-facts projection mutated state: gen %d->%d rows %d snapshots %d changes %d", generation, after, f.count(t, "core_configuration_agent_index"), f.count(t, "core_snapshots"), f.count(t, "core_changes"))
	}
}

func TestEnvironmentIndexSelectionEdgesKeepErrorOrder(t *testing.T) {
	f, commitID, selected, facts := newSeededEnvironment(t, 2)
	changed := facts[0]
	changed.AgentID = "agent-one-new"
	ids, err := f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID, []string{selected[0], selected[0]}, []model.ConfigurationAgentAssociation{changed})
	if err != nil || !reflect.DeepEqual(ids, []string{facts[0].AgentID, changed.AgentID}) {
		t.Fatalf("duplicate selected ids=%v err=%v", ids, err)
	}
	if got := environmentAgentRows(t, f, facts[0].AgentID); len(got) != 0 {
		t.Fatalf("replaced selected row retained: %+v", got)
	}
	if got := environmentAgentRows(t, f, changed.AgentID); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{changed}) {
		t.Fatalf("duplicate selected replacement: %+v", got)
	}
	if got := environmentAgentRows(t, f, facts[1].AgentID); !reflect.DeepEqual(got, []model.ConfigurationAgentAssociation{facts[1]}) {
		t.Fatalf("unselected row changed: %+v", got)
	}
	if rows := f.count(t, "core_configuration_agent_index"); rows != 2 {
		t.Fatalf("duplicate selection rows=%d", rows)
	}
	generation, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	charged := environmentChargedBytes(t, f)
	indexRows := f.count(t, "core_configuration_agent_index")
	unknown := facts[1]
	unknown.SlotID = "slot-999"
	_, err = f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID, []string{selected[1]}, []model.ConfigurationAgentAssociation{unknown})
	requireCode(t, err, "invalid_argument")
	_, err = f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID, nil, []model.ConfigurationAgentAssociation{facts[0]})
	requireCode(t, err, "invalid_argument")
	_, err = f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID+"-stale", nil, nil)
	requireCode(t, err, "conflict")
	_, err = f.core.ReplaceEnvironmentAssociations(f.ctx, f.domain, "experiment", commitID+"-stale", nil, []model.ConfigurationAgentAssociation{unknown})
	requireCode(t, err, "conflict")
	after, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != generation || environmentChargedBytes(t, f) != charged || f.count(t, "core_configuration_agent_index") != indexRows {
		t.Fatalf("rejected selections mutated state: gen %d->%d", generation, after)
	}
}

// This times the real Store driver/Go cost of one selected-slot no-op with
// other Environment rows present. It is not a latency or physical-I/O claim.
func BenchmarkEnvironmentSelectedSlotNoop(b *testing.B) {
	for _, n := range []int{8, 20, 100} {
		b.Run(fmt.Sprintf("rows-%d", n), func(b *testing.B) {
			core, db := benchCore(b)
			ctx := context.Background()
			domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
			robotDomain := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
			if err := coredata.DeclareConfigurationDomains(ctx, db, []model.ConfigurationDomainDeclaration{
				{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: domain.SchemaVersion, RegistryDigest: domain.RegistryDigest, MainVisibility: true},
				{Key: robotDomain.Key, SchemaIdentity: robotDomain.SchemaIdentity, SchemaVersion: robotDomain.SchemaVersion, RegistryDigest: robotDomain.RegistryDigest, MainVisibility: true},
			}); err != nil {
				b.Fatal(err)
			}
			commitID, selected, facts := environmentSeed(b, core, ctx, domain, robotDomain, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ids, err := core.ReplaceEnvironmentAssociations(ctx, domain, "experiment", commitID, selected[:1], facts[:1])
				if err != nil || len(ids) != 0 {
					b.Fatalf("selected-slot no-op ids=%v err=%v", ids, err)
				}
			}
		})
	}
}
