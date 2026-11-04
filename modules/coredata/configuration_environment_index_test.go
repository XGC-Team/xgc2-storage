package coredata_test

import (
	"reflect"
	"strings"
	"testing"

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
