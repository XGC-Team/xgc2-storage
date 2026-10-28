package coredata

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAffectedAgentReceiptTracksBeforeAfterAndReverseDependencies(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	create := func(id string, refs []model.ConfigurationReference, facts []model.ConfigurationAgentAssociation) (model.ConfigurationResourceCreate, model.ConfigurationMutationResult) {
		t.Helper()
		q := configurationCreateForTest(t, domain, id, refs)
		q.Snapshot.Associations = facts
		return q, configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	tracking := func(slot, id string) model.ConfigurationReference {
		return model.ConfigurationReference{Slot: slot, Mode: "tracking", TargetDomain: domain.Key, TargetResourceID: id, TargetBranch: "main"}
	}
	auto, autoCreated := create("auto", nil, []model.ConfigurationAgentAssociation{})
	_, _ = create("caller", []model.ConfigurationReference{tracking("workflow-call:step", "auto")}, []model.ConfigurationAgentAssociation{})
	robot, robotCreated := create("robot", []model.ConfigurationReference{tracking("workflow:run", "caller")}, []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "old-board"}})
	experiment, experimentCreated := create("experiment", []model.ConfigurationReference{tracking("robot:slot", "robot"), tracking("workflow-instance:run", "auto")}, []model.ConfigurationAgentAssociation{
		{Kind: model.AgentAssociationOnboardSlot, AgentID: "onboard", SlotID: "slot", RobotResourceID: "robot"},
		{Kind: model.AgentAssociationSlotHost, AgentID: "explicit", SlotID: "slot"},
		{Kind: model.AgentAssociationWorld, AgentID: "world"},
	})
	wantOld := []string{"explicit", "old-board", "onboard"}
	if !reflect.DeepEqual(experimentCreated.AffectedAgentIDs, wantOld) {
		t.Fatalf("create dependency set = %v", experimentCreated.AffectedAgentIDs)
	}
	// An Automation change follows current workflow-call and binding references
	// at the existing target index, including Robot ownership and Experiment
	// slots, while never reading any snapshot payload for dependency discovery.
	changeAuto := configurationCommitForTest(t, domain, autoCreated, "auto-change", "b", true, nil, auto.Snapshot.Manifest)
	afterAuto := configurationWrite(t, db, ctx, model.ResourceCommitOperation, changeAuto)
	if !reflect.DeepEqual(afterAuto.AffectedAgentIDs, wantOld) {
		t.Fatalf("automation dependencies = %v", afterAuto.AffectedAgentIDs)
	}
	// Reassignment preserves both old and new identities in the SAME durable
	// commit receipt, including transitive onboard associations.
	changeRobot := configurationCommitForTest(t, domain, robotCreated, "robot-change", "b", true, robot.Snapshot.References, robot.Snapshot.Manifest)
	changeRobot.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "new-board"}}
	afterRobot := configurationWrite(t, db, ctx, model.ResourceCommitOperation, changeRobot)
	wantBoth := []string{"explicit", "new-board", "old-board", "onboard"}
	if !reflect.DeepEqual(afterRobot.AffectedAgentIDs, wantBoth) {
		t.Fatalf("owner unlink union = %v", afterRobot.AffectedAgentIDs)
	}
	raw, err := run(t, db, ctx, model.ResourceCommitOperation, changeRobot)
	if err != nil {
		t.Fatal(err)
	}
	var replay model.ConfigurationMutationResult
	if err = json.Unmarshal(raw, &replay); err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || !reflect.DeepEqual(replay.AffectedAgentIDs, wantBoth) {
		t.Fatalf("receipt lost old owner: %+v", replay)
	}
	// Archive and restore contain the old/new source union; a rejected CAS
	// leaves both source and receipt unchanged.
	h := experimentCreated.Result.Head
	guard := model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}
	state := model.ConfigurationResourceState{Domain: domain, Mutation: model.ConfigurationMutation{Key: "archive-experiment", IntentDigest: strings.Repeat("c", 64), Actor: "tester"}, ResourceID: h.Resource.ID, ExpectedRevision: h.Resource.Revision, Main: guard, Archived: true}
	archive := configurationWrite(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	wantCurrent := []string{"explicit", "new-board", "onboard"}
	if !reflect.DeepEqual(archive.AffectedAgentIDs, wantCurrent) {
		t.Fatalf("archive union = %v", archive.AffectedAgentIDs)
	}
	state.Mutation.Key = "restore-experiment"
	state.ExpectedRevision = archive.Result.Head.Resource.Revision
	state.Archived = false
	restored := configurationWrite(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	if !reflect.DeepEqual(restored.AffectedAgentIDs, wantCurrent) {
		t.Fatalf("restore union = %v", restored.AffectedAgentIDs)
	}
	counts := configurationCounts(t, db, ctx)
	state.Mutation.Key = "bad-cas"
	state.ExpectedRevision = "1"
	state.Archived = true
	_, err = run(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	configurationCode(t, err, "conflict")
	if configurationCounts(t, db, ctx) != counts {
		t.Fatal("rejected transition changed index, receipt or quota")
	}
	if charged, live := configurationQuotaBytes(t, db, ctx); charged != live {
		t.Fatalf("index or receipt quota mismatch charged=%d live=%d", charged, live)
	}
	_ = experiment
}

func TestAffectedAgentObserverSkipsEqualSyncProjection(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	q := configurationCreateForTest(t, domain, "robot", nil)
	q.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "board"}}
	q.Snapshot.CatalogSummary = []byte(`{"_agentSyncDigest":"` + strings.Repeat("a", 64) + `","label":"before"}`)
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	if !reflect.DeepEqual(created.AffectedAgentIDs, []string{"board"}) {
		t.Fatalf("create effects %v", created.AffectedAgentIDs)
	}
	metadata := configurationCommitForTest(t, domain, created, "metadata", "b", true, nil, q.Snapshot.Manifest)
	metadata.Snapshot.Associations = q.Snapshot.Associations
	metadata.Snapshot.CatalogSummary = []byte(`{"_agentSyncDigest":"` + strings.Repeat("a", 64) + `","label":"after"}`)
	updated := configurationWrite(t, db, ctx, model.ResourceCommitOperation, metadata)
	if len(updated.AffectedAgentIDs) != 0 {
		t.Fatalf("unchanged workflow projection notified Agents: %v", updated.AffectedAgentIDs)
	}
	changed := configurationCommitForTest(t, domain, updated, "bindings", "c", true, nil, metadata.Snapshot.Manifest)
	changed.Snapshot.Associations = q.Snapshot.Associations
	changed.Snapshot.CatalogSummary = []byte(`{"_agentSyncDigest":"` + strings.Repeat("b", 64) + `","label":"after"}`)
	updated = configurationWrite(t, db, ctx, model.ResourceCommitOperation, changed)
	if !reflect.DeepEqual(updated.AffectedAgentIDs, []string{"board"}) {
		t.Fatalf("changed workflow projection lost Agent: %v", updated.AffectedAgentIDs)
	}
	bad := configurationCreateForTest(t, domain, "invalid-sync-projection", nil)
	bad.Snapshot.CatalogSummary = []byte(`{"_agentSyncDigest":"not-a-digest"}`)
	before := configurationCounts(t, db, ctx)
	_, err := run(t, db, ctx, model.ResourceCreateOperation, bad)
	configurationCode(t, err, "invalid_argument")
	if configurationCounts(t, db, ctx) != before {
		t.Fatal("invalid internal marker wrote a partial mutation")
	}
}
