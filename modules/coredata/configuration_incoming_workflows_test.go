package coredata_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestWorkflowRobotBindingsSelectedMaterialization(t *testing.T) {
	f := openStore(t)
	workflow := model.ConfigurationDomainGuard{Key: "automation", SchemaIdentity: "automation-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	robot := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("c", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{
		{Key: workflow.Key, SchemaIdentity: workflow.SchemaIdentity, SchemaVersion: 1, RegistryDigest: workflow.RegistryDigest, MainVisibility: true},
		{Key: robot.Key, SchemaIdentity: robot.SchemaIdentity, SchemaVersion: 1, RegistryDigest: robot.RegistryDigest, MainVisibility: true},
	}); err != nil {
		t.Fatal(err)
	}
	create := func(domain model.ConfigurationDomainGuard, id string, refs []model.ConfigurationReference) model.ConfigurationMutationResult {
		t.Helper()
		input := f.create(t, id)
		input.Domain = domain
		input.Snapshot.References = refs
		value, err := f.core.CreateResource(f.ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	create(workflow, "auto-1", nil)
	create(workflow, "auto-2", nil)
	refs := func(id string, count int) []model.ConfigurationReference {
		out := make([]model.ConfigurationReference, count)
		for i := range out {
			out[i] = model.ConfigurationReference{Slot: fmt.Sprintf("workflow-binding:%03d", i), Mode: "tracking", TargetDomain: workflow.Key, TargetResourceID: id, TargetBranch: "main"}
		}
		return out
	}
	create(robot, "robot-1", refs("auto-1", 1))
	create(robot, "robot-2", refs("auto-2", 64))
	read := func(ids ...string) model.ConfigurationWorkflowRobotResult {
		t.Helper()
		before, _ := f.db.Stats()
		out, err := f.core.ReadWorkflowRobotBindings(f.ctx, model.ConfigurationWorkflowRobotRead{WorkflowDomain: workflow, RobotDomain: robot, WorkflowIDs: ids})
		after, _ := f.db.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if after.Calls-before.Calls != 1 {
			t.Fatalf("one selected cut used %d owner admissions", after.Calls-before.Calls)
		}
		return out
	}
	one, many := read("auto-1"), read("auto-2")
	if len(one.Workflows) != 1 || len(many.Workflows) != 1 || len(one.Robots) != 1 || len(many.Robots) != 1 || len(one.Links) != 1 || len(many.Links) != 1 || one.Work.RobotHeaderDecodes != 1 || many.Work.RobotHeaderDecodes != 1 {
		t.Fatalf("incorrect deduplicated cut: one=%+v many=%+v", one.Work, many.Work)
	}
	if one.Work.RobotHeaderBytes != many.Work.RobotHeaderBytes {
		t.Fatalf("64 slots changed one Robot header materialization: %d/%d", one.Work.RobotHeaderBytes, many.Work.RobotHeaderBytes)
	}
	for i := 0; i < 16; i++ {
		create(workflow, fmt.Sprintf("caller-%02d", i), []model.ConfigurationReference{{Slot: "workflow-call:task", Mode: "tracking", TargetDomain: workflow.Key, TargetResourceID: "auto-2", TargetBranch: "main"}})
	}
	after := read("auto-2")
	if !reflect.DeepEqual(many.Work, after.Work) || !reflect.DeepEqual(many.Links, after.Links) {
		t.Fatalf("unrelated incoming sources changed the selected cut: before=%+v after=%+v", many.Work, after.Work)
	}
	batched := read("auto-2", "auto-1")
	if len(batched.Workflows) != 2 || len(batched.Robots) != 2 || len(batched.Links) != 2 || batched.Work.RobotHeaderDecodes != 2 {
		t.Fatalf("batch did not select exactly the two current labels: %+v", batched.Work)
	}
	t.Logf("one/64 slots plus 16 unrelated sources: Robot header decodes %d/%d/%d, actual selected header bytes %d/%d/%d, links %d/%d/%d; one owner call each", one.Work.RobotHeaderDecodes, many.Work.RobotHeaderDecodes, after.Work.RobotHeaderDecodes, one.Work.RobotHeaderBytes, many.Work.RobotHeaderBytes, after.Work.RobotHeaderBytes, one.Work.Links, many.Work.Links, after.Work.Links)
}

func TestWorkflowRobotBindingsModesBranchesArchiveAndScope(t *testing.T) {
	f := openStore(t)
	workflow := model.ConfigurationDomainGuard{Key: "automation", SchemaIdentity: "automation-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	robot := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("c", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{
		{Key: workflow.Key, SchemaIdentity: workflow.SchemaIdentity, SchemaVersion: 1, RegistryDigest: workflow.RegistryDigest, MainVisibility: true},
		{Key: robot.Key, SchemaIdentity: robot.SchemaIdentity, SchemaVersion: 1, RegistryDigest: robot.RegistryDigest, MainVisibility: true},
	}); err != nil {
		t.Fatal(err)
	}
	create := func(owner *coredata.Store, domain model.ConfigurationDomainGuard, id string, refs []model.ConfigurationReference) model.ConfigurationMutationResult {
		t.Helper()
		q := f.create(t, id)
		q.Domain = domain
		q.Snapshot.References = refs
		out, err := owner.CreateResource(f.ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	meta := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("c", 64), Actor: "tester"}
	}
	target := create(f.core, workflow, "selected", nil)
	h := target.Result.Head
	guard := model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}
	if _, err := f.core.CreateBranch(f.ctx, model.ConfigurationBranchCreate{Domain: workflow, Mutation: meta("target-review"), ResourceID: h.Resource.ID, ExpectedResourceRevision: h.Resource.Revision, Main: guard, ID: "selected-review", Name: "Review", FromCommitID: h.Commit.ID, FromContentDigest: h.Commit.ContentDigest}); err != nil {
		t.Fatal(err)
	}
	mainRef := model.ConfigurationReference{Slot: "workflow-binding:main", Mode: "tracking", TargetDomain: workflow.Key, TargetResourceID: h.Resource.ID, TargetBranch: "main"}
	good := create(f.core, robot, "robot-good", []model.ConfigurationReference{mainRef})
	pinned := mainRef
	pinned.Mode = "pinned"
	pinned.TargetCommitID = h.Commit.ID
	pinned.TargetVersion = h.Commit.Version
	pinned.TargetRootDigest = h.Commit.RootDigest
	create(f.core, robot, "robot-pinned", []model.ConfigurationReference{pinned})
	wrongSlot := mainRef
	wrongSlot.Slot = "other-binding:main"
	create(f.core, robot, "robot-slot", []model.ConfigurationReference{wrongSlot})
	review := mainRef
	review.TargetBranch = "review"
	create(f.core, robot, "robot-target-branch", []model.ConfigurationReference{review})
	component := mainRef
	component.TargetComponentID = "x"
	create(f.core, robot, "robot-component", []model.ConfigurationReference{component})
	history := create(f.core, robot, "robot-history", []model.ConfigurationReference{mainRef})
	snap, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: robot, ResourceID: history.Result.Head.Resource.ID, Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	commit := f.commit(t, snap, "history-next", "b")
	commit.Domain = robot
	commit.Snapshot.References = nil
	if _, err = f.core.CommitResource(f.ctx, commit); err != nil {
		t.Fatal(err)
	}
	off := create(f.core, robot, "robot-off-main", nil)
	offHead := off.Result.Head
	offGuard := model.ConfigurationBranchGuard{ID: offHead.Branch.ID, ExpectedRevision: offHead.Branch.Revision, CommitID: offHead.Commit.ID, ContentDigest: offHead.Commit.ContentDigest}
	if _, err = f.core.CreateBranch(f.ctx, model.ConfigurationBranchCreate{Domain: robot, Mutation: meta("robot-review"), ResourceID: offHead.Resource.ID, ExpectedResourceRevision: offHead.Resource.Revision, Main: offGuard, ID: "robot-review-branch", Name: "Review", FromCommitID: offHead.Commit.ID, FromContentDigest: offHead.Commit.ContentDigest}); err != nil {
		t.Fatal(err)
	}
	offSnap, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: robot, ResourceID: offHead.Resource.ID, Branch: "review"})
	if err != nil {
		t.Fatal(err)
	}
	offCommit := f.commit(t, offSnap, "off-main-change", "b")
	offCommit.Domain = robot
	offCommit.Snapshot.References = []model.ConfigurationReference{mainRef}
	if _, err = f.core.CommitResource(f.ctx, offCommit); err != nil {
		t.Fatal(err)
	}
	other, err := coredata.New(f.db, api.Scope{Namespace: "core", User: "other", Workspace: "station"})
	if err != nil {
		t.Fatal(err)
	}
	create(other, workflow, "selected", nil)
	create(other, robot, "other-scope-robot", []model.ConfigurationReference{mainRef})
	read := func(want ...string) {
		t.Helper()
		out, err := f.core.ReadWorkflowRobotBindings(f.ctx, model.ConfigurationWorkflowRobotRead{WorkflowDomain: workflow, RobotDomain: robot, WorkflowIDs: []string{"selected"}})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, link := range out.Links {
			got = append(got, link.RobotID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("selected links=%v want=%v", got, want)
		}
	}
	read("robot-good")
	state := model.ConfigurationResourceState{Domain: robot, Mutation: meta("good-archive"), ResourceID: good.Result.Head.Resource.ID, ExpectedRevision: good.Result.Head.Resource.Revision, Main: model.ConfigurationBranchGuard{ID: good.Result.Head.Branch.ID, ExpectedRevision: good.Result.Head.Branch.Revision, CommitID: good.Result.Head.Commit.ID, ContentDigest: good.Result.Head.Commit.ContentDigest}, Archived: true}
	archived, err := f.core.SetResourceState(f.ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	read()
	state.Mutation = meta("good-restore")
	state.ExpectedRevision = archived.Result.Head.Resource.Revision
	state.Archived = false
	if _, err = f.core.SetResourceState(f.ctx, state); err != nil {
		t.Fatal(err)
	}
	read("robot-good")
}
