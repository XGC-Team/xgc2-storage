package coredata_test

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRobotWorkflowEffectsTrackLiveMainAndDurableReceipts(t *testing.T) {
	f := openStore(t)
	robot := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("c", 64)}
	workflow := model.ConfigurationDomainGuard{Key: "automation", SchemaIdentity: "automation-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{
		{Key: robot.Key, SchemaIdentity: robot.SchemaIdentity, SchemaVersion: 1, RegistryDigest: robot.RegistryDigest, MainVisibility: true},
		{Key: workflow.Key, SchemaIdentity: workflow.SchemaIdentity, SchemaVersion: 1, RegistryDigest: workflow.RegistryDigest, MainVisibility: true},
	}); err != nil {
		t.Fatal(err)
	}
	f.domain = robot
	for _, id := range []string{"wf-a", "wf-b", "wf-c"} {
		q := f.create(t, id)
		q.Domain = workflow
		if _, err := f.core.CreateResource(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	mutation := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("d", 64), Actor: "tester"}
	}
	guard := func(h model.ConfigurationHead) model.ConfigurationBranchGuard {
		return model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}
	}
	refs := func(ids ...string) []model.ConfigurationReference {
		out := make([]model.ConfigurationReference, len(ids))
		for i, id := range ids {
			out[i] = model.ConfigurationReference{Slot: "workflow-binding:" + string(rune('a'+i)), Mode: "tracking", TargetDomain: "automation", TargetResourceID: id, TargetBranch: "main"}
		}
		return out
	}
	check := func(got []string, want ...string) {
		t.Helper()
		if len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
			t.Fatalf("effects=%v want=%v", got, want)
		}
	}
	flags := func(commit string, live, total int) {
		t.Helper()
		var l, n int
		err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT coalesce(sum(current_main_live),0),count(*) FROM core_references WHERE scope=? AND domain='robot' AND commit_id=?`, engine.ScopeID(f.scope), commit).Scan(&l, &n)
		})
		if err != nil || l != live || n != total {
			t.Fatalf("commit %s live/total=%d/%d want=%d/%d err=%v", commit, l, n, live, total, err)
		}
	}
	q := f.create(t, "robot-1")
	q.Snapshot.References = refs("wf-a", "wf-a", "wf-b")
	created, err := f.core.CreateResource(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	check(created.AffectedWorkflowIDs, "wf-a", "wf-b")
	flags(q.CommitID, 3, 3)
	replay, err := f.core.CreateResource(f.ctx, q)
	if err != nil || !replay.Replayed {
		t.Fatalf("create replay=%+v err=%v", replay, err)
	}
	check(replay.AffectedWorkflowIDs, "wf-a", "wf-b")
	flags(q.CommitID, 3, 3)
	read := func() model.ConfigurationResourceSnapshot { return f.read(t, "robot-1", "main", "") }
	before := read()
	unchanged := f.commit(t, before, "unchanged-label", "b")
	unchanged.Snapshot.References = before.References
	updated, err := f.core.CommitResource(f.ctx, unchanged)
	if err != nil {
		t.Fatal(err)
	}
	check(updated.AffectedWorkflowIDs)
	flags(q.CommitID, 0, 3)
	flags(unchanged.CommitID, 3, 3)
	base := read()
	rename := f.commit(t, base, "rename-label", "c")
	rename.Name = "Renamed"
	rename.NameKey = "renamed"
	rename.ExpectedResourceRevision = base.Head.Resource.Revision
	rename.Snapshot.References = base.References
	renamed, err := f.core.CommitResource(f.ctx, rename)
	if err != nil {
		t.Fatal(err)
	}
	check(renamed.AffectedWorkflowIDs, "wf-a", "wf-b")
	noopSnapshot := read()
	noop := f.commit(t, noopSnapshot, "actual-noop", "c")
	noop.Snapshot.References = noopSnapshot.References
	noChange, err := f.core.CommitResource(f.ctx, noop)
	if err != nil {
		t.Fatal(err)
	}
	if noChange.Result.Disposition != "noop" {
		t.Fatalf("not a noop: %+v", noChange)
	}
	check(noChange.AffectedWorkflowIDs)
	head := renamed.Result.Head
	branch, err := f.core.CreateBranch(f.ctx, model.ConfigurationBranchCreate{Domain: robot, Mutation: mutation("branch"), ResourceID: head.Resource.ID, ExpectedResourceRevision: head.Resource.Revision, Main: guard(head), ID: "draft-id", Name: "draft", FromCommitID: head.Commit.ID, FromContentDigest: head.Commit.ContentDigest})
	if err != nil {
		t.Fatal(err)
	}
	check(branch.AffectedWorkflowIDs)
	draft := f.read(t, "robot-1", "draft", "")
	draftWrite := f.commit(t, draft, "draft-change", "d")
	draftWrite.Snapshot.References = refs("wf-a", "wf-c")
	drafted, err := f.core.CommitResource(f.ctx, draftWrite)
	if err != nil {
		t.Fatal(err)
	}
	check(drafted.AffectedWorkflowIDs)
	flags(draftWrite.CommitID, 0, 2)
	main := read()
	draft = f.read(t, "robot-1", "draft", "")
	promote := f.commit(t, main, "promote", "e")
	promote.Snapshot.References = draft.References
	branchGuard := guard(draft.Head)
	promote.Source = &model.ConfigurationSourcePin{ResourceID: main.Head.Resource.ID, ExpectedResourceRevision: main.Head.Resource.Revision, CommitID: draft.Head.Commit.ID, ContentDigest: draft.Head.Commit.ContentDigest, Branch: &branchGuard, Main: main.CurrentMain.Branch}
	promoted, err := f.core.CommitResource(f.ctx, promote)
	if err != nil {
		t.Fatal(err)
	}
	check(promoted.AffectedWorkflowIDs, "wf-b", "wf-c")
	flags(rename.CommitID, 0, 3)
	flags(promote.CommitID, 2, 2)
	h := promoted.Result.Head
	state := model.ConfigurationResourceState{Domain: robot, Mutation: mutation("archive"), ResourceID: h.Resource.ID, ExpectedRevision: h.Resource.Revision, Main: guard(h), Archived: true}
	archived, err := f.core.SetResourceState(f.ctx, state)
	if err != nil {
		t.Fatal(err)
	}
	check(archived.AffectedWorkflowIDs, "wf-a", "wf-c")
	flags(promote.CommitID, 0, 2)
	restore := state
	restore.Mutation = mutation("restore")
	restore.ExpectedRevision = archived.Result.Head.Resource.Revision
	restore.Archived = false
	restored, err := f.core.SetResourceState(f.ctx, restore)
	if err != nil {
		t.Fatal(err)
	}
	check(restored.AffectedWorkflowIDs, "wf-a", "wf-c")
	flags(promote.CommitID, 2, 2)
	replay, err = f.core.SetResourceState(f.ctx, state)
	if err != nil || !replay.Replayed {
		t.Fatalf("archive replay=%+v err=%v", replay, err)
	}
	check(replay.AffectedWorkflowIDs, "wf-a", "wf-c")
	flags(promote.CommitID, 2, 2)
	current := read()
	clone := f.create(t, "robot-copy")
	clone.Snapshot.References = current.References
	clone.Source = &model.ConfigurationSourcePin{ResourceID: current.Head.Resource.ID, ExpectedResourceRevision: current.Head.Resource.Revision, CommitID: current.Head.Commit.ID, ContentDigest: current.Head.Commit.ContentDigest, Main: current.CurrentMain.Branch}
	copied, err := f.core.CreateResource(f.ctx, clone)
	if err != nil {
		t.Fatal(err)
	}
	check(copied.AffectedWorkflowIDs, "wf-a", "wf-c")
	flags(clone.CommitID, 2, 2)
	links, err := f.core.ReadWorkflowRobotBindings(f.ctx, model.ConfigurationWorkflowRobotRead{WorkflowDomain: workflow, RobotDomain: robot, WorkflowIDs: []string{"wf-a", "wf-b", "wf-c"}})
	if err != nil || len(links.Links) != 4 {
		t.Fatalf("selected current links=%+v err=%v", links.Links, err)
	}
	root := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
	sourceNS, err := f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: robot, Mutation: mutation("source-ns"), ID: "source-ns", ExpectedRevision: "0", Parent: &root, Name: "Source"})
	if err != nil {
		t.Fatal(err)
	}
	inside := f.create(t, "inside")
	inside.Namespace = model.ConfigurationNamespaceGuard{ID: sourceNS.Namespace.ID, ExpectedRevision: sourceNS.Namespace.Revision}
	inside.Snapshot.References = refs("wf-a", "wf-c")
	insideResult, err := f.core.CreateResource(f.ctx, inside)
	if err != nil {
		t.Fatal(err)
	}
	insideHead := insideResult.Result.Head
	copyInside := f.create(t, "copy-inside")
	copyInside.Name, copyInside.NameKey = insideHead.Resource.Name, insideHead.Resource.NameKey
	copyInside.Namespace = model.ConfigurationNamespaceGuard{ID: "copied-ns", ExpectedRevision: "1"}
	copyInside.Snapshot.References = inside.Snapshot.References
	copyInside.Source = &model.ConfigurationSourcePin{ResourceID: insideHead.Resource.ID, ExpectedResourceRevision: insideHead.Resource.Revision, CommitID: insideHead.Commit.ID, ContentDigest: insideHead.Commit.ContentDigest, Main: guard(insideHead)}
	cloneNamespace := model.ConfigurationNamespaceClone{Domain: robot, Mutation: mutation("clone-namespace"), SourceID: sourceNS.Namespace.ID, ExpectedRevision: sourceNS.Namespace.Revision, TargetParent: root, Name: "Copied",
		Namespaces: []model.ConfigurationNamespaceMapping{{SourceID: sourceNS.Namespace.ID, ExpectedRevision: sourceNS.Namespace.Revision, TargetID: "copied-ns"}}, Resources: []model.ConfigurationResourceCreate{copyInside}}
	cloned, err := f.core.CloneNamespace(f.ctx, cloneNamespace)
	if err != nil {
		t.Fatal(err)
	}
	check(cloned.AffectedWorkflowIDs, "wf-a", "wf-c")
	flags(copyInside.CommitID, 2, 2)
	clonedAgain, err := f.core.CloneNamespace(f.ctx, cloneNamespace)
	if err != nil || !clonedAgain.Replayed {
		t.Fatalf("namespace replay=%+v err=%v", clonedAgain, err)
	}
	check(clonedAgain.AffectedWorkflowIDs, "wf-a", "wf-c")
}
