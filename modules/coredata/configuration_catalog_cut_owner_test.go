package coredata_test

import (
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestCatalogCutTracksOnlyItsActualOwnerFacts(t *testing.T) {
	f := openStore(t)
	otherDomain := model.ConfigurationDomainGuard{Key: "other", SchemaIdentity: "other-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("c", 64)}
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{{Key: otherDomain.Key, SchemaIdentity: otherDomain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: otherDomain.RegistryDigest, MainVisibility: true}}); err != nil {
		t.Fatal(err)
	}
	mutation := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("d", 64), Actor: "tester"}
	}
	guard := func(h model.ConfigurationHead) model.ConfigurationBranchGuard {
		return model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}
	}
	create := func(id, selector string, domain model.ConfigurationDomainGuard) model.ConfigurationMutationResult {
		t.Helper()
		q := f.create(t, id)
		q.Domain = domain
		q.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"` + selector + `","sceneNamespace":"/scene"}`)
		out, err := f.core.CreateResource(f.ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := create("scene-a", "scene", f.domain)
	create("scene-b", "scene", f.domain)
	protected := create("scene-protected", "scene", f.domain)
	marked, err := f.core.UpdateResourceMetadata(f.ctx, model.ConfigurationResourceMetadata{Domain: f.domain,
		Mutation: mutation("protect"), ResourceID: protected.Result.Head.Resource.ID, ExpectedRevision: protected.Result.Head.Resource.Revision,
		Main: guard(protected.Result.Head), Protect: true})
	if err != nil {
		t.Fatal(err)
	}
	root := ""
	choice := model.ConfigurationCatalogPageRead{Catalog: model.ConfigurationCatalogRead{Domain: f.domain, Limit: 1}, Selector: "scene", OrdinaryOnly: true, ChoiceOnly: true}
	read := func(q model.ConfigurationCatalogPageRead) model.ConfigurationCatalogHeaderPage {
		t.Helper()
		before, _ := f.db.Stats()
		page, err := f.core.ReadCatalogHeaderPage(f.ctx, q)
		after, _ := f.db.Stats()
		if err != nil || after.Calls-before.Calls != 1 || after.ReadersActive != 0 {
			t.Fatalf("catalog page err=%v owner calls=%d readers=%d", err, after.Calls-before.Calls, after.ReadersActive)
		}
		return page
	}
	continuation := func(q model.ConfigurationCatalogPageRead, generation int64, wantConflict bool) {
		t.Helper()
		q.Catalog.Offset = 1
		q.ExpectedGeneration = &generation
		_, err := f.core.ReadCatalogHeaderPage(f.ctx, q)
		if wantConflict {
			requireCode(t, err, "conflict")
		} else if err != nil {
			t.Fatalf("unrelated change invalidated catalog: %v", err)
		}
	}
	initial := read(choice)
	if len(initial.Choices) != 1 || initial.Choices[0].Protected || len(initial.Headers) != 0 {
		t.Fatalf("ordinary first page = %+v", initial)
	}
	otherScope, err := coredata.New(f.db, api.Scope{Namespace: "core", User: "another", Workspace: "station"})
	if err != nil {
		t.Fatal(err)
	}
	otherScopeResource := f.create(t, "other-scope")
	otherScopeResource.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/scene"}`)
	if _, err = otherScope.CreateResource(f.ctx, otherScopeResource); err != nil {
		t.Fatal(err)
	}
	create("other-domain", "scene", otherDomain)
	create("other-selector", "other", f.domain)
	continuation(choice, initial.Generation, false)
	// A protected same-selector current-head update cannot change ordinary choices.
	protectedSnapshot := f.read(t, marked.Result.Head.Resource.ID, "main", "")
	protectedCommit := f.commit(t, protectedSnapshot, "protected-update", "b")
	protectedCommit.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/scene"}`)
	if _, err = f.core.CommitResource(f.ctx, protectedCommit); err != nil {
		t.Fatal(err)
	}
	continuation(choice, initial.Generation, false)

	rootChoice := choice
	rootChoice.Catalog.ParentID = &root
	rootBefore := read(rootChoice)
	rootGuard := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
	namespace, err := f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: mutation("namespace"), ID: "target-ns", ExpectedRevision: "0", Parent: &rootGuard, Name: "Target"})
	if err != nil {
		t.Fatal(err)
	}
	location := namespace.Namespace.ID
	nsChoice := choice
	nsChoice.Catalog.ParentID = &location
	nsBefore := read(nsChoice)
	allBefore := read(choice)
	full := choice
	full.ChoiceOnly = false
	fullBefore := read(full)
	to := model.ConfigurationNamespaceGuard{ID: location, ExpectedRevision: namespace.Namespace.Revision}
	moved, err := f.core.UpdateResourceMetadata(f.ctx, model.ConfigurationResourceMetadata{Domain: f.domain, Mutation: mutation("move"),
		ResourceID: first.Result.Head.Resource.ID, ExpectedRevision: first.Result.Head.Resource.Revision, Main: guard(first.Result.Head), MoveTo: &to})
	if err != nil {
		t.Fatal(err)
	}
	continuation(rootChoice, rootBefore.Generation, true)
	continuation(nsChoice, nsBefore.Generation, true)
	continuation(choice, allBefore.Generation, false) // All-location choice output is unchanged by this move.
	continuation(full, fullBefore.Generation, true)   // Full headers include the resource location and revision.

	beforeBranchChoice, beforeBranchFull := read(choice), read(full)
	branch, err := f.core.CreateBranch(f.ctx, model.ConfigurationBranchCreate{Domain: f.domain, Mutation: mutation("create-branch"),
		ResourceID: moved.Result.Head.Resource.ID, ExpectedResourceRevision: moved.Result.Head.Resource.Revision, Main: guard(moved.Result.Head),
		ID: "draft-branch", Name: "draft", FromCommitID: moved.Result.Head.Commit.ID, FromContentDigest: moved.Result.Head.Commit.ContentDigest})
	if err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeBranchChoice.Generation, false)
	continuation(full, beforeBranchFull.Generation, false)
	draft := f.read(t, branch.Result.Head.Resource.ID, "draft", "")
	draftCommit := f.commit(t, draft, "draft-only", "e")
	draftCommit.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/draft"}`)
	if _, err = f.core.CommitResource(f.ctx, draftCommit); err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeBranchChoice.Generation, false)
	continuation(full, beforeBranchFull.Generation, true)

	beforeHead := read(choice)
	current := f.read(t, moved.Result.Head.Resource.ID, "main", "")
	commit := f.commit(t, current, "change-head", "c")
	commit.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/changed"}`)
	commit.Snapshot.Identity = []byte(`{"active":false}`)
	commit.Snapshot.Payload = []byte(`{"identity":{"active":false},"body":{"value":"c"}}`)
	changed, err := f.core.CommitResource(f.ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeHead.Generation, true)
	beforeArchive := read(choice)
	archived, err := f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{Domain: f.domain, Mutation: mutation("archive"),
		ResourceID: changed.Result.Head.Resource.ID, ExpectedRevision: changed.Result.Head.Resource.Revision, Main: guard(changed.Result.Head), Archived: true})
	if err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeArchive.Generation, true)
	beforeRestore := read(choice)
	restored, err := f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{Domain: f.domain, Mutation: mutation("restore"),
		ResourceID: archived.Result.Head.Resource.ID, ExpectedRevision: archived.Result.Head.Resource.Revision, Main: guard(archived.Result.Head), Archived: false})
	if err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeRestore.Generation, true)
	beforeSelector := read(choice)
	otherChoice := choice
	otherChoice.Selector = "other"
	beforeOther := read(otherChoice)
	current = f.read(t, restored.Result.Head.Resource.ID, "main", "")
	selectorCommit := f.commit(t, current, "change-selector", "f")
	selectorCommit.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"other","sceneNamespace":"/changed"}`)
	if _, err = f.core.CommitResource(f.ctx, selectorCommit); err != nil {
		t.Fatal(err)
	}
	continuation(choice, beforeSelector.Generation, true)
	continuation(otherChoice, beforeOther.Generation, true)
}
