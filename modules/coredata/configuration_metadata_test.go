package coredata

import (
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strings"
	"testing"
)

func TestConfigurationResourceMetadataPreservesContentAndFencesMain(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, g, "r", nil))
	h := created.Result.Head
	q := model.ConfigurationResourceMetadata{Domain: g, Mutation: model.ConfigurationMutation{Key: "protect", IntentDigest: strings.Repeat("c", 64), Actor: "test"}, ResourceID: h.Resource.ID, ExpectedRevision: h.Resource.Revision, Main: model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}, Protect: true}
	changed := configurationWrite(t, db, ctx, model.ConfigurationResourceMetadataOperation, q)
	if !changed.Result.Head.Resource.System || changed.Result.Head.Resource.Revision != "2" || changed.Result.Head.Commit != h.Commit || changed.Result.Head.Branch != h.Branch {
		t.Fatal("metadata update fabricated content or lost protection")
	}
	replayed := configurationWrite(t, db, ctx, model.ConfigurationResourceMetadataOperation, q)
	if !replayed.Replayed || replayed.Result != changed.Result {
		t.Fatal("original metadata result was not replayed")
	}
	q.Mutation.Key = "stale"
	q.Main.ContentDigest = strings.Repeat("f", 64)
	q.ExpectedRevision = "2"
	_, err := run(t, db, ctx, model.ConfigurationResourceMetadataOperation, q)
	configurationCode(t, err, "conflict")
	q.Mutation.Key = "protected-move"
	q.Main.ContentDigest = h.Commit.ContentDigest
	q.MoveTo = &model.ConfigurationNamespaceGuard{ID: "other", ExpectedRevision: "1"}
	_, err = run(t, db, ctx, model.ConfigurationResourceMetadataOperation, q)
	if err == nil {
		t.Fatal("protected resource moved")
	}
}
