package coredata

import (
	"encoding/json"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strings"
	"testing"
)

func TestConfigurationExplicitSystemRetirementPreservesProtectionAndHistory(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	g.Key = "retirement"
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		t.Fatal(e)
	}
	e = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: g.Key, SchemaIdentity: g.SchemaIdentity, SchemaVersion: 1, RegistryDigest: g.RegistryDigest, MainVisibility: true, AllowSystemProvisioning: true}})
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	q := configurationCreateForTest(t, g, "retired-one", nil)
	q.System = true
	q.SystemKey = "retired.workflow"
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	state := model.ConfigurationResourceState{Domain: g, Mutation: model.ConfigurationMutation{Key: "retire", Actor: "system", IntentDigest: strings.Repeat("a", 64)}, ResourceID: q.ResourceID, ExpectedRevision: created.Result.Head.Resource.Revision, Main: model.ConfigurationBranchGuard{ID: created.Result.Head.Branch.ID, ExpectedRevision: created.Result.Head.Branch.Revision, CommitID: created.Result.Head.Commit.ID, ContentDigest: created.Result.Head.Commit.ContentDigest}, Archived: true}
	_, e = run(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	configurationCode(t, e, "failed_precondition")
	state.RetirementKey = "another.workflow"
	_, e = run(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	configurationCode(t, e, "failed_precondition")
	state.RetirementKey = q.SystemKey
	raw, e := run(t, db, ctx, model.ConfigurationResourceStateOperation, state)
	if e != nil {
		t.Fatal(e)
	}
	var retired model.ConfigurationMutationResult
	if e = json.Unmarshal(raw, &retired); e != nil {
		t.Fatal(e)
	}
	if !retired.Result.Head.Resource.System || retired.Result.Head.Resource.ArchivedAt == "" || retired.Result.Head.Commit.ID != created.Result.Head.Commit.ID {
		t.Fatal("retirement changed protection or content history")
	}
}
