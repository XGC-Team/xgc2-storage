package coredata

import (
	"encoding/json"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strings"
	"testing"
)

func TestConfigurationNamespaceCloneAtomicSourceFencesAndReplay(t *testing.T) {
	db, ctx, g := configurationFixture(t)
	rootGuard := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
	ns := model.ConfigurationNamespaceWrite{Domain: g, Mutation: model.ConfigurationMutation{Key: "ns", IntentDigest: strings.Repeat("1", 64), Actor: "test"}, ID: "source", Name: "Source", ExpectedRevision: "0", Parent: &rootGuard}
	if _, e := run(t, db, ctx, model.ConfigurationNamespaceCreateOperation, ns); e != nil {
		t.Fatal(e)
	}
	source := configurationCreateForTest(t, g, "r", nil)
	source.Namespace = model.ConfigurationNamespaceGuard{ID: "source", ExpectedRevision: "1"}
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, source)
	h := created.Result.Head
	clone := configurationCreateForTest(t, g, "copy-r", nil)
	clone.Name, clone.NameKey = h.Resource.Name, h.Resource.NameKey
	clone.Namespace = model.ConfigurationNamespaceGuard{ID: "copy-ns", ExpectedRevision: "1"}
	clone.Source = &model.ConfigurationSourcePin{ResourceID: h.Resource.ID, ExpectedResourceRevision: h.Resource.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest, Main: model.ConfigurationBranchGuard{ID: h.Branch.ID, ExpectedRevision: h.Branch.Revision, CommitID: h.Commit.ID, ContentDigest: h.Commit.ContentDigest}}
	q := model.ConfigurationNamespaceClone{Domain: g, Mutation: model.ConfigurationMutation{Key: "clone", IntentDigest: strings.Repeat("2", 64), Actor: "test"}, SourceID: "source", ExpectedRevision: "1", TargetParent: rootGuard, Name: "Copy", Namespaces: []model.ConfigurationNamespaceMapping{{SourceID: "source", ExpectedRevision: "1", TargetID: "copy-ns"}}, Resources: []model.ConfigurationResourceCreate{clone}}
	bad := q
	bad.Mutation.Key = "bad"
	bad.Resources = append([]model.ConfigurationResourceCreate(nil), q.Resources...)
	bad.Resources[0].Source = &model.ConfigurationSourcePin{}
	*bad.Resources[0].Source = *clone.Source
	bad.Resources[0].Source.ContentDigest = strings.Repeat("f", 64)
	before := configurationCounts(t, db, ctx)
	_, e := run(t, db, ctx, model.ConfigurationNamespaceCloneOperation, bad)
	configurationCode(t, e, "conflict")
	if configurationCounts(t, db, ctx) != before {
		t.Fatal("failed clone partially wrote")
	}
	raw, e := run(t, db, ctx, model.ConfigurationNamespaceCloneOperation, q)
	if e != nil {
		t.Fatal(e)
	}
	var out model.ConfigurationNamespaceResult
	if json.Unmarshal(raw, &out) != nil || out.Namespace.ID != "copy-ns" {
		t.Fatal(string(raw))
	}
	read, e := run(t, db, ctx, model.ResourceSnapshotOperation, model.ConfigurationResourceRead{Domain: g, ResourceID: clone.ResourceID, Branch: "main"})
	if e != nil {
		t.Fatal(e)
	}
	var got model.ConfigurationResourceSnapshot
	json.Unmarshal(read, &got)
	if got.Head.Resource.OriginResourceID != "r" || got.Head.Commit.SourceCommitID != h.Commit.ID || got.Head.Commit.BranchRevision != "1" {
		t.Fatal("clone origin or commit revision lost", got.Head)
	}
	q.Resources = nil
	q.Namespaces = nil
	raw, e = run(t, db, ctx, model.ConfigurationNamespaceCloneOperation, q)
	if e != nil {
		t.Fatal(e)
	}
	json.Unmarshal(raw, &out)
	if !out.Replayed || out.Namespace.ID != "copy-ns" {
		t.Fatal("replay examined new plan")
	}
}
