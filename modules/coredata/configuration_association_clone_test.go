package coredata_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestNamespaceCloneRetainsMoreThanSixteenThousandDerivedFacts(t *testing.T) {
	f := openStore(t)
	root := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
	meta := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("a", 64), Actor: "tester"}
	}
	_, err := f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain, Mutation: meta("source-namespace"), ID: "source-ns", ExpectedRevision: "0", Parent: &root, Name: "Source"})
	if err != nil {
		t.Fatal(err)
	}
	facts := make([]model.ConfigurationAgentAssociation, 513)
	for i := range facts {
		facts[i] = model.ConfigurationAgentAssociation{Kind: model.AgentAssociationSlotHost, AgentID: "a", SlotID: fmt.Sprintf("slot-%04d", i)}
	}
	clone := model.ConfigurationNamespaceClone{Domain: f.domain, Mutation: meta("clone-large"), SourceID: "source-ns", ExpectedRevision: "1", TargetParent: root, Name: "Copy", Namespaces: []model.ConfigurationNamespaceMapping{{SourceID: "source-ns", ExpectedRevision: "1", TargetID: "copy-ns"}}}
	for i := 0; i < 33; i++ {
		id := fmt.Sprintf("asset-%02d", i)
		source := f.create(t, id)
		source.Namespace = model.ConfigurationNamespaceGuard{ID: "source-ns", ExpectedRevision: "1"}
		source.Snapshot.Associations = facts
		created, err := f.core.CreateResource(f.ctx, source)
		if err != nil {
			t.Fatal(err)
		}
		head := created.Result.Head
		copy := f.create(t, "copy-"+id)
		copy.Name = head.Resource.Name
		copy.NameKey = head.Resource.NameKey
		copy.Namespace = model.ConfigurationNamespaceGuard{ID: "copy-ns", ExpectedRevision: "1"}
		copy.Snapshot.Associations = facts
		copy.Source = &model.ConfigurationSourcePin{ResourceID: head.Resource.ID, ExpectedResourceRevision: head.Resource.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest, Main: model.ConfigurationBranchGuard{ID: head.Branch.ID, ExpectedRevision: head.Branch.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest}}
		clone.Resources = append(clone.Resources, copy)
	}
	tree, err := f.core.NamespaceTree(f.ctx, model.NamespaceRead{Domain: f.domain.Key, ID: "source-ns"})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, resource := range tree.Resources {
		count += len(resource.Associations)
	}
	if count != 33*513 || count <= 16384 {
		t.Fatalf("source fact count %d", count)
	}
	result, err := f.core.CloneNamespace(f.ctx, clone)
	if err != nil {
		t.Fatal(err)
	}
	if result.Namespace.ID != "copy-ns" || len(result.AffectedAgentIDs) != 1 || result.AffectedAgentIDs[0] != "a" {
		t.Fatalf("clone result %+v", result)
	}
	tree, err = f.core.NamespaceTree(f.ctx, model.NamespaceRead{Domain: f.domain.Key, ID: "copy-ns"})
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for _, resource := range tree.Resources {
		count += len(resource.Associations)
	}
	if count != 33*513 {
		t.Fatalf("clone lost facts: %d", count)
	}
}
