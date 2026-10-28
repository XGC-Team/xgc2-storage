package coredata_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSelectedOrdinaryOwnerCatalogIncludesRealClones(t *testing.T) {
	f := openStore(t)
	mutation := func(key string) model.ConfigurationMutation {
		return model.ConfigurationMutation{Key: key, IntentDigest: strings.Repeat("b", 64), Actor: "tester"}
	}
	main := func(head model.ConfigurationHead) model.ConfigurationBranchGuard {
		return model.ConfigurationBranchGuard{ID: head.Branch.ID, ExpectedRevision: head.Branch.Revision, CommitID: head.Commit.ID, ContentDigest: head.Commit.ContentDigest}
	}
	create := func(id, selector string) model.ConfigurationHead {
		t.Helper()
		request := f.create(t, id)
		request.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"` + selector + `","sceneNamespace":"/scene"}`)
		result, err := f.core.CreateResource(f.ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		return result.Result.Head
	}
	protected := create("a-system", "scene")
	result, err := f.core.UpdateResourceMetadata(f.ctx, model.ConfigurationResourceMetadata{
		Domain: f.domain, Mutation: mutation("protect-scene"), ResourceID: protected.Resource.ID,
		ExpectedRevision: protected.Resource.Revision, Main: main(protected), Protect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	protected = result.Result.Head
	if !protected.Resource.System {
		t.Fatal("source did not become protected")
	}
	clone := func(id string, source model.ConfigurationHead) model.ConfigurationHead {
		t.Helper()
		request := f.create(t, id)
		request.Snapshot.CatalogSummary = []byte(`{"_catalogSelector":"scene","sceneNamespace":"/scene"}`)
		request.Source = &model.ConfigurationSourcePin{
			ResourceID: source.Resource.ID, ExpectedResourceRevision: source.Resource.Revision,
			CommitID: source.Commit.ID, ContentDigest: source.Commit.ContentDigest, Main: main(source),
		}
		result, err := f.core.CreateResource(f.ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		head := result.Result.Head
		if head.Resource.System || head.Resource.SystemKey != "" || head.Resource.OriginResourceID != source.Resource.ID || head.Resource.OriginCommitID != source.Commit.ID {
			t.Fatalf("clone did not retain ordinary ownership and immutable origin: %+v", head.Resource)
		}
		return head
	}
	clone("b-copy-system", protected)
	ordinary := create("c-operator", "scene")
	clone("d-copy-operator", ordinary)
	archived := create("e-archived", "scene")
	if _, err := f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{
		Domain: f.domain, Mutation: mutation("archive-scene"), ResourceID: archived.Resource.ID,
		ExpectedRevision: archived.Resource.Revision, Main: main(archived), Archived: true,
	}); err != nil {
		t.Fatal(err)
	}
	create("f-unrelated", "other")
	root := ""
	for _, scope := range []struct {
		name   string
		parent *string
	}{{"all", nil}, {"root", &root}} {
		t.Run(scope.name, func(t *testing.T) {
			var got []string
			for offset := 0; offset < 5; offset++ {
				rows, err := f.core.ReadSelectedResourceHeaders(f.ctx, model.ConfigurationSelectedCatalogRead{
					Catalog:  model.ConfigurationCatalogRead{Domain: f.domain, ParentID: scope.parent, Limit: 1, Offset: offset},
					Selector: "scene", OrdinaryOnly: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) == 0 {
					break
				}
				if len(rows) != 1 || rows[0].Head.Resource.System {
					t.Fatalf("unbounded page or protected source returned: %+v", rows)
				}
				got = append(got, rows[0].Head.Resource.ID)
			}
			want := []string{"b-copy-system", "c-operator", "d-copy-operator"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ordinary Scene rows=%v, want %v", got, want)
			}
		})
	}
}
