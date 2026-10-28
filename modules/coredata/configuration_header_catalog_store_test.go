package coredata_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestResourceHeadersOwnerPagesAndUnusedPayloadCorruption(t *testing.T) {
	f := openStore(t, func(config *engine.Config) {
		config.CheckpointDelay = time.Hour
		config.CheckpointBytes = 32 << 20
	})
	root, missing := "", "absent"
	namespaces := []string{root}
	for i := range 8 {
		id := fmt.Sprintf("namespace-%02d", i)
		parent := model.ConfigurationNamespaceGuard{ExpectedRevision: "0"}
		_, err := f.core.CreateNamespace(f.ctx, model.ConfigurationNamespaceWrite{Domain: f.domain,
			Mutation: model.ConfigurationMutation{Key: "create-" + id, IntentDigest: strings.Repeat("a", 64), Actor: "tester"},
			ID:       id, ExpectedRevision: "0", Parent: &parent, Name: id})
		if err != nil {
			t.Fatal(err)
		}
		namespaces = append(namespaces, id)
	}
	expected := map[string]model.ConfigurationHead{}
	for n, namespace := range namespaces {
		for i := range 2 {
			id := fmt.Sprintf("asset-%02d-%d", n, i)
			q := f.create(t, id)
			if namespace != "" {
				q.Namespace = model.ConfigurationNamespaceGuard{ID: namespace, ExpectedRevision: "1"}
			}
			q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 256<<10) + `"}}`)
			if _, err := f.core.CreateResource(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			expected[id] = f.read(t, id, "main", "").Head
		}
	}
	read := func(parent *string, limit, offset int, archived bool) []model.ConfigurationResourceHeader {
		t.Helper()
		before, _ := f.db.Stats()
		rows, err := f.core.ReadResourceHeaders(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain, ParentID: parent, Limit: limit, Offset: offset, IncludeArchived: archived})
		after, _ := f.db.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if after.Calls-before.Calls != 1 {
			t.Fatalf("%d admissions for %d headers", after.Calls-before.Calls, len(rows))
		}
		for i, row := range rows {
			if !reflect.DeepEqual(row.Head, expected[row.Head.Resource.ID]) {
				t.Fatalf("header differs from exact owner read: %+v", row)
			}
			if i > 0 && rows[i-1].Head.Resource.ID >= row.Head.Resource.ID {
				t.Fatal("unstable resource-ID ordering")
			}
			if parent != nil && row.Head.Resource.NamespaceID != *parent {
				t.Fatal("namespace selection escaped")
			}
		}
		return rows
	}
	all := read(nil, 0, 0, false)
	if len(all) != len(expected) {
		t.Fatal("all-namespace catalog lost rows")
	}
	for _, namespace := range namespaces {
		if len(read(&namespace, 0, 0, false)) != 2 {
			t.Fatalf("namespace %q changed membership", namespace)
		}
	}
	if rows := read(&missing, 0, 0, false); rows == nil || len(rows) != 0 {
		t.Fatal("empty namespace must return an empty page")
	}
	if page := read(nil, 4, 3, false); !reflect.DeepEqual(page, all[3:7]) {
		t.Fatal("explicit page is not the requested slice")
	}

	asset := f.read(t, "asset-01-0", "main", "")
	_, err := f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{Domain: f.domain,
		Mutation:   model.ConfigurationMutation{Key: "archive-one", IntentDigest: strings.Repeat("c", 64), Actor: "tester"},
		ResourceID: asset.Head.Resource.ID, ExpectedRevision: asset.Head.Resource.Revision, Main: asset.CurrentMain.Branch, Archived: true})
	if err != nil {
		t.Fatal(err)
	}
	archived, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: asset.Head.Resource.ID, Branch: "main", IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	expected[asset.Head.Resource.ID] = archived.Head
	if len(read(&namespaces[1], 0, 0, false)) != 1 || len(read(&namespaces[1], 0, 0, true)) != 2 {
		t.Fatal("archive selection changed")
	}

	f.exec(t, `UPDATE core_snapshots SET payload=? WHERE id='asset-00-0-v1'`, []byte(`{"identity":{"active":true},"body":{"graph":"tampered"}}`))
	if len(read(nil, 0, 0, false)) != len(expected)-1 {
		t.Fatal("unused corrupted body changed headers")
	}
	_, err = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: "asset-00-0", Branch: "main"})
	requireCode(t, err, "data_loss")

	otherScope := f.scope
	otherScope.Workspace = "other"
	other, err := coredata.New(f.db, otherScope)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := other.ReadResourceHeaders(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain}); err != nil || len(rows) != 0 {
		t.Fatalf("header escaped scope: %d / %v", len(rows), err)
	}
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	if rows, err := f.core.ReadResourceHeaders(canceled, model.ConfigurationCatalogRead{Domain: f.domain}); err == nil || rows != nil {
		t.Fatal("canceled read published a page")
	}
}

func TestResourceHeadersRejectBrokenRelations(t *testing.T) {
	for _, scenario := range []string{"main", "resource", "foreign-branch", "missing-commit", "identity", "summary"} {
		t.Run(scenario, func(t *testing.T) {
			f := openStore(t)
			for _, id := range []string{"first", "second"} {
				if _, err := f.core.CreateResource(f.ctx, f.create(t, id)); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "main":
				f.exec(t, `UPDATE core_branches SET head_commit_id='first-v1' WHERE id='second-main'`)
			case "resource":
				f.exec(t, `UPDATE core_resources SET name='changed' WHERE id='second'`)
			case "foreign-branch":
				f.exec(t, `UPDATE core_snapshots SET branch_id='first-main',body=json_set(CAST(body AS TEXT),'$.branch_id','first-main') WHERE id='second-v1'`)
			case "missing-commit":
				f.exec(t, `UPDATE core_resources SET main_commit_id='absent',body=json_set(CAST(body AS TEXT),'$.main_commit_id','absent') WHERE id='second'`)
			case "identity":
				f.exec(t, `UPDATE core_snapshots SET identity_json=? WHERE id='second-v1'`, []byte(`{"active":false}`))
			case "summary":
				f.exec(t, `UPDATE core_snapshots SET catalog_summary_json=? WHERE id='second-v1'`, []byte(`{"label":"changed"}`))
			}
			rows, err := f.core.ReadResourceHeaders(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain})
			requireCode(t, err, "data_loss")
			if rows != nil {
				t.Fatal("broken relation leaked a successful prefix")
			}
		})
	}
}

func TestHistoricalReadUsesCurrentMainProjectionWithoutOpeningItsPayload(t *testing.T) {
	f := openStore(t)
	create := f.create(t, "historical")
	if _, err := f.core.CreateResource(f.ctx, create); err != nil {
		t.Fatal(err)
	}
	initial := f.read(t, "historical", "main", "")
	commit := f.commit(t, initial, "next", "b")
	if _, err := f.core.CommitResource(f.ctx, commit); err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte(`not valid JSON`), commit.CommitID)
	historical, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: "historical", CommitID: create.CommitID})
	if err != nil || historical.Head.Commit.ID != create.CommitID || historical.CurrentMain.Branch.CommitID != commit.CommitID || string(historical.CurrentMain.Identity) != string(commit.Snapshot.Identity) {
		t.Fatalf("historical read opened unrelated main payload: %+v %v", historical.CurrentMain, err)
	}
	if _, err = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: "historical", Branch: "main"}); err == nil {
		t.Fatal("exact main detail accepted a corrupt body")
	}
	f.exec(t, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte(`also corrupt`), create.CommitID)
	identity, err := f.core.ReadCommitIdentity(f.ctx, f.domain, create.CommitID)
	if err != nil || identity.Commit.ID != create.CommitID || string(identity.Identity) != string(create.Snapshot.Identity) || identity.CurrentMain.Branch.CommitID != commit.CommitID {
		t.Fatalf("identity point opened a frozen payload: %+v %v", identity, err)
	}
	if _, err = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: "historical", CommitID: create.CommitID}); err == nil {
		t.Fatal("exact historical detail accepted a corrupt body")
	}
}
