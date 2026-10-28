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

func TestResourceCatalogOneAdmissionAndExactSnapshots(t *testing.T) {
	// The exact catalog admission counter excludes an unrelated scheduled
	// checkpoint. Keep the real owner/maintenance and its finite checkpoint
	// budget; only this fixture's timer is beyond the bounded measurement.
	f := openStore(t, func(config *engine.Config) {
		config.CheckpointDelay = time.Hour
		config.CheckpointBytes = 32 << 20
	})
	root, missing := "", "absent-namespace"
	namespaces := []string{root}
	for i := range 12 {
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
	expected := map[string]model.ConfigurationResourceSnapshot{}
	for n, namespace := range namespaces {
		for i := range 2 {
			id := fmt.Sprintf("resource-%02d-%d", n, i)
			q := f.create(t, id)
			if namespace != "" {
				q.Namespace = model.ConfigurationNamespaceGuard{ID: namespace, ExpectedRevision: "1"}
			}
			q.Snapshot.References = []model.ConfigurationReference{{Slot: "self", Mode: "tracking", TargetDomain: f.domain.Key, TargetResourceID: id, TargetBranch: "main", TargetComponentID: "x"}}
			if _, err := f.core.CreateResource(f.ctx, q); err != nil {
				t.Fatal(err)
			}
			selected := f.read(t, id, "main", "")
			if i == 1 {
				u := f.commit(t, selected, id+"-update", "b")
				u.Snapshot.References = q.Snapshot.References
				if _, err := f.core.CommitResource(f.ctx, u); err != nil {
					t.Fatal(err)
				}
				selected = f.read(t, id, "main", "")
			}
			expected[id] = selected
		}
	}
	read := func(parent *string, limit, offset int, archived bool) []model.ConfigurationResourceSnapshot {
		t.Helper()
		before, _ := f.db.Stats()
		rows, err := f.core.ReadResourceCatalog(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain, ParentID: parent, IncludeArchived: archived, Limit: limit, Offset: offset})
		after, _ := f.db.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if after.Calls-before.Calls != 1 {
			t.Fatalf("catalog used %d owner admissions for %d rows", after.Calls-before.Calls, len(rows))
		}
		for i, row := range rows {
			if !reflect.DeepEqual(row, expected[row.Head.Resource.ID]) {
				t.Fatalf("catalog changed exact frozen snapshot/current identity: %+v", row.Head)
			}
			if i > 0 && rows[i-1].Head.Resource.ID >= row.Head.Resource.ID {
				t.Fatal("catalog order is not stable resource-ID order")
			}
			if parent != nil && row.Head.Resource.NamespaceID != *parent {
				t.Fatal("selected catalog escaped its exact namespace")
			}
		}
		return rows
	}
	if got := len(read(nil, 0, 0, false)); got != len(expected) {
		t.Fatalf("all namespaces lost rows: %d", got)
	}
	for _, namespace := range namespaces {
		if got := len(read(&namespace, 0, 0, false)); got != 2 {
			t.Fatalf("namespace %q returned %d rows", namespace, got)
		}
	}
	if got := read(&missing, 0, 0, false); len(got) != 0 || got == nil {
		t.Fatal("empty selected catalog is not an empty array")
	}
	all, page := read(nil, 0, 0, false), read(nil, 3, 5, false)
	if !reflect.DeepEqual(page, all[5:8]) {
		t.Fatal("bounded catalog page changed membership")
	}
	selected := expected["resource-01-0"]
	_, err := f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{Domain: f.domain,
		Mutation:   model.ConfigurationMutation{Key: "archive-one", IntentDigest: strings.Repeat("c", 64), Actor: "tester"},
		ResourceID: selected.Head.Resource.ID, ExpectedRevision: selected.Head.Resource.Revision, Main: selected.CurrentMain.Branch, Archived: true})
	if err != nil {
		t.Fatal(err)
	}
	archived, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: selected.Head.Resource.ID, Branch: "main", IncludeArchived: true})
	if err != nil {
		t.Fatal(err)
	}
	expected[selected.Head.Resource.ID] = archived
	if len(read(&namespaces[1], 0, 0, false)) != 1 || len(read(&namespaces[1], 0, 0, true)) != 2 || len(read(nil, 0, 0, true)) != len(expected) {
		t.Fatal("live/archive namespace selections changed")
	}
	otherScope := f.scope
	otherScope.Workspace = "other"
	other, err := coredata.New(f.db, otherScope)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := other.ReadResourceCatalog(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain})
	if err != nil || len(rows) != 0 {
		t.Fatalf("catalog escaped scope: %v / %d", err, len(rows))
	}
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	if rows, err := f.core.ReadResourceCatalog(canceled, model.ConfigurationCatalogRead{Domain: f.domain}); err == nil || rows != nil {
		t.Fatal("canceled read published a successful catalog")
	}
}

func TestResourceCatalogRejectsCorruptionAndOverflowWithoutPartialResult(t *testing.T) {
	for _, scenario := range []string{"payload", "reference", "main", "resource", "foreign-branch", "response-bytes"} {
		t.Run(scenario, func(t *testing.T) {
			f := openStore(t)
			for _, id := range []string{"first", "second"} {
				q := f.create(t, id)
				q.Snapshot.References = []model.ConfigurationReference{{Slot: "self", Mode: "tracking", TargetDomain: f.domain.Key, TargetResourceID: id, TargetBranch: "main"}}
				if scenario == "response-bytes" {
					q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"large":"` + strings.Repeat("x", 7<<20) + `"}}`)
				}
				if _, err := f.core.CreateResource(f.ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "payload":
				f.exec(t, `UPDATE core_snapshots SET payload=? WHERE id='second-v1'`, []byte(`{"identity":{"active":true},"body":{"value":"tampered"}}`))
			case "reference":
				f.exec(t, `UPDATE core_references SET target_resource_id='first' WHERE commit_id='second-v1'`)
			case "main":
				f.exec(t, `UPDATE core_branches SET head_commit_id='first-v1' WHERE id='second-main'`)
			case "resource":
				f.exec(t, `UPDATE core_resources SET name='changed' WHERE id='second'`)
			case "foreign-branch":
				f.exec(t, `UPDATE core_snapshots SET branch_id='first-main',body=json_set(CAST(body AS TEXT),'$.branch_id','first-main') WHERE id='second-v1'`)
			}
			rows, err := f.core.ReadResourceCatalog(f.ctx, model.ConfigurationCatalogRead{Domain: f.domain})
			code := "data_loss"
			if scenario == "response-bytes" {
				code = "resource_exhausted"
			}
			requireCode(t, err, code)
			if rows != nil {
				t.Fatal("failed complete catalog leaked a successful prefix")
			}
		})
	}
}
