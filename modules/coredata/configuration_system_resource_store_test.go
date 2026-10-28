package coredata_test

import (
	"context"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSystemResourceOwnerReadAndVisibilityBoundary(t *testing.T) {
	f := openStore(t)
	f.exec(t, `UPDATE core_configuration_domains SET system_provisioning=1 WHERE domain='test'`)
	q := f.create(t, "non-derived-provisioned-id")
	q.System = true
	q.SystemKey = "xgc.system.robot-runtime"
	q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 1<<20) + `"}}`)
	if _, err := f.core.CreateResource(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	found, err := f.core.ReadSystemResource(f.ctx, f.domain, q.SystemKey)
	if err != nil || found.ID != q.ResourceID {
		t.Fatalf("system key lost non-derived resource: %+v / %v", found, err)
	}
	f.exec(t, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte(`{"identity":{"active":true},"body":{"graph":"tampered"}}`), q.CommitID)
	if found, err := f.core.ReadSystemResource(f.ctx, f.domain, q.SystemKey); err != nil || found.ID != q.ResourceID {
		t.Fatalf("unused body affected identity: %+v / %v", found, err)
	}
	_, err = f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: f.domain, ResourceID: q.ResourceID, Branch: "main"})
	requireCode(t, err, "data_loss")
	otherScope := f.scope
	otherScope.Workspace = "other"
	other, err := coredata.New(f.db, otherScope)
	if err != nil {
		t.Fatal(err)
	}
	_, err = other.ReadSystemResource(f.ctx, f.domain, q.SystemKey)
	requireCode(t, err, "not_found")
	retired := f.create(t, "retired-id")
	retired.System, retired.SystemKey = true, "xgc.system.retired"
	if _, err := f.core.CreateResource(f.ctx, retired); err != nil {
		t.Fatal(err)
	}
	selected := f.read(t, retired.ResourceID, "main", "")
	_, err = f.core.SetResourceState(f.ctx, model.ConfigurationResourceState{
		Domain: f.domain, ResourceID: retired.ResourceID, ExpectedRevision: selected.Head.Resource.Revision,
		Main: selected.CurrentMain.Branch, Archived: true, RetirementKey: retired.SystemKey,
		Mutation: model.ConfigurationMutation{Key: "retire-system", IntentDigest: strings.Repeat("c", 64), Actor: "tester"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.core.ReadSystemResource(f.ctx, f.domain, retired.SystemKey)
	requireCode(t, err, "not_found")
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err = f.core.ReadSystemResource(canceled, f.domain, q.SystemKey); err == nil {
		t.Fatal("canceled read succeeded")
	}
}
