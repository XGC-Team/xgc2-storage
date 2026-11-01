package coredata_test

import (
	"bytes"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"strings"
	"testing"
)

func TestExplicitSchemaAdvancePreservesHistoryAcrossOwnerReopen(t *testing.T) {
	f := openStore(t)
	old := f.domain
	create := f.create(t, "resource")
	first := must(f.core.CreateResource(f.ctx, create))
	before := f.read(t, "resource", "", create.CommitID)
	next := old
	next.SchemaVersion = 2
	next.SchemaIdentity = "catalog-v2"
	next.RegistryDigest = strings.Repeat("b", 64)
	declaration := model.ConfigurationDomainDeclaration{Key: next.Key, SchemaIdentity: next.SchemaIdentity, SchemaVersion: next.SchemaVersion, RegistryDigest: next.RegistryDigest, MainVisibility: true}
	requireCode(t, coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}), "conflict")
	wrong := old
	wrong.RegistryDigest = strings.Repeat("c", 64)
	declaration.PreviousSchema = &wrong
	requireCode(t, coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}), "conflict")
	declaration.PreviousSchema = &old
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}); err != nil {
		t.Fatal(err)
	}
	// Durable idempotent replay and a real reopen do not rewrite old snapshots.
	if err := coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}); err != nil {
		t.Fatal(err)
	}
	f.domain = next
	f.reopen(t)
	history := f.read(t, "resource", "", create.CommitID)
	if history.Head.Commit != first.Result.Head.Commit || !bytes.Equal(history.Payload, before.Payload) || !bytes.Equal(history.Manifest, before.Manifest) {
		t.Fatal("owner declaration changed immutable history")
	}
	_, err := f.core.ReadResource(f.ctx, model.ConfigurationResourceRead{Domain: old, ResourceID: "resource", CommitID: create.CommitID})
	requireCode(t, err, "conflict")
	changed := f.commit(t, history, "current-version", "b")
	second := must(f.core.CommitResource(f.ctx, changed))
	if second.Result.Head.Commit.SchemaVersion != 2 {
		t.Fatal("new commit was not authored with the current schema")
	}
	declaration.SchemaVersion = 1
	declaration.PreviousSchema = &next
	requireCode(t, coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}), "conflict")
	declaration.SchemaVersion = 2
	declaration.Capabilities = []string{"escalated"}
	requireCode(t, coredata.DeclareConfigurationDomains(f.ctx, f.db, []model.ConfigurationDomainDeclaration{declaration}), "conflict")
}
