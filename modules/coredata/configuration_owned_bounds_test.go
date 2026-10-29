package coredata_test

import (
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestConfigurationOversizedSnapshotStillRefusesAndReceiptReplays(t *testing.T) {
	f := openStore(t)
	original := f.create(t, "bound-source")
	written, err := f.core.CreateResource(f.ctx, original)
	if err != nil || !written.Found {
		t.Fatalf("initial resource: %+v %v", written, err)
	}
	replay := original
	replay.Snapshot.Payload = make([]byte, model.MaxConfigurationPayloadBytes+1)
	replayed, err := f.core.CreateResource(f.ctx, replay)
	if err != nil || !replayed.Found || !replayed.Replayed {
		t.Fatalf("original receipt replay changed: %+v %v", replayed, err)
	}
	fresh := f.create(t, "oversized-new")
	fresh.Snapshot.Payload = replay.Snapshot.Payload
	if _, err := f.core.CreateResource(f.ctx, fresh); errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized fresh create: %v", err)
	}
	current := f.read(t, "bound-source", "main", "")
	commit := f.commit(t, current, "oversized-commit", "b")
	commit.Snapshot.Payload = replay.Snapshot.Payload
	if _, err := f.core.CommitResource(f.ctx, commit); errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized commit: %v", err)
	}
	after := f.read(t, "bound-source", "main", "")
	if after.Head.Commit.ID != current.Head.Commit.ID || f.count(t, "core_resources") != 1 {
		t.Fatal("oversized attempt changed existing resource")
	}
}
