package coredata_test

import (
	"fmt"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestGetSessionsBatchesIdentitiesAndPreservesScope(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"one", "two"} {
		_, _, err := f.core.OpenSession(f.ctx, model.NewSession{ID: id, TargetID: id, ExperimentResourceID: "experiment", ExperimentCommitID: "commit", RunMode: "test"})
		if err != nil {
			t.Fatal(err)
		}
	}
	other, err := coredata.New(f.db, api.Scope{Namespace: f.scope.Namespace, User: "other", Workspace: f.scope.Workspace})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.OpenSession(f.ctx, model.NewSession{ID: "one", TargetID: "one", ExperimentResourceID: "foreign", ExperimentCommitID: "commit", RunMode: "test"}); err != nil {
		t.Fatal(err)
	}
	ids := []string{"one", "two", "one"}
	for len(ids) < 500 {
		ids = append(ids, fmt.Sprintf("missing-%d", len(ids)))
	}
	got, err := f.core.GetSessions(f.ctx, ids)
	if err != nil || len(got) != 2 {
		t.Fatalf("batch result = %+v, %v", got, err)
	}
	for _, record := range got {
		if record.ExperimentResourceID != "experiment" {
			t.Fatalf("batch crossed its scope: %+v", record)
		}
	}
	if _, err := f.core.GetSessions(f.ctx, append(ids, "too-many")); errCode(err) != "invalid_argument" {
		t.Fatalf("oversized batch error = %v", err)
	}
	if _, err := f.core.GetSessions(f.ctx, []string{""}); errCode(err) != "invalid_argument" {
		t.Fatalf("empty identity error = %v", err)
	}
	if got, err := f.core.GetSessions(f.ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("empty batch = %+v, %v", got, err)
	}
}
