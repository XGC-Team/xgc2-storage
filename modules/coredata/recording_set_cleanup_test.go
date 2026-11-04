package coredata_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRecordingSetCleanupRetainsProvenanceAndReleasesSharedOwnership(t *testing.T) {
	f := openStore(t)
	run := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("owner", "local", "", "experiment")))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, Termination: "completed", At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	add := func(id, kind, path string, metadata string) model.Recording {
		t.Helper()
		return must3(f.core.AddRecording(f.ctx, model.NewRecording{ID: id, RunID: run.ID, Kind: kind, Path: path, Metadata: json.RawMessage(metadata), At: epoch}))
	}
	first := add("first", "rosbag-set", "/set", `{"recordingId":"capture","parts":[{"path":"/a.bag"},{"path":"/b.bag"},{"path":"/c.bag"}]}`)
	second := add("second", "rosbag-set", "/set", `{"parts":[{"path":"/b.bag"}]}`)
	add("single", "rosbag", "/c.bag", `{}`)
	guard := func(v model.Recording) model.RecordingDelete {
		return model.RecordingDelete{ID: v.ID, RunID: v.RunID, SessionID: v.SessionID, Kind: v.Kind, Path: v.Path, CreatedAt: v.CreatedAt}
	}
	got, shared, err := f.core.RecordingSetCleanupPlan(f.ctx, guard(first))
	sort.Strings(shared)
	if err != nil || string(got.Metadata) != string(first.Metadata) || !reflect.DeepEqual(shared, []string{"/b.bag", "/c.bag"}) {
		t.Fatalf("selected source and shared paths: %+v %v %v", got, shared, err)
	}
	cleaned := must(f.core.CompleteRecordingSetCleanup(f.ctx, guard(first)))
	if !cleaned.Summary.LocalPayloadRemoved || string(cleaned.Metadata) != string(first.Metadata) {
		t.Fatalf("cleanup lost provenance: %+v", cleaned)
	}
	_, shared, err = f.core.RecordingSetCleanupPlan(f.ctx, guard(second))
	if err != nil || len(shared) != 0 {
		t.Fatalf("cleaned source still owns the other set's file: %v %v", shared, err)
	}
	replayed, created, err := f.core.AddRecording(f.ctx, model.NewRecording{ID: first.ID, RunID: run.ID, Kind: first.Kind, Path: first.Path, Metadata: first.Metadata, At: epoch})
	if err != nil || created || !replayed.Summary.LocalPayloadRemoved || string(replayed.Metadata) != string(first.Metadata) {
		t.Fatalf("immutable registration replay changed: %+v %v %v", replayed, created, err)
	}
}
