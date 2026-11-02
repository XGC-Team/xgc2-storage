package coredata_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The ROS bag archive uses ListRecordings with a ROSBag class filter. A
// collected set must retain the accepted producer Run, not the set row ID or
// the recording ID carried in metadata, including after reopening the store.
func TestROSBagArchiveListsSetWithAcceptedProducerRunAfterReopen(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "experiment", "Experiment", "managed")
	_, otherRun := acceptedRecordingSession(t, f, "other-session", "agent", "experiment", "Experiment", "managed")
	metadata := json.RawMessage(`{"recordingId":"capture-id"}`)
	rows := []model.NewRecording{
		{ID: "initial-bag", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/archive/initial.bag", Metadata: metadata, At: epoch},
		{ID: "collected-set", SessionID: "session", RunID: run.ID, Kind: "rosbag-set", Path: "/archive/collected", Metadata: metadata, At: epoch.Add(time.Second)},
		{ID: "foreign-set", SessionID: "other-session", RunID: otherRun.ID, Kind: "rosbag-set", Path: "/archive/foreign", At: epoch.Add(2 * time.Second)},
		{ID: "gallery", SessionID: "session", RunID: run.ID, Kind: "image-gallery", Path: "/archive/gallery.bag", At: epoch.Add(3 * time.Second)},
	}
	for _, row := range rows {
		if _, created, err := f.core.AddRecording(f.ctx, row); err != nil || !created {
			t.Fatalf("register %s: created=%v err=%v", row.ID, created, err)
		}
	}

	check := func(t *testing.T) {
		t.Helper()
		page, err := f.core.ListRecordings(f.ctx, model.RecordingFilter{
			SessionID: "session", ArchiveClass: model.RecordingArchiveROSBag,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Recordings) != 2 || page.Next != "" {
			t.Fatalf("session ROS bag archive: %+v", page)
		}
		kinds := map[string]string{"initial-bag": "rosbag", "collected-set": "rosbag-set"}
		for _, recording := range page.Recordings {
			kind, found := kinds[recording.ID]
			if !found || recording.Kind != kind || recording.ArchiveClass != model.RecordingArchiveROSBag ||
				recording.SessionID != "session" || recording.RunID != run.ID || recording.Summary.RecordingID != "capture-id" {
				t.Fatalf("unexpected archive record: %+v", recording)
			}
			if recording.ID == recording.RunID || recording.Summary.RecordingID == recording.RunID {
				t.Fatalf("row, capture and producer Run identities were conflated: %+v", recording)
			}
			delete(kinds, recording.ID)
		}
		if len(kinds) != 0 {
			t.Fatalf("archive omitted recordings: %v", kinds)
		}
	}
	t.Run("registered", check)
	f.reopen(t)
	t.Run("reopened", check)
}
