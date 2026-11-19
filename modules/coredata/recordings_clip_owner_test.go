package coredata_test

import (
	"encoding/json"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestVideoClipVerifiedAllocationKeepsProducerAndRepairsOnlyExactPriorRegistration(t *testing.T) {
	f := openStore(t)
	run := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("clip-run", "local", "", "")))
	q := model.NewRecording{ID: "cut", RunID: run.ID, Kind: "video-clip", Path: "/record/Results/cut.mp4",
		Metadata: json.RawMessage(`{"recordId":"original","experimentId":"untrusted"}`)}
	old, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || !created || old.ExperimentResourceID != "" {
		t.Fatal(old, created, err)
	}
	missing := model.RecordingFilter{Kind: "video-clip", UnassignedOnly: true}
	if page := must(f.core.ListRecordings(f.ctx, missing)); len(page.Recordings) != 1 {
		t.Fatal(page)
	}
	charged := storedRecordingBytes(t, f, q.ID)
	q.ArchiveOwner = &model.RecordingArchiveOwner{RecordID: "original", ExperimentID: "source-experiment"}
	repaired, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || created || repaired.ExperimentResourceID != "source-experiment" || repaired.SessionID != "" || repaired.RunID != run.ID {
		t.Fatal(repaired, created, err)
	}
	if got := storedRecordingBytes(t, f, q.ID); got != charged+int64(len(repaired.ExperimentResourceID)) {
		t.Fatalf("repaired charge %d, previous %d", got, charged)
	}
	if page := must(f.core.ListRecordings(f.ctx, missing)); len(page.Recordings) != 0 {
		t.Fatal(page)
	}
	if page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "source-experiment", ArchiveClass: model.RecordingArchiveScreen})); len(page.Recordings) != 1 || page.Recordings[0].ID != q.ID {
		t.Fatal(page)
	}
	again, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || created || again.ID != q.ID || storedRecordingBytes(t, f, q.ID) != charged+int64(len(again.ExperimentResourceID)) {
		t.Fatal(again, created, err)
	}
	q.ArchiveOwner.ExperimentID = "another"
	if _, _, err := f.core.AddRecording(f.ctx, q); err == nil {
		t.Fatal("rewrote a confirmed archive owner")
	}
	q.ID = "new-cut"
	q.ArchiveOwner.ExperimentID = "source-experiment"
	fresh, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || !created || fresh.ExperimentResourceID != "source-experiment" {
		t.Fatal(fresh, created, err)
	}
	_, other := acceptedRecordingSession(t, f, "another-session", "local", "other", "Other", "simulation")
	q.ID = "crossed"
	q.SessionID = other.SessionID
	q.RunID = other.ID
	if _, _, err := f.core.AddRecording(f.ctx, q); err == nil {
		t.Fatal("mixed a foreign Run and archive")
	}
	q.ArchiveOwner.RecordID = "not-the-source"
	if _, _, err := f.core.AddRecording(f.ctx, q); err == nil {
		t.Fatal("relabelled another Record")
	}
}
