package coredata_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestGallerySourceQueryKeepsProducerAndSelectsOnlyExactSource(t *testing.T) {
	f := openStore(t)
	_, plot := acceptedRecordingSession(t, f, "plot-session", "local", "experiment-a", "A", "simulation")
	_, foreign := acceptedRecordingSession(t, f, "foreign-session", "other-target", "experiment-b", "B", "simulation")
	active := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("active-record-b", "local", "plot-session", "experiment-a")))
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: active.ID, Status: model.RunRunning}); err != nil {
		t.Fatal(err)
	}
	active = must(f.core.GetRun(f.ctx, active.ID))
	for i, row := range []struct {
		id, source string
		run        model.Run
	}{
		{"old-a", "source-a", plot}, {"new-a", "source-a", plot}, {"newer-b", "source-b", plot}, {"foreign-a", "source-a", foreign},
	} {
		metadata, _ := json.Marshal(map[string]any{"sourceRecordingId": row.source, "scientificPayload": "not-in-list"})
		registered := must3(f.core.AddRecording(f.ctx, model.NewRecording{ID: row.id, RunID: row.run.ID, SessionID: row.run.SessionID, Kind: "image-gallery", Path: "/figures/" + row.id, Metadata: metadata, At: time.Unix(int64(i+1), 0)}))
		if registered.RunID != row.run.ID || registered.Summary.SourceRecordingID != row.source {
			t.Fatalf("provenance changed producer: %+v", registered)
		}
	}
	filter := model.RecordingFilter{ExperimentResourceID: "experiment-a", Kind: "image-gallery", SourceRecordingID: "source-a", Order: model.RecordingNewestFirst, Limit: 1}
	page := must(f.core.ListRecordings(f.ctx, filter))
	if len(page.Recordings) != 1 || page.Recordings[0].ID != "new-a" || len(page.Recordings[0].Metadata) != 0 || page.Next == "" {
		t.Fatalf("wrong exact summary page: %+v", page)
	}
	filter.Cursor = page.Next
	page = must(f.core.ListRecordings(f.ctx, filter))
	if len(page.Recordings) != 1 || page.Recordings[0].ID != "old-a" || page.Next != "" {
		t.Fatalf("wrong source page continuation: %+v", page)
	}
	unchanged := must(f.core.GetRun(f.ctx, active.ID))
	if unchanged.Status != active.Status {
		t.Fatalf("gallery lookup changed active recording: %s -> %s", active.Status, unchanged.Status)
	}
}
