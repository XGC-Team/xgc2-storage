package coredata_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRecordingTimestampProjectionStaysInsideNanosecondRange(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	base := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	minInstant := time.Date(1677, 9, 21, 0, 12, 43, 145224192, time.UTC)
	maxInstant := time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC)

	rows := []struct {
		id          string
		at          time.Time
		started     string
		ended       string
		finished    string
		wantStarted time.Time
		wantEnded   time.Time
		wantFinish  time.Time
	}{
		// Registration order is deliberately not chronological. The overflow
		// rows must sort at their registration time, not at a wrapped key.
		{"high-overflow", base.Add(6 * time.Hour), `"9999-12-31T23:59:59.999999999Z"`, `"9999-12-31T23:59:59Z"`, `"0001-01-01T00:00:00Z"`, time.Time{}, time.Time{}, time.Time{}},
		{"low-overflow", base.Add(time.Hour), `"1600-01-01T00:00:00Z"`, `"1677-09-21T00:12:43.145224191Z"`, `"2262-04-11T23:47:16.854775808Z"`, time.Time{}, time.Time{}, time.Time{}},
		{"min-boundary", base.Add(5 * time.Hour), `"1677-09-21T00:12:43.145224192Z"`, `"1677-09-21T00:12:43.145224192Z"`, `"1677-09-21T00:12:43.145224192Z"`, minInstant, minInstant, minInstant},
		{"max-boundary", base.Add(4 * time.Hour), `"2262-04-11T23:47:16.854775807Z"`, `"2262-04-11T23:47:16.854775807Z"`, `"2262-04-11T23:47:16.854775807Z"`, maxInstant, maxInstant, maxInstant},
		{"normal", base.Add(3 * time.Hour), `"2026-10-10T12:00:00Z"`, `"2026-10-10T12:05:00Z"`, `"2026-10-10T12:06:00Z"`, base, base.Add(5 * time.Minute), base.Add(6 * time.Minute)},
	}
	for _, r := range rows {
		metadata := json.RawMessage(fmt.Sprintf(`{"startedAt":%s,"endedAt":%s,"finishedAt":%s}`, r.started, r.ended, r.finished))
		rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
			ID: r.id, SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/" + r.id + ".bag", Metadata: metadata, At: r.at,
		}))
		if !rec.Summary.StartedAt.Equal(r.wantStarted) || !rec.Summary.EndedAt.Equal(r.wantEnded) || !rec.Summary.FinishedAt.Equal(r.wantFinish) {
			t.Errorf("%s summary: %+v", r.id, rec.Summary)
		}
		wantSort := r.wantStarted
		if wantSort.IsZero() {
			wantSort = r.at
		}
		if !rec.SortAt.Equal(wantSort) {
			t.Errorf("%s sort_at: %s, want %s", r.id, rec.SortAt, wantSort)
		}
		got := must(f.core.GetRecording(f.ctx, r.id))
		if !reflect.DeepEqual(got.Summary, rec.Summary) || !got.SortAt.Equal(rec.SortAt) {
			t.Errorf("%s readback: %+v", r.id, got)
		}
		if string(got.Metadata) != string(metadata) {
			t.Errorf("%s raw metadata changed: %s", r.id, got.Metadata)
		}
		// The stored summary and the stored index key must agree.
		var storedSort int64
		var storedSummary []byte
		if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, "SELECT sort_at,summary_json FROM recordings WHERE scope=? AND id=?", engine.ScopeID(f.scope), r.id).Scan(&storedSort, &storedSummary)
		}); err != nil {
			t.Fatal(err)
		}
		var projected model.RecordingSummary
		if json.Unmarshal(storedSummary, &projected) != nil {
			t.Fatalf("%s stored summary is invalid", r.id)
		}
		if !projected.StartedAt.Equal(rec.Summary.StartedAt) || !projected.EndedAt.Equal(rec.Summary.EndedAt) || !projected.FinishedAt.Equal(rec.Summary.FinishedAt) {
			t.Errorf("%s stored summary disagrees: %+v", r.id, projected)
		}
		if storedSort != rec.SortAt.UnixNano() {
			t.Errorf("%s stored sort_at %d, want %d", r.id, storedSort, rec.SortAt.UnixNano())
		}
	}

	collect := func(order model.RecordingOrder) string {
		var ids []string
		cursor := ""
		for pages := 0; ; pages++ {
			if pages > 10 {
				t.Fatal("paging does not terminate")
			}
			page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", Order: order, Limit: 1, Cursor: cursor}))
			for _, rec := range page.Recordings {
				if rec.Metadata != nil {
					t.Fatalf("list returned metadata for %s", rec.ID)
				}
				ids = append(ids, rec.ID)
			}
			if page.Next == "" {
				return strings.Join(ids, ",")
			}
			cursor = page.Next
		}
	}
	// The ascending first page ends at the minimum representable instant, so the
	// negative sort_at cursor is exercised on the next page.
	if got := collect(model.RecordingOldestFirst); got != "min-boundary,normal,low-overflow,high-overflow,max-boundary" {
		t.Fatalf("oldest first: %s", got)
	}
	if got := collect(model.RecordingNewestFirst); got != "max-boundary,high-overflow,low-overflow,normal,min-boundary" {
		t.Fatalf("newest first: %s", got)
	}
}
