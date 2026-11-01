package coredata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// seedRecordingHistory makes the plan checks meaningful: the indexes must
// still deliver one exact page when most rows belong to unrelated experiments,
// sessions, classes and Runs. Rows bypass owner registration because this
// fixture only exercises the reader's SQL shapes.
func seedRecordingHistory(t *testing.T, db *sql.DB, ctx context.Context, rows int) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO recordings(scope,id,session_id,run_id,experiment_resource_id,kind,archive_class,path,summary_json,metadata_json,sort_at,created_at,bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	kinds := []string{"rosbag", "screen-video", "image-gallery", "table"}
	classes := []string{"rosbag", "screen", "image-gallery", "other"}
	paths := []string{"/history.bag", "/history.mp4", "/history-gallery", "/history.csv"}
	for i := 0; i < rows; i++ {
		experiment, session := "", ""
		if i%3 != 0 {
			experiment = fmt.Sprintf("exp-%d", i%7)
			if i%2 == 0 {
				session = fmt.Sprintf("session-%d", i%11)
			}
		}
		if _, err := stmt.ExecContext(ctx, "s", fmt.Sprintf("history-%05d", i), session, fmt.Sprintf("run-%d", i%13),
			experiment, kinds[i%4], classes[i%4], paths[i%4], []byte(`{}`), []byte(`{}`), int64(i/2), int64(i/2), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Every listing of Runs, Sessions and recordings must be answered from an index
// and in the order it pages: a scan or a temporary sort would make each page as
// slow as the history is long. The statements are the ones the code builds.
func TestRecordQueriesUseTheirIndexes(t *testing.T) {
	db, ctx := fixture(t)
	seedRecordingHistory(t, db, ctx, 2048)
	cursor := encodeCursor(5, "x")
	runs := func(f model.RunFilter) (string, []any) {
		q, args, _, err := runQuery("s", f)
		if err != nil {
			t.Fatal(err)
		}
		return q, args
	}
	sessions := func(f model.SessionFilter) (string, []any) {
		q, args, _, err := sessionQuery("s", f)
		if err != nil {
			t.Fatal(err)
		}
		return q, args
	}
	recordings := func(f model.RecordingFilter) (string, []any) {
		q, args, _, err := recordingQuery("s", f)
		if err != nil {
			t.Fatal(err)
		}
		return q, args
	}
	type plan struct {
		query   string
		args    []any
		index   string
		ordered bool // the index delivers the page order, so no temporary sort
	}
	open := []model.RunStatus{model.RunQueued, model.RunRunning, model.RunStopping}
	cases := map[string]plan{}
	add := func(name, query string, args []any, index string, ordered bool) {
		cases[name] = plan{query, args, index, ordered}
	}

	q, a := runs(model.RunFilter{})
	add("runs, newest first", q, a, "runs_created", true)
	q, a = runs(model.RunFilter{ID: "exact", Projection: model.RunObservation, Limit: 1})
	add("exact Run observation", q, a, "sqlite_autoindex_runs_1", true)
	q, a = runs(model.RunFilter{Projection: model.RunIdentity, Cursor: cursor})
	add("runs, next page", q, a, "runs_created", true)
	q, a = runs(model.RunFilter{Since: time.Unix(1, 0), Until: time.Unix(9, 0)})
	add("runs in a time range", q, a, "runs_created", true)
	q, a = runs(model.RunFilter{TargetID: "t"})
	add("runs of a target", q, a, "runs_target", true)
	q, a = runs(model.RunFilter{TargetID: "t", Cursor: cursor, Statuses: []model.RunStatus{model.RunFailed}})
	add("runs of a target, next page of failures", q, a, "runs_target", true)
	q, a = runs(model.RunFilter{RootRunID: "r"})
	add("runs of a root", q, a, "runs_root", true)
	q, a = runs(model.RunFilter{SessionID: "x"})
	add("runs of a session", q, a, "runs_session", true)
	q, a = runs(model.RunFilter{WorkflowResourceID: "w"})
	add("runs of a workflow", q, a, "runs_workflow", true)
	q, a = runs(model.RunFilter{Statuses: open})
	add("open runs", q, a, "runs_open", true)
	q, a = runs(model.RunFilter{Statuses: []model.RunStatus{model.RunRunning}, Cursor: cursor})
	add("running runs, next page", q, a, "runs_open", true)
	q, a = runs(model.RunFilter{Statuses: []model.RunStatus{model.RunFailed, model.RunInterrupted}})
	add("failed runs", q, a, "runs_created", true)
	add("run by idempotency key", "SELECT "+runColumns+" FROM runs WHERE scope=? AND "+byKey, []any{"s", "k"}, "runs_idempotency", false)
	add("open runs at boot", interruptOpen, []any{1, 1, "s"}, "runs_open", false)
	add("next eligible expiry", pruneOldest, []any{"s"}, "runs_retention_eligible", true)
	add("prune candidates", pruneCandidates, []any{"s", 10}, "runs_retention_eligible", true)

	q, a = sessions(model.SessionFilter{})
	add("sessions, newest first", q, a, "sessions_opened", true)
	q, a = sessions(model.SessionFilter{TargetID: "t", Cursor: cursor})
	add("sessions of a target", q, a, "sessions_target", true)
	q, a = sessions(model.SessionFilter{ExperimentResourceID: "e"})
	add("sessions of an experiment", q, a, "sessions_experiment", true)
	add("live session of a target", liveSessionOfTarget, []any{"s", "t"}, "sessions_live_target", false)
	add("queued Session boot keys", queuedSessionBootKeys, []any{"s"}, "sessions_restart_queued_target", false)
	add("queued Session boot interruption", interruptQueuedSessions, []any{1, "s"}, "sessions_restart_queued_target", false)
	add("live Session boot interruption", interruptLiveSessions, []any{1, "s"}, "sessions_live_target", false)

	// Recording selections page from one index in the requested direction.
	// Kind-only pages use an exact (scope, kind) equality prefix; the direct
	// experiment index remains for Experiment-only pages.
	recordingShapes := []struct {
		name   string
		filter model.RecordingFilter
		index  string
	}{
		{"global chronological", model.RecordingFilter{}, "recordings_created"},
		{"global class", model.RecordingFilter{ArchiveClass: model.RecordingArchiveROSBag}, "recordings_class"},
		{"global kind", model.RecordingFilter{Kind: "rosbag"}, "recordings_kind"},
		{"experiment", model.RecordingFilter{ExperimentResourceID: "e"}, "recordings_experiment"},
		{"experiment and class", model.RecordingFilter{ExperimentResourceID: "e", ArchiveClass: model.RecordingArchiveGallery}, "recordings_experiment_class"},
		{"experiment data files", model.RecordingFilter{ExperimentResourceID: "e", DataFilesOnly: true}, "recordings_experiment_data"},
		{"session", model.RecordingFilter{SessionID: "s"}, "recordings_session"},
		{"session and class", model.RecordingFilter{SessionID: "s", ArchiveClass: model.RecordingArchiveROSBag}, "recordings_session"},
		{"experiment and session", model.RecordingFilter{ExperimentResourceID: "e", SessionID: "s"}, "recordings_session"},
		{"experiment, session and class", model.RecordingFilter{ExperimentResourceID: "e", SessionID: "s", ArchiveClass: model.RecordingArchiveROSBag}, "recordings_session_class"},
		{"experiment session data files", model.RecordingFilter{ExperimentResourceID: "e", SessionID: "s", DataFilesOnly: true}, "recordings_session_data"},
		{"run", model.RecordingFilter{RunID: "r"}, "recordings_run"},
	}
	kindPrefixes := map[string]bool{}
	for _, order := range []struct {
		name  string
		value model.RecordingOrder
	}{{"oldest first", model.RecordingOldestFirst}, {"newest first", model.RecordingNewestFirst}} {
		for _, shape := range recordingShapes {
			for _, next := range []bool{false, true} {
				filter := shape.filter
				filter.Order = order.value
				if next {
					filter.Cursor = cursor
				}
				q, a := recordings(filter)
				if strings.Contains(q, "metadata_json") {
					t.Fatalf("recording list selects arbitrary metadata: %s", q)
				}
				name := "recordings, " + order.name + ", " + shape.name
				if next {
					name += ", next page"
				}
				add(name, q, a, shape.index, true)
				if shape.name == "global kind" {
					kindPrefixes[name] = true
				}
			}
		}
	}

	for name, c := range cases {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+c.query, c.args...)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var detail string
		for rows.Next() {
			var id, parent, unused int
			var step string
			if err = rows.Scan(&id, &parent, &unused, &step); err != nil {
				t.Fatal(err)
			}
			detail += step + "; "
		}
		rows.Close()
		// Boot materializes one correlated scalar named local; this is not a
		// table traversal. Actual table scans still fail this assertion.
		tablePlan := strings.ReplaceAll(detail, "SCAN local", "READ local scalar")
		tablePlan = strings.ReplaceAll(tablePlan, "SCAN CONSTANT ROW", "READ constant row")
		t.Logf("%s: %s", name, detail)
		if kindPrefixes[name] && !strings.Contains(detail, "scope=? AND kind=?") {
			t.Errorf("%s: missing exact Kind equality prefix: %s", name, detail)
		}
		if strings.Contains(tablePlan, "SCAN ") || !strings.Contains(detail, c.index) || (c.ordered && strings.Contains(detail, "TEMP B-TREE")) {
			t.Errorf("%s: unexpected plan: %s", name, detail)
		}
	}
}
