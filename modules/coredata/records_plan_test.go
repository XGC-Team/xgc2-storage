package coredata

import (
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Every listing of Runs, Sessions and recordings must be answered from an index
// and in the order it pages: a scan or a temporary sort would make each page as
// slow as the history is long. The statements are the ones the code builds.
func TestRecordQueriesUseTheirIndexes(t *testing.T) {
	db, ctx := fixture(t)
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
	q, a = runs(model.RunFilter{OmitPayloads: true, Cursor: cursor})
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
	add("prune by age", pruneByAge, []any{"s", 5, 10}, "runs_finished", true)
	add("prune by count", pruneByCount, []any{"s", 10, 5}, "runs_finished", true)

	q, a = sessions(model.SessionFilter{})
	add("sessions, newest first", q, a, "sessions_opened", true)
	q, a = sessions(model.SessionFilter{TargetID: "t", Cursor: cursor})
	add("sessions of a target", q, a, "sessions_target", true)
	q, a = sessions(model.SessionFilter{ExperimentResourceID: "e"})
	add("sessions of an experiment", q, a, "sessions_experiment", true)
	add("live session of a target", liveSessionOfTarget, []any{"s", "t"}, "sessions_live_target", false)

	q, a = recordings(model.RecordingFilter{})
	add("recordings, oldest first", q, a, "recordings_created", true)
	q, a = recordings(model.RecordingFilter{Kind: "bag", Cursor: cursor})
	add("recordings of a kind, next page", q, a, "recordings_created", true)
	q, a = recordings(model.RecordingFilter{SessionID: "x"})
	add("recordings of a session", q, a, "recordings_session", true)
	q, a = recordings(model.RecordingFilter{RunID: "r"})
	add("recordings of a run", q, a, "recordings_run", true)

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
		if strings.Contains(detail, "SCAN ") || !strings.Contains(detail, c.index) || (c.ordered && strings.Contains(detail, "TEMP B-TREE")) {
			t.Errorf("%s: unexpected plan: %s", name, detail)
		}
	}
}
