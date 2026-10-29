package coredata_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func recordingDeleteGuard(rec model.Recording) model.RecordingDelete {
	return model.RecordingDelete{ID: rec.ID, RunID: rec.RunID, SessionID: rec.SessionID, Kind: rec.Kind, Path: rec.Path, CreatedAt: rec.CreatedAt}
}

func storedRecordingBytes(t *testing.T, f *store, id string) int64 {
	t.Helper()
	var stored int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT bytes FROM recordings WHERE scope=? AND id=?", engine.ScopeID(f.scope), id).Scan(&stored)
	}); err != nil {
		t.Fatal(err)
	}
	return stored
}

func TestRecordingDeletionRequiresFinishedRunAndLeavesPhysicalFile(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	path := filepath.Join(t.TempDir(), "capture.bag")
	if err := os.WriteFile(path, []byte("capture"), 0600); err != nil {
		t.Fatal(err)
	}
	rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "recording", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: path, At: epoch,
	}))
	sibling := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "sibling", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: path + ".other", At: epoch.Add(time.Second),
	}))
	guard := recordingDeleteGuard(rec)

	// The owning Run is queued: both lifecycle read and delete refuse.
	rowsBefore, bytesBefore := usageRows(t, f)
	if _, err := f.core.GetRecordingForDeletion(f.ctx, rec.ID); errCode(err) != "conflict" {
		t.Fatalf("open Run lifecycle read: %v", err)
	}
	if _, err := f.core.DeleteRecording(f.ctx, guard); errCode(err) != "conflict" {
		t.Fatalf("open Run delete: %v", err)
	}
	if got := must(f.core.GetRecording(f.ctx, rec.ID)); !reflect.DeepEqual(got, rec) {
		t.Fatalf("refused delete changed the row: %+v", got)
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore || bytes != bytesBefore {
		t.Fatalf("refused delete released quota: %d/%d -> %d/%d", rowsBefore, bytesBefore, rows, bytes)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("physical file changed before delete: %v", err)
	}

	// A terminal Run in the still-live Session is deletable.
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, Termination: "completed", At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	selected := must(f.core.GetRecordingForDeletion(f.ctx, rec.ID))
	if !reflect.DeepEqual(selected, rec) {
		t.Fatalf("selected deletion row changed: %+v", selected)
	}
	rowsBefore, bytesBefore = usageRows(t, f)
	stored := storedRecordingBytes(t, f, rec.ID)
	deleted := must(f.core.DeleteRecording(f.ctx, guard))
	if !reflect.DeepEqual(deleted, rec) {
		t.Fatalf("deleted row facts changed: %+v", deleted)
	}
	if _, err := f.core.GetRecording(f.ctx, rec.ID); errCode(err) != "not_found" {
		t.Fatalf("deleted row still readable: %v", err)
	}
	if _, err := f.core.GetRecordingForDeletion(f.ctx, rec.ID); errCode(err) != "not_found" {
		t.Fatalf("deleted row still selectable for deletion: %v", err)
	}
	if _, err := f.core.DeleteRecording(f.ctx, guard); errCode(err) != "not_found" {
		t.Fatalf("replayed delete: %v", err)
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore-1 || bytes != bytesBefore-stored {
		t.Fatalf("delete release: rows %d->%d bytes %d->%d (stored %d)", rowsBefore, rows, bytesBefore, bytes, stored)
	}
	if got := must(f.core.GetRecording(f.ctx, sibling.ID)); !reflect.DeepEqual(got, sibling) {
		t.Fatalf("delete changed another row: %+v", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("SDK touched the physical file: %v", err)
	}
}

func TestRecordingDeletionAllowsPrunedRun(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "recording", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/pruned.bag", At: epoch,
	}))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	pruned := must(f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(2 * time.Hour)}))
	if pruned.Deleted != 1 {
		t.Fatalf("owning Run was not pruned: %+v", pruned)
	}
	if _, err := f.core.GetRun(f.ctx, run.ID); errCode(err) != "not_found" {
		t.Fatalf("owning Run still exists: %v", err)
	}

	selected := must(f.core.GetRecordingForDeletion(f.ctx, rec.ID))
	guard := recordingDeleteGuard(selected)
	rowsBefore, bytesBefore := usageRows(t, f)
	stored := storedRecordingBytes(t, f, rec.ID)
	must(f.core.DeleteRecording(f.ctx, guard))
	if _, err := f.core.GetRecording(f.ctx, rec.ID); errCode(err) != "not_found" {
		t.Fatalf("pruned-owner recording survived: %v", err)
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore-1 || bytes != bytesBefore-stored {
		t.Fatalf("pruned-owner release: rows %d->%d bytes %d->%d (stored %d)", rowsBefore, rows, bytesBefore, bytes, stored)
	}
}

func TestRecordingDeletionGuardAndForeignScope(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "guarded", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/guarded.bag", At: epoch,
	}))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	guard := recordingDeleteGuard(rec)

	for name, changed := range map[string]model.RecordingDelete{
		"run":     func() model.RecordingDelete { q := guard; q.RunID = "other-run"; return q }(),
		"session": func() model.RecordingDelete { q := guard; q.SessionID = "other-session"; return q }(),
		"kind":    func() model.RecordingDelete { q := guard; q.Kind = "video"; return q }(),
		"path":    func() model.RecordingDelete { q := guard; q.Path = "/data/other.bag"; return q }(),
		"time":    func() model.RecordingDelete { q := guard; q.CreatedAt = guard.CreatedAt.Add(time.Second); return q }(),
	} {
		if _, err := f.core.DeleteRecording(f.ctx, changed); errCode(err) != "conflict" {
			t.Errorf("%s guard: %v", name, err)
		}
	}
	if got := must(f.core.GetRecording(f.ctx, rec.ID)); !reflect.DeepEqual(got, rec) {
		t.Fatalf("rejected guard changed the row: %+v", got)
	}

	// A reused ID with different facts rejects the old guard.
	must(f.core.DeleteRecording(f.ctx, guard))
	reused := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "guarded", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/reused.bag", At: epoch.Add(time.Minute),
	}))
	if _, err := f.core.DeleteRecording(f.ctx, guard); errCode(err) != "conflict" {
		t.Fatalf("old guard deleted a reused ID: %v", err)
	}
	if got := must(f.core.GetRecording(f.ctx, reused.ID)); !reflect.DeepEqual(got, reused) {
		t.Fatalf("old guard changed the reused row: %+v", got)
	}
	must(f.core.DeleteRecording(f.ctx, recordingDeleteGuard(reused)))

	// Another scope never sees or deletes this scope's recording.
	other := must(coredata.New(f.db, api.Scope{Namespace: "core", User: "other", Workspace: "station"}))
	must3(other.OpenSession(f.ctx, model.NewSession{ID: "foreign-session", TargetID: "agent", ExperimentResourceID: "foreign-exp", ExperimentCommitID: "foreign-commit", RunMode: "managed", ExperimentName: "Foreign", At: epoch}))
	foreignRun := must3(other.CreateRun(f.ctx, acceptedRecordingRun("foreign-run", "agent", "foreign-session", "foreign-exp")))
	must(other.FinishRun(f.ctx, model.RunFinish{ID: foreignRun.ID, Status: model.RunSucceeded, At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	foreign := must3(other.AddRecording(f.ctx, model.NewRecording{
		ID: "foreign-recording", SessionID: "foreign-session", RunID: foreignRun.ID, Kind: "rosbag", Path: "/data/foreign.bag", At: epoch,
	}))
	foreignGuard := recordingDeleteGuard(foreign)
	if _, err := f.core.GetRecordingForDeletion(f.ctx, foreign.ID); errCode(err) != "not_found" {
		t.Fatalf("foreign lifecycle read: %v", err)
	}
	if _, err := f.core.DeleteRecording(f.ctx, foreignGuard); errCode(err) != "not_found" {
		t.Fatalf("foreign delete: %v", err)
	}
	if got := must(other.GetRecording(f.ctx, foreign.ID)); !reflect.DeepEqual(got, foreign) {
		t.Fatalf("foreign scope row changed: %+v", got)
	}
}

func TestRecordingDeletionValidatesGuard(t *testing.T) {
	f := openStore(t)
	rowsBefore, bytesBefore := usageRows(t, f)
	for name, q := range map[string]model.RecordingDelete{
		"no id":       {RunID: "run", Kind: "kind", Path: "/data/a.bag", CreatedAt: epoch},
		"no run":      {ID: "id", Kind: "kind", Path: "/data/a.bag", CreatedAt: epoch},
		"no kind":     {ID: "id", RunID: "run", Path: "/data/a.bag", CreatedAt: epoch},
		"no path":     {ID: "id", RunID: "run", Kind: "kind", CreatedAt: epoch},
		"NUL path":    {ID: "id", RunID: "run", Kind: "kind", Path: "a\x00b", CreatedAt: epoch},
		"no time":     {ID: "id", RunID: "run", Kind: "kind", Path: "/data/a.bag"},
		"bad session": {ID: "id", RunID: "run", SessionID: "bad\nsession", Kind: "kind", Path: "/data/a.bag", CreatedAt: epoch},
	} {
		if _, err := f.core.DeleteRecording(f.ctx, q); errCode(err) != "invalid_argument" {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := f.core.GetRecordingForDeletion(f.ctx, ""); errCode(err) != "invalid_argument" {
		t.Fatalf("empty identity: %v", err)
	}
	if _, err := f.core.GetRecordingForDeletion(f.ctx, "missing"); errCode(err) != "not_found" {
		t.Fatalf("missing row: %v", err)
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore || bytes != bytesBefore {
		t.Fatalf("invalid guard changed quota: %d/%d -> %d/%d", rowsBefore, bytesBefore, rows, bytes)
	}
}
