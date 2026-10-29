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

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// acceptedRecordingSession opens one accepted Session and the Run it owns. The
// Run also carries the accepted experiment invocation projection so a
// standalone Run can register a recording without a Session.
func acceptedRecordingSession(t *testing.T, f *store, sessionID, target, experiment, name, mode string) (model.Session, model.Run) {
	t.Helper()
	session := must3(f.core.OpenSession(f.ctx, model.NewSession{
		ID: sessionID, TargetID: target, ExperimentResourceID: experiment,
		ExperimentCommitID: experiment + "-commit", RunMode: mode, ExperimentName: name, At: epoch,
	}))
	run := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun(sessionID+"-run", target, sessionID, experiment)))
	return session, run
}

func acceptedRecordingRun(id, target, sessionID, experiment string) model.NewRun {
	q := newRun(id)
	q.TargetID, q.SessionID = target, sessionID
	q.SourceObservation = json.RawMessage(fmt.Sprintf(`{"experimentInvocation":{"experimentId":%q}}`, experiment))
	return q
}

func recordingIDs(page model.RecordingPage) string {
	ids := make([]string, len(page.Recordings))
	for i, record := range page.Recordings {
		ids[i] = record.ID
	}
	return strings.Join(ids, ",")
}

// A caller's metadata cannot establish recording association or overwrite the
// owner facts the list surface shows.
func TestRecordingAssociationUsesAcceptedOwnersNotMetadata(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp-session", "Accepted experiment", "managed")
	forged := json.RawMessage(`{"experimentId":"forged","experimentResourceId":"forged","experimentName":"Forged name","runMode":"forged-mode","targetId":"forged-target","recordingId":"capture-1","recordId":"record-1"}`)

	sessionRecording := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "session-recording", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/session.bag", Metadata: forged, At: epoch,
	}))
	if sessionRecording.ExperimentResourceID != "exp-session" || sessionRecording.Summary.ExperimentName != "Accepted experiment" ||
		sessionRecording.Summary.RunMode != "managed" || sessionRecording.Summary.TargetID != "local" {
		t.Fatalf("metadata forged the Session association: %+v", sessionRecording)
	}
	if sessionRecording.Summary.RecordingID != "capture-1" || sessionRecording.Summary.RecordID != "record-1" {
		t.Fatalf("non-authoritative summary facts were dropped: %+v", sessionRecording.Summary)
	}
	if string(sessionRecording.Metadata) != string(forged) {
		t.Fatalf("the stored metadata changed: %s", sessionRecording.Metadata)
	}

	// A standalone Run has no Session name or run mode; forged values stay out.
	standalone := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("standalone-run", "agent-7", "", "exp-standalone")))
	unattached := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "standalone", RunID: standalone.ID, Kind: "screen-video", Path: "/data/screen.mp4", Metadata: forged, At: epoch,
	}))
	if unattached.ExperimentResourceID != "exp-standalone" || unattached.Summary.ExperimentName != "" ||
		unattached.Summary.RunMode != "" || unattached.Summary.TargetID != "agent-7" {
		t.Fatalf("standalone Run association: %+v", unattached)
	}

	// A legitimate standalone Run may have no experiment invocation at all.
	unassigned := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("unassigned-run", "agent-8", "", "")))
	loose := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "unassigned", RunID: unassigned.ID, Kind: "table", Path: "/data/table.csv", Metadata: forged, At: epoch,
	}))
	if loose.ExperimentResourceID != "" || loose.Summary.ExperimentName != "" || loose.Summary.RunMode != "" || loose.Summary.TargetID != "agent-8" {
		t.Fatalf("unassigned standalone association: %+v", loose)
	}
}

func TestRecordingRegistrationRejectsMissingForeignAndMismatchedOwners(t *testing.T) {
	f := openStore(t)
	_, runA := acceptedRecordingSession(t, f, "session-a", "local", "exp-a", "A", "managed")
	_, _ = acceptedRecordingSession(t, f, "session-b", "agent-b", "exp-b", "B", "manual")
	// Run creation does not validate a Session: these Runs prove registration
	// refuses the mismatched owner instead of accepting it.
	mismatchedTarget := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("mismatched-target", "local", "session-b", "exp-b")))
	orphanSession := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("orphan-session", "local", "foreign-session", "exp-x")))
	// A Run with no Session may not be registered as a Session recording.
	noRunSession := must3(f.core.CreateRun(f.ctx, acceptedRecordingRun("no-run-session", "local", "", "exp-x")))
	// A Session row with no experiment association is not a valid owner.
	_, emptySessionRun := acceptedRecordingSession(t, f, "session-empty", "empty-target", "exp-empty", "Empty", "managed")
	f.exec(t, "UPDATE sessions SET experiment_resource_id='' WHERE id='session-empty'")
	other := must(coredata.New(f.db, api.Scope{Namespace: "core", User: "other", Workspace: "station"}))
	foreign := must3(other.CreateRun(f.ctx, acceptedRecordingRun("foreign-run", "local", "", "exp-foreign")))

	rowsBefore, bytesBefore := usageRows(t, f)
	for name, c := range map[string]struct {
		query model.NewRecording
		code  string
	}{
		"missing run":                 {model.NewRecording{ID: "reject-missing-run", RunID: "missing-run", Kind: "rosbag", Path: "/data/a.bag"}, "not_found"},
		"foreign run":                 {model.NewRecording{ID: "reject-foreign-run", RunID: foreign.ID, Kind: "rosbag", Path: "/data/a.bag"}, "not_found"},
		"missing session":             {model.NewRecording{ID: "reject-missing-session", SessionID: "missing-session", RunID: runA.ID, Kind: "rosbag", Path: "/data/a.bag"}, "not_found"},
		"foreign session":             {model.NewRecording{ID: "reject-foreign-session", SessionID: "foreign-session", RunID: orphanSession.ID, Kind: "rosbag", Path: "/data/a.bag"}, "not_found"},
		"session run without session": {model.NewRecording{ID: "reject-no-session", RunID: runA.ID, Kind: "rosbag", Path: "/data/a.bag"}, "invalid_argument"},
		"empty run session":           {model.NewRecording{ID: "reject-empty-run-session", SessionID: "session-a", RunID: noRunSession.ID, Kind: "rosbag", Path: "/data/a.bag"}, "conflict"},
		"session without experiment":  {model.NewRecording{ID: "reject-empty-session", SessionID: "session-empty", RunID: emptySessionRun.ID, Kind: "rosbag", Path: "/data/a.bag"}, "invalid_argument"},
		"other session":               {model.NewRecording{ID: "reject-other-session", SessionID: "session-b", RunID: runA.ID, Kind: "rosbag", Path: "/data/a.bag"}, "conflict"},
		"other target":                {model.NewRecording{ID: "reject-other-target", SessionID: "session-b", RunID: mismatchedTarget.ID, Kind: "rosbag", Path: "/data/a.bag"}, "conflict"},
	} {
		if _, created, err := f.core.AddRecording(f.ctx, c.query); created || errCode(err) != c.code {
			t.Errorf("%s: created=%v error=%v", name, created, err)
		}
	}
	if f.count(t, "recordings") != 0 {
		t.Fatal("a rejected registration left a row")
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore || bytes != bytesBefore {
		t.Fatalf("a rejected registration changed quota: %d/%d -> %d/%d", rowsBefore, bytesBefore, rows, bytes)
	}
}

// The stored association belongs to the recording, not to a later owner read:
// exact replay and selection keep working after the owning Run is pruned.
func TestRecordingReplayAndSelectionSurviveOwnerPruning(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	q := model.NewRecording{
		ID: "recording", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/bag.bag",
		Metadata: json.RawMessage(`{"startedAt":"2026-10-10T11:00:00Z","recordingId":"capture"}`), At: epoch,
	}
	first := must3(f.core.AddRecording(f.ctx, q))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: run.ID, Status: model.RunSucceeded, Termination: "completed", At: epoch.Add(time.Minute)}, model.RunRetentionPolicy{}))
	pruned := must(f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(2 * time.Hour)}))
	if pruned.Deleted != 1 {
		t.Fatalf("owning Run was not pruned: %+v", pruned)
	}
	if _, err := f.core.GetRun(f.ctx, run.ID); errCode(err) != "not_found" {
		t.Fatalf("owning Run still exists: %v", err)
	}

	page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp"}))
	if len(page.Recordings) != 1 || page.Recordings[0].Metadata != nil || !reflect.DeepEqual(page.Recordings[0].Summary, first.Summary) {
		t.Fatalf("pruned owner changed the selected summary: %+v", page.Recordings)
	}
	if got := must(f.core.GetRecording(f.ctx, "recording")); !reflect.DeepEqual(got, first) {
		t.Fatalf("pruned owner changed the stored recording: %+v", got)
	}
	replay, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || created || !reflect.DeepEqual(replay, first) {
		t.Fatalf("exact replay after pruning: %+v created=%v error=%v", replay, created, err)
	}
	clash := q
	clash.Kind = "screen-video"
	if _, _, err = f.core.AddRecording(f.ctx, clash); errCode(err) != "conflict" {
		t.Fatalf("changed registration after pruning: %v", err)
	}
}

func TestRecordingArchiveClassAndDataFileSelection(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	_, otherRun := acceptedRecordingSession(t, f, "other-session", "agent", "other-exp", "Other", "managed")
	rows := []struct {
		id, kind, path, session, run string
		class                        model.RecordingArchiveClass
	}{
		{"gallery", "image-gallery", "/data/gallery", "session", run.ID, model.RecordingArchiveGallery},
		{"gallery-bag", "image-gallery", "/data/weird.bag", "session", run.ID, model.RecordingArchiveGallery},
		{"bag", "rosbag", "/data/bag.bag", "session", run.ID, model.RecordingArchiveROSBag},
		{"screen", "screen-video", "/data/screen.mp4", "session", run.ID, model.RecordingArchiveScreen},
		{"table", "table", "/data/table.csv", "session", run.ID, model.RecordingArchiveOther},
		{"movie", "video", "/data/movie.mkv", "session", run.ID, model.RecordingArchiveOther},
		{"foreign", "rosbag", "/data/foreign.bag", "other-session", otherRun.ID, model.RecordingArchiveROSBag},
	}
	for i, row := range rows {
		rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
			ID: row.id, SessionID: row.session, RunID: row.run, Kind: row.kind, Path: row.path, At: epoch.Add(time.Duration(i) * time.Second),
		}))
		if rec.ArchiveClass != row.class {
			t.Errorf("%s: class %q, want %q", row.id, rec.ArchiveClass, row.class)
		}
	}

	// Data files are every non-gallery archive class of exactly one Experiment.
	data := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", DataFilesOnly: true, Order: model.RecordingNewestFirst}))
	if got := recordingIDs(data); got != "movie,table,screen,bag" {
		t.Fatalf("data files: %s", got)
	}
	for _, record := range data.Recordings {
		if record.ArchiveClass == model.RecordingArchiveGallery {
			t.Fatalf("data files included %s", record.ID)
		}
	}
	if got := recordingIDs(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", ArchiveClass: model.RecordingArchiveGallery}))); got != "gallery,gallery-bag" {
		t.Fatalf("gallery class: %s", got)
	}
	if got := recordingIDs(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", ArchiveClass: model.RecordingArchiveROSBag}))); got != "bag" {
		t.Fatalf("rosbag class: %s", got)
	}
	if got := recordingIDs(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", SessionID: "session", ArchiveClass: model.RecordingArchiveScreen}))); got != "screen" {
		t.Fatalf("session and class: %s", got)
	}
	if got := recordingIDs(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", SessionID: "session", DataFilesOnly: true}))); got != "bag,screen,table,movie" {
		t.Fatalf("session data files: %s", got)
	}

	for name, filter := range map[string]model.RecordingFilter{
		"data without experiment": {DataFilesOnly: true},
		"data with class":         {ExperimentResourceID: "exp", ArchiveClass: model.RecordingArchiveROSBag, DataFilesOnly: true},
		"data with gallery kind":  {ExperimentResourceID: "exp", Kind: "image-gallery", DataFilesOnly: true},
		"gallery kind and class":  {Kind: "image-gallery", ArchiveClass: model.RecordingArchiveROSBag},
		"gallery class and kind":  {Kind: "rosbag", ArchiveClass: model.RecordingArchiveGallery},
		"unknown class":           {ArchiveClass: "unknown"},
		"unknown order":           {Order: "sideways"},
	} {
		if _, err := f.core.ListRecordings(f.ctx, filter); errCode(err) != "invalid_argument" {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// Registration order is not chronological order: a valid metadata start time
// positions a late row, and equal times page by identity in both directions.
func TestRecordingPaginationUsesStartedAtAndBreaksTiesByID(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	base := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		id      string
		at      time.Time
		started string
		sortAt  time.Time
	}{
		{"r-a", base.Add(3 * time.Hour), `"2026-10-10T12:00:02Z"`, base.Add(2 * time.Second)},
		{"r-b", base.Add(4 * time.Hour), `"2026-10-10T12:00:01Z"`, base.Add(time.Second)},
		{"r-c", base.Add(5 * time.Hour), `"2026-10-10T12:00:02Z"`, base.Add(2 * time.Second)},
		{"r-d", base.Add(6 * time.Hour), `"not-a-time"`, base.Add(6 * time.Hour)},
		{"r-e", base.Add(7 * time.Hour), `"2026-10-10T14:00:00+02:00"`, base},
		{"r-f", base.Add(8 * time.Hour), `""`, base.Add(8 * time.Hour)},
		{"r-g", base.Add(9 * time.Hour), ``, base.Add(9 * time.Hour)},
	}
	for _, row := range rows {
		metadata := json.RawMessage(`{}`)
		if row.started != "" {
			metadata = json.RawMessage(`{"startedAt":` + row.started + `}`)
		}
		rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
			ID: row.id, SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/" + row.id + ".bag", Metadata: metadata, At: row.at,
		}))
		if !rec.SortAt.Equal(row.sortAt) {
			t.Errorf("%s: sort_at %s, want %s", row.id, rec.SortAt, row.sortAt)
		}
	}

	ascending := func(cursor string, limit int) model.RecordingPage {
		return must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", Order: model.RecordingOldestFirst, Limit: limit, Cursor: cursor}))
	}
	descending := func(cursor string, limit int) model.RecordingPage {
		return must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp", Order: model.RecordingNewestFirst, Limit: limit, Cursor: cursor}))
	}
	collect := func(page func(string, int) model.RecordingPage) string {
		var seen []string
		seenNext := map[string]bool{}
		cursor := ""
		for pages := 0; ; pages++ {
			if pages > 10 {
				t.Fatal("paging does not terminate")
			}
			page := page(cursor, 2)
			next := page.Next
			if next != "" && seenNext[next] {
				t.Fatalf("repeated cursor %q", next)
			}
			seenNext[next] = true
			for _, record := range page.Recordings {
				seen = append(seen, record.ID)
			}
			if page.Next == "" {
				return strings.Join(seen, ",")
			}
			cursor = page.Next
		}
	}
	if got := collect(ascending); got != "r-e,r-b,r-a,r-c,r-d,r-f,r-g" {
		t.Fatalf("oldest first: %s", got)
	}
	if got := collect(descending); got != "r-g,r-f,r-d,r-c,r-a,r-b,r-e" {
		t.Fatalf("newest first: %s", got)
	}

	// A bag registered last but captured first is selected at its capture time.
	late := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "r-late", SessionID: "session", RunID: run.ID, Kind: "rosbag", Path: "/data/r-late.bag",
		Metadata: json.RawMessage(`{"startedAt":"2026-10-10T11:00:00Z"}`), At: base.Add(10 * time.Hour),
	}))
	if !late.SortAt.Equal(base.Add(-time.Hour)) {
		t.Fatalf("late capture start: %s", late.SortAt)
	}
	if got := collect(ascending); got != "r-late,r-e,r-b,r-a,r-c,r-d,r-f,r-g" {
		t.Fatalf("late registration order: %s", got)
	}
}

// The list is a stored typed projection. Arbitrary metadata stays out of the
// list and the quota includes the association, the projection and the metadata.
func TestRecordingListStoresTypedSummaryWithoutArbitraryMetadata(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	if model.MaxRecordingMetadata != 256<<10 {
		t.Fatalf("recording metadata ceiling: %d", model.MaxRecordingMetadata)
	}
	metadata := json.RawMessage(`{"blob":"` + strings.Repeat("a", 200<<10) + `","recordingId":"capture","durationMs":1500,"width":1920,"height":1080,"hasPoster":true,"status":"finalized"}`)

	rowsBefore, bytesBefore := usageRows(t, f)
	rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "large", SessionID: "session", RunID: run.ID, Kind: "screen-video", Path: "/data/large.mp4", Metadata: metadata, At: epoch,
	}))
	if rec.Summary.RecordingID != "capture" || rec.Summary.DurationMS != 1500 || rec.Summary.Width != 1920 ||
		rec.Summary.Height != 1080 || !rec.Summary.HasPoster || rec.Summary.Status != "finalized" {
		t.Fatalf("typed summary: %+v", rec.Summary)
	}
	rowsAfter, bytesAfter := usageRows(t, f)
	if rowsAfter != rowsBefore+1 || bytesAfter-bytesBefore < int64(len(metadata)) {
		t.Fatalf("metadata not charged: rows %d->%d bytes %d->%d", rowsBefore, rowsAfter, bytesBefore, bytesAfter)
	}
	var stored int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT coalesce(sum(bytes),0) FROM recordings WHERE scope=?", engine.ScopeID(f.scope)).Scan(&stored)
	}); err != nil {
		t.Fatal(err)
	}
	if stored != bytesAfter-bytesBefore {
		t.Fatalf("stored recording bytes %d differ from charged bytes %d", stored, bytesAfter-bytesBefore)
	}

	page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp"}))
	if len(page.Recordings) != 1 || page.Recordings[0].Metadata != nil {
		t.Fatalf("list returned arbitrary metadata: %+v", page.Recordings)
	}
	if !reflect.DeepEqual(page.Recordings[0].Summary, rec.Summary) {
		t.Fatalf("list projection changed: %+v", page.Recordings[0].Summary)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 4096 || len(encoded) > model.MaxResponseBytes {
		t.Fatalf("list page carries %d bytes", len(encoded))
	}
	got := must(f.core.GetRecording(f.ctx, "large"))
	if string(got.Metadata) != string(metadata) {
		t.Fatalf("Get did not return the full metadata (%d bytes)", len(got.Metadata))
	}
	// The list never reads the metadata column, so a corrupt unused detail
	// cannot change the stored summary projection.
	f.exec(t, "UPDATE recordings SET metadata_json=x'01' WHERE id='large'")
	page = must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp"}))
	if len(page.Recordings) != 1 || !reflect.DeepEqual(page.Recordings[0].Summary, rec.Summary) || page.Recordings[0].Metadata != nil {
		t.Fatalf("unused metadata changed the list: %+v", page.Recordings)
	}

	oversized := json.RawMessage(`{"blob":"` + strings.Repeat("b", model.MaxRecordingMetadata) + `"}`)
	if _, _, err := f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "oversized", RunID: run.ID, Kind: "table", Path: "/data/oversized.csv", Metadata: oversized,
	}); errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized metadata: %v", err)
	}
}

// The list payload is the fixed summary projection, never the arbitrary
// metadata. A typed summary field therefore cannot smuggle an oversized list
// payload even though the same bytes remain valid in an opaque Get-only field.
func TestRecordingRejectsOversizedTypedSummary(t *testing.T) {
	f := openStore(t)
	_, run := acceptedRecordingSession(t, f, "session", "local", "exp", "Experiment", "managed")
	if model.MaxRecordingSummaryBytes != 4<<10 {
		t.Fatalf("recording summary ceiling: %d", model.MaxRecordingSummaryBytes)
	}
	rowsBefore, bytesBefore := usageRows(t, f)
	oversized := json.RawMessage(`{"recordingId":"capture","mediaType":"` + strings.Repeat("v", 200<<10) + `"}`)
	if _, created, err := f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "oversized-summary", SessionID: "session", RunID: run.ID, Kind: "screen-video", Path: "/data/oversized.mp4", Metadata: oversized, At: epoch,
	}); created || errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized typed summary: created=%v error=%v", created, err)
	}
	if f.count(t, "recordings") != 0 {
		t.Fatal("rejected summary left a row")
	}
	if rows, bytes := usageRows(t, f); rows != rowsBefore || bytes != bytesBefore {
		t.Fatalf("rejected summary changed quota: %d/%d -> %d/%d", rowsBefore, bytesBefore, rows, bytes)
	}

	small := json.RawMessage(`{"recordingId":"capture","mediaType":"video/mp4"}`)
	rec := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "small-summary", SessionID: "session", RunID: run.ID, Kind: "screen-video", Path: "/data/small.mp4", Metadata: small, At: epoch,
	}))
	page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{ExperimentResourceID: "exp"}))
	if len(page.Recordings) != 1 || !reflect.DeepEqual(page.Recordings[0].Summary, rec.Summary) || page.Recordings[0].Metadata != nil {
		t.Fatalf("small summary: %+v", page.Recordings)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= model.MaxRecordingSummaryBytes {
		t.Fatalf("list page carries %d bytes", len(encoded))
	}
	if got := must(f.core.GetRecording(f.ctx, "small-summary")); string(got.Metadata) != string(small) {
		t.Fatalf("small metadata lost: %s", got.Metadata)
	}
}

// The live quota counts the stored association and the typed projection, not
// only the caller's metadata.
func TestRecordingQuotaCountsStoredAssociationAndSummary(t *testing.T) {
	f := openStore(t)
	_, runA := acceptedRecordingSession(t, f, "session-aa", "local", "e", "A", "managed")
	_, runB := acceptedRecordingSession(t, f, "session-bb", "agent", "e-longer", "A-longer-name", "managed")

	rowsBefore, bytesBefore := usageRows(t, f)
	recA := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "rec-a", SessionID: "session-aa", RunID: runA.ID, Kind: "rosbag", Path: "/data/x.bag", At: epoch,
	}))
	_, bytesAfterA := usageRows(t, f)
	recB := must3(f.core.AddRecording(f.ctx, model.NewRecording{
		ID: "rec-b", SessionID: "session-bb", RunID: runB.ID, Kind: "rosbag", Path: "/data/y.bag", At: epoch,
	}))
	rowsAfter, bytesAfterB := usageRows(t, f)

	want := int64(len(recB.Summary.ExperimentName) - len(recA.Summary.ExperimentName) +
		len(recB.ExperimentResourceID) - len(recA.ExperimentResourceID) + len(recB.Summary.TargetID) - len(recA.Summary.TargetID))
	if got := (bytesAfterB - bytesAfterA) - (bytesAfterA - bytesBefore); got != want {
		t.Fatalf("association and summary not charged: extra %d, want %d", got, want)
	}
	if rowsAfter-rowsBefore != 2 {
		t.Fatalf("recording rows: %d", rowsAfter-rowsBefore)
	}
}
