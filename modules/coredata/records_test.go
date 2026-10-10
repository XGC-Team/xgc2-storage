package coredata_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

var epoch = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func newRun(id string) model.NewRun {
	return model.NewRun{ID: id, TargetID: "local", WorkflowResourceID: "wf", WorkflowCommitID: "wf-v1", DefinitionDigest: strings.Repeat("d", 64), ActionID: "run",
		Inputs: []byte(`{"session":{"id":"s1"},"n":9007199254740993}`), Trigger: []byte(`{"kind":"manual"}`), At: epoch}
}

func childRun(id, parent, root string, depth int) model.NewRun {
	q := newRun(id)
	q.ParentRunID, q.RootRunID, q.Depth, q.CallNodeID = parent, root, depth, "call"
	return q
}

// commits reports the durable and relaxed commits since a Stats snapshot.
func commits(t *testing.T, f *store, since engine.Stats) (durable, relaxed uint64) {
	t.Helper()
	now, err := f.db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	return now.CommitsDurable - since.CommitsDurable, now.CommitsRelaxed - since.CommitsRelaxed
}

func stats(t *testing.T, f *store) engine.Stats {
	t.Helper()
	s, err := f.db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func usageRows(t *testing.T, f *store) (rows, bytes int64) {
	t.Helper()
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT coalesce(sum(rows),0),coalesce(sum(bytes),0) FROM core_data_usage").Scan(&rows, &bytes)
	}); err != nil {
		t.Fatal(err)
	}
	return
}

func TestCreateRunIsDurableIdempotentAndKeepsItsLineage(t *testing.T) {
	f := openStore(t)
	before := stats(t, f)
	q := newRun("run-1")
	q.IdempotencyKey = "start-1"
	run, created, err := f.core.CreateRun(f.ctx, q)
	if err != nil || !created || run.Status != model.RunQueued || run.RootRunID != "run-1" || run.Revision != 1 || !run.CreatedAt.Equal(epoch) || !run.StartedAt.IsZero() {
		t.Fatalf("created run: %+v %v", run, err)
	}
	if d, r := commits(t, f, before); d != 1 || r != 0 {
		t.Fatalf("CreateRun must be one durable commit: durable=%d relaxed=%d", d, r)
	}
	// The frozen inputs keep their exact bytes, including a 64-bit integer.
	got := must(f.core.GetRun(f.ctx, "run-1"))
	if string(got.Inputs) != string(q.Inputs) || string(got.Trigger) != string(q.Trigger) {
		t.Fatalf("frozen inputs changed: %s", got.Inputs)
	}
	// A replayed start finds its Run, even with another id.
	again := newRun("other-id")
	again.IdempotencyKey = "start-1"
	replay, created, err := f.core.CreateRun(f.ctx, again)
	if err != nil || created || replay.ID != "run-1" {
		t.Fatalf("replay: %+v created=%v %v", replay, created, err)
	}
	found, ok, err := f.core.FindRunByIdempotencyKey(f.ctx, "start-1")
	if err != nil || !ok || found.ID != "run-1" {
		t.Fatalf("find by key: %+v %v %v", found, ok, err)
	}
	if _, ok, _ = f.core.FindRunByIdempotencyKey(f.ctx, "never"); ok {
		t.Fatal("found a run for an unused key")
	}
	if _, _, err = f.core.CreateRun(f.ctx, newRun("run-1")); errCode(err) != "conflict" {
		t.Fatalf("reused run identity: %v", err)
	}
	child, _, err := f.core.CreateRun(f.ctx, childRun("child-1", "run-1", "run-1", 1))
	if err != nil || child.ParentRunID != "run-1" || child.Depth != 1 {
		t.Fatalf("child: %+v %v", child, err)
	}
	for name, q := range map[string]model.NewRun{
		"root with a depth":   func() model.NewRun { q := newRun("a"); q.Depth = 1; return q }(),
		"child without root":  func() model.NewRun { q := childRun("b", "run-1", "", 1); return q }(),
		"child as own root":   childRun("c", "run-1", "c", 1),
		"child at depth zero": childRun("d", "run-1", "run-1", 0),
		"terminal on accept":  func() model.NewRun { q := newRun("e"); q.Status = model.RunSucceeded; return q }(),
		"inputs not object":   func() model.NewRun { q := newRun("f"); q.Inputs = []byte(`[1]`); return q }(),
		"no trigger":          func() model.NewRun { q := newRun("g"); q.Trigger = nil; return q }(),
		"invalid JSON":        func() model.NewRun { q := newRun("h"); q.Inputs = []byte(`{`); return q }(),
		"no workflow":         func() model.NewRun { q := newRun("i"); q.WorkflowResourceID = ""; return q }(),
	} {
		if _, _, err = f.core.CreateRun(f.ctx, q); errCode(err) != "invalid_argument" {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
	big := newRun("big")
	big.Inputs = []byte(`{"x":"` + strings.Repeat("a", model.MaxRunInputsBytes) + `"}`)
	if _, _, err = f.core.CreateRun(f.ctx, big); errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized inputs: %v", err)
	}
}

func TestRunStatusUpdatesAreRelaxedBatchableAndForwardOnly(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"a", "b", "c"} {
		must3(f.core.CreateRun(f.ctx, newRun(id)))
	}
	before := stats(t, f)
	at := epoch.Add(time.Minute)
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "a", Status: model.RunRunning, At: at}, model.RunStatusUpdate{ID: "b", Status: model.RunRunning, At: at}, model.RunStatusUpdate{ID: "c", Status: model.RunStopping, At: at}); err != nil {
		t.Fatal(err)
	}
	if d, r := commits(t, f, before); d != 0 || r != 1 {
		t.Fatalf("a batch of transitions must be one relaxed commit: durable=%d relaxed=%d", d, r)
	}
	a := must(f.core.GetRun(f.ctx, "a"))
	c := must(f.core.GetRun(f.ctx, "c"))
	if a.Status != model.RunRunning || !a.StartedAt.Equal(at) || a.Revision != 2 || c.Status != model.RunStopping || !c.StartedAt.IsZero() {
		t.Fatalf("transitions: %+v %+v", a, c)
	}
	// Repeating a transition changes nothing; going back is refused.
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "a", Status: model.RunRunning}); err != nil || must(f.core.GetRun(f.ctx, "a")).Revision != 2 {
		t.Fatalf("repeat bumped the revision: %v", err)
	}
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "c", Status: model.RunRunning}); errCode(err) != "failed_precondition" {
		t.Fatalf("stopping went back to running: %v", err)
	}
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "a", Status: model.RunSucceeded}); errCode(err) != "invalid_argument" {
		t.Fatalf("a terminal status through the transition call: %v", err)
	}
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "missing", Status: model.RunRunning}); errCode(err) != "not_found" {
		t.Fatalf("missing run: %v", err)
	}
	// One bad entry rolls the whole batch back.
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "b", Status: model.RunStopping}, model.RunStatusUpdate{ID: "missing", Status: model.RunRunning}); err == nil {
		t.Fatal("bad batch committed")
	}
	if got := must(f.core.GetRun(f.ctx, "b")); got.Status != model.RunRunning {
		t.Fatalf("partial batch applied: %s", got.Status)
	}
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "b", Status: model.RunFailed}))
	if err := f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "b", Status: model.RunStopping}); errCode(err) != "failed_precondition" {
		t.Fatalf("a finished run moved: %v", err)
	}
}

func TestFinishRunRecordsTheOutcomeOnce(t *testing.T) {
	f := openStore(t)
	must3(f.core.CreateRun(f.ctx, newRun("run")))
	rowsBefore, bytesBefore := usageRows(t, f)
	before := stats(t, f)
	finish := model.RunFinish{ID: "run", Status: model.RunSucceeded, Termination: "completed", Result: []byte(`{"ok":true}`), Nodes: []byte(`[{"id":"n","status":"succeeded"}]`), CleanupErrors: []byte(`[]`), At: epoch.Add(time.Hour)}
	run, err := f.core.FinishRun(f.ctx, finish)
	if err != nil || run.Status != model.RunSucceeded || !run.FinishedAt.Equal(finish.At) || string(run.Nodes) != string(finish.Nodes) || run.Termination != "completed" || run.Revision != 2 {
		t.Fatalf("finished run: %+v %v", run, err)
	}
	if d, r := commits(t, f, before); d != 1 || r != 0 {
		t.Fatalf("FinishRun must be one durable commit: durable=%d relaxed=%d", d, r)
	}
	rowsAfter, bytesAfter := usageRows(t, f)
	if rowsAfter != rowsBefore || bytesAfter <= bytesBefore {
		t.Fatalf("the outcome must grow the row's bytes only: rows %d->%d bytes %d->%d", rowsBefore, rowsAfter, bytesBefore, bytesAfter)
	}
	if _, err = f.core.FinishRun(f.ctx, finish); errCode(err) != "failed_precondition" {
		t.Fatalf("a run finished twice: %v", err)
	}
	for name, bad := range map[string]model.RunFinish{
		"open status":  {ID: "run", Status: model.RunRunning},
		"invalid JSON": {ID: "run", Status: model.RunFailed, Error: []byte(`{`)},
		"no run":       {ID: "", Status: model.RunFailed},
	} {
		if _, err = f.core.FinishRun(f.ctx, bad); errCode(err) != "invalid_argument" {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err = f.core.FinishRun(f.ctx, model.RunFinish{ID: "ghost", Status: model.RunFailed}); errCode(err) != "not_found" {
		t.Fatalf("missing run: %v", err)
	}
	// A queued Run that never started can be canceled; started_at stays empty.
	must3(f.core.CreateRun(f.ctx, newRun("never-started")))
	canceled := must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "never-started", Status: model.RunCanceled, Termination: "canceled by operator"}))
	if canceled.Status != model.RunCanceled || !canceled.StartedAt.IsZero() || canceled.FinishedAt.IsZero() {
		t.Fatalf("canceled run: %+v", canceled)
	}
}

func TestListRunsFiltersAndPagesNewestFirst(t *testing.T) {
	f := openStore(t)
	for i := 0; i < 12; i++ {
		q := newRun(fmt.Sprintf("run-%02d", i))
		// Pairs share a creation time: the id breaks the tie, so no row is skipped or repeated.
		q.At = epoch.Add(time.Duration(i/2) * time.Second)
		q.TargetID = []string{"local", "agent-1"}[i%2]
		q.WorkflowResourceID = []string{"wf-a", "wf-b", "wf-c"}[i%3]
		if i >= 8 {
			q.SessionID = "s1"
		}
		must3(f.core.CreateRun(f.ctx, q))
	}
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "run-00", Status: model.RunFailed}))
	check(f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "run-01", Status: model.RunRunning}))
	var seen []string
	cursor := ""
	for pages := 0; ; pages++ {
		page, err := f.core.ListRuns(f.ctx, model.RunFilter{Limit: 5, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Runs {
			seen = append(seen, r.ID)
		}
		if page.Next == "" {
			break
		}
		if pages > 5 {
			t.Fatal("paging does not terminate")
		}
		cursor = page.Next
	}
	want := []string{"run-11", "run-10", "run-09", "run-08", "run-07", "run-06", "run-05", "run-04", "run-03", "run-02", "run-01", "run-00"}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("pages %v, want %v", seen, want)
	}
	count := func(filter model.RunFilter) int {
		filter.Limit = 100
		return len(must(f.core.ListRuns(f.ctx, filter)).Runs)
	}
	if n := count(model.RunFilter{TargetID: "agent-1"}); n != 6 {
		t.Errorf("target filter: %d", n)
	}
	if n := count(model.RunFilter{WorkflowResourceID: "wf-b"}); n != 4 {
		t.Errorf("workflow filter: %d", n)
	}
	if n := count(model.RunFilter{SessionID: "s1"}); n != 4 {
		t.Errorf("session filter: %d", n)
	}
	if n := count(model.RunFilter{Statuses: []model.RunStatus{model.RunFailed, model.RunRunning}}); n != 2 {
		t.Errorf("status filter: %d", n)
	}
	if n := count(model.RunFilter{Since: epoch.Add(2 * time.Second), Until: epoch.Add(4 * time.Second)}); n != 4 {
		t.Errorf("time window: %d", n)
	}
	if n := count(model.RunFilter{RootRunID: "run-03"}); n != 1 {
		t.Errorf("root filter: %d", n)
	}
	light := must(f.core.ListRuns(f.ctx, model.RunFilter{Limit: 3, OmitPayloads: true}))
	if len(light.Runs) != 3 || light.Runs[0].Inputs != nil || light.Runs[0].Trigger != nil || light.Runs[0].ID != "run-11" {
		t.Fatalf("summary list: %+v", light.Runs)
	}
	if _, err := f.core.ListRuns(f.ctx, model.RunFilter{Cursor: "garbage"}); errCode(err) != "invalid_argument" {
		t.Fatalf("bad cursor: %v", err)
	}
	if _, err := f.core.ListRuns(f.ctx, model.RunFilter{Limit: 1001}); errCode(err) != "invalid_argument" {
		t.Fatalf("page limit: %v", err)
	}
	if _, err := f.core.ListRuns(f.ctx, model.RunFilter{Statuses: []model.RunStatus{"weird"}}); errCode(err) != "invalid_argument" {
		t.Fatalf("status value: %v", err)
	}
}

func TestInterruptOpenRunsAtBoot(t *testing.T) {
	f := openStore(t)
	for _, id := range []string{"queued", "running", "stopping", "done"} {
		must3(f.core.CreateRun(f.ctx, newRun(id)))
	}
	check(f.core.UpdateRunStatus(f.ctx, model.RunStatusUpdate{ID: "running", Status: model.RunRunning}, model.RunStatusUpdate{ID: "stopping", Status: model.RunStopping}))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "done", Status: model.RunSucceeded}))
	before := stats(t, f)
	at := epoch.Add(time.Hour)
	n, err := f.core.InterruptOpenRuns(f.ctx, at)
	if err != nil || n != 3 {
		t.Fatalf("interrupted %d: %v", n, err)
	}
	if d, _ := commits(t, f, before); d != 1 {
		t.Fatalf("interrupting is one durable commit, got %d", d)
	}
	for _, id := range []string{"queued", "running", "stopping"} {
		r := must(f.core.GetRun(f.ctx, id))
		if r.Status != model.RunInterrupted || r.Termination != "interrupted" || !r.FinishedAt.Equal(at) {
			t.Errorf("%s: %+v", id, r)
		}
	}
	if r := must(f.core.GetRun(f.ctx, "done")); r.Status != model.RunSucceeded {
		t.Errorf("a finished run changed: %+v", r)
	}
	if n, err = f.core.InterruptOpenRuns(f.ctx, at); err != nil || n != 0 {
		t.Fatalf("second boot interrupted %d: %v", n, err)
	}
}

func TestPruneRunsByAgeAndCountFreesQuotaAndKeepsLiveTrees(t *testing.T) {
	f := openStore(t)
	rows0, bytes0 := usageRows(t, f)
	for i := 0; i < 10; i++ {
		q := newRun(fmt.Sprintf("old-%d", i))
		q.At = epoch.Add(time.Duration(i) * time.Minute)
		must3(f.core.CreateRun(f.ctx, q))
		must(f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Result: []byte(`{"ok":true}`), At: epoch.Add(time.Duration(i)*time.Minute + time.Second)}))
	}
	// A live root keeps its finished children.
	must3(f.core.CreateRun(f.ctx, newRun("live-root")))
	must3(f.core.CreateRun(f.ctx, childRun("old-child", "live-root", "live-root", 1)))
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "old-child", Status: model.RunSucceeded, At: epoch}))
	rowsFull, bytesFull := usageRows(t, f)
	before := stats(t, f)
	if _, err := f.core.PruneRuns(f.ctx, model.RunRetention{}); errCode(err) != "invalid_argument" {
		t.Fatalf("prune without a rule: %v", err)
	}
	n, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(4*time.Minute + 30*time.Second)})
	if err != nil || n != 5 {
		t.Fatalf("age prune deleted %d: %v", n, err)
	}
	if d, r := commits(t, f, before); d != 0 || r != 1 {
		t.Fatalf("housekeeping must be relaxed: durable=%d relaxed=%d", d, r)
	}
	if _, err = f.core.GetRun(f.ctx, "old-child"); err != nil {
		t.Fatalf("a child of a live root was pruned: %v", err)
	}
	n, err = f.core.PruneRuns(f.ctx, model.RunRetention{KeepNewest: 2})
	if err != nil || n != 3 {
		t.Fatalf("count prune deleted %d: %v", n, err)
	}
	page := must(f.core.ListRuns(f.ctx, model.RunFilter{Limit: 100}))
	var left []string
	for _, r := range page.Runs {
		left = append(left, r.ID)
	}
	if strings.Join(left, ",") != "old-9,old-8,old-child,live-root" {
		t.Fatalf("remaining runs: %v", left)
	}
	rowsAfter, bytesAfter := usageRows(t, f)
	if rowsAfter != rowsFull-8 || bytesAfter >= bytesFull || rowsAfter <= rows0 || bytesAfter <= bytes0 {
		t.Fatalf("quota after prune: rows %d (full %d) bytes %d (full %d)", rowsAfter, rowsFull, bytesAfter, bytesFull)
	}
	// Deleting everything finished returns the counters to the live remainder.
	must(f.core.FinishRun(f.ctx, model.RunFinish{ID: "live-root", Status: model.RunSucceeded}))
	n, err = f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(24 * 365 * time.Hour), Limit: 2})
	if err != nil || n != 2 {
		t.Fatalf("limit not honored: %d %v", n, err)
	}
}

func TestSessionLifecycleOneLivePerTarget(t *testing.T) {
	f := openStore(t)
	before := stats(t, f)
	open := model.NewSession{ID: "s1", TargetID: "local", ExperimentResourceID: "exp", ExperimentCommitID: "exp-v1", RootRunID: "root", At: epoch}
	s1, created, err := f.core.OpenSession(f.ctx, open)
	if err != nil || !created || s1.Status != model.SessionOpen || s1.Revision != 1 || !s1.OpenedAt.Equal(epoch) || !s1.ClosedAt.IsZero() {
		t.Fatalf("open: %+v %v", s1, err)
	}
	if d, r := commits(t, f, before); d != 1 || r != 0 {
		t.Fatalf("OpenSession must be one durable commit: durable=%d relaxed=%d", d, r)
	}
	again, created, err := f.core.OpenSession(f.ctx, open)
	if err != nil || created || again.ID != "s1" {
		t.Fatalf("replayed open: %+v %v %v", again, created, err)
	}
	other := open
	other.ID = "s2"
	if _, _, err = f.core.OpenSession(f.ctx, other); errCode(err) != "conflict" || !strings.Contains(err.Error(), "s1") {
		t.Fatalf("second live session on a target: %v", err)
	}
	other.TargetID = "agent-1"
	if _, _, err = f.core.OpenSession(f.ctx, other); err != nil {
		t.Fatalf("another target must be free: %v", err)
	}
	mismatch := open
	mismatch.ExperimentCommitID = "exp-next"
	if _, _, err = f.core.OpenSession(f.ctx, mismatch); errCode(err) != "conflict" {
		t.Fatalf("session identity reused for another commit: %v", err)
	}
	// The stop intent is immutable.
	stopping, err := f.core.RequestStop(f.ctx, "s1", []byte(`{"by":"alice","reason":"done"}`))
	if err != nil || stopping.Status != model.SessionStopping || string(stopping.StopIntent) != `{"by":"alice","reason":"done"}` || stopping.Revision != 2 {
		t.Fatalf("stop: %+v %v", stopping, err)
	}
	same, err := f.core.RequestStop(f.ctx, "s1", []byte(`{"by":"mallory"}`))
	if err != nil || string(same.StopIntent) != `{"by":"alice","reason":"done"}` || same.Revision != 2 {
		t.Fatalf("the intent changed: %+v %v", same, err)
	}
	if _, err = f.core.RequestStop(f.ctx, "s2", []byte(`[]`)); errCode(err) != "invalid_argument" {
		t.Fatalf("intent must be an object: %v", err)
	}
	if _, err = f.core.RequestStop(f.ctx, "ghost", []byte(`{}`)); errCode(err) != "not_found" {
		t.Fatalf("missing session: %v", err)
	}
	// A stopping session still occupies its target.
	third := open
	third.ID = "s3"
	if _, _, err = f.core.OpenSession(f.ctx, third); errCode(err) != "conflict" {
		t.Fatalf("stopping session released its target: %v", err)
	}
	closed, err := f.core.CloseSession(f.ctx, "s1", epoch.Add(time.Hour))
	if err != nil || closed.Status != model.SessionClosed || !closed.ClosedAt.Equal(epoch.Add(time.Hour)) {
		t.Fatalf("close: %+v %v", closed, err)
	}
	again2, err := f.core.CloseSession(f.ctx, "s1", epoch.Add(2*time.Hour))
	if err != nil || !again2.ClosedAt.Equal(closed.ClosedAt) || again2.Revision != closed.Revision {
		t.Fatalf("closing twice changed the session: %+v %v", again2, err)
	}
	if kept, _ := f.core.RequestStop(f.ctx, "s1", []byte(`{"late":true}`)); kept.Status != model.SessionClosed || string(kept.StopIntent) != `{"by":"alice","reason":"done"}` {
		t.Fatalf("a closed session took a stop: %+v", kept)
	}
	if _, _, err = f.core.OpenSession(f.ctx, third); err != nil {
		t.Fatalf("the target is free after the close: %v", err)
	}
	// Listing.
	page := must(f.core.ListSessions(f.ctx, model.SessionFilter{TargetID: "local"}))
	if len(page.Sessions) != 2 || page.Sessions[0].ID != "s3" {
		t.Fatalf("list by target: %+v", page.Sessions)
	}
	live := must(f.core.ListSessions(f.ctx, model.SessionFilter{Statuses: []model.SessionStatus{model.SessionOpen, model.SessionStopping}}))
	if len(live.Sessions) != 2 {
		t.Fatalf("live sessions: %+v", live.Sessions)
	}
	if _, err = f.core.GetSession(f.ctx, "nope"); errCode(err) != "not_found" {
		t.Fatalf("get missing: %v", err)
	}
}

func TestCloseOpenSessionsAtBoot(t *testing.T) {
	f := openStore(t)
	must3(f.core.OpenSession(f.ctx, model.NewSession{ID: "a", TargetID: "t1", ExperimentResourceID: "e", ExperimentCommitID: "c"}))
	must3(f.core.OpenSession(f.ctx, model.NewSession{ID: "b", TargetID: "t2", ExperimentResourceID: "e", ExperimentCommitID: "c"}))
	must3(f.core.OpenSession(f.ctx, model.NewSession{ID: "c", TargetID: "t3", ExperimentResourceID: "e", ExperimentCommitID: "c"}))
	must(f.core.RequestStop(f.ctx, "b", []byte(`{}`)))
	must(f.core.CloseSession(f.ctx, "c", epoch))
	at := epoch.Add(time.Hour)
	n, err := f.core.CloseOpenSessions(f.ctx, at)
	if err != nil || n != 2 {
		t.Fatalf("interrupted %d: %v", n, err)
	}
	for _, id := range []string{"a", "b"} {
		if s := must(f.core.GetSession(f.ctx, id)); s.Status != model.SessionInterrupted || !s.ClosedAt.Equal(at) {
			t.Errorf("%s: %+v", id, s)
		}
	}
	if s := must(f.core.GetSession(f.ctx, "c")); s.Status != model.SessionClosed {
		t.Errorf("a closed session changed: %+v", s)
	}
	if _, _, err = f.core.OpenSession(f.ctx, model.NewSession{ID: "d", TargetID: "t1", ExperimentResourceID: "e", ExperimentCommitID: "c"}); err != nil {
		t.Fatalf("an interrupted session still holds its target: %v", err)
	}
}

func TestRecordingsAreIndexedDurablyAndListedOldestFirst(t *testing.T) {
	f := openStore(t)
	before := stats(t, f)
	q := model.NewRecording{ID: "rec-1", SessionID: "s1", RunID: "run-1", Kind: "rosbag", Path: "/data/bags/rec-1.bag", Metadata: []byte(`{"topics":["/a","/b"]}`), At: epoch}
	rec, created, err := f.core.AddRecording(f.ctx, q)
	if err != nil || !created || string(rec.Metadata) != string(q.Metadata) {
		t.Fatalf("add: %+v %v", rec, err)
	}
	if d, r := commits(t, f, before); d != 1 || r != 0 {
		t.Fatalf("AddRecording must be one durable commit: durable=%d relaxed=%d", d, r)
	}
	if again, created, err := f.core.AddRecording(f.ctx, q); err != nil || created || again.ID != "rec-1" {
		t.Fatalf("replay: %+v %v %v", again, created, err)
	}
	clash := q
	clash.Path = "/elsewhere"
	if _, _, err = f.core.AddRecording(f.ctx, clash); errCode(err) != "conflict" {
		t.Fatalf("clashing facts: %v", err)
	}
	for name, bad := range map[string]model.NewRecording{
		"no path":      {ID: "x", RunID: "r", Kind: "k"},
		"NUL in path":  {ID: "x", RunID: "r", Kind: "k", Path: "a\x00b"},
		"no run":       {ID: "x", Kind: "k", Path: "/p"},
		"metadata arr": {ID: "x", RunID: "r", Kind: "k", Path: "/p", Metadata: []byte(`[]`)},
	} {
		if _, _, err = f.core.AddRecording(f.ctx, bad); errCode(err) != "invalid_argument" {
			t.Errorf("%s: %v", name, err)
		}
	}
	for i := 2; i <= 7; i++ {
		must3(f.core.AddRecording(f.ctx, model.NewRecording{ID: fmt.Sprintf("rec-%d", i), SessionID: []string{"s1", "s2"}[i%2], RunID: "run-" + fmt.Sprint(i%3), Kind: []string{"rosbag", "video"}[i%2], Path: "/p", At: epoch.Add(time.Duration(i/2) * time.Second)}))
	}
	var ids []string
	cursor := ""
	for {
		page := must(f.core.ListRecordings(f.ctx, model.RecordingFilter{Limit: 3, Cursor: cursor}))
		for _, r := range page.Recordings {
			ids = append(ids, r.ID)
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if strings.Join(ids, ",") != "rec-1,rec-2,rec-3,rec-4,rec-5,rec-6,rec-7" {
		t.Fatalf("oldest-first paging: %v", ids)
	}
	if n := len(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{SessionID: "s2"})).Recordings); n != 3 {
		t.Errorf("session filter: %d", n)
	}
	if n := len(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{Kind: "video"})).Recordings); n != 3 {
		t.Errorf("kind filter: %d", n)
	}
	if n := len(must(f.core.ListRecordings(f.ctx, model.RecordingFilter{RunID: "run-1"})).Recordings); n < 1 {
		t.Errorf("run filter: %d", n)
	}
	if _, err = f.core.GetRecording(f.ctx, "ghost"); errCode(err) != "not_found" {
		t.Fatalf("get missing: %v", err)
	}
}

func TestExecutionFactsSurviveARestart(t *testing.T) {
	f := openStore(t)
	run := newRun("run")
	run.IdempotencyKey = "k"
	must3(f.core.CreateRun(f.ctx, run))
	must3(f.core.OpenSession(f.ctx, model.NewSession{ID: "s", TargetID: "local", ExperimentResourceID: "e", ExperimentCommitID: "c", RootRunID: "run"}))
	must3(f.core.AddRecording(f.ctx, model.NewRecording{ID: "rec", RunID: "run", Kind: "k", Path: "/p"}))
	f.reopen(t)
	if got := must(f.core.GetRun(f.ctx, "run")); got.IdempotencyKey != "k" || got.Status != model.RunQueued {
		t.Fatalf("run after restart: %+v", got)
	}
	if s := must(f.core.GetSession(f.ctx, "s")); s.Status != model.SessionOpen {
		t.Fatalf("session after restart: %+v", s)
	}
	if _, err := f.core.GetRecording(f.ctx, "rec"); err != nil {
		t.Fatal(err)
	}
	// The boot sequence of Core: interrupt what the stopped process left open.
	if n, err := f.core.InterruptOpenRuns(f.ctx, epoch); err != nil || n != 1 {
		t.Fatalf("boot interrupt: %d %v", n, err)
	}
	if n, err := f.core.CloseOpenSessions(f.ctx, epoch); err != nil || n != 1 {
		t.Fatalf("boot close: %d %v", n, err)
	}
}

func TestFailedRunWriteLeavesNoRowAndNoQuota(t *testing.T) {
	f := openStore(t)
	f.exec(t, `CREATE TRIGGER fail_run BEFORE INSERT ON runs WHEN NEW.id='doomed' BEGIN SELECT RAISE(ABORT,'late run fault'); END`)
	rows, bytes := usageRows(t, f)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("doomed")); err == nil {
		t.Fatal("late fault committed")
	}
	if f.count(t, "runs") != 0 {
		t.Fatal("failed insert left a row")
	}
	if r, b := usageRows(t, f); r != rows || b != bytes {
		t.Fatalf("failed insert leaked quota: %d/%d -> %d/%d", rows, bytes, r, b)
	}
}

func TestLiveQuotaStopsNewRunsButNeverTheOutcomeOfAdmittedOnes(t *testing.T) {
	f := openStore(t)
	must3(f.core.CreateRun(f.ctx, newRun("admitted")))
	f.exec(t, "UPDATE core_data_usage SET rows=?,bytes=?", coredata.MaxScopeRows, coredata.MaxScopeBytes)
	if _, _, err := f.core.CreateRun(f.ctx, newRun("refused")); errCode(err) != "resource_exhausted" {
		t.Fatalf("a full quota admitted a run: %v", err)
	}
	if _, _, err := f.core.OpenSession(f.ctx, model.NewSession{ID: "s", TargetID: "t", ExperimentResourceID: "e", ExperimentCommitID: "c"}); errCode(err) != "resource_exhausted" {
		t.Fatalf("a full quota admitted a session: %v", err)
	}
	if _, _, err := f.core.AddRecording(f.ctx, model.NewRecording{ID: "r", RunID: "admitted", Kind: "k", Path: "/p"}); errCode(err) != "resource_exhausted" {
		t.Fatalf("a full quota admitted a recording: %v", err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: "admitted", Status: model.RunFailed, Result: []byte(`{"why":"disk"}`)}); err != nil {
		t.Fatalf("the outcome of an admitted run must always be recorded: %v", err)
	}
	// Pruning gives the room back.
	if n, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: time.Now().Add(time.Hour)}); err != nil || n != 1 {
		t.Fatalf("prune: %d %v", n, err)
	}
	if _, _, err := f.core.OpenSession(f.ctx, model.NewSession{ID: "s", TargetID: "t", ExperimentResourceID: "e", ExperimentCommitID: "c"}); err != nil {
		t.Fatalf("freed room not reusable: %v", err)
	}
}

func TestConcurrentStartsWithOneKeyCreateOneRun(t *testing.T) {
	f := openStore(t)
	var wg sync.WaitGroup
	created := make([]bool, 8)
	ids := make([]string, 8)
	for i := range created {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := newRun(fmt.Sprintf("attempt-%d", i))
			q.IdempotencyKey = "same-start"
			run, c, err := f.core.CreateRun(f.ctx, q)
			if err != nil {
				t.Error(err)
				return
			}
			created[i], ids[i] = c, run.ID
		}(i)
	}
	wg.Wait()
	wins := 0
	for i := range created {
		if created[i] {
			wins++
		}
		if ids[i] != ids[0] {
			t.Fatalf("callers got different runs: %v", ids)
		}
	}
	if wins != 1 || f.count(t, "runs") != 1 {
		t.Fatalf("%d creations, %d rows", wins, f.count(t, "runs"))
	}
}

func TestNewScopeRequiresTheModule(t *testing.T) {
	f := openStore(t)
	other := f.scope
	other.Namespace = "nope"
	if _, err := coredata.New(f.db, other); err == nil {
		t.Fatal("an undeclared namespace was bound")
	}
	if _, err := coredata.New(nil, f.scope); err == nil {
		t.Fatal("nil owner accepted")
	}
}
