package coredata_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestSessionActionInstanceSelectorPersistsAndAccounts(t *testing.T) {
	f := openStore(t)
	id := strings.Repeat("🙂", 128)
	q := newRun("selected-session-action")
	q.SessionID = "session"
	q.Trigger, _ = json.Marshal(map[string]string{"kind": "session-action", "instanceId": id, "actionId": "run"})
	q.SourceObservation, _ = json.Marshal(map[string]any{"invocation": map[string]string{"instanceId": id, "actionId": "run"}})
	baseRows, baseBytes := usageRows(t, f)
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	rows, bytes := usageRows(t, f)
	if rows != baseRows+1 || bytes <= baseBytes+int64(len(id)) {
		t.Fatalf("derived selector was not charged: rows=%d bytes=%d", rows-baseRows, bytes-baseBytes)
	}
	f.reopen(t)
	filter := model.RunFilter{SessionID: "session", SessionInstanceID: id, Projection: model.RunObservation, Statuses: []model.RunStatus{model.RunQueued, model.RunRunning, model.RunStopping}, Limit: 1}
	page, err := f.core.ListRuns(f.ctx, filter)
	if err != nil || len(page.Runs) != 1 || page.Runs[0].ID != q.ID {
		t.Fatalf("reopened selector: %+v %v", page, err)
	}
	filter.SessionID = ""
	if _, err := f.core.ListRuns(f.ctx, filter); errCode(err) != "invalid_argument" {
		t.Fatalf("instance-only unbounded session search: %v", err)
	}
	filter.SessionID, filter.SessionInstanceID = "session", id+"x"
	if _, err := f.core.ListRuns(f.ctx, filter); errCode(err) != "invalid_argument" {
		t.Fatalf("oversized instance filter: %v", err)
	}
	bad := q
	bad.ID = "oversized-session-action"
	bad.Trigger, _ = json.Marshal(map[string]string{"kind": "session-action", "instanceId": id + "x"})
	if _, _, err := f.core.CreateRun(f.ctx, bad); errCode(err) != "invalid_argument" {
		t.Fatalf("unindexable accepted Session Action: %v", err)
	}
	gotRows, gotBytes := usageRows(t, f)
	if gotRows != rows || gotBytes != bytes {
		t.Fatal("rejected Session Action changed quota")
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, At: epoch}); err != nil {
		t.Fatal(err)
	}
	pruned, err := f.core.PruneRuns(f.ctx, model.RunRetention{OlderThan: epoch.Add(time.Hour)})
	if err != nil || pruned.Deleted != 1 {
		t.Fatalf("selector Run was not retired: %+v %v", pruned, err)
	}
	gotRows, gotBytes = usageRows(t, f)
	if gotRows != baseRows || gotBytes != baseBytes {
		t.Fatalf("retirement did not release derived selector quota: rows=%d bytes=%d", gotRows-baseRows, gotBytes-baseBytes)
	}
}
