package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func commandIntent() model.CommandRequest {
	return model.CommandRequest{CommandID: "command", RequestID: "business-request", IdempotencyKey: "intent", Actor: "operator", Risk: "low", Target: "run", Action: "run.start", Payload: []byte(`{"at":9007199254740993,"parameters":{"x":1}}`)}
}

// Deliberately COMMIT an errored outer transaction, as in the group tests.
// The data operation's own savepoint must discard all provisional facts.
func commandTx(t *testing.T, db *sql.DB, ctx context.Context, scope string, work func(*sql.Tx) error) error {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	err = atomicData(ctx, tx, func() error { return work(tx) })
	if commit := tx.Commit(); commit != nil {
		t.Fatal(commit)
	}
	return err
}

func TestCommandExactIntentAndTerminalOnce(t *testing.T) {
	db, ctx := fixture(t)
	r := commandIntent()
	var original, terminal model.CommandReceipt
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		var err error
		var created bool
		original, created, err = acceptCommand(ctx, tx, testScope, r)
		if err == nil && (!created || original.Status != "accepted") {
			t.Fatal("new intent not accepted")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var beforeBytes int64
	if err := db.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&beforeBytes); err != nil {
		t.Fatal(err)
	}
	r.Payload = []byte(`{ "parameters": {"x":1}, "at":9007199254740993 }`)
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		got, created, err := acceptCommand(ctx, tx, testScope, r)
		if created || got.CreatedAt != original.CreatedAt || string(got.Payload) != string(original.Payload) {
			t.Fatal("replay changed original receipt")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for name, alter := range map[string]func(*model.CommandRequest){
		"adjacent >2^53":  func(r *model.CommandRequest) { r.Payload = []byte(`{"at":9007199254740992,"parameters":{"x":1}}`) },
		"number encoding": func(r *model.CommandRequest) { r.Payload = []byte(`{"at":9007199254740993,"parameters":{"x":1.0}}`) },
		"actor":           func(r *model.CommandRequest) { r.Actor = "other" },
		"risk":            func(r *model.CommandRequest) { r.Risk = "high" },
		"reason":          func(r *model.CommandRequest) { r.Reason = "different" },
		"request id":      func(r *model.CommandRequest) { r.RequestID = "different" },
		"command id":      func(r *model.CommandRequest) { r.CommandID = "different" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := commandIntent()
			alter(&changed)
			err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
				_, _, err := acceptCommand(ctx, tx, testScope, changed)
				return err
			})
			if errorCode(err) != "conflict" {
				t.Fatalf("changed replay accepted: %v", err)
			}
		})
	}
	var afterBytes int64
	if err := db.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&afterBytes); err != nil || afterBytes != beforeBytes {
		t.Fatalf("replay changed quota %d→%d: %v", beforeBytes, afterBytes, err)
	}
	completion := model.CommandCompletion{CommandID: "command", Result: model.CommandResult{Status: "succeeded", ResultRef: "run", Result: []byte(`{"at":9007199254740993,"ok":true}`)}}
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		var err error
		terminal, err = completeCommand(ctx, tx, testScope, completion)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	completion.Result.Result = []byte(`{ "ok":true, "at":9007199254740993 }`)
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		got, err := completeCommand(ctx, tx, testScope, completion)
		if got.CompletedAt == nil || !got.CompletedAt.Equal(*terminal.CompletedAt) || string(got.Result) != string(terminal.Result) {
			t.Fatal("terminal replay changed receipt")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	completion.Result.Result = []byte(`{"at":9007199254740992,"ok":true}`)
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		_, err := completeCommand(ctx, tx, testScope, completion)
		return err
	}); errorCode(err) != "conflict" {
		t.Fatalf("terminal overwritten: %v", err)
	}
}

func eventInputs(n int) []model.ExecutionEventInput {
	inputs := make([]model.ExecutionEventInput, n)
	for i := range inputs {
		inputs[i] = model.ExecutionEventInput{EntityType: "run", EntityID: fmt.Sprintf("run-%d", i%3), Type: "run.changed", Payload: []byte(`{"at":9007199254740993,"html":"<>&"}`)}
	}
	return inputs
}

func TestCommandLastEventFailureRollsBackCountersQuotaAndCompletion(t *testing.T) {
	db, ctx := fixture(t)
	execSQL(t, db, ctx, `CREATE TRIGGER fail_final_event BEFORE INSERT ON core_execution_events WHEN NEW.offset=1000 BEGIN SELECT RAISE(ABORT,'final event fault'); END`)
	err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		if _, _, err := acceptCommand(ctx, tx, testScope, commandIntent()); err != nil {
			return err
		}
		if _, err := completeCommand(ctx, tx, testScope, model.CommandCompletion{CommandID: "command", Result: model.CommandResult{Status: "succeeded"}}); err != nil {
			return err
		}
		_, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(1000))
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "final event fault") {
		t.Fatalf("late failure not observed: %v", err)
	}
	for _, table := range []string{"core_commands", "core_execution_events", "core_event_offsets", "core_event_sequences", "core_data_usage"} {
		if got := count(t, db, ctx, table); got != 0 {
			t.Fatalf("partial %s=%d", table, got)
		}
	}
	execSQL(t, db, ctx, "DROP TRIGGER fail_final_event")
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		_, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(1000))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		page, err := readExecutionEvents(ctx, tx, testScope, model.EventRead{AfterOffset: "0", Limit: 1000})
		if err != nil {
			return err
		}
		if len(page.Events) != 1000 || page.Cursor.LatestOffset != "1000" || page.Events[999].Seq != "334" || page.Events[999].Offset != "1000" || page.Events[0].Level != "info" {
			t.Fatalf("counter allocation incorrect: %+v", page.Events[999])
		}
		if string(page.Events[0].Payload) != string(eventInputs(1)[0].Payload) {
			t.Fatal("event bytes changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEventScopeCursorAndBoundedHighWatermark(t *testing.T) {
	db, ctx := fixture(t)
	var first model.EventPage
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		empty, err := eventCursor(ctx, tx, testScope)
		if err != nil || empty.LatestOffset != "0" || empty.StreamID == "" {
			t.Fatalf("empty stream %+v: %v", empty, err)
		}
		if _, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(6)); err != nil {
			return err
		}
		first, err = readExecutionEvents(ctx, tx, testScope, model.EventRead{AfterOffset: "0", Limit: 2})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		if _, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(3)); err != nil {
			return err
		}
		page, err := readExecutionEvents(ctx, tx, testScope, model.EventRead{AfterOffset: first.NextOffset, Through: first.Through, Limit: 1000})
		if err != nil {
			return err
		}
		if len(page.Events) != 4 || page.NextOffset != "6" || page.Through != "6" || page.Cursor.LatestOffset != "9" || page.Cursor.StreamID != first.Cursor.StreamID {
			t.Fatalf("mutable append escaped highwatermark: %+v", page)
		}
		filtered, err := readExecutionEvents(ctx, tx, testScope, model.EventRead{AfterOffset: "0", EntityType: "run", EntityID: "run-0", AfterSeq: "1", Limit: 1000})
		if err == nil && (len(filtered.Events) != 2 || filtered.Events[0].Offset != "4") {
			t.Fatal("entity/seq filter incorrect")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := commandTx(t, db, ctx, "other-scope", func(tx *sql.Tx) error {
		cursor, err := eventCursor(ctx, tx, "other-scope")
		if err == nil && (cursor.LatestOffset != "0" || cursor.StreamID == first.Cursor.StreamID) {
			t.Fatal("scopes share stream")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []model.EventRead{{AfterOffset: "10", Limit: 1000}, {AfterOffset: "0", Through: "10", Limit: 1000}, {AfterOffset: "00", Limit: 1000}, {AfterOffset: "0", AfterSeq: "1", Limit: 1000}} {
		if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error { _, err := readExecutionEvents(ctx, tx, testScope, r); return err }); err == nil {
			t.Fatalf("invalid cursor accepted: %+v", r)
		}
	}
}

func TestEventCounterOverflowDoesNotCreateFact(t *testing.T) {
	db, ctx := fixture(t)
	execSQL(t, db, ctx, "INSERT INTO core_event_offsets VALUES(?,?)", testScope, int64(math.MaxInt64))
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		_, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(1))
		return err
	}); errorCode(err) != "resource_exhausted" {
		t.Fatalf("counter overflow accepted: %v", err)
	}
	if count(t, db, ctx, "core_execution_events") != 0 || count(t, db, ctx, "core_data_usage") != 0 {
		t.Fatal("counter overflow left facts/quota")
	}
	if exactCommandJSON(nil, []byte("null")) || exactCommandJSON([]byte("{} {}"), []byte("{} {}")) || !exactCommandJSON([]byte("null"), []byte(" null ")) {
		t.Fatal("JSON absence/one-value semantics changed")
	}
	var input model.ExecutionEventInput
	if err := json.Unmarshal([]byte(`{"payload":"e30="}`), &input); err != nil || string(input.Payload) != "{}" {
		t.Fatal("payload is not an explicit byte wire")
	}
}

func TestExistingCommandCompletionFailureRestoresAcceptedReceipt(t *testing.T) {
	db, ctx := fixture(t)
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error { _, _, err := acceptCommand(ctx, tx, testScope, commandIntent()); return err }); err != nil {
		t.Fatal(err)
	}
	var originalBytes int64
	if err := db.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&originalBytes); err != nil {
		t.Fatal(err)
	}
	execSQL(t, db, ctx, `CREATE TRIGGER fail_terminal_event BEFORE INSERT ON core_execution_events BEGIN SELECT RAISE(ABORT,'terminal event fault'); END`)
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		if _, err := completeCommand(ctx, tx, testScope, model.CommandCompletion{CommandID: "command", Result: model.CommandResult{Status: "failed", Result: []byte(`{"code":"failure"}`)}}); err != nil {
			return err
		}
		_, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(1))
		return err
	}); err == nil || !strings.Contains(err.Error(), "terminal event fault") {
		t.Fatalf("completion fault missing: %v", err)
	}
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		saved, err := readCommand(ctx, tx, testScope, model.CommandRead{ID: "command"})
		if err == nil && (!saved.Found || saved.Receipt.Status != "accepted" || saved.Receipt.CompletedAt != nil) {
			t.Fatal("failed transaction changed old accepted receipt")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var afterBytes int64
	if err := db.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&afterBytes); err != nil || afterBytes != originalBytes || count(t, db, ctx, "core_execution_events") != 0 {
		t.Fatalf("failed terminal commit changed quota: %v", err)
	}
}

func TestEventRestartIdentityAndIndexes(t *testing.T) {
	db, ctx := fixture(t)
	var original model.EventCursor
	if err := commandTx(t, db, ctx, testScope, func(tx *sql.Tx) error {
		if _, err := appendExecutionEvents(ctx, tx, testScope, eventInputs(6)); err != nil {
			return err
		}
		var err error
		original, err = eventCursor(ctx, tx, testScope)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`SELECT body FROM core_execution_events WHERE scope='s' AND offset>0 AND offset<=1000 ORDER BY offset LIMIT 1000`,
		`SELECT body FROM core_execution_events WHERE scope='s' AND entity_type='run' AND entity_id='id' AND seq>1 AND offset>0 AND offset<=1000 ORDER BY seq LIMIT 1000`,
		`SELECT body FROM core_commands WHERE scope='s' AND idempotency_key='key'`,
		`SELECT command_id FROM core_commands WHERE scope='s' AND action='run.start' AND status='accepted' ORDER BY command_id LIMIT 1000`,
	} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan.String(), "SEARCH") || strings.Contains(plan.String(), "TEMP B-TREE") {
			t.Fatalf("unindexed/sorted history access: %s\n%s", query, plan.String())
		}
	}
	var sequence int
	var name, path string
	if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("fixture DB path missing: %s", path)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := commandTx(t, reopened, ctx, testScope, func(tx *sql.Tx) error {
		got, err := eventCursor(ctx, tx, testScope)
		if err == nil && got != original {
			t.Fatalf("restart changed stream identity/counter %+v→%+v", original, got)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	differentDB, differentCtx := fixture(t)
	if err := commandTx(t, differentDB, differentCtx, testScope, func(tx *sql.Tx) error {
		got, err := eventCursor(differentCtx, tx, testScope)
		if err == nil && got.StreamID == original.StreamID {
			t.Fatal("new database reused event stream identity")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
