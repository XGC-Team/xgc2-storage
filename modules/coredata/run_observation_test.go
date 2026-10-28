package coredata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunObservationPageByteBoundaryKeepsEverySameTimeIdentity(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	source := json.RawMessage(`{"opaque":"` + strings.Repeat("s", model.MaxRunSourceObservationBytes-len(`{"opaque":""}`)) + `"}`)
	full := json.RawMessage(`"` + strings.Repeat("full", 3000) + `"`)
	preview := json.RawMessage(`"` + strings.Repeat("p", model.MaxRunErrorSummaryBytes-2) + `"`)
	const total = 800
	at := time.Unix(100, 123).UTC()
	for i := range total {
		// Admitted internal controls/quotes maximize JSON identifier escaping.
		id := fmt.Sprintf("%03d-%s", i, strings.Repeat("\x01\"", 125))
		q := model.NewRun{ID: id, TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: json.RawMessage(`{}`), Trigger: json.RawMessage(`{}`), SourceObservation: source, At: at}
		if _, _, err = createRun(ctx, tx, testScope, q); err != nil {
			t.Fatal(err)
		}
		if _, err = finishRun(ctx, tx, testScope, model.RunFinish{ID: id, Status: model.RunFailed, Error: full, ErrorSummary: preview}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		page, err := listRuns(ctx, tx, testScope, model.RunFilter{Projection: model.RunObservation, Limit: model.MaxPageSize, Cursor: cursor})
		if err != nil || len(page.Runs) == 0 {
			t.Fatalf("empty/rejected continuation: %s %v", cursor, err)
		}
		encoded, err := json.Marshal(page)
		if err != nil || len(encoded) > model.MaxResponseBytes {
			t.Fatalf("response budget: %d %v", len(encoded), err)
		}
		for _, row := range page.Runs {
			if seen[row.ID] {
				t.Fatalf("repeated identity: %q", row.ID)
			}
			seen[row.ID] = true
			if !bytes.Equal(row.Error, preview) || len(row.Trigger)+len(row.CleanupErrors) != 0 {
				t.Fatal("observation fetched full detail")
			}
		}
		pages++
		if page.Next == "" {
			break
		}
		_, last, err := decodeCursor(page.Next)
		if err != nil || last != page.Runs[len(page.Runs)-1].ID || page.Next == cursor {
			t.Fatal("cursor skipped lookahead or made no progress")
		}
		cursor = page.Next
	}
	if pages < 2 || len(seen) != total {
		t.Fatalf("byte pagination lost identities: %d pages %d rows", pages, len(seen))
	}
	if _, err := listRuns(ctx, tx, testScope, model.RunFilter{Cursor: "invalid"}); err == nil {
		t.Fatal("invalid cursor accepted")
	}
}

func TestLargeFullRunRefusesAnEmptyRepeatedListPage(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	object := func(size int) json.RawMessage {
		return json.RawMessage(`{"data":"` + strings.Repeat("x", size-len(`{"data":""}`)) + `"}`)
	}
	q := model.NewRun{ID: "large", TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: object(model.MaxRunInputsBytes), Trigger: object(model.MaxRunTriggerBytes)}
	if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	full, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Result: object(model.MaxRunResultBytes), Nodes: object(model.MaxRunNodesBytes)})
	if err != nil {
		t.Fatal(err)
	}
	if page, err := listRuns(ctx, tx, testScope, model.RunFilter{}); err == nil || len(page.Runs) != 0 || page.Next != "" {
		t.Fatal("oversize first row became an empty success/continuation")
	}
	if len(full.Nodes) != model.MaxRunNodesBytes || len(full.Inputs) != model.MaxRunInputsBytes {
		t.Fatal("detail was truncated to fit a list")
	}
	thin, err := listRuns(ctx, tx, testScope, model.RunFilter{Projection: model.RunObservation})
	if err != nil || len(thin.Runs) != 1 || !thin.Runs[0].HasResult {
		t.Fatalf("thin exact identity unavailable: %v", err)
	}
}

func TestRunOutcomeProjectionValidationIsAtomicAndKeepsCleanupCount(t *testing.T) {
	db, ctx := fixture(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	q := model.NewRun{ID: "outcome", TargetID: "local", WorkflowResourceID: "workflow", WorkflowCommitID: "commit", DefinitionDigest: "digest", ActionID: "run", Inputs: json.RawMessage(`{}`), Trigger: json.RawMessage(`{}`)}
	if _, _, err := createRun(ctx, tx, testScope, q); err != nil {
		t.Fatal(err)
	}
	var initial int64
	if err := tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&initial); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{`{}`, `"bad"`, `true`, `42`} {
		if _, err := finishRun(ctx, tx, testScope, model.RunFinish{ID: q.ID, Status: model.RunFailed, CleanupErrors: json.RawMessage(invalid)}); err == nil {
			t.Fatalf("invalid cleanup shape accepted: %s", invalid)
		}
		row, err := getRun(ctx, tx, testScope, q.ID)
		if err != nil || row.Status != model.RunQueued || row.Revision != 1 {
			t.Fatal("invalid outcome partially committed")
		}
		var current int64
		if err := tx.QueryRowContext(ctx, "SELECT bytes FROM core_data_usage WHERE scope=?", testScope).Scan(&current); err != nil || current != initial {
			t.Fatal("rejection changed quota")
		}
	}
	full := json.RawMessage(`"` + strings.Repeat("x", 5000) + `"`)
	finish := model.RunFinish{ID: q.ID, Status: model.RunFailed, Error: full, CleanupErrors: json.RawMessage(`[{"resource":"a"},{"resource":"b"}]`)}
	if _, err := finishRun(ctx, tx, testScope, finish); err == nil {
		t.Fatal("large error silently lost observation")
	}
	finish.ErrorSummary = json.RawMessage(`{"message":"explicit preview","truncated":true}`)
	row, err := finishRun(ctx, tx, testScope, finish)
	if err != nil || row.CleanupFailed != 2 || !bytes.Equal(row.Error, full) {
		t.Fatalf("terminal projection: %+v %v", row, err)
	}
	if _, err := finishRun(ctx, tx, testScope, finish); err != nil {
		t.Fatal("exact finish retry changed meaning", err)
	}
	assertRunUsage(t, ctx, tx, testScope)
}
