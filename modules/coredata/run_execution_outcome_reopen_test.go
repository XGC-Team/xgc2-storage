package coredata_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunExecutionOutcomeSurvivesStorageReopen(t *testing.T) {
	f := openStore(t)
	request := childRun("outcome", "parent", "root", 2)
	request.TargetID = "agent"
	request.SourceObservation = json.RawMessage(`{"invocation":{"instanceId":"exact"}}`)
	if _, _, err := f.core.CreateRun(f.ctx, request); err != nil {
		t.Fatal(err)
	}
	result := json.RawMessage(`{"exact":9007199254740993,"data":"` + strings.Repeat("z", 1<<20) + `"}`)
	nodes := json.RawMessage(`[{"id":"only-full-detail","data":"` + strings.Repeat("x", 1<<20) + `"}]`)
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: request.ID, Status: model.RunSucceeded, Result: result, Nodes: nodes}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	got, err := f.core.GetRunExecutionOutcome(f.ctx, request.ID)
	if err != nil || got.Projection != model.RunExecutionOutcome || !bytes.Equal(got.Result, result) || !bytes.Equal(got.SourceObservation, request.SourceObservation) || len(got.Nodes) != 0 || len(got.Inputs) != 0 || len(got.Trigger) != 0 {
		t.Fatalf("reopened outcome did not remain exact and selected: resultBytes=%d nodesBytes=%d inputsBytes=%d triggerBytes=%d error=%v", len(got.Result), len(got.Nodes), len(got.Inputs), len(got.Trigger), err)
	}
	full, err := f.core.GetRun(f.ctx, request.ID)
	if err != nil || !bytes.Equal(full.Result, result) || !bytes.Equal(full.Nodes, nodes) {
		t.Fatalf("explicit complete Get lost detail after reopening: %v", err)
	}
}
