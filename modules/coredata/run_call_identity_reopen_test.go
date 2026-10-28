package coredata_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunCallIdentitySurvivesStorageReopen(t *testing.T) {
	f := openStore(t)
	q := childRun("child", "parent", "root", 2)
	q.TargetID = "agent"
	q.Trigger = json.RawMessage(`{"kind":"workflow-call","parentSource":{"targetId":"local","runId":"parent","workflow":{"resourceId":"wf","commitId":"c1","digest":"digest","name":""}}}`)
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Result: json.RawMessage(`{"exact":true}`)}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	got, err := f.core.GetRunCallIdentity(f.ctx, q.ID)
	if err != nil || got.TargetID != q.TargetID || got.RootRunID != q.RootRunID || got.ParentRunID != q.ParentRunID || got.CallNodeID != q.CallNodeID || got.Depth != q.Depth || !bytes.Equal(got.Trigger, q.Trigger) {
		t.Fatalf("reopened canonical call: %+v %v", got, err)
	}
	full, err := f.core.GetRun(f.ctx, q.ID)
	if err != nil || !bytes.Equal(full.Result, json.RawMessage(`{"exact":true}`)) {
		t.Fatalf("exact full Get changed: %+v %v", full, err)
	}
}
