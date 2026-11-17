package coredata_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestRunNodeProjectionPublicReadAfterReopen(t *testing.T) {
	f := openStore(t)
	q := childRun("selected", "parent", "root", 2)
	q.Inputs = json.RawMessage(`{"input":{"accepted":9007199254740993}}`)
	q.SourceObservation = json.RawMessage(`{"invocation":{"instanceId":"owned"}}`)
	if _, _, err := f.core.CreateRun(f.ctx, q); err != nil {
		t.Fatal(err)
	}
	nodes := json.RawMessage(`[{"id":"serve","status":"succeeded","output":{"value":"accepted"}}]`)
	result := json.RawMessage(`{"exact":9007199254740993}`)
	if _, err := f.core.FinishRun(f.ctx, model.RunFinish{ID: q.ID, Status: model.RunSucceeded, Nodes: nodes, Result: result}, model.RunRetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	for _, include := range []bool{false, true} {
		r, err := f.core.GetRunNodeProjection(f.ctx, q.ID, include)
		if err != nil || r.Projection != model.RunNodeProjection || !bytes.Equal(r.Nodes, nodes) || !bytes.Equal(r.Result, result) || !bytes.Equal(r.SourceObservation, q.SourceObservation) || include && !bytes.Equal(r.Inputs, q.Inputs) || !include && len(r.Inputs) != 0 {
			t.Fatalf("reopened projection: include=%v row=%+v err=%v", include, r, err)
		}
		r.Nodes[0], r.Result[0], r.SourceObservation[0] = '!', '!', '!'
	}
	r, err := f.core.GetRunNodeProjection(f.ctx, q.ID, true)
	if err != nil || !bytes.Equal(r.Nodes, nodes) || !bytes.Equal(r.Result, result) || !bytes.Equal(r.Inputs, q.Inputs) || !bytes.Equal(r.SourceObservation, q.SourceObservation) {
		t.Fatal("mutable returned bytes changed durable execution", err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.core.GetRunNodeProjection(ctx, q.ID, false); errCode(err) != "cancelled" {
		t.Fatalf("canceled exact read: %v", err)
	}
}
