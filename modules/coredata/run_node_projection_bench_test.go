package coredata_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// This measures Go read/decode allocations for one exact retained Run, not
// physical SQLite I/O or latency under an adopted station workload.
func BenchmarkRunNodeProjection(b *testing.B) {
	core, _ := benchCore(b)
	ctx := context.Background()
	q := benchRun("selected-node")
	q.Inputs = json.RawMessage(`{"input":"` + strings.Repeat("i", 512<<10) + `"}`)
	q.Trigger = json.RawMessage(`{"diagnostic":"` + strings.Repeat("t", 512<<10) + `"}`)
	if _, _, err := core.CreateRun(ctx, q); err != nil {
		b.Fatal(err)
	}
	f := model.RunFinish{ID: q.ID, Status: model.RunFailed,
		Result:        json.RawMessage(`{"value":"` + strings.Repeat("r", 128<<10) + `"}`),
		Error:         json.RawMessage(`{"code":"exact","message":"kept"}`),
		Nodes:         json.RawMessage(`[{"id":"unrelated","output":"` + strings.Repeat("n", 150<<10) + `"},{"id":"selected","output":"owned"}]`),
		CleanupErrors: json.RawMessage(`[{"message":"` + strings.Repeat("c", 128<<10) + `"}]`)}
	if _, err := core.FinishRun(ctx, f, model.RunRetentionPolicy{}); err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"full", "execution-node", "read-node-with-input"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var r model.Run
				var err error
				if name == "full" {
					r, err = core.GetRun(ctx, q.ID)
				} else {
					r, err = core.GetRunNodeProjection(ctx, q.ID, name == "read-node-with-input")
				}
				if err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(len(r.Inputs)+len(r.Trigger)+len(r.Error)+len(r.Result)+len(r.Nodes)+len(r.CleanupErrors)), "decoded-payload-B/op")
			}
		})
	}
}
