package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
)

func TestSnapshotDoesNotCopyUnusedInvalidEquality(t *testing.T) {
	s, _, ctx := setup(t)
	local, err := client.NewLocal(s, testScope, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.Repeat([]byte{'x'}, 16<<20)
	cases := []struct {
		name    string
		queries []api.Query
	}{
		{"no-index", []api.Query{{Collection: "state", Equal: []json.RawMessage{input}}}},
		{"unknown-index", []api.Query{{Collection: "state", Index: "unknown", Equal: []json.RawMessage{input}}}},
		{"wrong-arity", []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{input, input}}}},
		{"deleted-index", []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{input}, IncludeDeleted: true}}},
		{"exact-key", []api.Query{{Collection: "state", Keys: []string{"a"}, Equal: []json.RawMessage{input}}}},
		{"header", []api.Query{{Collection: "state", HeaderIndex: "unique", Keys: []string{"a"}, Equal: []json.RawMessage{input}}}},
		{"unreached-query", []api.Query{{Collection: "state", Equal: []json.RawMessage{input}}, {Collection: "state", Index: "unique", Equal: []json.RawMessage{input}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := api.SnapshotRequest{Scope: testScope, Queries: tc.queries}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			response, err := local.Snapshot(ctx, "invalid-equality", request)
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(request)
			if code(err) != "invalid_argument" || len(response.Results) != 0 {
				t.Fatalf("invalid plan: %+v %v", response, err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			if allocated >= uint64(len(input)) {
				t.Fatalf("invalid query copied unused %d-byte equality: allocated=%d", len(input), allocated)
			}
			t.Logf("invalid %s: %d Go bytes for %d unused equality bytes", tc.name, allocated, len(input))
		})
	}
	stats, err := s.Stats()
	if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("snapshot owner did not return to idle: %+v %v", stats, err)
	}
}

func TestSnapshotBoundsIndexedEqualityBeforeCopy(t *testing.T) {
	s, _, ctx := setup(t)
	local, err := client.NewLocal(s, testScope, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.Repeat([]byte{'x'}, 16<<20)
	input[0], input[len(input)-1] = '"', '"'
	request := api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{input}}}}
	for _, tc := range []struct {
		name string
		code string
	}{{"oversized-valid-string", "resource_exhausted"}, {"oversized-malformed-string", "invalid_argument"}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.code == "invalid_argument" {
				input[len(input)-2] = '\\' // escapes the only closing quote
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			response, err := local.Snapshot(ctx, "oversized-indexed-equality", request)
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(request)
			if code(err) != tc.code || len(response.Results) != 0 {
				t.Fatalf("indexed input: %+v %v, want %s", response, err, tc.code)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			if allocated >= uint64(len(input)) {
				t.Fatalf("copied or decoded oversized indexed scalar: allocated=%d input=%d", allocated, len(input))
			}
			t.Logf("%s: %d Go bytes for %d input bytes", tc.name, allocated, len(input))
		})
	}
	stats, err := s.Stats()
	if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("indexed input did not return to idle: %+v %v", stats, err)
	}
}

func TestSnapshotIndexedEscapesAndWhitespaceKeepExistingTupleBound(t *testing.T) {
	s, _, ctx := setup(t)
	name := strings.Repeat("a", 3000)
	body, err := json.Marshal(map[string]string{"name": name})
	if err != nil {
		t.Fatal(err)
	}
	initial := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: initial.Token, Mutations: []api.Mutation{{Collection: "state", Key: "a", ExpectedVersion: "0", Data: body}}}); err != nil {
		t.Fatal(err)
	}
	padding := strings.Repeat(" \t\n\r", 32<<10)
	for _, tc := range []struct {
		name    string
		equal   string
		wantKey string
		code    string
	}{
		{"escaped-match-padded", padding + `"` + strings.Repeat(`\u0061`, 3000) + `"` + padding, "a", ""},
		{"exact-canonical-bound", `"` + strings.Repeat(`\u0061`, 4092) + `"`, "", ""},
		{"over-canonical-bound", `"` + strings.Repeat(`\u0061`, 4093) + `"`, "", "resource_exhausted"},
		{"unicode-surrogates", `"\ud83d\ude00"`, "", ""},
		{"malformed-escape", `"` + strings.Repeat("a", 25000) + `\q"`, "", "invalid_argument"},
		{"number-too-large", "1" + strings.Repeat("0", 25000), "", "resource_exhausted"},
		{"malformed-number", "1" + strings.Repeat("0", 25000) + ".", "", "invalid_argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{json.RawMessage(tc.equal)}}}})
			if code(err) != tc.code {
				t.Fatalf("indexed equality: %v, want %s", err, tc.code)
			}
			if tc.code != "" {
				if len(result.Results) != 0 {
					t.Fatalf("failed equality returned partial results: %+v", result)
				}
				return
			}
			if len(result.Results) != 1 {
				t.Fatalf("missing indexed result: %+v", result)
			}
			records := result.Results[0].Records
			if tc.wantKey == "" && len(records) != 0 || tc.wantKey != "" && (len(records) != 1 || records[0].Key != tc.wantKey) {
				t.Fatalf("indexed result: %+v", records)
			}
		})
	}
}

func TestSnapshotLargeOverwrittenIndexValuesKeepLastField(t *testing.T) {
	ctx := budget(t)
	m := manifest()
	m.Namespaces[0].Collections[0].Indexes[0].Fields = []string{"name", "name"}
	m.Namespaces[0].Collections[0].Indexes = append(m.Namespaces[0].Collections[0].Indexes, api.Index{ID: "pair", Fields: []string{"name", "other"}})
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, Config{Path: filepath.Join(dir, "repeated-index.db"), Create: true, Manifest: m})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	initial := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: initial.Token, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"kept"}`)}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		first string
		code  string
	}{
		{"string", `"` + strings.Repeat("x", 25000) + `"`, ""},
		{"number", "1" + strings.Repeat("0", 25000), ""},
		{"array", `[` + strings.Repeat("null,", 6000) + `{"key":[1,true,"ok"]}]`, ""},
		{"object", `{"long":"` + strings.Repeat("x", 25000) + `","nested":[null,false]}`, ""},
		{"invalid-string", `"` + strings.Repeat("x", 25000) + `\q"`, "invalid_argument"},
		{"invalid-array", `[` + strings.Repeat("null,", 6000) + `]`, "invalid_argument"},
		{"invalid-object", `{"long":"` + strings.Repeat("x", 25000) + `",}`, "invalid_argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := api.Query{Collection: "state", Index: "unique", Equal: []json.RawMessage{json.RawMessage(tc.first), json.RawMessage(`"kept"`)}}
			result, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{q}})
			if code(err) != tc.code {
				t.Fatalf("repeated field equality: %v, want %s", err, tc.code)
			}
			if tc.code != "" {
				if len(result.Results) != 0 {
					t.Fatalf("invalid equality returned partial results: %+v", result)
				}
				return
			}
			if len(result.Results) != 1 || len(result.Results[0].Records) != 1 || result.Results[0].Records[0].Key != "a" {
				t.Fatalf("earlier field was not overwritten: %+v", result)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		first  string
		second string
		code   string
	}{
		{"large-string-before-nonscalar", `"` + strings.Repeat("x", 25000) + `"`, `[]`, "invalid_argument"},
		{"large-number-before-nonscalar", "1" + strings.Repeat("0", 25000), `[]`, "resource_exhausted"},
		{"nonscalar-before-large-number", `[]`, "1" + strings.Repeat("0", 25000), "invalid_argument"},
		{"malformed-before-large-number", `"unterminated`, "1" + strings.Repeat("0", 25000), "invalid_argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := api.Query{Collection: "state", Index: "pair", Equal: []json.RawMessage{json.RawMessage(tc.first), json.RawMessage(tc.second)}}
			result, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{q}})
			if code(err) != tc.code || len(result.Results) != 0 {
				t.Fatalf("indexed validation order: %+v %v, want %s", result, err, tc.code)
			}
		})
	}
}

func TestSnapshotIndexedInputRemainsOwnedAfterReadDeadline(t *testing.T) {
	s, _, ctx := setup(t)
	initial := snapshot(t, s, ctx, "a")
	if _, err := s.Batch(ctx, api.BatchRequest{Scope: testScope, Expected: initial.Token, Mutations: []api.Mutation{mutation("state", "a", "0", `{"name":"original"}`)}}); err != nil {
		t.Fatal(err)
	}
	entered, release := GateReaderForModuleTest(t, s, "SELECT revision FROM scopes", "query")
	defer release()
	value := json.RawMessage(`"original"`)
	request := api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{value}, Limit: 1}}}
	callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	type answer struct {
		result api.SnapshotResponse
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := s.Snapshot(callCtx, request); done <- answer{result, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("real revision query did not enter")
	}
	got := <-done
	var outcome *ReadError
	if !errors.As(got.err, &outcome) || !errors.Is(got.err, context.DeadlineExceeded) || len(got.result.Results) != 0 {
		t.Fatalf("held indexed snapshot: %+v %v", got.result, got.err)
	}
	for i := range value {
		value[i] = 'x'
	}
	request.Queries[0].Equal[0] = json.RawMessage(`"different"`)
	release()
	if err := outcome.WaitSettlement(ctx); err != nil {
		t.Fatalf("indexed read physical join: %v", err)
	}
	current, err := s.Snapshot(ctx, api.SnapshotRequest{Scope: testScope, Queries: []api.Query{{Collection: "state", Index: "unique", Equal: []json.RawMessage{json.RawMessage(`"original"`)}, Limit: 1}}})
	if err != nil || len(current.Results) != 1 || len(current.Results[0].Records) != 1 || current.Results[0].Records[0].Key != "a" {
		t.Fatalf("indexed reader reuse: %+v %v", current, err)
	}
	stats, err := s.Stats()
	if err != nil || stats.ReadersActive != 0 || stats.WritersQueued != 0 {
		t.Fatalf("after-join baseline: %+v %v", stats, err)
	}
}
