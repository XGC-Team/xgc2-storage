//go:build linux

package faults_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func sizedObject(size int, character string) []byte {
	return []byte(`{"padding":"` + strings.Repeat(character, size-len(`{"padding":""}`)) + `"}`)
}

func capacityGroup(id string, token api.Token, members, parameterBytes int) model.GroupPrepare {
	g := model.GroupPrepare{ID: id, ParentID: "parent", InvocationID: "invocation", GroupKey: id,
		Body:            json.RawMessage(`{"policy":"all","counter":9223372036854775806}`),
		ParentGuard:     model.RecordGuard{Collection: "runs", Key: "parent", Version: token.Revision},
		InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: token.Revision},
		PinGuard:        model.RecordGuard{Collection: "definitions", Key: "pin", Version: token.Revision}}
	faultGroupCondition(&g)
	for i := 0; i < members; i++ {
		size := parameterBytes / members
		if i < parameterBytes%members {
			size++
		}
		g.Members = append(g.Members, model.GroupMember{ItemKey: fmt.Sprintf("%s-item-%d", id, i),
			ChildID: fmt.Sprintf("%s-child-%d", id, i), EventID: fmt.Sprintf("%s-event-%d", id, i),
			Parameters: sizedObject(size, "<"), Event: json.RawMessage(`{"type":"prepared"}`),
			Link: json.RawMessage(`{"parent":"parent"}`), Body: json.RawMessage(`{"phase":"prepared"}`)})
	}
	return g
}

func capacityRequest(g model.GroupPrepare, token api.Token) api.NamedRequest {
	payload, _ := json.Marshal(g)
	return api.NamedRequest{Scope: scope, DatabaseID: token.DatabaseID, Schema: token.Schema,
		Module: model.Module, Operation: model.GroupPrepareOperation, RequestID: g.ID + "-request", Payload: payload}
}

type countingCaller struct {
	inner xrpc.Caller
	calls int
}

func (c *countingCaller) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	c.calls++
	return c.inner.Call(ctx, call)
}

func noCapacityCommit(t *testing.T, d *daemon, req api.NamedRequest, before api.Token) {
	t.Helper()
	if nativeRead(t, d).Token != before {
		t.Fatal("rejected capacity request changed scope revision")
	}
	if _, err := d.client.Receipt(deadline(t), "capacity-receipt", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
		t.Fatalf("rejected capacity request published a receipt: %v", err)
	}
	if _, err := d.client.NamedResult(deadline(t), "capacity-result", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
		t.Fatalf("rejected capacity request published a named result: %v", err)
	}
	var g model.GroupPrepare
	if err := json.Unmarshal(req.Payload, &g); err != nil {
		t.Fatal(err)
	}
	readPayload, _ := json.Marshal(model.GroupRead{ID: g.ID})
	read := req
	read.Operation, read.RequestID, read.Payload = model.GroupSnapshotOperation, "capacity-missing-group", readPayload
	if _, err := d.client.Named(deadline(t), read); code(err) != "not_found" {
		t.Fatalf("rejected capacity request materialized a group: %v", err)
	}
}

func verifyCapacityGroup(t *testing.T, d *daemon, g model.GroupPrepare, req api.NamedRequest) {
	t.Helper()
	before := nativeRead(t, d).Token
	prepared, err := d.client.Named(deadline(t), req)
	wire, _ := json.Marshal(req)
	bridge, _ := protojson.Marshal(pb.NamedInput(req))
	if err != nil || prepared.Receipt == nil {
		var callError *xrpc.CallError
		disposition := ""
		if errors.As(err, &callError) {
			disposition = string(callError.Disposition)
		}
		noCapacityCommit(t, d, req, before)
		evidence(t, map[string]any{"profile": d.ref.Profile, "wire_json_bytes": len(wire), "protobuf_json_bytes": len(bridge),
			"protobuf_binary_bytes": proto.Size(pb.NamedInput(req)), "failure": fmt.Sprint(err), "disposition": disposition, "revision_unchanged": true})
		t.Fatalf("legal native group rejected: %v", err)
	}
	readPayload, _ := json.Marshal(model.GroupRead{ID: g.ID})
	read := req
	read.Operation, read.RequestID, read.Payload = model.GroupSnapshotOperation, g.ID+"-read", readPayload
	out, err := d.client.Named(deadline(t), read)
	if err != nil {
		t.Fatalf("legal native group response rejected: %v", err)
	}
	var saved model.GroupSnapshot
	if err = json.Unmarshal(out.Result, &saved); err != nil || len(saved.Members) != len(g.Members) || !equalJSON(t, saved.Body, g.Body) {
		t.Fatalf("native group snapshot incomplete: %v", err)
	}
	parameters := 0
	for i, want := range g.Members {
		got := saved.Members[i]
		parameters += len(want.Parameters)
		if got.ItemKey != want.ItemKey || got.ChildID != want.ChildID || got.EventID != want.EventID || !bytes.Equal(got.Parameters, want.Parameters) ||
			!equalJSON(t, got.Event, want.Event) || !equalJSON(t, got.Link, want.Link) || !equalJSON(t, got.Body, want.Body) {
			t.Fatalf("native member %d lost data or exact parameter bytes", i)
		}
	}
	retained, err := d.client.NamedResult(deadline(t), "capacity-resolve", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(retained, prepared) || nativeRead(t, d).Token != prepared.Receipt.Token {
		t.Fatalf("native retained outcome or readonly revision mismatch: %v", err)
	}
	evidence(t, map[string]any{"profile": d.ref.Profile, "parameter_bytes": parameters, "members": len(g.Members),
		"wire_json_bytes": len(wire), "protobuf_json_bytes": len(bridge), "protobuf_binary_bytes": proto.Size(pb.NamedInput(req)),
		"complete_native_request_snapshot_and_retained_result": true, "byte_exact_html_parameters": true,
		"process_after_work": processResources(t, d.cmd.Process.Pid)})
}

func TestFaultNativeNamedCapacityLayers(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			d := startProfileManifest(t, privateDir(t), true, "", coreConfig("", false).Manifest, profile)
			initial := nativeRead(t, d)
			guard, err := d.client.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: initial.Token, RequestID: "capacity-guards",
				Mutations: []api.Mutation{
					{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: faultPreparationBody(false)},
					{Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: faultPreparationBody(true)},
					{Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"immutable":true}`)}}})
			if err != nil {
				t.Fatal(err)
			}
			counter := &countingCaller{inner: d.caller}
			d.client, err = client.New(counter, d.ref)
			if err != nil {
				t.Fatal(err)
			}
			t.Run("small-control", func(t *testing.T) {
				g := capacityGroup("small", guard.Token, 1, 128)
				verifyCapacityGroup(t, d, g, capacityRequest(g, guard.Token))
			})
			t.Run("exact-eight-mib", func(t *testing.T) {
				g := capacityGroup("exact", guard.Token, 1000, model.MaxGroupParameterBytes)
				verifyCapacityGroup(t, d, g, capacityRequest(g, guard.Token))
			})
			t.Run("parameter-overflow", func(t *testing.T) {
				g := capacityGroup("overflow", guard.Token, 1000, model.MaxGroupParameterBytes+1)
				req := capacityRequest(g, guard.Token)
				before := nativeRead(t, d).Token
				_, err := d.client.Named(deadline(t), req)
				if code(err) != "resource_exhausted" || !strings.Contains(fmt.Sprint(err), "parameters exceed 8 MiB") {
					t.Fatalf("overflow rejected at unexpected layer: %v", err)
				}
				noCapacityCommit(t, d, req, before)
				evidence(t, map[string]any{"profile": profile, "layer": "module parameter admission", "parameter_bytes": model.MaxGroupParameterBytes + 1, "failure": fmt.Sprint(err), "business_unchanged": true})
			})
			t.Run("client-envelope-overflow", func(t *testing.T) {
				g := capacityGroup("client-overflow", guard.Token, 1000, model.MaxGroupParameterBytes)
				g.Body = sizedObject(6<<20, "x")
				req := capacityRequest(g, guard.Token)
				before := nativeRead(t, d).Token
				calls := counter.calls
				_, err := d.client.Named(deadline(t), req)
				if code(err) != "resource_exhausted" || counter.calls != calls {
					t.Fatalf("oversized application envelope reached transport: %v", err)
				}
				noCapacityCommit(t, d, req, before)
				evidence(t, map[string]any{"profile": profile, "layer": "storage client before caller", "failure": fmt.Sprint(err), "transport_not_called": true})
			})
			t.Run("native-host-overflow", func(t *testing.T) {
				g := capacityGroup("host-overflow", guard.Token, 1000, model.MaxGroupParameterBytes)
				g.Body = sizedObject(6<<20, "x")
				req := capacityRequest(g, guard.Token)
				before := nativeRead(t, d).Token
				var failure string
				if profile == xrpc.HTTP {
					transport, err := httpx.New(httpx.Config{LocalTargetID: d.ref.TargetID, Service: d.ref, MaxRequestBytes: 32 << 20, MaxResponseBytes: 16 << 20, MaxConnections: 1})
					if err != nil {
						t.Fatal(err)
					}
					defer transport.Close()
					raw, _ := json.Marshal(req)
					body, statusCode, _, err := transport.DoWithHeaders(deadline(t), "POST", "/v1/named", req.RequestID, "application/json", raw, map[string]string{"Authorization": "Bearer " + grant})
					if err != nil || statusCode != 413 || !strings.Contains(string(body), "request body exceeds host limit") {
						t.Fatalf("HTTP native host did not reject before data dispatch: status=%d body=%s err=%v", statusCode, body, err)
					}
					failure = string(body)
				} else {
					connection, err := grpcx.Dial(d.ref, grpcx.DialOptions{LocalTargetID: d.ref.TargetID, MaxRequestBytes: 32 << 20, MaxResponseBytes: 16 << 20, Metadata: metadata.Pairs("authorization", "Bearer "+grant)})
					if err != nil {
						t.Fatal(err)
					}
					defer connection.Close()
					ctx := metadata.AppendToOutgoingContext(deadline(t), "x-request-id", req.RequestID)
					_, err = pb.NewStorageClient(connection).Named(ctx, pb.NamedInput(req))
					if status.Code(err) != codes.ResourceExhausted || !strings.Contains(fmt.Sprint(err), "larger than max") {
						t.Fatalf("gRPC native host did not reject oversized binary message: %v", err)
					}
					failure = fmt.Sprint(err)
				}
				noCapacityCommit(t, d, req, before)
				evidence(t, map[string]any{"profile": profile, "layer": "native host before data dispatch", "failure": failure, "business_unchanged": true})
			})
			t.Run("legal-envelope-bridge-expansion", func(t *testing.T) {
				g := capacityGroup("bridge", guard.Token, 1000, model.MaxGroupParameterBytes)
				g.Body = sizedObject(2<<20, "x")
				req := capacityRequest(g, guard.Token)
				raw, _ := json.Marshal(req)
				if len(raw) >= model.MaxRequestBytes {
					t.Fatal("bridge fixture exceeds legal application envelope")
				}
				verifyCapacityGroup(t, d, g, req)
			})
		})
	}
}
