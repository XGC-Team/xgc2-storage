//go:build linux

package faults_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
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

func capacityDocument(operation string, padding int) json.RawMessage {
	raw, _ := json.Marshal(map[string]string{"operation": operation, "padding": strings.Repeat("x", padding)})
	return raw
}

func TestFaultNativeDocumentCeilingWithoutNamedHost(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			d := startProfileManifest(t, privateDir(t), true, "", manifest(), profile)
			initial := nativeRead(t, d)
			const operation = "ordinary-document-ceiling"
			empty := capacityDocument(operation, 0)
			data := capacityDocument(operation, (3<<20)-len(empty))
			req := api.BatchRequest{Scope: scope, Expected: initial.Token, RequestID: operation,
				Mutations: []api.Mutation{{Collection: "state", Key: "large", ExpectedVersion: "0", Data: data}}}
			wire, _ := json.Marshal(req)
			bridge, _ := protojson.Marshal(pb.BatchInput(req))
			if len(data) != 3<<20 || len(wire) >= api.MaxRequestBytes || proto.Size(pb.BatchInput(req)) >= api.MaxRequestBytes {
				t.Fatal("ordinary document fixture exceeds registered record/application/native bounds")
			}
			commit, err := d.client.Batch(deadline(t), req)
			if err != nil {
				var failure *xrpc.CallError
				disposition := ""
				if errors.As(err, &failure) {
					disposition = string(failure.Disposition)
				}
				noDocumentCapacityCommit(t, d, req, initial.Token)
				evidence(t, map[string]any{"profile": profile, "wire_limit_bytes": api.MaxRequestBytes,
					"record_bytes": len(data), "application_bytes": len(wire), "protobuf_binary_bytes": proto.Size(pb.BatchInput(req)),
					"protobuf_json_bytes": len(bridge), "failure": fmt.Sprint(err), "disposition": disposition, "business_unchanged": true})
				t.Fatalf("legal document within unexpanded 4 MiB native budget rejected: %v", err)
			}
			saved, err := d.client.Snapshot(deadline(t), "ordinary-document-read", api.SnapshotRequest{Scope: scope,
				Queries: []api.Query{{Collection: "state", Keys: []string{"large"}}}})
			if err != nil || saved.Token != commit.Token || len(saved.Results) != 1 || len(saved.Results[0].Records) != 1 ||
				!bytes.Equal(saved.Results[0].Records[0].Data, data) {
				t.Fatalf("ordinary native document response lost exact bytes: %v", err)
			}
			evidence(t, map[string]any{"profile": profile, "wire_limit_bytes": api.MaxRequestBytes,
				"record_bytes": len(data), "application_bytes": len(wire), "protobuf_binary_bytes": proto.Size(pb.BatchInput(req)),
				"protobuf_json_bytes": len(bridge), "full_native_request_and_snapshot_bytes_verified": true})
		})
	}
}

func noDocumentCapacityCommit(t *testing.T, d *daemon, req api.BatchRequest, before api.Token) {
	t.Helper()
	keys := make([]string, len(req.Mutations))
	for i, mutation := range req.Mutations {
		keys[i] = mutation.Key
	}
	out, err := d.client.Snapshot(deadline(t), "document-overflow-read", api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "state", Keys: keys}}})
	if err != nil || out.Token != before || len(out.Results) != 1 || len(out.Results[0].Records) != len(keys) {
		t.Fatalf("rejected document request changed token/read shape: %+v %v", out.Token, err)
	}
	for _, record := range out.Results[0].Records {
		if !record.Missing {
			t.Fatal("rejected document request published a record")
		}
	}
	if _, err = d.client.Receipt(deadline(t), "document-overflow-receipt", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
		t.Fatalf("rejected document request published a receipt: %v", err)
	}
}

func TestFaultNativeDocumentBudgetUnderNamedHost(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			d := startProfileManifest(t, privateDir(t), true, "", coreConfig("", false).Manifest, profile)
			counter := &countingCaller{inner: d.caller}
			var err error
			d.client, err = client.New(counter, d.ref)
			if err != nil {
				t.Fatal(err)
			}
			initial := nativeRead(t, d).Token
			positive := api.BatchRequest{Scope: scope, Expected: initial, RequestID: "document-large-positive",
				Mutations: []api.Mutation{{Collection: "state", Key: "large", ExpectedVersion: "0",
					Data: capacityDocument("document-large-positive", (3<<20)-256)}}}
			commit, err := d.client.Batch(deadline(t), positive)
			if err != nil {
				t.Fatalf("legal document below existing record and operation caps rejected: %v", err)
			}
			saved, err := d.client.Snapshot(deadline(t), "document-large-read", api.SnapshotRequest{Scope: scope,
				Queries: []api.Query{{Collection: "state", Keys: []string{"large"}}}})
			if err != nil || saved.Token != commit.Token || len(saved.Results) != 1 || len(saved.Results[0].Records) != 1 ||
				!bytes.Equal(saved.Results[0].Records[0].Data, positive.Mutations[0].Data) {
				t.Fatalf("legal large document lost exact bytes on native snapshot: %v", err)
			}
			// Each record is legal in the 3 MiB fixture collection. Only the
			// aggregate Batch budget is exceeded, while the named host admits 16 MiB.
			overflow := api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "document-operation-overflow",
				Mutations: []api.Mutation{
					{Collection: "state", Key: "overflow-a", ExpectedVersion: "0", Data: capacityDocument("overflow-a", (2<<20)+1024)},
					{Collection: "state", Key: "overflow-b", ExpectedVersion: "0", Data: capacityDocument("overflow-b", (2<<20)+1024)}}}
			raw, _ := json.Marshal(overflow)
			if len(raw) <= api.MaxRequestBytes || len(raw) >= 16<<20 || proto.Size(pb.BatchInput(overflow)) >= 16<<20 {
				t.Fatal("document overflow fixture does not isolate per-operation admission")
			}
			calls := counter.calls
			_, err = d.client.Batch(deadline(t), overflow)
			if code(err) != "resource_exhausted" || counter.calls != calls {
				t.Fatalf("4 MiB document client admission changed under named host: %v", err)
			}
			noDocumentCapacityCommit(t, d, overflow, commit.Token)
			var failure string
			if profile == xrpc.HTTP {
				transport, err := httpx.New(httpx.Config{LocalTargetID: d.ref.TargetID, Service: d.ref,
					MaxRequestBytes: 16 << 20, MaxResponseBytes: 4 << 20, MaxConnections: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer transport.Close()
				body, statusCode, _, err := transport.DoWithHeaders(deadline(t), "POST", "/v1/batch", overflow.RequestID,
					"application/json", raw, map[string]string{"Authorization": "Bearer " + grant})
				if err != nil || statusCode != 429 || !strings.Contains(string(body), "operation request byte limit exceeded") {
					t.Fatalf("HTTP document operation cap lost under named host: status=%d body=%s err=%v", statusCode, body, err)
				}
				failure = string(body)
			} else {
				connection, err := grpcx.Dial(d.ref, grpcx.DialOptions{LocalTargetID: d.ref.TargetID,
					MaxRequestBytes: 16 << 20, MaxResponseBytes: 4 << 20, Metadata: metadata.Pairs("authorization", "Bearer "+grant)})
				if err != nil {
					t.Fatal(err)
				}
				defer connection.Close()
				ctx := metadata.AppendToOutgoingContext(deadline(t), "x-request-id", overflow.RequestID)
				_, err = pb.NewStorageClient(connection).Batch(ctx, pb.BatchInput(overflow))
				if status.Code(err) != codes.ResourceExhausted || !strings.Contains(fmt.Sprint(err), "batch raw payload byte limit exceeded") {
					t.Fatalf("gRPC document operation cap lost under named host: %v", err)
				}
				failure = fmt.Sprint(err)
			}
			noDocumentCapacityCommit(t, d, overflow, commit.Token)
			small := api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "document-small-recovery",
				Mutations: []api.Mutation{{Collection: "state", Key: "small", ExpectedVersion: "0", Data: capacityDocument("small-recovery", 32)}}}
			recovered, err := d.client.Batch(deadline(t), small)
			if err != nil || recovered.Token.Revision != "2" {
				t.Fatalf("document overflow left native owner unable to commit small control: %v", err)
			}
			evidence(t, map[string]any{"profile": profile, "named_host_limit_bytes": 16 << 20,
				"document_operation_limit_bytes": api.MaxRequestBytes, "positive_exact_document_bytes": len(positive.Mutations[0].Data),
				"overflow_application_bytes": len(raw), "overflow_protobuf_binary_bytes": proto.Size(pb.BatchInput(overflow)),
				"client_transport_not_called": true, "native_operation_failure": failure,
				"negative_revision_receipt_and_records_unchanged": true, "small_control_recovered": true})
		})
	}
}
