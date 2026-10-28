package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/protobuf/encoding/protojson"
)

// This is an invalid external wire reply, not a persistence substitute. The
// positive HTTP/gRPC cases use the real Storage owner in server/native_test.
type snapshotWireReply struct{ payload []byte }

func (reply snapshotWireReply) Call(context.Context, xrpc.Call) (xrpc.Result, error) {
	return xrpc.Result{Payload: reply.payload}, nil
}

func TestSnapshotHeaderCannotSilentlyFallBackToFullWireData(t *testing.T) {
	response := api.SnapshotResponse{Results: []api.QueryResult{{Collection: "items", Records: []api.Record{{Key: "selected", Version: "1", Data: json.RawMessage(`{"value":"full body"}`)}}}}}
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			var raw []byte
			var err error
			if profile == xrpc.GRPC {
				raw, err = protojson.Marshal(pb.SnapshotOutput(response))
			} else {
				raw, err = json.Marshal(response)
			}
			if err != nil {
				t.Fatal(err)
			}
			ref := xrpc.ServiceRef{TargetID: "fixture", Service: api.Service, APIVersion: api.Version, InstanceID: "fixture-instance", Profile: profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/tmp/storage-projection-fixture.sock"}}
			client, err := New(snapshotWireReply{payload: raw}, ref)
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.Snapshot(t.Context(), "read-headers", api.SnapshotRequest{Scope: api.Scope{Namespace: "test", User: "operator", Workspace: "station"},
				Queries: []api.Query{{Collection: "items", HeaderIndex: "retention", Keys: []string{"selected"}}}})
			var call *xrpc.CallError
			if !errors.As(err, &call) || call.Code != "data_loss" || call.Disposition != xrpc.ResponseReceived || len(result.Results) != 0 {
				t.Fatalf("unsupported projection became a full-data success or unknown outcome: %v", err)
			}
		})
	}
}
