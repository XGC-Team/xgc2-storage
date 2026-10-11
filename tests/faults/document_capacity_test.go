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
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func capacityDocument(operation string, padding int) json.RawMessage {
	raw, _ := json.Marshal(map[string]string{"operation": operation, "padding": strings.Repeat("x", padding)})
	return raw
}

func TestFaultNativeDocumentCeiling(t *testing.T) {
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
