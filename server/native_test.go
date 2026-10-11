package server_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/engine"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestNativeProfilesPrecisionAuthAndReceipt(t *testing.T) {
	for _, profile := range []string{xrpc.HTTP, xrpc.GRPC} {
		t.Run(profile, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			dir, e := os.MkdirTemp("", "storage-native-")
			if e != nil {
				t.Fatal(e)
			}
			defer os.RemoveAll(dir)
			m := api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "docs", Owner: "tests", Schema: "docs.v1", MaxScopes: 2, MaxReceipts: 16, ReceiptTTLSeconds: 3600, Collections: []api.Collection{{ID: "documents", MaxRecordBytes: 3 << 20, MaxRecords: 100, MaxBytes: 8 << 20, Retention: "tests", Recovery: "backup"}}}}}
			store, e := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "fixture.db"), Create: true, Manifest: m})
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			token := "0123456789abcdef0123456789abcdef"
			grants := []server.Grant{{Token: token, Namespace: "docs", User: "user", Workspace: "workspace"}}
			socket := filepath.Join(dir, "rpc.sock")
			lease, e := unixlease.Reserve(ctx, socket, unixlease.Options{})
			if e != nil {
				t.Fatal(e)
			}
			listener, e := lease.Listen()
			if e != nil {
				t.Fatal(e)
			}
			ref := xrpc.ServiceRef{TargetID: "fixture", Service: api.Service, APIVersion: api.Version, InstanceID: "native-instance", Profile: profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
			var caller xrpc.Caller
			if profile == xrpc.HTTP {
				handler, e := server.HTTP(store, grants)
				if e != nil {
					t.Fatal(e)
				}
				host, e := httpx.Serve(listener, lease, handler, httpx.HostOptions{InstanceID: ref.InstanceID, MaxBodyBytes: api.MaxRequestBytes, MaxResponseBytes: api.MaxResponseBytes})
				if e != nil {
					t.Fatal(e)
				}
				defer func() {
					shut, c := context.WithTimeout(context.Background(), time.Second)
					defer c()
					host.Shutdown(shut)
				}()
				transport, e := httpx.New(httpx.Config{LocalTargetID: "fixture", Service: ref, MaxRequestBytes: api.MaxRequestBytes, MaxResponseBytes: api.MaxResponseBytes})
				if e != nil {
					t.Fatal(e)
				}
				defer transport.Close()
				caller = client.HTTPCaller{Transport: transport, Grant: token}
			} else {
				host, e := grpcx.ServeWithOptions(listener, lease, func(reg grpc.ServiceRegistrar) {
					pb.RegisterStorageServer(reg, &server.GRPC{Store: store, Grants: grants})
				}, grpcx.HostOptions{InstanceID: ref.InstanceID, MaxCallTime: 30 * time.Second, MaxInFlight: 32, MaxRequestBytes: api.MaxRequestBytes, MaxResponseBytes: api.MaxResponseBytes})
				if e != nil {
					t.Fatal(e)
				}
				defer host.Stop()
				transport := grpcx.NewProfile(grpcx.DialOptions{LocalTargetID: "fixture", MaxRequestBytes: api.MaxRequestBytes, MaxResponseBytes: api.MaxResponseBytes, Metadata: metadata.Pairs("authorization", "Bearer "+token)}, protoregistry.GlobalFiles)
				defer transport.Close()
				caller = transport
			}
			c, e := client.New(caller, ref)
			if e != nil {
				t.Fatal(e)
			}
			if profile == xrpc.HTTP {
				wrong := ref
				wrong.TargetID = "other-target"
				bad, e := client.New(caller, wrong)
				if e != nil {
					t.Fatal(e)
				}
				_, e = bad.Snapshot(ctx, "wrong-ref", api.SnapshotRequest{Scope: api.Scope{Namespace: "docs", User: "user", Workspace: "workspace"}, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}})
				if xrpc.Code(e) != "invalid_argument" {
					t.Fatalf("bound reference was ignored: %v", e)
				}
			}
			scope := api.Scope{Namespace: "docs", User: "user", Workspace: "workspace"}
			read, e := c.Snapshot(ctx, "read", api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout", "active"}}}})
			if e != nil {
				t.Fatal(e)
			}
			req := api.BatchRequest{Scope: scope, Expected: read.Token, RequestID: "save", Mutations: []api.Mutation{{Collection: "documents", Key: "layout", ExpectedVersion: "0", Data: json.RawMessage(`{"counter":9223372036854775806,"name":"Layout"}`)}, {Collection: "documents", Key: "active", ExpectedVersion: "0", Data: json.RawMessage(`{"id":"layout"}`)}}}
			receipt, e := c.Batch(ctx, req)
			if e != nil {
				t.Fatal(e)
			}
			if receipt.Token.Revision != "1" || receipt.Durability != "sqlite-full" {
				t.Fatalf("receipt %+v", receipt)
			}
			again, e := c.Receipt(ctx, "read-receipt", api.ReceiptRequest{Scope: scope, RequestID: "save"})
			if e != nil || again.Digest != receipt.Digest {
				t.Fatalf("receipt %v %v", again, e)
			}
			read, e = c.Snapshot(ctx, "read-saved", api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout", "active"}}}})
			if e != nil {
				t.Fatal(e)
			}
			if string(read.Results[0].Records[0].Data) != `{"counter":9223372036854775806,"name":"Layout"}` {
				t.Fatalf("precision lost: %s", read.Results[0].Records[0].Data)
			}
			unauthorized := scope
			unauthorized.User = "other"
			if _, e = c.Snapshot(ctx, "forbidden", api.SnapshotRequest{Scope: unauthorized, Queries: []api.Query{{Collection: "documents", Limit: 1}}}); e == nil {
				t.Fatal("unauthorized scope succeeded")
			}
			if _, e = c.Batch(ctx, req); e != nil {
				t.Fatalf("idempotent replay %v", e)
			}
			large, _ := json.Marshal(map[string]string{"large": strings.Repeat("x", (3<<20)-64)})
			if _, e = c.Batch(ctx, api.BatchRequest{Scope: scope, Expected: read.Token, RequestID: "large", Mutations: []api.Mutation{{Collection: "documents", Key: "layout", ExpectedVersion: "1", Data: large}}}); e != nil {
				t.Fatalf("legal large save: %v", e)
			}
			largeRead, e := c.Snapshot(ctx, "large-read", api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}})
			if e != nil {
				t.Fatalf("legal large response: %v", e)
			}
			if string(largeRead.Results[0].Records[0].Data) != string(large) {
				t.Fatal("large document changed")
			}
		})
	}
}
