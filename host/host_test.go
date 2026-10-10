package host

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

func TestDescriptionReturnsActualHTTPReference(t *testing.T) {
	ref := xrpc.ServiceRef{TargetID: "local", Service: api.Service, APIVersion: api.Version, InstanceID: "actual-boot", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: "/run/xgc2/storage.sock"}}
	called := false
	handler := withDescription(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }), ref)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/describe", nil))
	var result struct {
		ServiceRef xrpc.ServiceRef `json:"service_ref"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != http.StatusOK || result.ServiceRef != ref || called {
		t.Fatalf("description changed boot reference: status=%d result=%+v err=%v called=%v", w.Code, result, err, called)
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/describe", nil))
	if w.Code != http.StatusMethodNotAllowed || called {
		t.Fatal("description accepted a mutation")
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/snapshot", nil))
	if !called {
		t.Fatal("storage operation was not forwarded")
	}
}

func testManifest() api.Manifest {
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "docs", Owner: "tests", Schema: "docs.v1",
		MaxScopes: 2, MaxReceipts: 16, ReceiptTTLSeconds: 3600,
		Collections: []api.Collection{{ID: "documents", MaxRecordBytes: 4096, MaxRecords: 100, MaxBytes: 1 << 20, Retention: "tests", Recovery: "backup"}}}}}
}

func TestEmbeddedOpenNeedsNoListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "storage-host-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	scope := api.Scope{Namespace: "docs", User: "user", Workspace: "workspace"}
	owner, err := Open(ctx, Config{Path: filepath.Join(dir, "embedded.db"), Create: true, Manifest: testManifest()})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	local, err := owner.Client(scope)
	if err != nil {
		t.Fatal(err)
	}
	read, err := local.Snapshot(ctx, "read", api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}})
	if err != nil || read.Token.DatabaseID != owner.DatabaseID() {
		t.Fatalf("embedded owner identity: %+v %v", read, err)
	}
	if _, err = owner.Client(api.Scope{Namespace: "undeclared", User: "user", Workspace: "workspace"}); err == nil {
		t.Fatal("embedded owner granted an undeclared namespace")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sock" || entry.Name() == "bootstrap.json" || entry.Name() == "refs.json" {
			t.Fatalf("embedded mode created %s", entry.Name())
		}
	}
}

func TestLocalAndExternalClientsShareOwnerReceiptAndIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "storage-host-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	token := "0123456789abcdef0123456789abcdef"
	scope := api.Scope{Namespace: "docs", User: "user", Workspace: "workspace"}
	config := Config{Path: filepath.Join(dir, "fixture.db"), Create: true, Manifest: testManifest()}
	serve := ServeConfig{TargetID: "fixture", HTTPSocket: filepath.Join(dir, "rpc.sock"),
		Grants: []server.Grant{{Token: token, Namespace: scope.Namespace, User: scope.User, Workspace: scope.Workspace}}}
	owner, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	exposure, err := owner.Serve(ctx, serve)
	if err != nil {
		t.Fatal(err)
	}
	local, err := owner.Client(scope)
	if err != nil {
		t.Fatal(err)
	}
	read, err := local.Snapshot(context.Background(), "read-local", api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}})
	if err != nil || read.Token.DatabaseID != owner.DatabaseID() {
		t.Fatalf("local owner identity: %+v %v", read, err)
	}
	request := api.BatchRequest{Scope: scope, Expected: read.Token, RequestID: "save-local",
		Mutations: []api.Mutation{{Collection: "documents", Key: "layout", ExpectedVersion: "0", Data: json.RawMessage(`{"counter":9223372036854775806}`)}}}
	saved, err := local.Batch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ref := exposure.References()[0]
	transport, err := httpx.New(httpx.Config{LocalTargetID: "fixture", Service: ref,
		MaxRequestBytes: api.MaxRequestBytes, MaxResponseBytes: api.MaxResponseBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	remote, err := client.New(client.HTTPCaller{Transport: transport, Grant: token}, ref)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := remote.Batch(ctx, request)
	if err != nil || replayed.Digest != saved.Digest || replayed.Token != saved.Token {
		t.Fatalf("one durable receipt: %+v %v", replayed, err)
	}
	read, err = remote.Snapshot(ctx, "read-remote", api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}})
	if err != nil || read.Token != saved.Token || string(read.Results[0].Records[0].Data) != string(request.Mutations[0].Data) {
		t.Fatalf("one typed/wire state: %+v %v", read, err)
	}
	forbidden := scope
	forbidden.User = "other"
	if _, err = local.Snapshot(ctx, "forbidden", api.SnapshotRequest{Scope: forbidden}); err == nil {
		t.Fatal("local data port crossed its fixed scope")
	}
	if _, ok := any(remote).(client.ReadSnapshotClient); ok {
		t.Fatal("remote client advertised a local transaction")
	}
	var borrowed context.Context
	err = local.WithReadSnapshot(ctx, func(view context.Context) error {
		borrowed = view
		query := api.SnapshotRequest{Scope: scope, At: &saved.Token, Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}}
		if _, err := local.Snapshot(view, "view-before", query); err != nil {
			return err
		}
		advance := api.BatchRequest{Scope: scope, Expected: saved.Token, RequestID: "advance-outside-view",
			Mutations: []api.Mutation{{Collection: "documents", Key: "layout", ExpectedVersion: "1", Data: json.RawMessage(`{"counter":2}`)}}}
		if _, err := local.Batch(view, advance); xrpc.Code(err) != "failed_precondition" {
			t.Fatalf("read view admitted a mutation: %v", err)
		}
		if _, err := remote.Batch(ctx, advance); err != nil {
			return err
		}
		// The view holds no reader, so the write above is visible to the next
		// read; the view must fail instead of mixing revisions.
		if after, err := local.Snapshot(view, "view-after", query); xrpc.Code(err) != "conflict" {
			t.Fatalf("local read view mixed revisions: %+v %v", after, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = local.Snapshot(borrowed, "expired-view", api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}}); err == nil {
		t.Fatal("read view remained usable after its callback")
	}
	if err = owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	config.Create = false
	restarted, err := Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close(ctx)
	reexposure, err := restarted.Serve(ctx, serve)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.DatabaseID() != owner.DatabaseID() || reexposure.References()[0].InstanceID == ref.InstanceID {
		t.Fatal("restart changed database identity or reused process incarnation")
	}
	if _, err = remote.Snapshot(ctx, "old-incarnation", api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "documents", Keys: []string{"layout"}}}}); err == nil {
		t.Fatal("old XRPC instance remained usable after owner restart")
	}
}
