package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
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
