// Package host owns one storage instance and its optional external XRPC ports.
// An embedding process borrows typed data clients, never the SQL/database owner.
package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	"github.com/XGC-Team/xgc2-storage/registry"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
)

type Config struct {
	Path                 string
	Create               bool
	Manifest             api.Manifest
	ConfigurationDomains []model.ConfigurationDomainDeclaration
	Grants               []server.Grant
	HTTPSocket           string
	GRPCSocket           string
	TargetID             string
	Policy               *xrpc.Policy
	MaxDBBytes           int64
}

type Host struct {
	store           *engine.Store
	grants          []server.Grant
	refs            []xrpc.ServiceRef
	databaseID      string
	callBudget      time.Duration
	http            *httpx.Host
	grpc            *grpcx.Host
	diagnostics     *xrpc.Diagnostics
	maintenanceStop context.CancelFunc
	maintenanceDone chan struct{}
	done            chan struct{}
	stop            chan struct{}
	doneOnce        sync.Once
	closeOnce       sync.Once
	closeDone       chan struct{}
	closeErr        error
	closed          atomic.Bool
}

// ResolvePolicy retains the standalone owner's finite transport limits.
// Both the CLI and embedded owner use this same deployment policy.
func ResolvePolicy(environment []string, manifest api.Manifest) (*xrpc.Policy, error) {
	requestBytes, responseBytes := registry.TransportBounds(manifest)
	return xrpc.ResolvePolicy(xrpc.PolicyOptions{
		Environment: environment, DefaultSource: "storage deployment manifest",
		Defaults: map[string]string{"MAX_REQUEST_BYTES": strconv.Itoa(requestBytes), "MAX_RESPONSE_BYTES": strconv.Itoa(responseBytes),
			"HOST_MAX_CONNECTIONS": "4", "HOST_MAX_IN_FLIGHT": "8", "GRPC_MAX_STREAMS_PER_CONNECTION": "1"},
		Ceilings: map[string]int64{"MAX_REQUEST_BYTES": int64(requestBytes), "MAX_RESPONSE_BYTES": int64(responseBytes),
			"HOST_MAX_CONNECTIONS": 4, "HOST_MAX_IN_FLIGHT": 8, "GRPC_MAX_STREAMS_PER_CONNECTION": 1, "CALL_TIMEOUT_MS": 30000},
		Capabilities: []string{"host", "http", "rpc", "transport", "grpc", "diagnostics"},
	})
}

// Open completes owner admission and starts the declared external endpoints.
// ctx bounds startup only. Close owns shutdown, so signal cancellation cannot
// destroy storage before the embedding application's durable Stop finishes.
func Open(ctx context.Context, config Config) (_ *Host, resultErr error) {
	if config.Path == "" || config.TargetID == "" || config.Policy == nil || (config.HTTPSocket == "" && config.GRPCSocket == "") {
		return nil, errors.New("storage: path, target, policy and an external endpoint required")
	}
	if err := server.ValidateGrants(config.Grants); err != nil {
		return nil, err
	}
	if len(config.ConfigurationDomains) > 256 {
		return nil, errors.New("storage: configuration declarations exceed 256 domains")
	}
	budget, err := config.Policy.Integer("CALL_TIMEOUT_MS")
	if err != nil || budget <= 0 {
		return nil, errors.New("storage: finite call policy required")
	}
	h := &Host{grants: append([]server.Grant(nil), config.Grants...), callBudget: time.Duration(budget) * time.Millisecond,
		done: make(chan struct{}), stop: make(chan struct{}), closeDone: make(chan struct{})}
	defer func() {
		if resultErr != nil {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, h.Close(shutdown))
		}
	}()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	instance := hex.EncodeToString(random[:])
	var httpRef xrpc.ServiceRef
	for _, endpoint := range []struct{ address, profile string }{{config.HTTPSocket, xrpc.HTTP}, {config.GRPCSocket, xrpc.GRPC}} {
		if endpoint.address == "" {
			continue
		}
		ref := xrpc.ServiceRef{TargetID: config.TargetID, Service: api.Service, APIVersion: api.Version, InstanceID: instance,
			Profile: endpoint.profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: endpoint.address}}
		if err := ref.ValidateInternal(); err != nil {
			return nil, err
		}
		h.refs = append(h.refs, ref)
		if endpoint.profile == xrpc.HTTP {
			httpRef = ref
		}
	}
	modules := registry.Compiled()
	if len(config.ConfigurationDomains) != 0 {
		modules = registry.ConfigurationDeployment(config.ConfigurationDomains)
	}
	h.store, err = engine.Open(startup, engine.Config{Path: config.Path, Create: config.Create, Manifest: config.Manifest,
		Modules: modules, MaxDBBytes: config.MaxDBBytes})
	if err != nil {
		return nil, err
	}
	stats, err := h.store.Stats()
	if err != nil {
		return nil, err
	}
	h.databaseID = stats.DatabaseID
	h.diagnostics, err = xrpc.NewDiagnostics(config.Policy, xrpc.DiagnosticOptions{Sink: os.Stderr, MaxQueuedRecords: 128, MaxRecordBytes: 4096})
	if err != nil {
		return nil, err
	}
	httpOptions, err := (httpx.HostOptions{InstanceID: instance, Service: api.Service, DiscoveryPaths: []string{"/v1/describe"}, Diagnostics: h.diagnostics}).WithPolicy(config.Policy)
	if err != nil {
		return nil, err
	}
	grpcOptions, err := (grpcx.HostOptions{InstanceID: instance, Service: api.Service, Diagnostics: h.diagnostics}).WithPolicy(config.Policy)
	if err != nil {
		return nil, err
	}
	if config.HTTPSocket != "" {
		lease, err := unixlease.Reserve(startup, config.HTTPSocket, unixlease.Options{ExistingPath: unixlease.ReclaimUnreachable})
		if err != nil {
			return nil, err
		}
		listener, err := lease.Listen()
		if err != nil {
			lease.Close()
			return nil, err
		}
		handler, err := server.HTTP(h.store, h.grants)
		if err != nil {
			lease.Close()
			return nil, err
		}
		h.http, err = httpx.Serve(listener, lease, withDescription(handler, httpRef), httpOptions)
		if err != nil {
			lease.Close()
			return nil, err
		}
	}
	if config.GRPCSocket != "" {
		lease, err := unixlease.Reserve(startup, config.GRPCSocket, unixlease.Options{ExistingPath: unixlease.ReclaimUnreachable})
		if err != nil {
			return nil, err
		}
		listener, err := lease.Listen()
		if err != nil {
			lease.Close()
			return nil, err
		}
		h.grpc, err = grpcx.ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
			pb.RegisterStorageServer(r, &server.GRPC{Store: h.store, Grants: h.grants})
		}, grpcOptions)
		if err != nil {
			lease.Close()
			return nil, err
		}
	}
	maintenanceCtx, maintenanceStop := context.WithCancel(context.Background())
	h.maintenanceStop = maintenanceStop
	h.maintenanceDone = make(chan struct{})
	go h.maintain(maintenanceCtx, instance)
	var httpDone, grpcDone <-chan struct{}
	if h.http != nil {
		httpDone = h.http.Done()
	}
	if h.grpc != nil {
		grpcDone = h.grpc.Done()
	}
	go func() {
		select {
		case <-httpDone:
		case <-grpcDone:
		case <-h.stop:
		}
		h.doneOnce.Do(func() { close(h.done) })
	}()
	return h, nil
}

func (h *Host) Client(scope api.Scope) (client.Interface, error) {
	if h == nil || h.closed.Load() {
		return nil, errors.New("storage: owner is closed")
	}
	for _, grant := range h.grants {
		if grant.Namespace == scope.Namespace && (grant.User == scope.User || grant.User == "*") && (grant.Workspace == scope.Workspace || grant.Workspace == "*") {
			return client.NewLocal(h.store, scope, h.callBudget)
		}
	}
	return nil, errors.New("storage: local scope not granted by deployment")
}

func (h *Host) DatabaseID() string            { return h.databaseID }
func (h *Host) References() []xrpc.ServiceRef { return append([]xrpc.ServiceRef(nil), h.refs...) }
func (h *Host) Done() <-chan struct{}         { return h.done }

func (h *Host) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		close(h.stop)
		h.doneOnce.Do(func() { close(h.done) })
		go h.closeResources()
	})
	select {
	case <-h.closeDone:
		return h.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Caller cancellation bounds waiting, not ownership. The owner keeps its DB
// lease until admitted calls quiesce, including after a shutdown timeout.
func (h *Host) closeResources() {
	defer close(h.closeDone)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if h.http != nil {
		h.closeErr = errors.Join(h.closeErr, h.http.Shutdown(shutdown))
	}
	if h.grpc != nil {
		h.closeErr = errors.Join(h.closeErr, h.grpc.Shutdown(shutdown))
		h.grpc.Stop()
	}
	cancel()
	if h.maintenanceStop != nil {
		h.maintenanceStop()
		<-h.maintenanceDone
	}
	if h.store != nil {
		h.closeErr = errors.Join(h.closeErr, h.store.Close())
	}
	if h.diagnostics != nil {
		shutdown, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		h.closeErr = errors.Join(h.closeErr, h.diagnostics.Close(shutdown))
		cancel()
	}
}

func (h *Host) maintain(ctx context.Context, instance string) {
	defer close(h.maintenanceDone)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			budget, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			_, pruneError := h.store.PruneExpiredReceipts(budget, time.Now(), 256)
			cancel()
			// A failed expiry write must not suppress WAL space recovery.
			budget, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
			_, checkpointError := h.store.Checkpoint(budget)
			cancel()
			if err := errors.Join(pruneError, checkpointError); err != nil && ctx.Err() == nil {
				category := xrpc.Code(err)
				var domain *api.Error
				if errors.As(err, &domain) {
					category = domain.Code
				}
				h.diagnostics.Emit(xrpc.Diagnostic{Time: time.Now(), Level: "warn", Event: "storage.maintenance", Service: api.Service, InstanceID: instance, Category: category})
			}
		}
	}
}

func withDescription(handler http.Handler, ref xrpc.ServiceRef) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/describe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			ServiceRef xrpc.ServiceRef `json:"service_ref"`
		}{ServiceRef: ref})
	})
	mux.Handle("/", handler)
	return mux
}
