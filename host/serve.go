package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	"github.com/XGC-Team/xgc2-storage/registry"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
)

// Limits bound the transport state of one exposure. A zero field selects the
// default named on it.
type Limits struct {
	// MaxConnections bounds accepted connections per socket (default 4).
	MaxConnections int
	// MaxInFlight bounds admitted calls per socket (default 8).
	MaxInFlight int
	// StreamsPerConnection bounds native gRPC streams per connection (default 1).
	StreamsPerConnection uint32
	// MaxRequestBytes and MaxResponseBytes bound one message. The default is the
	// largest request and response the registered operations declare.
	MaxRequestBytes  int
	MaxResponseBytes int
}

// ServeConfig selects the optional XRPC exposure of an open owner.
type ServeConfig struct {
	TargetID   string
	HTTPSocket string
	GRPCSocket string
	// Grants authorize remote callers per scope. Nothing is served without one.
	Grants []server.Grant
	Limits Limits
}

// Server is one XRPC exposure of the owner. Local and remote calls share the
// same engine, so they share receipts, quotas and the single writer.
type Server struct {
	refs []xrpc.ServiceRef
	http *httpx.Host
	grpc *grpcx.Host
	done chan struct{}
	stop chan struct{}
	once sync.Once
	err  error
}

// Serve starts the HTTP and gRPC hosts on the given private Unix sockets.
// ctx bounds startup only.
func (h *Host) Serve(ctx context.Context, config ServeConfig) (_ *Server, resultErr error) {
	if h == nil || h.closed.Load() {
		return nil, errors.New("storage: owner is closed")
	}
	if config.TargetID == "" || (config.HTTPSocket == "" && config.GRPCSocket == "") {
		return nil, errors.New("storage: target and an external endpoint required")
	}
	if err := server.ValidateGrants(config.Grants); err != nil {
		return nil, err
	}
	limits := config.Limits
	requestBytes, responseBytes := registry.TransportBounds(h.manifest)
	if limits.MaxRequestBytes == 0 {
		limits.MaxRequestBytes = requestBytes
	}
	if limits.MaxResponseBytes == 0 {
		limits.MaxResponseBytes = responseBytes
	}
	if limits.MaxConnections == 0 {
		limits.MaxConnections = 4
	}
	if limits.MaxInFlight == 0 {
		limits.MaxInFlight = 8
	}
	if limits.StreamsPerConnection == 0 {
		limits.StreamsPerConnection = 1
	}
	s := &Server{done: make(chan struct{}), stop: make(chan struct{})}
	defer func() {
		if resultErr != nil {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, s.Close(shutdown))
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
		s.refs = append(s.refs, ref)
		if endpoint.profile == xrpc.HTTP {
			httpRef = ref
		}
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
		handler, err := server.HTTP(h.store, config.Grants)
		if err != nil {
			lease.Close()
			return nil, err
		}
		s.http, err = httpx.Serve(listener, lease, withDescription(handler, httpRef), httpx.HostOptions{InstanceID: instance, Service: api.Service,
			DiscoveryPaths: []string{"/v1/describe"}, MaxBodyBytes: int64(limits.MaxRequestBytes), MaxResponseBytes: int64(limits.MaxResponseBytes),
			MaxConnections: limits.MaxConnections, MaxInFlight: limits.MaxInFlight, MaxCallTime: h.callBudget, Diagnostics: h.diagnostics})
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
		s.grpc, err = grpcx.ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) {
			pb.RegisterStorageServer(r, &server.GRPC{Store: h.store, Grants: config.Grants})
		}, grpcx.HostOptions{InstanceID: instance, Service: api.Service, MaxRequestBytes: limits.MaxRequestBytes, MaxResponseBytes: limits.MaxResponseBytes,
			MaxConnections: limits.MaxConnections, MaxInFlight: limits.MaxInFlight, MaxConcurrentStreams: limits.StreamsPerConnection,
			MaxCallTime: h.callBudget, Diagnostics: h.diagnostics})
		if err != nil {
			lease.Close()
			return nil, err
		}
	}
	var httpDone, grpcDone <-chan struct{}
	if s.http != nil {
		httpDone = s.http.Done()
	}
	if s.grpc != nil {
		grpcDone = s.grpc.Done()
	}
	go func() {
		select {
		case <-httpDone:
		case <-grpcDone:
		case <-s.stop:
		}
		close(s.done)
	}()
	h.mu.Lock()
	h.servers = append(h.servers, s)
	h.mu.Unlock()
	return s, nil
}

// References returns the fully bound ServiceRef of every started endpoint.
func (s *Server) References() []xrpc.ServiceRef { return append([]xrpc.ServiceRef(nil), s.refs...) }

// Done is closed when an endpoint exits or the exposure is closed.
func (s *Server) Done() <-chan struct{} { return s.done }

// Close stops admission and drains the running calls within ctx.
func (s *Server) Close(ctx context.Context) error {
	s.once.Do(func() {
		close(s.stop)
		if s.http != nil {
			s.err = errors.Join(s.err, s.http.Shutdown(ctx))
		}
		if s.grpc != nil {
			s.err = errors.Join(s.err, s.grpc.Shutdown(ctx))
			s.grpc.Stop()
		}
	})
	return s.err
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
