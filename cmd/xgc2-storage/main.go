package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	"github.com/XGC-Team/xgc2-storage/registry"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"google.golang.org/grpc"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	var path, manifest, grantsFile, socket, grpcSocket, target, refOut string
	var create bool
	flag.StringVar(&path, "db", "", "explicit managed database file grant (existing by default)")
	flag.StringVar(&manifest, "manifest", "", "reviewed deployment manifest")
	flag.StringVar(&grantsFile, "grants", "", "private mode0600 owner grant JSON array")
	flag.StringVar(&socket, "http-socket", "", "private XRPC HTTP Unix endpoint")
	flag.StringVar(&grpcSocket, "grpc-socket", "", "private XRPC gRPC Unix endpoint")
	flag.StringVar(&target, "target-id", "", "local target identity")
	flag.StringVar(&refOut, "ref-out", "", "private runtime file for bound ServiceRef array")
	flag.BoolVar(&create, "create", false, "explicitly initialize absent DB; never replace existing")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("storage: unexpected positional arguments")
	}
	if path == "" || manifest == "" || grantsFile == "" || target == "" || refOut == "" || (socket == "" && grpcSocket == "") {
		return errors.New("storage: db/manifest/grants/target-id/ref-out and a socket required")
	}
	file, e := os.Open(manifest)
	if e != nil {
		return e
	}
	m, e := engine.DecodeManifest(file)
	file.Close()
	if e != nil {
		return e
	}
	st, e := os.Lstat(grantsFile)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return errors.New("storage: grants must be regular mode0600")
	}
	grantFile, e := os.Open(grantsFile)
	if e != nil {
		return e
	}
	raw, e := io.ReadAll(io.LimitReader(grantFile, (1<<20)+1))
	grantFile.Close()
	if e != nil {
		return e
	}
	if len(raw) > 1<<20 {
		return errors.New("storage: grant configuration exceeds 1 MiB")
	}
	var grants []server.Grant
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if e = decoder.Decode(&grants); e != nil {
		return e
	}
	var tail any
	if e = decoder.Decode(&tail); e != io.EOF {
		return errors.New("storage: one owner grant array required")
	}
	if e = server.ValidateGrants(grants); e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var random [16]byte
	if _, e = rand.Read(random[:]); e != nil {
		return e
	}
	instance := hex.EncodeToString(random[:])
	var refs []xrpc.ServiceRef
	for _, endpoint := range []struct {
		address string
		profile string
	}{{socket, xrpc.HTTP}, {grpcSocket, xrpc.GRPC}} {
		if endpoint.address == "" {
			continue
		}
		ref := xrpc.ServiceRef{TargetID: target, Service: api.Service, APIVersion: api.Version, InstanceID: instance, Profile: endpoint.profile, Endpoint: xrpc.Endpoint{Kind: "unix", Address: endpoint.address}}
		if e = ref.ValidateInternal(); e != nil {
			return e
		}
		refs = append(refs, ref)
	}
	requestBytes, responseBytes := registry.TransportBounds(m)
	policy, e := xrpc.ResolvePolicy(xrpc.PolicyOptions{
		Environment: os.Environ(), DefaultSource: "storage deployment manifest",
		Defaults:     map[string]string{"MAX_REQUEST_BYTES": strconv.Itoa(requestBytes), "MAX_RESPONSE_BYTES": strconv.Itoa(responseBytes), "HOST_MAX_CONNECTIONS": "4", "HOST_MAX_IN_FLIGHT": "8", "GRPC_MAX_STREAMS_PER_CONNECTION": "1"},
		Ceilings:     map[string]int64{"MAX_REQUEST_BYTES": int64(requestBytes), "MAX_RESPONSE_BYTES": int64(responseBytes), "HOST_MAX_CONNECTIONS": 4, "HOST_MAX_IN_FLIGHT": 8, "GRPC_MAX_STREAMS_PER_CONNECTION": 1, "CALL_TIMEOUT_MS": 30000},
		Capabilities: []string{"host", "http", "rpc", "transport", "grpc", "diagnostics"},
	})
	if e != nil {
		return e
	}
	store, e := engine.Open(startup, engine.Config{Path: path, Create: create, Manifest: m, Modules: registry.Compiled()})
	if e != nil {
		return e
	}
	defer store.Close()
	diagnostics, e := xrpc.NewDiagnostics(policy, xrpc.DiagnosticOptions{Sink: os.Stderr, MaxQueuedRecords: 128, MaxRecordBytes: 4096})
	if e != nil {
		return e
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = diagnostics.Close(ctx)
	}()
	maintenanceCtx, maintenanceStop := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-maintenanceCtx.Done():
				return
			case <-tick.C:
				budget, cancel := context.WithTimeout(maintenanceCtx, 500*time.Millisecond)
				_, pruneError := store.PruneExpiredReceipts(budget, time.Now(), 256)
				cancel()
				// A failed expiry write must not suppress WAL space recovery.
				budget, cancel = context.WithTimeout(maintenanceCtx, 500*time.Millisecond)
				_, checkpointError := store.Checkpoint(budget)
				cancel()
				checkpointError = errors.Join(pruneError, checkpointError)
				if checkpointError != nil && maintenanceCtx.Err() == nil {
					category := xrpc.Code(checkpointError)
					var domain *api.Error
					if errors.As(checkpointError, &domain) {
						category = domain.Code
					}
					diagnostics.Emit(xrpc.Diagnostic{Time: time.Now(), Level: "warn", Event: "storage.maintenance", Service: api.Service, InstanceID: instance, Category: category})
				}
			}
		}
	}()
	defer func() { maintenanceStop(); <-maintenanceDone }()
	httpOptions, e := (httpx.HostOptions{InstanceID: instance, Service: api.Service, Diagnostics: diagnostics}).WithPolicy(policy)
	if e != nil {
		return e
	}
	grpcOptions, e := (grpcx.HostOptions{InstanceID: instance, Service: api.Service, Diagnostics: diagnostics}).WithPolicy(policy)
	if e != nil {
		return e
	}
	var httpHost *httpx.Host
	var grpcHost *grpcx.Host
	// SDK leases and hosts own all socket lifecycle and transport resources.
	if socket != "" {
		lease, e := unixlease.Reserve(startup, socket, unixlease.Options{ExistingPath: unixlease.ReclaimUnreachable})
		if e != nil {
			return e
		}
		listener, e := lease.Listen()
		if e != nil {
			lease.Close()
			return e
		}
		handler, e := server.HTTP(store, grants)
		if e != nil {
			lease.Close()
			return e
		}
		httpHost, e = httpx.Serve(listener, lease, handler, httpOptions)
		if e != nil {
			lease.Close()
			return e
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpHost.Shutdown(shutdown)
		}()
	}
	if grpcSocket != "" {
		lease, e := unixlease.Reserve(startup, grpcSocket, unixlease.Options{ExistingPath: unixlease.ReclaimUnreachable})
		if e != nil {
			return e
		}
		listener, e := lease.Listen()
		if e != nil {
			lease.Close()
			return e
		}
		grpcHost, e = grpcx.ServeWithOptions(listener, lease, func(r grpc.ServiceRegistrar) { pb.RegisterStorageServer(r, &server.GRPC{Store: store, Grants: grants}) }, grpcOptions)
		if e != nil {
			lease.Close()
			return e
		}
		defer grpcHost.Stop()
	}
	if e = publishRefs(refOut, refs); e != nil {
		return e
	}
	var doneHTTP, doneGRPC <-chan struct{}
	if httpHost != nil {
		doneHTTP = httpHost.Done()
	}
	if grpcHost != nil {
		doneGRPC = grpcHost.Done()
	}
	select {
	case <-ctx.Done():
	case <-doneHTTP:
	case <-doneGRPC:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if httpHost != nil {
		err = errors.Join(err, httpHost.Shutdown(shutdown))
	}
	if grpcHost != nil {
		err = errors.Join(err, grpcHost.Shutdown(shutdown))
	}
	return err
}
func publishRefs(path string, refs []xrpc.ServiceRef) error {
	parent := filepath.Dir(path)
	st, e := os.Lstat(parent)
	if e != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return errors.New("storage: ref directory must be private")
	}
	if st, e = os.Lstat(path); e == nil && (!st.Mode().IsRegular() || st.Mode().Perm() != 0600) {
		return errors.New("storage: unsafe existing reference file")
	}
	f, e := os.CreateTemp(parent, ".storage-ref-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return e
	}
	e = json.NewEncoder(f).Encode(refs)
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	dir, e := os.Open(parent)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
