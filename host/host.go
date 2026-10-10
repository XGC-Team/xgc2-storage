// Package host owns one storage instance. Open needs no listener: an embedding
// process calls typed Go functions against the owner. Serve exposes the same
// owner through the XRPC HTTP and gRPC hosts for consumers in other processes.
package host

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"github.com/XGC-Team/xgc2-storage/registry"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

// Config configures the owner. Zero limits select the documented defaults.
type Config struct {
	Path     string
	Create   bool
	Manifest api.Manifest
	// ConfigurationDomains are applied by the owner before it serves any call.
	ConfigurationDomains []model.ConfigurationDomainDeclaration
	// Readers is the number of concurrent read connections (default 4, at most 16).
	Readers int
	// WriterQueue bounds the writers admitted at once (default 32). A caller
	// beyond it waits until its own deadline instead of failing.
	WriterQueue int
	// CallBudget is the longest time one call may run (default 30 s).
	CallBudget time.Duration
	// MaxDBBytes is the finite capacity of the database file.
	MaxDBBytes int64
	// Diagnostics receives maintenance failures. It is optional.
	Diagnostics *xrpc.Diagnostics
}

type Host struct {
	store           *engine.Store
	manifest        api.Manifest
	databaseID      string
	callBudget      time.Duration
	diagnostics     *xrpc.Diagnostics
	maintenanceStop context.CancelFunc
	maintenanceDone chan struct{}
	mu              sync.Mutex
	servers         []*Server
	closeOnce       sync.Once
	closeDone       chan struct{}
	closeErr        error
	closed          atomic.Bool
}

// Open completes owner admission. ctx bounds startup only. Close owns
// shutdown, so signal cancellation cannot destroy storage before the embedding
// application's durable Stop finishes.
func Open(ctx context.Context, config Config) (_ *Host, resultErr error) {
	if config.Path == "" {
		return nil, errors.New("storage: database path required")
	}
	if len(config.ConfigurationDomains) > 256 {
		return nil, errors.New("storage: configuration declarations exceed 256 domains")
	}
	modules := registry.Compiled()
	if len(config.ConfigurationDomains) != 0 {
		modules = registry.ConfigurationDeployment(config.ConfigurationDomains)
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	store, err := engine.Open(startup, engine.Config{Path: config.Path, Create: config.Create, Manifest: config.Manifest, Modules: modules,
		Readers: config.Readers, WriterQueue: config.WriterQueue, MaxCallTime: config.CallBudget, MaxDBBytes: config.MaxDBBytes})
	if err != nil {
		return nil, err
	}
	stats, err := store.Stats()
	if err != nil {
		store.Close()
		return nil, err
	}
	h := &Host{store: store, manifest: config.Manifest, databaseID: stats.DatabaseID, callBudget: config.CallBudget, diagnostics: config.Diagnostics, closeDone: make(chan struct{})}
	if h.callBudget == 0 {
		h.callBudget = 30 * time.Second
	}
	maintenanceCtx, maintenanceStop := context.WithCancel(context.Background())
	h.maintenanceStop = maintenanceStop
	h.maintenanceDone = make(chan struct{})
	go h.maintain(maintenanceCtx)
	return h, nil
}

// Client returns the in-process document port for one scope. It calls the
// owner directly: no wire codec, token or loopback transport is involved.
func (h *Host) Client(scope api.Scope) (client.ReadSnapshotClient, error) {
	if h == nil || h.closed.Load() {
		return nil, errors.New("storage: owner is closed")
	}
	if err := h.store.CheckScope(scope); err != nil {
		return nil, err
	}
	return client.NewLocal(h.store, scope, h.callBudget)
}

func (h *Host) DatabaseID() string { return h.databaseID }

// Stats reports counters and sizes for diagnostics and benchmarks.
func (h *Host) Stats() (engine.Stats, error) { return h.store.Stats() }

// Close stops every exposure, then the owner. Caller cancellation bounds
// waiting, not ownership: the owner keeps its DB lease until admitted calls
// quiesce, including after a shutdown timeout.
func (h *Host) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		go h.closeResources()
	})
	select {
	case <-h.closeDone:
		return h.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) closeResources() {
	defer close(h.closeDone)
	h.mu.Lock()
	servers := append([]*Server(nil), h.servers...)
	h.mu.Unlock()
	for _, s := range servers {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		h.closeErr = errors.Join(h.closeErr, s.Close(shutdown))
		cancel()
	}
	if h.maintenanceStop != nil {
		h.maintenanceStop()
		<-h.maintenanceDone
	}
	h.closeErr = errors.Join(h.closeErr, h.store.Close())
}

func (h *Host) maintain(ctx context.Context) {
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
				h.diagnostics.Emit(xrpc.Diagnostic{Time: time.Now(), Level: "warn", Event: "storage.maintenance", Service: api.Service, Category: category})
			}
		}
	}
}
