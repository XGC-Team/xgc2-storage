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
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
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
	// WriterQueue bounds the writers admitted at once (default 64). A caller
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
	store       *engine.Store
	manifest    api.Manifest
	databaseID  string
	callBudget  time.Duration
	diagnostics *xrpc.Diagnostics
	mu          sync.Mutex
	servers     []*Server
	serving     sync.WaitGroup
	close       *closeAttempt
	serverErr   error
	closed      atomic.Bool
}

type closeAttempt struct {
	done     chan struct{}
	err      error
	storeErr error
}

// Open completes owner admission. ctx bounds startup only. Close owns
// shutdown, so signal cancellation cannot destroy storage before the embedding
// application's durable Stop finishes.
func Open(ctx context.Context, config Config) (_ *Host, err error) {
	if config.Path == "" {
		return nil, errors.New("storage: database path required")
	}
	if len(config.ConfigurationDomains) > 256 {
		return nil, errors.New("storage: configuration declarations exceed 256 domains")
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	h := &Host{manifest: config.Manifest, callBudget: config.CallBudget, diagnostics: config.Diagnostics}
	if h.callBudget == 0 {
		h.callBudget = 30 * time.Second
	}
	h.store, err = engine.Open(startup, engine.Config{Path: config.Path, Create: config.Create, Manifest: config.Manifest, Modules: registry.Compiled(),
		Readers: config.Readers, WriterQueue: config.WriterQueue, CallBudget: config.CallBudget, MaxDBBytes: config.MaxDBBytes,
		OnMaintenanceError: h.maintenanceFailed})
	if err != nil {
		return nil, err
	}
	stats, err := h.store.Stats()
	if err != nil {
		h.store.Close()
		return nil, err
	}
	h.databaseID = stats.DatabaseID
	if len(config.ConfigurationDomains) != 0 {
		if err = coredata.DeclareConfigurationDomains(startup, h.store, config.ConfigurationDomains); err != nil {
			h.store.Close()
			return nil, err
		}
	}
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

// Core returns the typed Core data API (configuration, Runs, Sessions,
// recordings) for one scope whose namespace uses the Core data module.
func (h *Host) Core(scope api.Scope) (*coredata.Store, error) {
	if h == nil || h.closed.Load() {
		return nil, errors.New("storage: owner is closed")
	}
	return coredata.New(h.store, scope)
}

func (h *Host) DatabaseID() string { return h.databaseID }

// Stats reports counters and sizes for diagnostics and benchmarks.
func (h *Host) Stats() (engine.Stats, error) { return h.store.Stats() }

// Close stops every exposure, then the owner. Caller cancellation bounds
// waiting, not ownership: the owner keeps its DB lease until admitted calls
// quiesce, including after a shutdown timeout. Concurrent calls join one
// close attempt; after a physical Store-close failure, a later explicit call
// retries the same retained owner and preserves the earlier caller's error.
func (h *Host) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	attempt := h.close
	first := attempt == nil
	if attempt != nil {
		select {
		case <-attempt.done:
			if attempt.storeErr == nil {
				err := attempt.err
				h.mu.Unlock()
				return err
			}
			attempt = nil // an explicit later call retries the same Store owner
		default:
		}
	}
	if attempt == nil {
		h.closed.Store(true)
		attempt = &closeAttempt{done: make(chan struct{})}
		h.close = attempt
		go h.closeResources(attempt, first)
	}
	h.mu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) closeResources(attempt *closeAttempt, first bool) {
	if first {
		// Serve registrations begin under mu before shutdown starts. Join them
		// before taking the exposure snapshot so no late server escapes Close.
		h.serving.Wait()
		h.mu.Lock()
		servers := append([]*Server(nil), h.servers...)
		h.mu.Unlock()
		for _, s := range servers {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			h.serverErr = errors.Join(h.serverErr, s.Close(shutdown))
			cancel()
		}
	}
	storeErr := h.store.Close()
	h.mu.Lock()
	attempt.storeErr = storeErr
	attempt.err = errors.Join(h.serverErr, storeErr)
	close(attempt.done)
	h.mu.Unlock()
}

// maintenanceFailed reports a failed background checkpoint or receipt expiry.
// The counters in Stats keep the total; the diagnostics sink, when the process
// has one, gets a bounded record.
func (h *Host) maintenanceFailed(err error) {
	category := xrpc.Code(err)
	var domain *api.Error
	if errors.As(err, &domain) {
		category = domain.Code
	}
	h.diagnostics.Emit(xrpc.Diagnostic{Time: time.Now(), Level: "warn", Event: "storage.maintenance", Service: api.Service, Category: category})
}
