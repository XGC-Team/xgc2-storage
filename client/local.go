package client

import (
	"context"
	"errors"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

// Backend is implemented by the storage owner. It exposes no database handle,
// SQL/transaction handle or maintenance authority to a data consumer.
type Backend interface {
	Snapshot(context.Context, api.SnapshotRequest) (api.SnapshotResponse, error)
	Batch(context.Context, api.BatchRequest) (api.Receipt, error)
	Receipt(context.Context, api.ReceiptRequest) (api.Receipt, error)
	Named(context.Context, api.NamedRequest) (api.NamedResponse, error)
	NamedResult(context.Context, api.ReceiptRequest) (api.NamedResponse, error)
	WithReadSnapshot(context.Context, api.Scope, func(context.Context) error) error
}

func (c *local) WithReadSnapshot(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("storage: read snapshot callback required")
	}
	ctx, cancel, err := c.call(ctx, c.scope)
	if err != nil {
		return err
	}
	defer cancel()
	var callbackErr error
	err = c.store.WithReadSnapshot(ctx, c.scope, func(view context.Context) error {
		callbackErr = fn(view)
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return localError(err)
}

type local struct {
	store  Backend
	scope  api.Scope
	budget time.Duration
}

// NewLocal borrows the same owner used by the XRPC servers. Calls are ordinary
// typed function calls with a fixed scope and finite lifetime; no wire codec,
// loopback transport or second storage implementation is involved.
func NewLocal(store Backend, scope api.Scope, budget time.Duration) (ReadSnapshotClient, error) {
	if store == nil || scope.Namespace == "" || scope.User == "" || scope.Workspace == "" || budget <= 0 {
		return nil, errors.New("storage: local owner, explicit scope and finite call budget required")
	}
	return &local{store: store, scope: scope, budget: budget}, nil
}

func (c *local) call(ctx context.Context, scope api.Scope) (context.Context, context.CancelFunc, error) {
	if scope != c.scope {
		return nil, nil, xrpc.Failure("permission_denied", xrpc.NotSent, errors.New("storage: scope differs from local owner grant"))
	}
	ctx, cancel := context.WithTimeout(ctx, c.budget)
	return ctx, cancel, nil
}

// Only an actual owner result can be a confirmed rejection. Cancellation or
// storage I/O failure does not prove a mutation's durable outcome.
func localError(err error) error {
	if err == nil {
		return nil
	}
	var domain *api.Error
	if !errors.As(err, &domain) {
		return xrpc.Failure("internal", xrpc.OutcomeUnknown, err)
	}
	disposition := xrpc.ResponseReceived
	switch domain.Code {
	case "deadline_exceeded", "cancelled", "io_error", "internal":
		disposition = xrpc.OutcomeUnknown
	}
	return xrpc.Failure(domain.Code, disposition, err)
}

func (c *local) Snapshot(ctx context.Context, _ string, r api.SnapshotRequest) (api.SnapshotResponse, error) {
	ctx, cancel, err := c.call(ctx, r.Scope)
	if err != nil {
		return api.SnapshotResponse{}, err
	}
	defer cancel()
	out, err := c.store.Snapshot(ctx, r)
	return out, localError(err)
}

func (c *local) Batch(ctx context.Context, r api.BatchRequest) (api.Receipt, error) {
	ctx, cancel, err := c.call(ctx, r.Scope)
	if err != nil {
		return api.Receipt{}, err
	}
	defer cancel()
	out, err := c.store.Batch(ctx, r)
	return out, localError(err)
}

func (c *local) Receipt(ctx context.Context, _ string, r api.ReceiptRequest) (api.Receipt, error) {
	ctx, cancel, err := c.call(ctx, r.Scope)
	if err != nil {
		return api.Receipt{}, err
	}
	defer cancel()
	out, err := c.store.Receipt(ctx, r)
	return out, localError(err)
}

func (c *local) Named(ctx context.Context, r api.NamedRequest) (api.NamedResponse, error) {
	ctx, cancel, err := c.call(ctx, r.Scope)
	if err != nil {
		return api.NamedResponse{}, err
	}
	defer cancel()
	out, err := c.store.Named(ctx, r)
	return out, localError(err)
}

func (c *local) NamedResult(ctx context.Context, _ string, r api.ReceiptRequest) (api.NamedResponse, error) {
	ctx, cancel, err := c.call(ctx, r.Scope)
	if err != nil {
		return api.NamedResponse{}, err
	}
	defer cancel()
	out, err := c.store.NamedResult(ctx, r)
	return out, localError(err)
}

var _ Interface = (*local)(nil)
var _ ReadSnapshotClient = (*local)(nil)
