package client

import (
	"context"

	"github.com/XGC-Team/xgc2-storage/api"
)

// Interface is the storage data port, shared by local and XRPC consumers.
// Read request IDs are transport correlation only; mutation IDs stay in the
// typed request and retain their durable receipt semantics.
type Interface interface {
	Snapshot(context.Context, string, api.SnapshotRequest) (api.SnapshotResponse, error)
	Batch(context.Context, api.BatchRequest) (api.Receipt, error)
	Receipt(context.Context, string, api.ReceiptRequest) (api.Receipt, error)
}

// ReadSnapshotClient is a local-only capability. Its callback borrows a read
// view that fences every read to one revision without holding a reader, so the
// consumer may compute between reads. A write in between makes the next read
// fail with a conflict. The remote Client deliberately does not implement it.
type ReadSnapshotClient interface {
	Interface
	WithReadSnapshot(context.Context, func(context.Context) error) error
}

var _ Interface = (*Client)(nil)
