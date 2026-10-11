package server

import (
	"context"
	"errors"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type GRPC struct {
	pb.UnimplementedStorageServer
	Store  *engine.Store
	Grants []Grant
}

func (s *GRPC) check(ctx context.Context, scope api.Scope) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || !authorize(s.Grants, strings.TrimPrefix(values[0], "Bearer "), scope) {
		return grpcx.ApplicationError(ctx, status.Error(codes.PermissionDenied, "storage scope not granted"))
	}
	return nil
}
func grpcError(ctx context.Context, e error) error {
	if e == nil {
		return nil
	}
	code := codes.Internal
	known := true
	var domain *api.Error
	if errors.As(e, &domain) {
		switch domain.Code {
		case "invalid_argument":
			code = codes.InvalidArgument
		case "not_found":
			code = codes.NotFound
		case "conflict":
			code = codes.Aborted
		case "failed_precondition":
			code = codes.FailedPrecondition
		case "resource_exhausted", "disk_full":
			code = codes.ResourceExhausted
		case "deadline_exceeded":
			code = codes.DeadlineExceeded
			known = false
		case "cancelled":
			code = codes.Canceled
			known = false
		case "unavailable", "busy":
			code = codes.Unavailable
		case "permission_denied":
			code = codes.PermissionDenied
		case "corrupt":
			code = codes.DataLoss
		case "io_error":
			code = codes.Internal
		default:
			known = false
		}
	} else {
		known = false
	}
	result := status.Error(code, e.Error())
	if known && ctx.Err() == nil {
		return grpcx.ApplicationError(ctx, result)
	}
	return result
}
func (s *GRPC) Snapshot(ctx context.Context, r *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	request := r.API()
	if e := s.check(ctx, request.Scope); e != nil {
		return nil, e
	}
	out, e := s.Store.Snapshot(ctx, request)
	if e != nil {
		return nil, grpcError(ctx, e)
	}
	return pb.SnapshotOutput(out), nil
}
func (s *GRPC) Batch(ctx context.Context, r *pb.BatchRequest) (*pb.ReceiptResponse, error) {
	request := r.API()
	if e := s.check(ctx, request.Scope); e != nil {
		return nil, e
	}
	ids := metadata.ValueFromIncomingContext(ctx, "x-request-id")
	if len(ids) != 1 || ids[0] != request.RequestID {
		return nil, grpcx.ApplicationError(ctx, status.Error(codes.InvalidArgument, "body/header identity mismatch"))
	}
	out, e := s.Store.Batch(ctx, request)
	if e != nil {
		return nil, grpcError(ctx, e)
	}
	return pb.ReceiptOutput(out), nil
}
func (s *GRPC) Receipt(ctx context.Context, r *pb.ReceiptRequest) (*pb.ReceiptResponse, error) {
	request := r.API()
	if e := s.check(ctx, request.Scope); e != nil {
		return nil, e
	}
	out, e := s.Store.Receipt(ctx, request)
	if e != nil {
		return nil, grpcError(ctx, e)
	}
	return pb.ReceiptOutput(out), nil
}
