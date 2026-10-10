// Package client consumes XRPC, with no SQLite/ORM dependency or retry loop.
package client

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/api"
	pb "github.com/XGC-Team/xgc2-storage/protocol"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"google.golang.org/protobuf/encoding/protojson"
)

type Client struct {
	caller xrpc.Caller
	ref    xrpc.ServiceRef
}

func New(caller xrpc.Caller, ref xrpc.ServiceRef) (*Client, error) {
	if caller == nil {
		return nil, errors.New("storage: XRPC caller required")
	}
	if err := ref.ValidateInternal(); err != nil {
		return nil, err
	}
	if ref.Service != api.Service || ref.APIVersion != api.Version {
		return nil, errors.New("storage: storage-v1 service reference required")
	}
	return &Client{caller: caller, ref: ref}, nil
}
func (c *Client) call(ctx context.Context, id, path, method string, request, response any) error {
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	requestMaximum, responseMaximum := api.MaxRequestBytes, api.MaxResponseBytes
	if len(raw) > requestMaximum {
		return &api.Error{Code: "resource_exhausted", Message: "request exceeds storage byte limit"}
	}
	call := xrpc.Call{Service: c.ref, Method: "POST", Path: path, RequestID: id, Payload: raw}
	if c.ref.Profile == xrpc.GRPC {
		call.Method = "/" + api.Service + "/" + method
		call.Path = ""
		switch v := request.(type) {
		case api.SnapshotRequest:
			call.Payload, err = protojson.Marshal(pb.SnapshotInput(v))
		case api.BatchRequest:
			call.Payload, err = protojson.Marshal(pb.BatchInput(v))
		case api.ReceiptRequest:
			call.Payload, err = protojson.Marshal(pb.ReceiptInput(v))
		default:
			return errors.New("storage: unsupported typed request")
		}
		if err != nil {
			return err
		}
	}
	result, err := c.caller.Call(ctx, call)
	if err != nil {
		return err
	}
	maximum := responseMaximum
	if c.ref.Profile == xrpc.GRPC {
		maximum = responseMaximum*4/3 + (64 << 10)
	}
	if len(result.Payload) > maximum {
		return &api.Error{Code: "resource_exhausted", Message: "response exceeds storage byte limit"}
	}
	if c.ref.Profile == xrpc.GRPC {
		switch v := response.(type) {
		case *api.SnapshotResponse:
			var out pb.SnapshotResponse
			if err = protojson.Unmarshal(result.Payload, &out); err != nil {
				return err
			}
			*v = out.API()
			return checkResponse(v)
		case *api.Receipt:
			var out pb.ReceiptResponse
			if err = protojson.Unmarshal(result.Payload, &out); err != nil {
				return err
			}
			*v = out.API()
			return checkResponse(v)
		}
	}
	return json.Unmarshal(result.Payload, response)
}
func checkResponse(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > api.MaxResponseBytes {
		return &api.Error{Code: "resource_exhausted", Message: "decoded response exceeds storage byte limit"}
	}
	return nil
}
func (c *Client) Snapshot(ctx context.Context, id string, r api.SnapshotRequest) (out api.SnapshotResponse, err error) {
	err = c.call(ctx, id, "/v1/snapshot", "Snapshot", r, &out)
	return
}
func (c *Client) Batch(ctx context.Context, r api.BatchRequest) (out api.Receipt, err error) {
	err = c.call(ctx, r.RequestID, "/v1/batch", "Batch", r, &out)
	return
}
func (c *Client) Receipt(ctx context.Context, id string, r api.ReceiptRequest) (out api.Receipt, err error) {
	err = c.call(ctx, id, "/v1/receipt", "Receipt", r, &out)
	return
}
