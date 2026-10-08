package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

// HTTPCaller uses the SDK's owned pooled client, including deadlines, instance
// fencing and response bounds. It adds the storage owner grant, no socket code.
type HTTPCaller struct {
	Transport *httpx.Client
	Grant     string
}

func (c HTTPCaller) Call(ctx context.Context, call xrpc.Call) (xrpc.Result, error) {
	if c.Transport == nil || len(c.Grant) < 32 {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("storage: transport and owner grant required"))
	}
	if call.Service != c.Transport.Reference() {
		return xrpc.Result{}, xrpc.Failure("invalid_argument", xrpc.NotSent, errors.New("storage: HTTP caller service reference differs from bound transport"))
	}
	// Storage grants are scoped capabilities; SDK DoWithHeaders owns transport.
	out, status, _, err := c.Transport.DoWithHeaders(ctx, call.Method, call.Path, call.RequestID, "application/json", call.Payload, map[string]string{"Authorization": "Bearer " + c.Grant})
	if err != nil {
		return xrpc.Result{}, err
	}
	if status < 200 || status >= 300 {
		var body struct {
			Error api.Error `json:"error"`
		}
		if e := json.Unmarshal(out, &body); e != nil || body.Error.Code == "" {
			return xrpc.Result{}, xrpc.Failure("internal", xrpc.ResponseReceived, errors.New(strings.TrimSpace(string(out))))
		}
		return xrpc.Result{}, xrpc.Failure(body.Error.Code, xrpc.ResponseReceived, &body.Error)
	}
	return xrpc.Result{Status: status, Payload: out}, nil
}
