package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
)

type Grant struct {
	Token     string `json:"token"`
	Namespace string `json:"namespace"`
	User      string `json:"user"`
	Workspace string `json:"workspace"`
}

func ValidateGrants(grants []Grant) error {
	if len(grants) == 0 || len(grants) > 256 {
		return errors.New("storage: 1..256 owner grants required")
	}
	for _, g := range grants {
		if len(g.Token) < 32 || len(g.Token) > 256 || g.Namespace == "" || g.User == "" || g.Workspace == "" {
			return errors.New("storage: explicit namespace/user/workspace grant and private >=32-byte token required")
		}
		for _, c := range g.Token {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~+/=", c)) {
				return errors.New("storage: owner token must use portable ASCII Bearer token bytes")
			}
		}
	}
	return nil
}
func authorize(grants []Grant, token string, s api.Scope) bool {
	ok := false
	for _, g := range grants {
		same := subtle.ConstantTimeCompare([]byte(g.Token), []byte(token)) == 1
		if same && g.Namespace == s.Namespace && (g.User == s.User || g.User == "*") && (g.Workspace == s.Workspace || g.Workspace == "*") {
			ok = true
		}
	}
	return ok
}
func write(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		code := "internal"
		var domain *api.Error
		if errors.As(err, &domain) {
			code = domain.Code
		}
		status := 500
		switch code {
		case "invalid_argument":
			status = 400
		case "permission_denied":
			status = 403
		case "not_found":
			status = 404
		case "conflict", "failed_precondition":
			status = 409
		case "resource_exhausted":
			status = 429
		case "deadline_exceeded":
			status = 504
		case "cancelled":
			status = 499
		case "busy", "unavailable":
			status = 503
		case "disk_full":
			status = 507
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": api.Error{Code: code, Message: err.Error()}})
		return
	}
	raw, e := json.Marshal(v)
	if e != nil {
		w.WriteHeader(500)
		return
	}
	_, _ = w.Write(raw)
}
func decode(r *http.Request, v any) error {
	maximum := int64(api.MaxRequestBytes)
	if _, ok := v.(*api.NamedRequest); ok {
		maximum = 16 << 20
	}
	if r.ContentLength > maximum {
		return &api.Error{Code: "resource_exhausted", Message: "operation request byte limit exceeded"}
	}
	r.Body = http.MaxBytesReader(nil, r.Body, maximum)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(e, &tooLarge) {
			return &api.Error{Code: "resource_exhausted", Message: "operation request byte limit exceeded"}
		}
		return &api.Error{Code: "invalid_argument", Message: e.Error()}
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(e, &tooLarge) {
			return &api.Error{Code: "resource_exhausted", Message: "operation request byte limit exceeded"}
		}
		return &api.Error{Code: "invalid_argument", Message: "one request object required"}
	}
	return nil
}
func HTTP(store *engine.Store, grants []Grant) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("storage: store required")
	}
	if e := ValidateGrants(grants); e != nil {
		return nil, e
	}
	grants = append([]Grant(nil), grants...)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			write(w, nil, &api.Error{Code: "invalid_argument", Message: "POST required"})
			return
		}
		auth := r.Header.Values("Authorization")
		if len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer ") {
			write(w, nil, &api.Error{Code: "permission_denied", Message: "owner grant required"})
			return
		}
		token := strings.TrimPrefix(auth[0], "Bearer ")
		check := func(s api.Scope) bool {
			if !authorize(grants, token, s) {
				write(w, nil, &api.Error{Code: "permission_denied", Message: "scope not granted"})
				return false
			}
			return true
		}
		switch r.URL.Path {
		case "/v1/snapshot":
			var req api.SnapshotRequest
			if e := decode(r, &req); e != nil {
				write(w, nil, e)
				return
			}
			if !check(req.Scope) {
				return
			}
			out, e := store.Snapshot(r.Context(), req)
			write(w, out, e)
		case "/v1/batch":
			var req api.BatchRequest
			if e := decode(r, &req); e != nil {
				write(w, nil, e)
				return
			}
			if !check(req.Scope) {
				return
			}
			if req.RequestID != r.Header.Get("X-Request-ID") {
				write(w, nil, &api.Error{Code: "invalid_argument", Message: "body/header identity mismatch"})
				return
			}
			out, e := store.Batch(r.Context(), req)
			write(w, out, e)
		case "/v1/receipt":
			var req api.ReceiptRequest
			if e := decode(r, &req); e != nil {
				write(w, nil, e)
				return
			}
			if !check(req.Scope) {
				return
			}
			out, e := store.Receipt(r.Context(), req)
			write(w, out, e)
		case "/v1/named":
			var req api.NamedRequest
			if e := decode(r, &req); e != nil {
				write(w, nil, e)
				return
			}
			if !check(req.Scope) {
				return
			}
			if req.RequestID != r.Header.Get("X-Request-ID") {
				write(w, nil, &api.Error{Code: "invalid_argument", Message: "body/header identity mismatch"})
				return
			}
			out, e := store.Named(r.Context(), req)
			write(w, out, e)
		case "/v1/named-result":
			var req api.ReceiptRequest
			if e := decode(r, &req); e != nil {
				write(w, nil, e)
				return
			}
			if !check(req.Scope) {
				return
			}
			out, e := store.NamedResult(r.Context(), req)
			write(w, out, e)
		default:
			write(w, nil, &api.Error{Code: "not_found", Message: "storage operation not found"})
		}
	}), nil
}
