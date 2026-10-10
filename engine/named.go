package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

func (s *Store) named(scope api.Scope, module, operation string) (api.Namespace, DataModule, api.NamedOperation, error) {
	n, e := s.scope(scope)
	if e != nil {
		return n, DataModule{}, api.NamedOperation{}, e
	}
	for _, m := range n.Modules {
		if m.ID == module {
			for _, o := range m.Operations {
				if o.ID == operation {
					return n, s.modules[m.ID], o, nil
				}
			}
		}
	}
	return n, DataModule{}, api.NamedOperation{}, fail("invalid_argument", "named operation is not registered for namespace")
}

// Named executes only deployment registered data code under the owner's finite
// resources. Product intent arrives as typed data; SQL/callbacks never arrive.
func (s *Store) Named(ctx context.Context, r api.NamedRequest) (out api.NamedResponse, err error) {
	defer func() { err = classify(err) }()
	n, module, operation, err := s.named(r.Scope, r.Module, r.Operation)
	if err != nil {
		return out, err
	}
	if !identifier(r.RequestID) || r.DatabaseID != s.dbid || r.Schema != n.Schema {
		return out, fail("conflict", "named operation database/schema binding mismatch")
	}
	if len(r.Payload) == 0 || len(r.Payload) > operation.MaxRequestBytes {
		return out, fail("resource_exhausted", "named request payload byte limit exceeded")
	}
	view := snapshotContext(ctx)
	if view != nil && !operation.ReadOnly {
		return out, fail("failed_precondition", "mutation cannot use a read snapshot")
	}
	if view == nil {
		var release func()
		ctx, release, err = s.beginCall(ctx, !operation.ReadOnly)
		if err != nil {
			return out, err
		}
		defer release()
	}
	canonical, _, err := canonicalObject(r.Payload)
	if err != nil {
		return out, err
	}
	r.Payload = canonical
	wire, err := json.Marshal(r)
	if err != nil {
		return out, err
	}
	if len(wire) > operation.MaxRequestBytes {
		return out, fail("resource_exhausted", "named wire byte limit exceeded")
	}
	id := scopeID(r.Scope)
	digest := hash(r)
	if operation.ReadOnly {
		var tx *sql.Tx
		if view != nil {
			unlock, e := view.borrow(ctx, s, r.Scope)
			if e != nil {
				return out, e
			}
			defer unlock()
			tx = view.tx
		} else {
			var e error
			tx, e = s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if e != nil {
				return out, e
			}
			defer tx.Rollback()
		}
		out.Result, err = module.Execute(ctx, tx, id, operation.ID, r.Payload)
		if err != nil {
			return out, err
		}
		materialized, e := json.Marshal(out)
		if e != nil {
			return out, e
		}
		if len(materialized) > operation.MaxResponseBytes {
			return out, fail("resource_exhausted", "named materialized response exceeded limit")
		}
		if view == nil {
			err = tx.Commit()
		}
		return out, err
	}
	var cachedDigest string
	var cachedBody []byte
	cacheError := s.reader.QueryRowContext(ctx, "SELECT r.digest,n.body FROM receipts r LEFT JOIN named_results n ON n.scope=r.scope AND n.request_id=r.request_id WHERE r.scope=? AND r.request_id=?", id, r.RequestID).Scan(&cachedDigest, &cachedBody)
	if cacheError == nil {
		if cachedDigest != digest {
			return out, fail("conflict", "request identity reused with different named intent")
		}
		err = json.Unmarshal(cachedBody, &out)
		return out, err
	}
	if !errors.Is(cacheError, sql.ErrNoRows) {
		return out, cacheError
	}
	if err = s.diskCheck(); err != nil {
		return out, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET id=id WHERE id=1"); err != nil {
		return out, err
	}
	var priorDigest string
	err = tx.QueryRowContext(ctx, "SELECT digest FROM receipts WHERE scope=? AND request_id=?", id, r.RequestID).Scan(&priorDigest)
	if err == nil {
		if priorDigest != digest {
			return out, fail("conflict", "request identity reused with different named intent")
		}
		var raw []byte
		if err = tx.QueryRowContext(ctx, "SELECT body FROM named_results WHERE scope=? AND request_id=?", id, r.RequestID).Scan(&raw); err != nil {
			return out, err
		}
		err = json.Unmarshal(raw, &out)
		return out, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var rev int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM scopes WHERE scope=?", id).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE namespace=?", n.ID).Scan(&count); err != nil {
			return out, err
		}
		if count >= n.MaxScopes {
			return out, fail("resource_exhausted", "namespace scope quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,?,0)", id, n.ID); err != nil {
			return out, err
		}
		rev = 0
	} else if err != nil {
		return out, err
	}
	if rev == math.MaxInt64 {
		return out, fail("resource_exhausted", "revision space exhausted")
	}
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM receipts WHERE scope=?", id).Scan(&count); err != nil {
		return out, err
	}
	if count >= n.MaxReceipts {
		return out, fail("resource_exhausted", "named receipt quota reached")
	}
	out.Result, err = module.Execute(ctx, tx, id, operation.ID, r.Payload)
	if err != nil {
		return out, err
	}
	if !json.Valid(out.Result) {
		return out, fail("internal", "module result is invalid JSON")
	}
	now := time.Now().UTC()
	out.Receipt = &api.Receipt{RequestID: r.RequestID, Digest: digest, Token: s.token(n, rev+1), CommittedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(time.Duration(n.ReceiptTTLSeconds) * time.Second).Format(time.RFC3339Nano), Durability: "sqlite-full", Versions: []api.Record{}}
	raw, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if len(raw) > operation.MaxResponseBytes {
		return out, fail("resource_exhausted", "named result plus receipt exceeds response limit")
	}
	receiptRaw, err := json.Marshal(out.Receipt)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE scopes SET revision=? WHERE scope=?", rev+1, id); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO receipts VALUES(?,?,?,?,?)", id, r.RequestID, digest, now.Unix()+int64(n.ReceiptTTLSeconds), receiptRaw); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO named_results VALUES(?,?,?)", id, r.RequestID, raw); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return api.NamedResponse{}, err
	}
	return out, nil
}

// NamedResult recovers the original committed result without executing a plan.
func (s *Store) NamedResult(ctx context.Context, r api.ReceiptRequest) (out api.NamedResponse, err error) {
	defer func() { err = classify(err) }()
	if _, err = s.scope(r.Scope); err != nil {
		return out, err
	}
	if !identifier(r.RequestID) {
		return out, fail("invalid_argument", "invalid named receipt identity")
	}
	ctx, reader, release, err := s.readRows(ctx, r.Scope)
	if err != nil {
		return out, err
	}
	defer release()
	var raw []byte
	err = reader.QueryRowContext(ctx, "SELECT body FROM named_results WHERE scope=? AND request_id=?", scopeID(r.Scope), r.RequestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fail("not_found", "named result not retained; outcome may remain unknown")
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
