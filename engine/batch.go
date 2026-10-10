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

// errReplayed rolls back a transaction that found its own receipt already stored.
var errReplayed = &api.Error{Code: "replayed", Message: "request already committed"}

// Batch applies one atomic compare-and-set group of document mutations. A
// request with a RequestID is idempotent: its receipt is stored and a replay
// of the same intent returns it. A request without one leaves no receipt.
func (s *Store) Batch(ctx context.Context, r api.BatchRequest) (out api.Receipt, err error) {
	defer func() { err = classify(err) }()
	if viewOf(ctx) != nil {
		return out, fail("failed_precondition", "mutation cannot use a read snapshot")
	}
	r.Mutations = append([]api.Mutation(nil), r.Mutations...)
	n, err := s.scope(r.Scope)
	if err != nil {
		return out, err
	}
	if len(r.Mutations) == 0 || len(r.Mutations) > api.MaxOperations || (r.RequestID != "" && !identifier(r.RequestID)) {
		return out, fail("invalid_argument", "1..256 mutations and a canonical request identity (or none) required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.CallBudget)
	defer cancel()
	prepared, err := prepareMutations(ctx, n, r.Mutations, api.MaxOperations, api.MaxRequestBytes)
	if err != nil {
		return out, err
	}
	r.Mutations = prepared.mutations
	id := ScopeID(r.Scope)
	var digest string
	if r.RequestID != "" {
		raw, e := json.Marshal(r)
		if e != nil {
			return out, e
		}
		if len(raw) > api.MaxRequestBytes {
			return out, fail("resource_exhausted", "batch byte limit exceeded")
		}
		digest = hash(r)
		// Replays read durable facts before applying new-write disk admission.
		replay, found, e := s.storedReceipt(ctx, id, r.RequestID, digest)
		if e != nil {
			return out, e
		}
		if found {
			return replay, nil
		}
	}
	class := Durable
	if r.Relaxed {
		class = Relaxed
	}
	var expires int64
	err = s.Write(ctx, class, func(ctx context.Context, tx *sql.Tx) error {
		var rev, receipts int64
		err := tx.QueryRowContext(ctx, "SELECT revision,receipts FROM scopes WHERE scope=?", id).Scan(&rev, &receipts)
		if errors.Is(err, sql.ErrNoRows) {
			var scopes int
			if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE namespace=?", n.ID).Scan(&scopes); err != nil {
				return err
			}
			if scopes >= n.MaxScopes {
				return fail("resource_exhausted", "namespace scope quota reached")
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,?,0,0)", id, n.ID); err != nil {
				return err
			}
			rev, receipts, err = 0, 0, nil
		}
		if err != nil {
			return err
		}
		if r.RequestID != "" {
			var previous string
			var body []byte
			err = tx.QueryRowContext(ctx, "SELECT digest,body FROM receipts WHERE scope=? AND request_id=?", id, r.RequestID).Scan(&previous, &body)
			if err == nil {
				if previous != digest {
					return fail("conflict", "request identity reused with different plan")
				}
				if err = json.Unmarshal(body, &out); err != nil {
					return err
				}
				return errReplayed
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if receipts >= int64(n.MaxReceipts) {
				return fail("resource_exhausted", "receipt quota reached; wait for retained receipts to expire")
			}
		}
		if err = s.checkToken(n, r.Expected, rev); err != nil {
			return err
		}
		if rev == math.MaxInt64 {
			return fail("resource_exhausted", "revision space exhausted")
		}
		next := rev + 1
		out = api.Receipt{RequestID: r.RequestID, Digest: digest, Token: s.token(n, next), Durability: class.String(), Versions: []api.Record{}}
		out.Versions, err = s.applyMutations(ctx, tx, id, next, prepared)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		out.CommittedAt = now.Format(time.RFC3339Nano)
		added := int64(0)
		if r.RequestID != "" {
			expires = now.Unix() + int64(n.ReceiptTTLSeconds)
			out.ExpiresAt = now.Add(time.Duration(n.ReceiptTTLSeconds) * time.Second).Format(time.RFC3339Nano)
			body, err := json.Marshal(out)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO receipts VALUES(?,?,?,?,?)", id, r.RequestID, digest, expires, body); err != nil {
				return err
			}
			added = 1
		}
		_, err = tx.ExecContext(ctx, "UPDATE scopes SET revision=?,receipts=receipts+? WHERE scope=?", next, added, id)
		return err
	})
	if errors.Is(err, errReplayed) {
		return out, nil
	}
	if err != nil {
		return api.Receipt{}, err
	}
	if expires != 0 && s.maintenance != nil {
		s.maintenance.armExpiry(expires)
	}
	return out, nil
}

// storedReceipt returns an earlier outcome of the same request intent.
func (s *Store) storedReceipt(ctx context.Context, scope, requestID, digest string) (out api.Receipt, found bool, err error) {
	var previous string
	var body []byte
	err = s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, "SELECT digest,body FROM receipts WHERE scope=? AND request_id=?", scope, requestID).Scan(&previous, &body)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil || !found {
		return out, false, err
	}
	if previous != digest {
		return out, false, fail("conflict", "request identity reused with different plan")
	}
	return out, true, json.Unmarshal(body, &out)
}

func (s *Store) Receipt(ctx context.Context, r api.ReceiptRequest) (out api.Receipt, err error) {
	if _, err = s.scope(r.Scope); err != nil {
		return out, classify(err)
	}
	if !identifier(r.RequestID) {
		return out, fail("invalid_argument", "invalid receipt identity")
	}
	var raw []byte
	found := false
	err = s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, "SELECT body FROM receipts WHERE scope=? AND request_id=?", ScopeID(r.Scope), r.RequestID).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return out, err
	}
	if !found {
		return out, fail("not_found", "receipt not retained; previous outcome may still be unknown")
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
