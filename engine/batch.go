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

func (s *Store) Batch(ctx context.Context, r api.BatchRequest) (out api.Receipt, err error) {
	r.Mutations = append([]api.Mutation(nil), r.Mutations...)
	defer func() { err = classify(err) }()
	n, err := s.scope(r.Scope)
	if err != nil {
		return out, err
	}
	if !identifier(r.RequestID) || len(r.Mutations) == 0 || len(r.Mutations) > api.MaxOperations {
		return out, fail("invalid_argument", "request identity and 1..256 mutations required")
	}
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return out, err
	}
	defer release()
	prepared, err := prepareMutations(ctx, n, r.Mutations, api.MaxOperations, api.MaxRequestBytes)
	if err != nil {
		return out, err
	}
	r.Mutations = prepared.mutations
	raw, e := json.Marshal(r)
	if e != nil {
		return out, e
	}
	if len(raw) > api.MaxRequestBytes {
		return out, fail("resource_exhausted", "batch byte limit exceeded")
	}
	digest := hash(r)
	// Replays read durable facts before applying new-write disk admission.
	var cachedDigest string
	var cachedBody []byte
	cacheError := s.reader.QueryRowContext(ctx, "SELECT digest,body FROM receipts WHERE scope=? AND request_id=?", scopeID(r.Scope), r.RequestID).Scan(&cachedDigest, &cachedBody)
	if cacheError == nil {
		if cachedDigest != digest {
			return out, fail("conflict", "request identity reused with different plan")
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
	// Acquire the WAL writer before any read; no snapshot-upgrade busy loop.
	if _, err = tx.ExecContext(ctx, "UPDATE storage_meta SET id=id WHERE id=1"); err != nil {
		return out, err
	}
	id := scopeID(r.Scope)
	var previousDigest string
	var receiptRaw []byte
	var expires int64
	err = tx.QueryRowContext(ctx, "SELECT digest,body,expires FROM receipts WHERE scope=? AND request_id=?", id, r.RequestID).Scan(&previousDigest, &receiptRaw, &expires)
	if err == nil {
		if previousDigest != digest {
			return out, fail("conflict", "request identity reused with different plan")
		}
		if err = json.Unmarshal(receiptRaw, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var rev int64
	err = tx.QueryRowContext(ctx, "SELECT revision FROM scopes WHERE scope=?", id).Scan(&rev)
	if errors.Is(err, sql.ErrNoRows) {
		rev = 0
		var scopes int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM scopes WHERE namespace=?", n.ID).Scan(&scopes); err != nil {
			return out, err
		}
		if scopes >= n.MaxScopes {
			return out, fail("resource_exhausted", "namespace scope quota reached")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO scopes VALUES(?,?,0)", id, n.ID); err != nil {
			return out, err
		}
	} else if err != nil {
		return out, err
	}
	if err = s.checkToken(n, r.Expected, rev); err != nil {
		return out, err
	}
	if rev == math.MaxInt64 {
		return out, fail("resource_exhausted", "revision space exhausted")
	}
	next := rev + 1
	var receiptCount int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM receipts WHERE scope=?", id).Scan(&receiptCount); err != nil {
		return out, err
	}
	if receiptCount >= n.MaxReceipts {
		return out, fail("resource_exhausted", "receipt quota reached; owner maintenance required")
	}
	out = api.Receipt{RequestID: r.RequestID, Digest: digest, Token: s.token(n, next), Durability: "sqlite-full", Versions: []api.Record{}}
	out.Versions, err = s.applyMutations(ctx, tx, id, next, prepared)
	if err != nil {
		return out, err
	}
	now := time.Now().UTC()
	out.CommittedAt = now.Format(time.RFC3339Nano)
	out.ExpiresAt = now.Add(time.Duration(n.ReceiptTTLSeconds) * time.Second).Format(time.RFC3339Nano)
	receiptRaw, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE scopes SET revision=? WHERE scope=?", next, id); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO receipts VALUES(?,?,?,?,?)", id, r.RequestID, digest, now.Unix()+int64(n.ReceiptTTLSeconds), receiptRaw); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return api.Receipt{}, err
	}
	return out, nil
}
func (s *Store) Receipt(ctx context.Context, r api.ReceiptRequest) (out api.Receipt, err error) {
	defer func() { err = classify(err) }()
	if _, err = s.scope(r.Scope); err != nil {
		return out, err
	}
	if !identifier(r.RequestID) {
		return out, fail("invalid_argument", "invalid receipt identity")
	}
	ctx, release, err := s.beginCall(ctx, false)
	if err != nil {
		return out, err
	}
	defer release()
	var raw []byte
	err = s.reader.QueryRowContext(ctx, "SELECT body FROM receipts WHERE scope=? AND request_id=?", scopeID(r.Scope), r.RequestID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, fail("not_found", "receipt not retained; previous outcome may still be unknown")
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}
