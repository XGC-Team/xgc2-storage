package coredata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"github.com/google/uuid"
)

const MaxExecutionEvents = model.MaxExecutionEvents

func optionalIdentity(value string, maximum int, required bool) bool {
	return value == "" && !required || value != "" && utf8.ValidString(value) && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func encodedJSON(raw []byte) bool { return len(raw) == 0 || json.Valid(raw) }

// Keep json.Number representations. In particular, adjacent Unix nanoseconds
// above 2^53 and the distinct encodings 1/1.0 must not collapse through float64.
func exactCommandJSON(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == 0 && len(right) == 0
	}
	canonical := func(raw []byte) ([]byte, error) {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var value any
		if err := d.Decode(&value); err != nil {
			return nil, err
		}
		var tail any
		if err := d.Decode(&tail); err != io.EOF {
			return nil, errors.New("one JSON value required")
		}
		return encode(value)
	}
	a, aerr := canonical(left)
	b, berr := canonical(right)
	return aerr == nil && berr == nil && bytes.Equal(a, b)
}

func validateCommand(r model.CommandRequest) error {
	if !optionalIdentity(r.CommandID, 64, false) || !optionalIdentity(r.RequestID, 128, false) || !optionalIdentity(r.IdempotencyKey, 255, true) || strings.TrimSpace(r.Target) == "" || strings.TrimSpace(r.Action) == "" || !encodedJSON(r.Payload) {
		return failure("invalid_argument", "valid command identity, target, action and encoded JSON required")
	}
	return checkSize(r)
}

func readCommand(ctx context.Context, tx *sql.Tx, scope string, r model.CommandRead) (model.CommandFound, error) {
	var out model.CommandFound
	var body []byte
	var commandID, idempotencyKey, status, action, target string
	if (r.ID == "") == (r.IdempotencyKey == "") || !optionalIdentity(r.ID, 64, false) || !optionalIdentity(r.IdempotencyKey, 255, false) {
		return out, failure("invalid_argument", "exactly one command identity required")
	}
	query, key := "SELECT command_id,idempotency_key,status,action,target,body FROM core_commands WHERE scope=? AND command_id=?", r.ID
	if r.IdempotencyKey != "" {
		query, key = "SELECT command_id,idempotency_key,status,action,target,body FROM core_commands WHERE scope=? AND idempotency_key=?", r.IdempotencyKey
	}
	err := tx.QueryRowContext(ctx, query, scope, key).Scan(&commandID, &idempotencyKey, &status, &action, &target, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	var receipt model.CommandReceipt
	if err = json.Unmarshal(body, &receipt); err != nil || receipt.CommandID != commandID || receipt.IdempotencyKey != idempotencyKey || receipt.Status != status || receipt.Action != action || receipt.Target != target || !encodedJSON(receipt.Payload) || !encodedJSON(receipt.Result) {
		return out, failure("data_loss", "invalid stored command receipt")
	}
	out.Found, out.Receipt = true, &receipt
	return out, nil
}

func findCommand(ctx context.Context, tx *sql.Tx, scope string, r model.CommandRequest) (model.CommandFound, error) {
	if err := validateCommand(r); err != nil {
		return model.CommandFound{}, err
	}
	out, err := readCommand(ctx, tx, scope, model.CommandRead{IdempotencyKey: r.IdempotencyKey})
	if err != nil || !out.Found {
		return out, err
	}
	e := out.Receipt
	if e.Actor != r.Actor || e.Risk != r.Risk || e.Target != r.Target || e.Action != r.Action || e.Reason != r.Reason || r.RequestID != "" && e.RequestID != r.RequestID || r.CommandID != "" && e.CommandID != r.CommandID || !exactCommandJSON(e.Payload, r.Payload) {
		return out, failure("conflict", "command idempotency key reused with another intent")
	}
	return out, nil
}

func randomIdentity() (string, error) {
	id, err := uuid.NewRandom()
	return id.String(), err
}

func acceptCommand(ctx context.Context, tx *sql.Tx, scope string, r model.CommandRequest) (model.CommandReceipt, bool, error) {
	prior, err := findCommand(ctx, tx, scope, r)
	if err != nil {
		return model.CommandReceipt{}, false, err
	}
	if prior.Found {
		return *prior.Receipt, false, nil
	}
	if r.CommandID == "" {
		if r.CommandID, err = randomIdentity(); err != nil {
			return model.CommandReceipt{}, false, err
		}
	}
	if r.RequestID == "" {
		if r.RequestID, err = randomIdentity(); err != nil {
			return model.CommandReceipt{}, false, err
		}
	}
	saved := model.CommandReceipt{CommandRequest: r, Status: "accepted", CreatedAt: time.Now().UTC()}
	body, err := encode(saved)
	if err != nil {
		return model.CommandReceipt{}, false, err
	}
	if err = reserve(ctx, tx, scope, 1, int64(len(body))+1024); err != nil {
		return model.CommandReceipt{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO core_commands VALUES(?,?,?,?,?,?,?)", scope, r.CommandID, r.IdempotencyKey, saved.Status, r.Action, r.Target, body)
	return saved, err == nil, err
}

func completeCommand(ctx context.Context, tx *sql.Tx, scope string, r model.CommandCompletion) (model.CommandReceipt, error) {
	if !optionalIdentity(r.CommandID, 64, true) || r.Result.Status != "succeeded" && r.Result.Status != "failed" && r.Result.Status != "rejected" || !encodedJSON(r.Result.Result) {
		return model.CommandReceipt{}, failure("invalid_argument", "terminal command identity and result required")
	}
	if err := checkSize(r); err != nil {
		return model.CommandReceipt{}, err
	}
	found, err := readCommand(ctx, tx, scope, model.CommandRead{ID: r.CommandID})
	if err != nil {
		return model.CommandReceipt{}, err
	}
	if !found.Found {
		return model.CommandReceipt{}, failure("not_found", "command receipt not found")
	}
	saved := *found.Receipt
	if saved.Status != "accepted" {
		if saved.Status != r.Result.Status || saved.ResultRef != r.Result.ResultRef || saved.ErrorCode != r.Result.ErrorCode || saved.ErrorMessage != r.Result.ErrorMessage || !exactCommandJSON(saved.Result, r.Result.Result) {
			return model.CommandReceipt{}, failure("conflict", "command already has a different terminal result")
		}
		return saved, nil
	}
	old, err := encode(saved)
	if err != nil {
		return model.CommandReceipt{}, err
	}
	at := time.Now().UTC()
	saved.Status, saved.ResultRef, saved.Result = r.Result.Status, r.Result.ResultRef, r.Result.Result
	saved.ErrorCode, saved.ErrorMessage, saved.CompletedAt = r.Result.ErrorCode, r.Result.ErrorMessage, &at
	body, err := encode(saved)
	if err != nil {
		return model.CommandReceipt{}, err
	}
	if err = reserve(ctx, tx, scope, 0, int64(len(body)-len(old))); err != nil {
		return model.CommandReceipt{}, err
	}
	changed, err := tx.ExecContext(ctx, "UPDATE core_commands SET status=?,body=? WHERE scope=? AND command_id=? AND status='accepted'", saved.Status, body, scope, r.CommandID)
	if err != nil {
		return model.CommandReceipt{}, err
	}
	n, err := changed.RowsAffected()
	if err == nil && n != 1 {
		err = failure("conflict", "command terminal transition changed")
	}
	return saved, err
}

func eventCursor(ctx context.Context, tx *sql.Tx, scope string) (model.EventCursor, error) {
	var token string
	if err := tx.QueryRowContext(ctx, "SELECT token FROM core_execution_identity WHERE id=1").Scan(&token); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.EventCursor{}, failure("data_loss", "execution stream identity missing")
		}
		return model.EventCursor{}, err
	}
	if _, err := uuid.Parse(token); err != nil {
		return model.EventCursor{}, failure("data_loss", "invalid execution stream identity")
	}
	h := sha256.Sum256([]byte(token + "\x00" + scope))
	var offset int64
	err := tx.QueryRowContext(ctx, "SELECT last_offset FROM core_event_offsets WHERE scope=?", scope).Scan(&offset)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return model.EventCursor{StreamID: hex.EncodeToString(h[:]), LatestOffset: strconv.FormatInt(offset, 10)}, err
}

func appendExecutionEvents(ctx context.Context, tx *sql.Tx, scope string, inputs []model.ExecutionEventInput) ([]model.ExecutionEvent, error) {
	if len(inputs) > MaxExecutionEvents {
		return nil, failure("resource_exhausted", "execution event batch exceeds 1000")
	}
	events := make([]model.ExecutionEvent, 0, len(inputs))
	for _, input := range inputs {
		if strings.TrimSpace(input.EntityType) == "" || strings.TrimSpace(input.EntityID) == "" || strings.TrimSpace(input.Type) == "" || !encodedJSON(input.Payload) || !optionalIdentity(input.CommandID, 64, false) {
			return nil, failure("invalid_argument", "event entity, type and encoded JSON required")
		}
		if input.Level == "" {
			input.Level = "info"
		}
		if input.CreatedAt.IsZero() {
			input.CreatedAt = time.Now().UTC()
		}
		if err := checkSize(input); err != nil {
			return nil, err
		}
		var priorOffset, priorSeq int64
		err := tx.QueryRowContext(ctx, "SELECT last_offset FROM core_event_offsets WHERE scope=?", scope).Scan(&priorOffset)
		globalNew := errors.Is(err, sql.ErrNoRows)
		if err != nil && !globalNew {
			return nil, err
		}
		err = tx.QueryRowContext(ctx, "SELECT last_seq FROM core_event_sequences WHERE scope=? AND entity_type=? AND entity_id=?", scope, input.EntityType, input.EntityID).Scan(&priorSeq)
		entityNew := errors.Is(err, sql.ErrNoRows)
		if err != nil && !entityNew {
			return nil, err
		}
		if priorOffset == math.MaxInt64 || priorSeq == math.MaxInt64 {
			return nil, failure("resource_exhausted", "execution event counter exhausted")
		}
		event := model.ExecutionEvent{ExecutionEventInput: input, Offset: strconv.FormatInt(priorOffset+1, 10), Seq: strconv.FormatInt(priorSeq+1, 10)}
		body, err := encode(event)
		if err != nil {
			return nil, err
		}
		rows := int64(1)
		if globalNew {
			rows++
		}
		if entityNew {
			rows++
		}
		if err = reserve(ctx, tx, scope, rows, int64(len(body))+rows*1024); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_event_offsets VALUES(?,?) ON CONFLICT(scope) DO UPDATE SET last_offset=excluded.last_offset", scope, priorOffset+1); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_event_sequences VALUES(?,?,?,?) ON CONFLICT(scope,entity_type,entity_id) DO UPDATE SET last_seq=excluded.last_seq", scope, input.EntityType, input.EntityID, priorSeq+1); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO core_execution_events VALUES(?,?,?,?,?,?,?)", scope, priorOffset+1, input.EntityType, input.EntityID, priorSeq+1, input.Type, body); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func decimalCounter(value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil || v < 0 || strconv.FormatInt(v, 10) != value {
		return 0, failure("invalid_argument", "canonical nonnegative decimal counter required")
	}
	return v, nil
}

func readExecutionEvents(ctx context.Context, tx *sql.Tx, scope string, r model.EventRead) (model.EventPage, error) {
	var out model.EventPage
	after, err := decimalCounter(r.AfterOffset)
	if err != nil {
		return out, err
	}
	if r.Limit < 1 || r.Limit > 1000 || r.AfterSeq != "" && (r.EntityType == "" || r.EntityID == "") {
		return out, failure("invalid_argument", "event page limit 1..1000 and complete entity for seq required")
	}
	out.Cursor, err = eventCursor(ctx, tx, scope)
	if err != nil {
		return out, err
	}
	latest, _ := decimalCounter(out.Cursor.LatestOffset)
	through := latest
	if r.Through != "" {
		if through, err = decimalCounter(r.Through); err != nil {
			return out, err
		}
	}
	if after > latest || through > latest || after > through {
		return out, failure("out_of_range", "event cursor ahead of durable stream")
	}
	out.Through, out.NextOffset = strconv.FormatInt(through, 10), r.AfterOffset
	out.Events = []model.ExecutionEvent{}
	query := "SELECT offset,entity_type,entity_id,seq,type,body FROM core_execution_events WHERE scope=? AND offset>? AND offset<=?"
	args := []any{scope, after, through}
	for _, filter := range []struct{ column, value string }{{"entity_type", r.EntityType}, {"entity_id", r.EntityID}, {"type", r.Type}} {
		if filter.value != "" {
			query += " AND " + filter.column + "=?"
			args = append(args, filter.value)
		}
	}
	if r.AfterSeq != "" {
		seq, err := decimalCounter(r.AfterSeq)
		if err != nil {
			return out, err
		}
		query += " AND seq>?"
		args = append(args, seq)
	}
	if r.EntityType != "" && r.EntityID != "" && r.AfterSeq != "" {
		// Each entity's seq and global offset increase together. Use its seq
		// index without sorting the entity's remaining event history by offset.
		query += " ORDER BY seq LIMIT ?"
	} else {
		query += " ORDER BY offset LIMIT ?"
	}
	args = append(args, r.Limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	bytes := 1024
	for rows.Next() {
		var body []byte
		var offset, seq int64
		var entityType, entityID, eventType string
		if err = rows.Scan(&offset, &entityType, &entityID, &seq, &eventType, &body); err != nil {
			return out, err
		}
		bytes += len(body) + 1
		if bytes > MaxResponseBytes {
			return out, failure("resource_exhausted", "execution event page byte limit exceeded")
		}
		var event model.ExecutionEvent
		if err = json.Unmarshal(body, &event); err != nil || event.Offset != strconv.FormatInt(offset, 10) || event.Seq != strconv.FormatInt(seq, 10) || event.EntityType != entityType || event.EntityID != entityID || event.Type != eventType || !encodedJSON(event.Payload) {
			return out, failure("data_loss", "invalid stored execution event")
		}
		out.Events = append(out.Events, event)
		out.NextOffset = event.Offset
	}
	return out, rows.Err()
}
