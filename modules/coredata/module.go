// Package coredata executes bounded relational data operations inside the
// storage owner's transaction. It owns no connection, RPC, scheduler or provider.
package coredata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"

	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/api"
)

const Schema = model.Schema
const MaxRequestBytes = model.MaxRequestBytes
const MaxResponseBytes = model.MaxResponseBytes
const MaxMembers = model.MaxGroupMembers
const MaxParameterBytes = model.MaxGroupParameterBytes
const MaxCloneNamespaces = 1024
const MaxCloneResources = 4096
const MaxReferences = 16384
const MaxScopeRows = 1000000
const MaxScopeBytes = 512 << 20

//go:embed schema.sql
var SchemaSQL string

// Digest the compiled data-module source, including DDL, without including
// tests or deployment files. Engine registration rejects a differing module.
//
//go:embed module.go group.go group_conditions.go namespace.go execution.go execution_commit.go schema.sql model/*.go
var moduleSource embed.FS

func Spec() api.Module {
	h := sha256.New()
	for _, path := range []string{"module.go", "group.go", "group_conditions.go", "namespace.go", "execution.go", "execution_commit.go", "schema.sql", "model/types.go", "model/digest.go", "model/execution.go", "model/session.go", "model/contract.go", "model/group_conditions.go"} {
		b, _ := moduleSource.ReadFile(path)
		h.Write([]byte(path + "\x00"))
		h.Write(b)
	}
	return api.Module{ID: model.Module, Schema: Schema, Digest: hex.EncodeToString(h.Sum(nil)), Operations: []api.NamedOperation{
		{ID: model.GroupPrepareOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.GroupSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.GroupMemberSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.NamespaceCloneOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.NamespaceGetOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.NamespaceSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ExecutionCommitOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ExecutionCommandGetOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ExecutionEventCursorOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ExecutionEventReadOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
	}}
}

// Initialize is only for an explicit new-schema creation, in the owner's
// creation transaction. Existing databases are verified by the engine; no
// compatibility detection, migration, aliases or IF NOT EXISTS are used here.
func Initialize(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, SchemaSQL); err != nil {
		return err
	}
	identity, err := randomIdentity()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO core_execution_identity VALUES(1,?)", identity)
	return err
}

func failure(code, message string) error { return &api.Error{Code: code, Message: message} }
func textKey(s string) bool {
	return s != "" && len(s) <= 255 && utf8.ValidString(s) && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n")
}
func positiveRevision(s string) bool {
	v, err := strconv.ParseInt(s, 10, 64)
	return err == nil && v > 0 && strconv.FormatInt(v, 10) == s
}
func object(raw json.RawMessage) bool {
	return len(raw) > 0 && json.Valid(raw) && bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{"))
}
func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
func digest(v any) string {
	b, _ := encode(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func decode(raw json.RawMessage, into any) error {
	if len(raw) > MaxRequestBytes {
		return failure("resource_exhausted", "core data request byte limit exceeded")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return failure("invalid_argument", "invalid named data request: "+err.Error())
	}
	var tail any
	if err := d.Decode(&tail); err != io.EOF {
		return failure("invalid_argument", "one JSON request required")
	}
	return nil
}
func checkSize(v any) error {
	b, err := encode(v)
	if err != nil {
		return failure("invalid_argument", "invalid JSON data")
	}
	if len(b) > MaxRequestBytes {
		return failure("resource_exhausted", "core data request byte limit exceeded")
	}
	return nil
}

// Execute accepts only these deployment-declared operations. scope is the
// engine's authenticated, canonical scope identity, never an arbitrary path.
// The owner must hold the writer before calling, commit its receipt in this same
// transaction, and roll back on any error. Results are provisional until COMMIT.
func Execute(ctx context.Context, tx *sql.Tx, scope, operation string, raw json.RawMessage) (json.RawMessage, error) {
	if scope == "" || len(scope) > 2048 || !utf8.ValidString(scope) {
		return nil, failure("invalid_argument", "authenticated scope required")
	}
	var encoded json.RawMessage
	err := atomicData(ctx, tx, func() error {
		var err error
		var result any
		switch operation {
		case model.GroupPrepareOperation:
			var r model.GroupPrepare
			if err = decode(raw, &r); err == nil {
				result, err = prepareGroup(ctx, tx, scope, r)
			}
		case model.GroupSnapshotOperation:
			var r model.GroupRead
			if err = decode(raw, &r); err == nil {
				result, err = groupSnapshot(ctx, tx, scope, r)
			}
		case model.GroupMemberSnapshotOperation:
			var r model.GroupMemberRead
			if err = decode(raw, &r); err == nil {
				result, err = groupMemberSnapshot(ctx, tx, scope, r)
			}
		case model.NamespaceCloneOperation:
			var r model.NamespaceClone
			if err = decode(raw, &r); err == nil {
				result, err = cloneNamespace(ctx, tx, scope, r)
			}
		case model.NamespaceSnapshotOperation:
			var r model.NamespaceRead
			if err = decode(raw, &r); err == nil {
				result, err = namespaceSnapshot(ctx, tx, scope, r)
			}
		case model.NamespaceGetOperation:
			var r model.NamespaceRead
			if err = decode(raw, &r); err == nil {
				result, err = namespaceGet(ctx, tx, scope, r)
			}
		case model.ExecutionCommitOperation:
			var r model.ExecutionCommit
			if err = decode(raw, &r); err == nil {
				result, err = commitExecution(ctx, tx, scope, r)
			}
		case model.ExecutionCommandGetOperation:
			var r model.CommandRead
			if err = decode(raw, &r); err == nil {
				result, err = readCommand(ctx, tx, scope, r)
			}
		case model.ExecutionEventCursorOperation:
			var r struct{}
			if err = decode(raw, &r); err == nil {
				result, err = eventCursor(ctx, tx, scope)
			}
		case model.ExecutionEventReadOperation:
			var r model.EventRead
			if err = decode(raw, &r); err == nil {
				result, err = readExecutionEvents(ctx, tx, scope, r)
			}
		default:
			err = failure("invalid_argument", "unregistered core data operation")
		}
		if err != nil {
			return err
		}
		encoded, err = encode(result)
		if err == nil && len(encoded) > MaxResponseBytes {
			return failure("resource_exhausted", "core data response byte limit exceeded")
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func atomicData(ctx context.Context, tx *sql.Tx, work func() error) error {
	if tx == nil {
		return failure("invalid_argument", "owner transaction required")
	}
	if _, err := tx.ExecContext(ctx, "SAVEPOINT core_data_operation"); err != nil {
		return err
	}
	if err := work(); err != nil {
		// Cleanup has its own finite budget when the caller context was canceled.
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, rollback := tx.ExecContext(cleanup, "ROLLBACK TO core_data_operation")
		_, release := tx.ExecContext(cleanup, "RELEASE core_data_operation")
		return errors.Join(err, rollback, release)
	}
	_, err := tx.ExecContext(ctx, "RELEASE core_data_operation")
	return err
}

func reserve(ctx context.Context, tx *sql.Tx, scope string, rows, bytes int64) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO core_data_usage VALUES(?,0,0) ON CONFLICT(scope) DO NOTHING", scope); err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "UPDATE core_data_usage SET rows=rows+?,bytes=bytes+? WHERE scope=? AND rows+?<=? AND bytes+?<=?", rows, bytes, scope, rows, MaxScopeRows, bytes, MaxScopeBytes)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		return failure("resource_exhausted", "core relational scope quota reached")
	}
	return err
}
