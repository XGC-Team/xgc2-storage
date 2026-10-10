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
//go:embed module.go namespace.go schema.sql model/types.go model/digest.go model/contract.go model/configuration.go model/configuration_limits.go configuration_declaration.go configuration_rows.go configuration_mutation.go configuration_references.go configuration_catalog.go configuration_namespace.go configuration_state.go model/configuration_state.go configuration_metadata.go model/configuration_metadata.go model/configuration_catalog.go model/configuration_declaration.go model/configuration_digest.go model/configuration_manifest.go configuration_source.go configuration_clone.go model/configuration_clone.go configuration_incoming.go model/configuration_incoming.go
var moduleSource embed.FS

func Spec() api.Module {
	h := sha256.New()
	for _, path := range []string{"module.go", "namespace.go", "schema.sql", "model/types.go", "model/digest.go", "model/contract.go", "model/configuration.go", "model/configuration_limits.go", "configuration_declaration.go", "configuration_rows.go", "configuration_mutation.go", "configuration_references.go", "configuration_catalog.go", "configuration_namespace.go", "configuration_state.go", "model/configuration_state.go", "configuration_metadata.go", "model/configuration_metadata.go", "model/configuration_catalog.go", "model/configuration_declaration.go", "model/configuration_digest.go", "model/configuration_manifest.go", "configuration_source.go", "configuration_clone.go", "model/configuration_clone.go", "configuration_incoming.go", "model/configuration_incoming.go"} {
		b, _ := moduleSource.ReadFile(path)
		h.Write([]byte(path + "\x00"))
		h.Write(b)
	}
	return api.Module{ID: model.Module, Schema: Schema, Digest: hex.EncodeToString(h.Sum(nil)), Operations: []api.NamedOperation{
		{ID: model.NamespaceSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationBranchCreateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationBranchArchiveOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationResourceStateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationResourceMetadataOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ResourceCreateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ResourceCommitOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ResourceSnapshotOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationReceiptOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationIncomingOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespaceCloneOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespaceCloneReceiptOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespaceCreateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespaceUpdateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespaceStateOperation, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationNamespacesOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationResourcesOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationBranchesOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationCommitsOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
		{ID: model.ConfigurationChangesOperation, ReadOnly: true, MaxRequestBytes: MaxRequestBytes, MaxResponseBytes: MaxResponseBytes},
	}}
}

// Initialize is only for an explicit new-schema creation, in the owner's
// creation transaction. Existing databases are verified by the engine; no
// compatibility detection, migration, aliases or IF NOT EXISTS are used here.
func Initialize(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, SchemaSQL)
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

// sha256Hex reports whether value is a canonical lowercase SHA-256 digest.
func sha256Hex(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
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
		case model.NamespaceSnapshotOperation:
			var r model.NamespaceRead
			if err = decode(raw, &r); err == nil {
				result, err = namespaceSnapshot(ctx, tx, scope, r)
			}
		case model.ConfigurationBranchCreateOperation:
			var r model.ConfigurationBranchCreate
			if err = decode(raw, &r); err == nil {
				result, err = configurationBranchCreate(ctx, tx, scope, r)
			}
		case model.ConfigurationBranchArchiveOperation:
			var r model.ConfigurationBranchArchive
			if err = decode(raw, &r); err == nil {
				result, err = configurationBranchArchive(ctx, tx, scope, r)
			}
		case model.ConfigurationResourceStateOperation:
			var r model.ConfigurationResourceState
			if err = decode(raw, &r); err == nil {
				result, err = configurationResourceState(ctx, tx, scope, r)
			}
		case model.ConfigurationResourceMetadataOperation:
			var r model.ConfigurationResourceMetadata
			if err = decode(raw, &r); err == nil {
				result, err = configurationResourceMetadata(ctx, tx, scope, r)
			}
		case model.ResourceCreateOperation:
			var r model.ConfigurationResourceCreate
			if err = decode(raw, &r); err == nil {
				result, err = configurationCreate(ctx, tx, scope, r)
			}
		case model.ResourceCommitOperation:
			var r model.ConfigurationResourceCommit
			if err = decode(raw, &r); err == nil {
				result, err = configurationCommit(ctx, tx, scope, r)
			}
		case model.ResourceSnapshotOperation:
			var r model.ConfigurationResourceRead
			if err = decode(raw, &r); err == nil {
				result, err = configurationRead(ctx, tx, scope, r)
			}
		case model.ConfigurationIncomingOperation:
			var r model.ConfigurationIncomingRead
			if err = decode(raw, &r); err == nil {
				result, err = configurationIncomingRead(ctx, tx, scope, r)
			}
		case model.ConfigurationNamespaceCloneOperation:
			var r model.ConfigurationNamespaceClone
			if err = decode(raw, &r); err == nil {
				result, err = configurationClone(ctx, tx, scope, r)
			}
		case model.ConfigurationNamespaceCloneReceiptOperation:
			var r model.ConfigurationNamespaceCloneReceipt
			if err = decode(raw, &r); err == nil {
				result, err = configurationCloneReceipt(ctx, tx, scope, r)
			}
		case model.ConfigurationNamespaceCreateOperation, model.ConfigurationNamespaceUpdateOperation, model.ConfigurationNamespaceStateOperation:
			var r model.ConfigurationNamespaceWrite
			if err = decode(raw, &r); err == nil {
				result, err = configurationNamespaceWrite(ctx, tx, scope, operation, r)
			}
		case model.ConfigurationNamespacesOperation, model.ConfigurationResourcesOperation, model.ConfigurationBranchesOperation, model.ConfigurationCommitsOperation, model.ConfigurationChangesOperation:
			var r model.ConfigurationCatalogRead
			if err = decode(raw, &r); err == nil {
				result, err = configurationCatalog(ctx, tx, scope, operation, r)
			}
		case model.ConfigurationReceiptOperation:
			var r model.ConfigurationReceipt
			if err = decode(raw, &r); err == nil {
				result, err = configurationReceipt(ctx, tx, scope, r)
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
