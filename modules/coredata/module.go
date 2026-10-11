// Package coredata is the typed Core data module: configuration resources with
// branches and immutable commits, and the durable facts of Runs, Sessions and
// recordings. Every operation is a Go function that runs in one owner
// transaction. The package owns no connection, RPC, scheduler or provider, and
// it decides nothing about workflows; Core keeps those decisions.
package coredata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const Schema = model.Schema
const MaxResponseBytes = model.MaxResponseBytes
const MaxCloneNamespaces = 1024
const MaxCloneResources = 4096
const MaxReferences = 16384

// A scope may hold this many live rows and bytes of Core data. Deleted rows
// free their share.
const MaxScopeRows = 1000000
const MaxScopeBytes = 512 << 20

//go:embed schema.sql
var SchemaSQL string

// RecordsSQL creates the Run, Session and recording tables.
//
//go:embed records.sql
var RecordsSQL string

// SchemaVersion is the current schema version of the Core data module. Version
// 1 is the module that also held the workflow engine's tables.
const SchemaVersion = 2

// Module registers the Core data module with the storage engine.
func Module() engine.Module {
	return engine.Module{ID: model.Module, Version: SchemaVersion, Install: Initialize, Legacy: legacy,
		Migrations: []engine.Migration{{From: 1, Apply: migrate1}}}
}

// Initialize creates the current schema in a database that has none.
func Initialize(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, SchemaSQL+RecordsSQL)
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

// release returns rows and bytes that were deleted to the scope's live total.
func release(ctx context.Context, tx *sql.Tx, scope string, rows, bytes int64) error {
	_, err := tx.ExecContext(ctx, "UPDATE core_data_usage SET rows=max(rows-?,0),bytes=max(bytes-?,0) WHERE scope=?", rows, bytes, scope)
	return err
}

// charge accounts rows and bytes without a limit check, for a change that
// finishes work already admitted (a Run's outcome must be recorded).
func charge(ctx context.Context, tx *sql.Tx, scope string, rows, bytes int64) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO core_data_usage VALUES(?,0,0) ON CONFLICT(scope) DO NOTHING", scope); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE core_data_usage SET rows=max(rows+?,0),bytes=max(bytes+?,0) WHERE scope=?", rows, bytes, scope)
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
