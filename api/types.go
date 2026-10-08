// Package api defines finite data plans. It never imports an SQL driver.
package api

import "encoding/json"

const Service = "xgc2.storage.v1.Storage"
const Version = "1"
const MaxRequestBytes = 4 << 20
const MaxResponseBytes = 4 << 20
const MaxOperations = 256
const MaxQueries = 64
const MaxRows = 2048

// Revisions are canonical decimal strings to preserve precision in every SDK.
type Scope struct {
	Namespace string `json:"namespace"`
	User      string `json:"user"`
	Workspace string `json:"workspace"`
}
type Token struct {
	DatabaseID string `json:"database_id"`
	Schema     string `json:"schema"`
	Revision   string `json:"revision"`
}
type Query struct {
	Collection     string            `json:"collection"`
	Keys           []string          `json:"keys,omitempty"`
	Index          string            `json:"index,omitempty"`
	Equal          []json.RawMessage `json:"equal,omitempty"`
	After          string            `json:"after,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	IncludeDeleted bool              `json:"include_deleted,omitempty"`
}
type SnapshotRequest struct {
	Scope   Scope   `json:"scope"`
	Queries []Query `json:"queries"`
	// On subsequent pages, this pins the revision; change requires restarting.
	At *Token `json:"at,omitempty"`
}
type Record struct {
	Collection string          `json:"collection,omitempty"`
	Key        string          `json:"key"`
	Version    string          `json:"version"`
	Deleted    bool            `json:"deleted,omitempty"`
	Missing    bool            `json:"missing,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}
type QueryResult struct {
	Collection string   `json:"collection"`
	Records    []Record `json:"records"`
	NextAfter  string   `json:"next_after,omitempty"`
}
type SnapshotResponse struct {
	Scope   Scope         `json:"scope"`
	Token   Token         `json:"token"`
	Results []QueryResult `json:"results"`
}
type Mutation struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	// "0" means never present. A tombstone has a nonzero version.
	ExpectedVersion string          `json:"expected_version"`
	Delete          bool            `json:"delete,omitempty"`
	Data            json.RawMessage `json:"data,omitempty"`
}
type BatchRequest struct {
	Scope     Scope      `json:"scope"`
	Expected  Token      `json:"expected"`
	RequestID string     `json:"request_id"`
	Mutations []Mutation `json:"mutations"`
}
type Receipt struct {
	RequestID   string   `json:"request_id"`
	Digest      string   `json:"digest"`
	Token       Token    `json:"token"`
	CommittedAt string   `json:"committed_at"`
	ExpiresAt   string   `json:"expires_at"`
	Durability  string   `json:"durability"`
	Versions    []Record `json:"versions"`
}
type ReceiptRequest struct {
	Scope     Scope  `json:"scope"`
	RequestID string `json:"request_id"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Manifest is a reviewed deployment input, never a caller supplied RPC schema.
type Manifest struct {
	Format     string      `json:"format"`
	Namespaces []Namespace `json:"namespaces"`
}
type Namespace struct {
	ID                string       `json:"id"`
	Owner             string       `json:"owner"`
	Schema            string       `json:"schema"`
	MaxScopes         int          `json:"max_scopes"`
	MaxReceipts       int          `json:"max_receipts"`
	ReceiptTTLSeconds int          `json:"receipt_ttl_seconds"`
	Collections       []Collection `json:"collections"`
	Modules           []Module     `json:"modules,omitempty"`
}
type Module struct {
	ID         string           `json:"id"`
	Schema     string           `json:"schema"`
	Digest     string           `json:"digest"`
	Operations []NamedOperation `json:"operations"`
}
type NamedOperation struct {
	ID               string `json:"id"`
	ReadOnly         bool   `json:"read_only"`
	MaxRequestBytes  int    `json:"max_request_bytes"`
	MaxResponseBytes int    `json:"max_response_bytes"`
}
type NamedRequest struct {
	Scope      Scope           `json:"scope"`
	DatabaseID string          `json:"database_id"`
	Schema     string          `json:"schema"`
	Module     string          `json:"module"`
	Operation  string          `json:"operation"`
	RequestID  string          `json:"request_id"`
	Payload    json.RawMessage `json:"payload"`
}
type NamedResponse struct {
	Receipt *Receipt        `json:"receipt,omitempty"`
	Result  json.RawMessage `json:"result"`
}
type Collection struct {
	ID             string `json:"id"`
	MaxRecordBytes int    `json:"max_record_bytes"`
	// Includes tombstones, so delete/recreate cannot evade capacity or CAS.
	MaxRecords int     `json:"max_records"`
	MaxBytes   int64   `json:"max_bytes"`
	Indexes    []Index `json:"indexes,omitempty"`
	Retention  string  `json:"retention"`
	Recovery   string  `json:"recovery"`
}
type Index struct {
	ID     string   `json:"id"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique,omitempty"`
}
