// Package model is the SQL-free Core data wire model shared with consumers.
package model

import "encoding/json"

type NamespaceRow struct {
	ID       string          `json:"id"`
	ParentID string          `json:"parent_id"`
	Name     string          `json:"name"`
	NameKey  string          `json:"name_key"`
	Revision string          `json:"revision"`
	Body     json.RawMessage `json:"body"`
}
type Reference struct {
	Slot             string          `json:"slot"`
	TargetDomain     string          `json:"target_domain"`
	TargetResourceID string          `json:"target_resource_id"`
	TargetCommitID   string          `json:"target_commit_id"`
	Body             json.RawMessage `json:"body"`
}
type ResourceSnapshot struct {
	ID             string          `json:"id"`
	NamespaceID    string          `json:"namespace_id"`
	Name           string          `json:"name"`
	NameKey        string          `json:"name_key"`
	Revision       string          `json:"revision"`
	CommitID       string          `json:"commit_id"`
	BranchID       string          `json:"branch_id"`
	BranchRevision string          `json:"branch_revision"`
	ContentDigest  string          `json:"content_digest"`
	Body           json.RawMessage `json:"body"`
	BranchBody     json.RawMessage `json:"branch_body"`
	CommitBody     json.RawMessage `json:"commit_body"`
	Payload        []byte          `json:"payload"`
	Manifest       []byte          `json:"manifest"`
	References     []Reference     `json:"references"`
}
type NamespaceRead struct {
	Domain string `json:"domain"`
	ID     string `json:"id"`
}
type NamespaceTree struct {
	Namespaces []NamespaceRow     `json:"namespaces"`
	Resources  []ResourceSnapshot `json:"resources"`
}
