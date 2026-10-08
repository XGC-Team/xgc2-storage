// Package model is the SQL-free Core data wire model shared with consumers.
package model

import "encoding/json"

// Core decides policy and immutable pins; storage preserves their complete
// bodies. Parameters have one authoritative copy, shared by event/link views.
type GroupMember struct {
	ItemKey    string          `json:"item_key"`
	ChildID    string          `json:"child_id"`
	EventID    string          `json:"event_id"`
	Parameters []byte          `json:"parameters"`
	Event      json.RawMessage `json:"event"`
	Link       json.RawMessage `json:"link"`
	Body       json.RawMessage `json:"body"`
}

// Guards and Condition fence first admission only. They are excluded from the
// immutable group intent; refreshing them must not create a second preparation.
type GroupPrepare struct {
	ID              string                `json:"id"`
	ParentID        string                `json:"parent_id"`
	InvocationID    string                `json:"invocation_id"`
	GroupKey        string                `json:"group_key"`
	ParentGuard     RecordGuard           `json:"parent_guard"`
	InvocationGuard RecordGuard           `json:"invocation_guard"`
	PinGuard        RecordGuard           `json:"pin_guard"`
	Condition       GroupPrepareCondition `json:"condition"`
	Body            json.RawMessage       `json:"body"`
	Members         []GroupMember         `json:"members"`
}
type GroupPrepared struct {
	ID               string `json:"id"`
	MemberCount      int    `json:"member_count"`
	MembershipDigest string `json:"membership_digest"`
}

type GroupRead struct {
	ID string `json:"id"`
}
type GroupSnapshot struct {
	GroupPrepared
	Body    json.RawMessage `json:"body"`
	Members []GroupMember   `json:"members"`
}

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
type NamespaceCopy struct {
	SourceID         string          `json:"source_id"`
	ExpectedRevision string          `json:"expected_revision"`
	TargetID         string          `json:"target_id"`
	Body             json.RawMessage `json:"body"`
}

// Payload/manifest/references are prepared by the authorized Core domain before
// submission. The service neither interprets them nor invokes a rewrite callback.
type ResourceCopy struct {
	SourceID               string          `json:"source_id"`
	ExpectedRevision       string          `json:"expected_revision"`
	ExpectedBranchRevision string          `json:"expected_branch_revision"`
	SourceCommitID         string          `json:"source_commit_id"`
	SourceContentDigest    string          `json:"source_content_digest"`
	TargetID               string          `json:"target_id"`
	TargetBranchID         string          `json:"target_branch_id"`
	TargetCommitID         string          `json:"target_commit_id"`
	Body                   json.RawMessage `json:"body"`
	BranchBody             json.RawMessage `json:"branch_body"`
	CommitBody             json.RawMessage `json:"commit_body"`
	Payload                []byte          `json:"payload"`
	Manifest               []byte          `json:"manifest"`
	References             []Reference     `json:"references"`
}
type NamespaceClone struct {
	Domain                       string          `json:"domain"`
	SourceID                     string          `json:"source_id"`
	ExpectedRevision             string          `json:"expected_revision"`
	TargetParentID               string          `json:"target_parent_id"`
	ExpectedTargetParentRevision string          `json:"expected_target_parent_revision"`
	TargetID                     string          `json:"target_id"`
	Name                         string          `json:"name"`
	NameKey                      string          `json:"name_key"`
	Namespaces                   []NamespaceCopy `json:"namespaces"`
	Resources                    []ResourceCopy  `json:"resources"`
	ChangeID                     string          `json:"change_id"`
	Change                       json.RawMessage `json:"change"`
}
type NamespaceCloned struct {
	ID             string `json:"id"`
	NamespaceCount int    `json:"namespace_count"`
	ResourceCount  int    `json:"resource_count"`
}

type RecordGuard struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	// Version is the storage Record.Version, not a product body revision.
	Version string `json:"version"`
}
