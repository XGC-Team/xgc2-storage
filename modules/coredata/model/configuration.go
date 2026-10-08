// Configuration first-group wire contract, frozen with the consumer v5 DTO.
// This SQL-free package is the only DTO authority. Names do not imply executable
// registration: coredata.Spec lists only operations implemented by Execute.
package model

// First atomic group operation identities. Deployment availability is separate.
const (
	ConfigurationReceiptOperation = "configuration.receipt"
	ResourceSnapshotOperation     = "resource.snapshot"
	ResourceCreateOperation       = "resource.create"
	ResourceCommitOperation       = "resource.commit"
)

type ConfigurationDomainGuard struct {
	Key            string `json:"key"`
	SchemaIdentity string `json:"schema_identity"`
	SchemaVersion  int    `json:"schema_version"`
	RegistryDigest string `json:"registry_digest"`
}

// Durable product identity, distinct from the expiring SDK request receipt.
// Fix IntentDigest before provider lookup, mutable pin resolution or random IDs.
// Key/IntentDigest stay stable across public retries. The NamedRequest RequestID
// belongs to a distinct prepared full-plan attempt and stays outside this DTO;
// never derive it from Key or reuse it with changed full-plan bytes.
type ConfigurationMutation struct {
	Key          string `json:"key"`
	IntentDigest string `json:"intent_digest"`
	Actor        string `json:"actor"`
	Reason       string `json:"reason"`
}

// Structural root is ID="", ExpectedRevision="0", not a namespace row.
// Every non-root expected revision is a positive canonical decimal string.
type ConfigurationNamespaceGuard struct {
	ID               string `json:"id"`
	ExpectedRevision string `json:"expected_revision"`
}

type ConfigurationBranchGuard struct {
	ID               string `json:"id"`
	ExpectedRevision string `json:"expected_revision"`
	CommitID         string `json:"commit_id"`
	ContentDigest    string `json:"content_digest"`
}

// Only the outgoing target is supplied. Storage derives source kind, resource,
// branch and commit from the operation. TargetVersion is also decimal string.
type ConfigurationReference struct {
	Slot              string `json:"slot"`
	Mode              string `json:"mode"`
	TargetDomain      string `json:"target_domain"`
	TargetResourceID  string `json:"target_resource_id"`
	TargetBranch      string `json:"target_branch"`
	TargetComponentID string `json:"target_component_id"`
	TargetCommitID    string `json:"target_commit_id"`
	TargetVersion     string `json:"target_version"`
	TargetRootDigest  string `json:"target_root_digest"`
}

type ConfigurationNodeChange struct {
	Operation    string `json:"operation"`
	NodeID       string `json:"node_id"`
	BeforePath   string `json:"before_path"`
	AfterPath    string `json:"after_path"`
	BeforeDigest string `json:"before_digest"`
	AfterDigest  string `json:"after_digest"`
}

// One indexed aggregate change may hold the complete ordered node diff.
// No physical per-node table/row requirement; history must expose every delta.
type ConfigurationChange struct {
	ID      string                    `json:"id"`
	Summary string                    `json:"summary"`
	Nodes   []ConfigurationNodeChange `json:"nodes"`
}

// Payload and Manifest are byte slices/base64 on the outer wire. Storage keeps
// their exact decoded bytes. Manifest is the canonical generic node tree JSON;
// payload is a domain-owned frozen codec object, not a SQL row or opaque ID list.
// A robot extension codec uses byte[] for its raw JSON member to retain bytes.
type PreparedConfigurationSnapshot struct {
	RootDigest string                   `json:"root_digest"`
	Payload    []byte                   `json:"payload"`
	Manifest   []byte                   `json:"manifest"`
	References []ConfigurationReference `json:"references"`
	Change     ConfigurationChange      `json:"change"`
}

type ConfigurationResourceCreate struct {
	Domain     ConfigurationDomainGuard      `json:"domain"`
	Mutation   ConfigurationMutation         `json:"mutation"`
	ResourceID string                        `json:"resource_id"`
	BranchID   string                        `json:"branch_id"`
	CommitID   string                        `json:"commit_id"`
	Namespace  ConfigurationNamespaceGuard   `json:"namespace"`
	Name       string                        `json:"name"`
	NameKey    string                        `json:"name_key"`
	System     bool                          `json:"system"`
	SystemKey  string                        `json:"system_key"`
	Snapshot   PreparedConfigurationSnapshot `json:"snapshot"`
}

// With valid guards, equal RootDigest + append=false + changed canonical NameKey
// is ErrInvalid before noop/identity-only; a discarded rename is never success.
// Reference validation uses the prospective transaction state: only this
// replaced branch old outgoing set is excluded from incoming blockers. The
// complete new self-reference set still must resolve against the new manifest.
type ConfigurationResourceCommit struct {
	Domain     ConfigurationDomainGuard `json:"domain"`
	Mutation   ConfigurationMutation    `json:"mutation"`
	ResourceID string                   `json:"resource_id"`
	Branch     ConfigurationBranchGuard `json:"branch"`
	// Exact current-main visibility pin from the same snapshot; mandatory when
	// the domain uses current-main visibility, including nonmain commits.
	Main     ConfigurationBranchGuard `json:"main"`
	CommitID string                   `json:"commit_id"`
	Name     string                   `json:"name"`
	NameKey  string                   `json:"name_key"`
	// nil means retain location; nonnil root guard means move to root.
	MoveTo *ConfigurationNamespaceGuard `json:"move_to,omitempty"`
	// Optional for an ordinary content write, required on global identity change.
	ExpectedResourceRevision string                        `json:"expected_resource_revision,omitempty"`
	AppendUnchangedSnapshot  bool                          `json:"append_unchanged_snapshot"`
	AllowSystemRename        bool                          `json:"allow_system_rename"`
	Snapshot                 PreparedConfigurationSnapshot `json:"snapshot"`
}

// All revisions, NextVersion and Version are positive canonical decimal strings.
// Timestamps are storage-owned UTC RFC3339Nano strings; empty ArchivedAt is live.
type ConfigurationResource struct {
	ID               string `json:"id"`
	NamespaceID      string `json:"namespace_id"`
	Name             string `json:"name"`
	NameKey          string `json:"name_key"`
	Revision         string `json:"revision"`
	MainCommitID     string `json:"main_commit_id"`
	NextVersion      string `json:"next_version"`
	System           bool   `json:"system"`
	SystemKey        string `json:"system_key"`
	OriginResourceID string `json:"origin_resource_id"`
	OriginCommitID   string `json:"origin_commit_id"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	ArchivedAt       string `json:"archived_at"`
}
type ConfigurationBranch struct {
	ID                  string `json:"id"`
	ResourceID          string `json:"resource_id"`
	Name                string `json:"name"`
	NameKey             string `json:"name_key"`
	Revision            string `json:"revision"`
	HeadCommitID        string `json:"head_commit_id"`
	CreatedFromCommitID string `json:"created_from_commit_id"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
	ArchivedAt          string `json:"archived_at"`
}
type ConfigurationCommit struct {
	ID             string `json:"id"`
	ResourceID     string `json:"resource_id"`
	BranchID       string `json:"branch_id"`
	Version        string `json:"version"`
	ParentCommitID string `json:"parent_commit_id"`
	SourceCommitID string `json:"source_commit_id"`
	RootDigest     string `json:"root_digest"`
	ContentDigest  string `json:"content_digest"`
	SchemaVersion  int    `json:"schema_version"`
	Actor          string `json:"actor"`
	Reason         string `json:"reason"`
	ChangeSummary  string `json:"change_summary"`
	CreatedAt      string `json:"created_at"`
}
type ConfigurationHead struct {
	Resource ConfigurationResource `json:"resource"`
	Branch   ConfigurationBranch   `json:"branch"`
	Commit   ConfigurationCommit   `json:"commit"`
}
type ConfigurationPublished struct {
	// created | committed | noop | identity-only; persisted facts only.
	Disposition string            `json:"disposition"`
	Head        ConfigurationHead `json:"head"`
}

// configuration.receipt is catalogued ReadOnly. Its historical persisted
// metadata does not assert current typed target/current-main availability.
// Authorization and outer admission still apply; their errors, NotSent and
// Unknown are returned without automatic lookup, resend or compensation.
type ConfigurationReceipt struct {
	Domain       ConfigurationDomainGuard `json:"domain"`
	Key          string                   `json:"key"`
	IntentDigest string                   `json:"intent_digest"`
	// Normally one operation; calibration named-upsert explicitly allows both.
	Operations []string `json:"operations"`
}
type ConfigurationMutationResult struct {
	Found        bool                   `json:"found"`
	Replayed     bool                   `json:"replayed"`
	Key          string                 `json:"key"`
	Domain       string                 `json:"domain"`
	Operation    string                 `json:"operation"`
	IntentDigest string                 `json:"intent_digest"`
	PlanDigest   string                 `json:"plan_digest"`
	Result       ConfigurationPublished `json:"result"`
}

// Exactly one selector: resource+branch, resource+commit, or namespace+name_key.
// The latter uses current main. Branch defaults explicitly to main in Core.
type ConfigurationResourceRead struct {
	Domain          ConfigurationDomainGuard `json:"domain"`
	ResourceID      string                   `json:"resource_id"`
	Branch          string                   `json:"branch"`
	CommitID        string                   `json:"commit_id"`
	NamespaceID     string                   `json:"namespace_id"`
	NameKey         string                   `json:"name_key"`
	IncludeArchived bool                     `json:"include_archived"`
}
type ConfigurationResourceSnapshot struct {
	Head        ConfigurationHead          `json:"head"`
	CurrentMain ConfigurationVisibilityPin `json:"current_main"`
	Payload     []byte                     `json:"payload"`
	Manifest    []byte                     `json:"manifest"`
	References  []ConfigurationReference   `json:"references"`
}

// Audit lookup is a later operation, not an RPC in this first atomic group.
// Generic manifests use Configuration's existing NodeDraft JSON field names.

// Identity is a generic projection from the authoritative frozen payload
// envelope identity member, not an independently authored/stored copy.
// Envelope shape: {"identity":{...domain selection facts...},"body":{...}}.
// Storage only projects that generic member; Core owns its policy and codecs.
// Selected identity is in Payload; current-main identity is included even when
// an exact historical commit/named branch was selected. If selected==main,
// derive both from the one selected immutable blob; never fetch it twice.
// Maximum identity JSON is 16KiB. Inactive identity is rejected before decoding
// opaque body bytes; typed DecodeStored verifies the envelope's consistency.
type ConfigurationVisibilityPin struct {
	Branch        ConfigurationBranchGuard `json:"branch"`
	SchemaVersion int                      `json:"schema_version"`
	Identity      []byte                   `json:"identity"`
}
