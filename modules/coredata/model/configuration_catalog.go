package model

// These are the existing configuration UI's bounded metadata reads. They
// return catalog rows, never materialize typed payloads or accept SQL text.
const (
	ConfigurationNamespacesOperation = "configuration.namespaces"
	ConfigurationResourcesOperation  = "configuration.resources"
	ConfigurationBranchesOperation   = "configuration.branches"
	ConfigurationCommitsOperation    = "configuration.commits"
	ConfigurationChangesOperation    = "configuration.changes"
)

type ConfigurationCatalogRead struct {
	Domain ConfigurationDomainGuard `json:"domain"`
	ID     string                   `json:"id,omitempty"`
	// nil selects all locations; pointer to empty string selects structural root.
	ParentID        *string `json:"parent_id,omitempty"`
	IncludeArchived bool    `json:"include_archived"`
	Limit           int     `json:"limit"`
	Offset          int     `json:"offset"`
}

// ConfigurationSelectedCatalogRead selects one exact owner-derived current
// summary key, with the same scope, namespace and page rules as catalog reads.
type ConfigurationSelectedCatalogRead struct {
	Catalog      ConfigurationCatalogRead `json:"catalog"`
	Selector     string                   `json:"selector"`
	OrdinaryOnly bool                     `json:"ordinary_only"`
}

// ConfigurationCatalogPageRead binds a page to the revision of precisely the
// selected current-main catalog. A nil ExpectedGeneration starts a new cut.
type ConfigurationCatalogPageRead struct {
	Catalog      ConfigurationCatalogRead `json:"catalog"`
	Selector     string                   `json:"selector,omitempty"`
	OrdinaryOnly bool                     `json:"ordinary_only"`
	// ChoiceOnly fences only facts used by an ordinary current-main choice:
	// identity, name, selector, location, visibility, and current commit.
	ChoiceOnly         bool   `json:"choice_only"`
	ExpectedGeneration *int64 `json:"expected_generation,omitempty"`
}

type ConfigurationCatalogHeaderPage struct {
	Headers    []ConfigurationResourceHeader `json:"headers"`
	Choices    []ConfigurationCatalogChoice  `json:"choices"`
	Generation int64                         `json:"generation"`
}

// ConfigurationCatalogChoice omits lineage, branch metadata and authored
// content. Identity and Summary are the immutable admitted projection.
type ConfigurationCatalogChoice struct {
	ResourceID string `json:"resource_id"`
	Name       string `json:"name"`
	CommitID   string `json:"commit_id"`
	Protected  bool   `json:"protected"`
	Identity   []byte `json:"identity"`
	Summary    []byte `json:"summary"`
}

// Body retains namespace metadata written by its owner; identity, location,
// name and revision have their own relational columns.
type ConfigurationNamespace struct {
	ID         string `json:"id"`
	ParentID   string `json:"parent_id"`
	Name       string `json:"name"`
	NameKey    string `json:"name_key"`
	Revision   string `json:"revision"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	ArchivedAt string `json:"archived_at"`
}

// One storage-owned audit aggregate retains the complete ordered node diff.
type ConfigurationChangeRecord struct {
	CreatedAt  string                `json:"created_at,omitempty"`
	Operation  string                `json:"operation"`
	ResourceID string                `json:"resource_id"`
	CommitID   string                `json:"commit_id"`
	Mutation   ConfigurationMutation `json:"mutation"`
	Change     ConfigurationChange   `json:"change"`
	BeforePath string                `json:"before_path"`
	AfterPath  string                `json:"after_path"`
}

const (
	ConfigurationNamespaceCreateOperation = "configuration.namespace.create"
	ConfigurationNamespaceUpdateOperation = "configuration.namespace.update"
	ConfigurationNamespaceStateOperation  = "configuration.namespace.state"
)

type ConfigurationNamespaceWrite struct {
	Domain           ConfigurationDomainGuard     `json:"domain"`
	Mutation         ConfigurationMutation        `json:"mutation"`
	ID               string                       `json:"id"`
	ExpectedRevision string                       `json:"expected_revision"`
	Parent           *ConfigurationNamespaceGuard `json:"parent,omitempty"`
	Name             string                       `json:"name"`
	Archived         bool                         `json:"archived"`
}
type ConfigurationNamespaceResult struct {
	AffectedAgentIDs    []string               `json:"affected_agent_ids"`
	AffectedWorkflowIDs []string               `json:"affected_workflow_ids"`
	Namespace           ConfigurationNamespace `json:"namespace"`
	Replayed            bool                   `json:"replayed"`
}
