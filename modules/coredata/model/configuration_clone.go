package model

const ConfigurationNamespaceCloneOperation = "configuration.namespace.clone"

type ConfigurationNamespaceMapping struct {
	SourceID         string `json:"source_id"`
	ExpectedRevision string `json:"expected_revision"`
	TargetID         string `json:"target_id"`
}
type ConfigurationNamespaceClone struct {
	Domain           ConfigurationDomainGuard        `json:"domain"`
	Mutation         ConfigurationMutation           `json:"mutation"`
	SourceID         string                          `json:"source_id"`
	ExpectedRevision string                          `json:"expected_revision"`
	TargetParent     ConfigurationNamespaceGuard     `json:"target_parent"`
	Name             string                          `json:"name"`
	Namespaces       []ConfigurationNamespaceMapping `json:"namespaces"`
	Resources        []ConfigurationResourceCreate   `json:"resources"`
}
type ConfigurationNamespaceCloneReceipt struct {
	Domain   ConfigurationDomainGuard `json:"domain"`
	Mutation ConfigurationMutation    `json:"mutation"`
}
