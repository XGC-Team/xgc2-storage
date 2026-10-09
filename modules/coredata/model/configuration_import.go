package model

import "encoding/json"

// ConfigurationImport is an explicit offline owner input, never a Named
// operation. The caller creates a fresh target and supplies its deployment.
type ConfigurationImport struct {
	Format       string                           `json:"format"`
	SourceSHA256 string                           `json:"source_sha256"`
	Domains      []ConfigurationDomainDeclaration `json:"domains"`
	Namespaces   []ConfigurationImportNamespace   `json:"namespaces"`
	Resources    []ConfigurationImportResource    `json:"resources"`
	Branches     []ConfigurationImportBranch      `json:"branches"`
	Snapshots    []ConfigurationImportSnapshot    `json:"snapshots"`
	Changes      []ConfigurationImportChange      `json:"changes"`
}
type ConfigurationImportNamespace struct {
	Domain string                 `json:"domain"`
	Row    ConfigurationNamespace `json:"row"`
}
type ConfigurationImportResource struct {
	Domain string                `json:"domain"`
	Row    ConfigurationResource `json:"row"`
}
type ConfigurationImportBranch struct {
	Domain string              `json:"domain"`
	Row    ConfigurationBranch `json:"row"`
}
type ConfigurationImportSnapshot struct {
	Domain     string                   `json:"domain"`
	Commit     ConfigurationCommit      `json:"commit"`
	Payload    []byte                   `json:"payload"`
	Manifest   []byte                   `json:"manifest"`
	References []ConfigurationReference `json:"references"`
}
type ConfigurationImportChange struct {
	Domain   string          `json:"domain"`
	ID       string          `json:"id"`
	EntityID string          `json:"entity_id"`
	Body     json.RawMessage `json:"body"`
}
