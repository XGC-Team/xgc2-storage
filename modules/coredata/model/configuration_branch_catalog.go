package model

// ConfigurationBranchCatalog is one resource's bounded branch list with the
// same-reader current-main visibility fence and immutable head projections.
type ConfigurationBranchCatalog struct {
	Resource    ConfigurationResource       `json:"resource"`
	CurrentMain ConfigurationVisibilityPin  `json:"current_main"`
	Items       []ConfigurationBranchHeader `json:"items"`
}
type ConfigurationBranchHeader struct {
	Branch   ConfigurationBranch `json:"branch"`
	Commit   ConfigurationCommit `json:"commit"`
	Identity []byte              `json:"identity"`
}
