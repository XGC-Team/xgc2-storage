package model

// Wire identity and limits are shared by the compiled module and consumers.
// The named request/response envelope belongs to storage/api; this package
// defines no second envelope or transport client.
const (
	Schema = "core-relational-v1"
	Module = "coredata"

	NamespaceSnapshotOperation = "namespace.snapshot"

	MaxRequestBytes  = 16 << 20
	MaxResponseBytes = 16 << 20
)
