package model

// Identity and limits are shared by the compiled module and its consumers.
const (
	Schema = "core-relational-v1"
	Module = "coredata"

	NamespaceSnapshotOperation = "namespace.snapshot"

	MaxRequestBytes  = 16 << 20
	MaxResponseBytes = 16 << 20
)
