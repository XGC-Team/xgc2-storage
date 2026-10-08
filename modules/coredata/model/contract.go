package model

// Wire identity and limits are shared by the compiled module and consumers.
// The named request/response envelope belongs to storage/api; this package
// defines no second envelope or transport client.
const (
	Schema = "core-relational-v1"
	Module = "coredata"

	GroupPrepareOperation         = "group.prepare"
	GroupSnapshotOperation        = "group.snapshot"
	GroupMemberSnapshotOperation  = "group.member.snapshot"
	NamespaceCloneOperation       = "namespace.clone"
	NamespaceGetOperation         = "namespace.get"
	NamespaceSnapshotOperation    = "namespace.snapshot"
	ExecutionCommitOperation      = "execution.commit"
	ExecutionCommandGetOperation  = "execution.command.get"
	ExecutionEventCursorOperation = "execution.events.cursor"
	ExecutionEventReadOperation   = "execution.events.read"

	MaxRequestBytes        = 16 << 20
	MaxResponseBytes       = 16 << 20
	MaxGroupMembers        = 1000
	MaxGroupParameterBytes = 8 << 20
	MaxExecutionEvents     = 1000
)

// This exact consumer operation is under development. Its name alone does not
// imply registration; coredata.Spec is the executable operation catalog.
const (
	SessionWorkflowLogSnapshotOperation = "session.workflow_logs.snapshot"
)
