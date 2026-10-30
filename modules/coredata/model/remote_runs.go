package model

import "time"

// RemoteRunPhase is the durable settlement state of one outbound remote
// control relation. Phases are monotonic: open → settled → released, then the
// relation may resolve. An unbound reservation (no remote process identity)
// stays open and may resolve directly as definitively unsent.
type RemoteRunPhase string

const (
	RemoteRunOpen     RemoteRunPhase = "open"
	RemoteRunSettled  RemoteRunPhase = "settled"
	RemoteRunReleased RemoteRunPhase = "released"
)

// RemoteRunLink is an unresolved control/join relation owned by its accepted Run. It is not a mirror of the remote execution state.
type RemoteRunLink struct {
	OwnerRunID string `json:"ownerRunId"`
	RootRunID  string `json:"rootRunId"`
	TargetID   string `json:"targetId"`
	RunID      string `json:"runId"`
	CallNodeID string `json:"callNodeId"`
	// BootID is the callee's remote-process boot identity (the remote Agent's
	// own boot), bound once at Start. It is never the caller's identity: the
	// two boot-bearing columns must never be compared against each other.
	BootID string `json:"bootId,omitempty"`
	// Phase is durable settlement state, not request input. Callers leave it
	// empty when reserving or resolving; SettleRemoteRun and
	// MarkRemoteRunReleased are the only writers of later phases.
	Phase RemoteRunPhase `json:"phase"`
	// SettledDisposition is the verified terminal/join evidence recorded at
	// settle; SettledAt is the observed remote finish instant. Both are empty
	// until phase leaves open.
	SettledDisposition string    `json:"settledDisposition,omitempty"`
	SettledAt          time.Time `json:"settledAt,omitzero"`
	// ReleasedAt records when the callee's release of the exact incoming hold
	// was durably acknowledged on this relation.
	ReleasedAt time.Time `json:"releasedAt,omitzero"`
}

// RemoteRunSettlement is the caller's verified terminal/join evidence for one
// bound outbound relation. Disposition is the clean remote join outcome and
// FinishedAt the observed remote finish instant; both are required so a
// settled phase always carries its proof.
type RemoteRunSettlement struct {
	Disposition string    `json:"disposition"`
	FinishedAt  time.Time `json:"finishedAt"`
}

// IncomingRemoteHold is the stamped incoming control relation owned by a
// remote-called child Run: the caller's transport-authenticated stable
// identity at admission. TargetID is the caller's enrolled routing target
// and PrincipalID is the enrolled caller principal bound by the admitting
// enrollment scope — which enrollment scope binds it (the crypto pin is the
// caller's key fingerprint; a configured CoreID is only
// configured/hostname-derived) is the ingress owner's contract, not
// re-authenticated here. It is never an ephemeral session or boot value, so
// a restarted caller presenting the same principal still authenticates. The
// Run's immutable parent/root/call lineage completes the exact release
// identity, so the hold needs no separate ledger row. Released marks the
// caller's release; a released hold keeps its stamped identity so exact
// release retries stay idempotent.
type IncomingRemoteHold struct {
	TargetID    string `json:"targetId,omitempty"`
	PrincipalID string `json:"principalId,omitempty"`
	Released    bool   `json:"released,omitempty"`
}

// Active reports whether a stamped hold still awaits the caller's release.
func (h IncomingRemoteHold) Active() bool { return h.TargetID != "" && !h.Released }

// IncomingRunRelease is the caller's exact presented fact for releasing one
// incoming hold: TargetID and PrincipalID are the caller's stable identities
// on the currently accepted transport (enrolled routing target and caller
// principal), and the lineage fields are the historical call facts. Every
// field must equal the stamped hold and the Run's immutable call lineage; a
// disagreement is a foreign or stale call, never a release.
type IncomingRunRelease struct {
	ParentRunID string `json:"parentRunId"`
	RootRunID   string `json:"rootRunId"`
	CallNodeID  string `json:"callNodeId"`
	TargetID    string `json:"targetId"`
	PrincipalID string `json:"principalId"`
}

// RemoteRunResolveResult reports the committed transition of one exact
// remote-control relation. LastForOwner is true only when this call removed
// the final relation owned by OwnerRunID. LastForRoot independently reports
// the final indexed relation for RootRunID. The root and owner finish facts
// are meaningful only then; RootFound distinguishes a missing root from a
// present but still-open root. A zero finish time means that Run is open.
// These are facts at the transaction cut, not a promise that later writes
// cannot create a new relation or otherwise protect history.
type RemoteRunResolveResult struct {
	RootRunID       string             `json:"rootRunId"`
	Changed         bool               `json:"changed"`
	LastForOwner    bool               `json:"lastForOwner"`
	LastForRoot     bool               `json:"lastForRoot"`
	RootFound       bool               `json:"rootFound"`
	RootFinishedAt  time.Time          `json:"rootFinishedAt,omitzero"`
	OwnerFinishedAt time.Time          `json:"ownerFinishedAt,omitzero"`
	Retention       RunRetentionEffect `json:"retention"`
}

type RemoteRunPage struct {
	Links []RemoteRunLink `json:"links"`
	// Next continues after the last selected target relationship.
	Next string `json:"next,omitempty"`
}

const MaxRemoteRunLinks = 1024
const MaxRemoteRunLinksBytes = 512 << 10
const MaxRemoteRunPageSize = 128
