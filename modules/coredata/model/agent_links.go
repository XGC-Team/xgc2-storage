package model

import (
	"encoding/json"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
)

const (
	MaxAgentLinkRecords     = 2048
	MaxAgentLinkBytes       = 64 << 20
	MaxAgentLinkRecordBytes = 256 << 10
)

// AgentLinkFacts are static enrollment facts. Presence and boot identity stay
// with the caller and never enter this durable representation.
type AgentLinkFacts struct {
	ProtocolMajor                uint32          `json:"protocolMajor"`
	ProtocolMinor                uint32          `json:"protocolMinor"`
	DisplayName                  string          `json:"displayName"`
	BuildIdentity                string          `json:"buildIdentity"`
	AdvertisedManagementEndpoint string          `json:"advertisedManagementEndpoint"`
	PersistentAccessToken        string          `json:"persistentAccessToken"`
	CapabilityManifest           json.RawMessage `json:"capabilityManifest"`
	Capabilities                 []string        `json:"capabilities"`
}

type AgentLinkRecord struct {
	AgentID              string `json:"agentId"`
	Enrollment           string `json:"enrollment"`
	PublicKeyFingerprint string `json:"publicKeyFingerprint"`
	AgentLinkFacts
}

// AgentLinkIdentity binds guards to the physical owner, exact scope and
// admitted Coredata epoch; it does not include a scope-wide revision.
type AgentLinkIdentity struct {
	DatabaseID          string    `json:"databaseId"`
	Scope               api.Scope `json:"scope"`
	ModuleSchemaVersion int       `json:"moduleSchemaVersion"`
}

type AgentLinkGuard struct {
	Identity AgentLinkIdentity `json:"identity"`
	AgentID  string            `json:"agentId"`
	Version  string            `json:"version"`
}

type AgentLinkStored struct {
	Record    AgentLinkRecord `json:"record"`
	Guard     AgentLinkGuard  `json:"guard"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

type AgentLinkList struct {
	Identity AgentLinkIdentity `json:"identity"`
	Records  []AgentLinkStored `json:"records"`
}

type AgentLinkWrite struct {
	Expected AgentLinkGuard  `json:"expected"`
	Record   AgentLinkRecord `json:"record"`
}

type AgentLinkWriteResult struct {
	Stored  AgentLinkStored `json:"stored"`
	Changed bool            `json:"changed"`
}
