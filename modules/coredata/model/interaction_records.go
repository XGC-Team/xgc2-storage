package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	InteractionsCollection         = "interactions"
	InteractionMutationsCollection = "interaction_mutations"
)

// InteractionCollections participates in the existing finite execution commit.
func InteractionCollections() []api.Collection {
	return []api.Collection{
		{ID: InteractionsCollection, MaxRecordBytes: 3 << 20, MaxRecords: 100000, MaxBytes: 256 << 20, Retention: "Core explicit interaction history retention", Recovery: "storage-owned consistent backup/restore", Indexes: []api.Index{
			{ID: "by_scope", Fields: []string{"targetScope"}},
			{ID: "by_status", Fields: []string{"targetScope", "status"}},
			{ID: "by_kind", Fields: []string{"targetScope", "kind"}},
			{ID: "by_origin", Fields: []string{"targetScope", "originRef"}},
			{ID: "by_idempotency", Fields: []string{"targetScope", "idempotencyKey"}, Unique: true},
			{ID: "by_status_key", Fields: []string{"targetScope", "statusKey"}, Unique: true},
		}},
		{ID: InteractionMutationsCollection, MaxRecordBytes: 16 << 10, MaxRecords: 250000, MaxBytes: 256 << 20, Retention: "Replay ledger retained with interaction history", Recovery: "storage-owned consistent backup/restore"},
	}
}
