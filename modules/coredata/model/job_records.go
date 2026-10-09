package model

import "github.com/XGC-Team/xgc2-storage/api"

const (
	JobRunsCollection      = "job_runs"
	JobAttemptsCollection  = "job_attempts"
	JobArtifactsCollection = "job_artifacts"
	JobCapacityCollection  = "job_capacity"
)

// JobCollections declares finite Job facts participating in execution.commit.
func JobCollections() []api.Collection {
	return []api.Collection{
		{ID: JobRunsCollection, MaxRecordBytes: 3 << 20, MaxRecords: 100000, MaxBytes: 256 << 20, Retention: "Core explicit Job history retention", Recovery: "storage-owned consistent backup/restore", Indexes: []api.Index{
			{ID: "by_target", Fields: []string{"targetId"}},
			{ID: "by_status", Fields: []string{"status"}},
			{ID: "by_dedupe", Fields: []string{"targetId", "dedupeKey"}, Unique: true},
			{ID: "by_public_key", Fields: []string{"idempotencyKey"}},
			{ID: "by_target_public_key", Fields: []string{"targetId", "idempotencyKey"}},
		}},
		{ID: JobAttemptsCollection, MaxRecordBytes: 3 << 20, MaxRecords: 100000, MaxBytes: 256 << 20, Retention: "Core explicit Job history retention", Recovery: "storage-owned consistent backup/restore", Indexes: []api.Index{
			{ID: "by_job", Fields: []string{"runId"}}, {ID: "by_number", Fields: []string{"runId", "number"}, Unique: true},
		}},
		{ID: JobArtifactsCollection, MaxRecordBytes: 3 << 20, MaxRecords: 100000, MaxBytes: 256 << 20, Retention: "Core explicit Job history retention", Recovery: "storage-owned consistent backup/restore", Indexes: []api.Index{{ID: "by_job", Fields: []string{"runId"}}}},
		{ID: JobCapacityCollection, MaxRecordBytes: 64 << 10, MaxRecords: 1, MaxBytes: 64 << 10, Retention: "Current finite scheduler capacity", Recovery: "storage-owned consistent backup/restore"},
	}
}
