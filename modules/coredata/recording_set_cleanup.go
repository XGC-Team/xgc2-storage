package coredata

import (
	"context"
	"database/sql"
	"errors"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type recordingSetCleanupPlan struct {
	record model.Recording
	shared []string
}

// RecordingSetCleanupPlan selects the immutable recording and the paths of
// possible other owners. Sets are selected by their recorded member path or
// digest; single bags have no mandatory digest, so their exact paths are kept.
// It leaves the source metadata and the gallery publications untouched.
func (s *Store) RecordingSetCleanupPlan(ctx context.Context, guard model.RecordingDelete) (model.Recording, []string, error) {
	plan, err := read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (recordingSetCleanupPlan, error) {
		var plan recordingSetCleanupPlan
		if err := validateRecordingDelete(guard); err != nil {
			return plan, err
		}
		record, err := getRecordingForDeletion(ctx, tx, scope, guard.ID)
		if err != nil {
			return plan, err
		}
		if !sameRecordingIdentity(record, guard) {
			return plan, failure("conflict", "recording identity was reused with other facts")
		}
		if record.Kind != "rosbag-set" {
			return plan, failure("invalid_argument", "selected recording is not a rosbag set")
		}
		plan.record = record
		if record.Summary.LocalPayloadRemoved {
			return plan, nil
		}
		rows, err := tx.QueryContext(ctx, `
WITH selected AS (
  SELECT json_extract(value, '$.path') AS path,
         COALESCE(json_extract(value, '$.sha256'), '') AS sha
  FROM json_each(?, '$.parts')
), candidates AS (
  SELECT other.path AS path FROM recordings AS other
  WHERE other.scope = ? AND other.id <> ? AND other.archive_class = 'rosbag'
    AND other.kind <> 'rosbag-set'
  UNION
  SELECT json_extract(part.value, '$.path') AS path
  FROM recordings AS other, json_each(other.metadata_json, '$.parts') AS part
  WHERE other.scope = ? AND other.id <> ? AND other.kind = 'rosbag-set'
    AND COALESCE(json_extract(other.summary_json, '$.localPayloadRemoved'), 0) <> 1
    AND EXISTS (
      SELECT 1 FROM selected
      WHERE selected.path = json_extract(part.value, '$.path')
         OR selected.sha = '' OR COALESCE(json_extract(part.value, '$.sha256'), '') = ''
         OR selected.sha = json_extract(part.value, '$.sha256')
    )
)
SELECT DISTINCT path FROM candidates WHERE path IS NOT NULL`, string(record.Metadata), scope, record.ID, scope, record.ID)
		if err != nil {
			return plan, err
		}
		defer rows.Close()
		for rows.Next() {
			var path string
			if err := rows.Scan(&path); err != nil {
				return plan, err
			}
			plan.shared = append(plan.shared, path)
		}
		return plan, rows.Err()
	})
	return plan.record, plan.shared, err
}

// CompleteRecordingSetCleanup records release of the set's data while retaining
// its original metadata and source identity for scientific gallery provenance.
func (s *Store) CompleteRecordingSetCleanup(ctx context.Context, guard model.RecordingDelete) (model.Recording, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.Recording{}, err
	}
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Recording, error) {
		if err := validateRecordingDelete(guard); err != nil {
			return model.Recording{}, err
		}
		var previous int64
		record, err := scanRecording(tx.QueryRowContext(ctx, "SELECT "+recordingDeleteColumns+" FROM recordings WHERE scope=? AND id=?", scope, guard.ID), &previous)
		if errors.Is(err, sql.ErrNoRows) {
			return record, failure("not_found", "recording not found")
		}
		if err != nil {
			return record, err
		}
		if !sameRecordingIdentity(record, guard) {
			return record, failure("conflict", "recording identity was reused with other facts")
		}
		if record.Kind != "rosbag-set" {
			return record, failure("invalid_argument", "selected recording is not a rosbag set")
		}
		if err := recordingRunDeletable(ctx, tx, scope, record.RunID); err != nil {
			return record, err
		}
		if record.Summary.LocalPayloadRemoved {
			return record, nil
		}
		record.Summary.LocalPayloadRemoved = true
		summary, err := encode(record.Summary)
		if err != nil {
			return record, err
		}
		bytes := recordingBytes(record, summary)
		if _, err = tx.ExecContext(ctx, "UPDATE recordings SET summary_json=?,bytes=? WHERE scope=? AND id=?", summary, bytes, scope, record.ID); err != nil {
			return record, err
		}
		if err = charge(ctx, tx, scope, 0, bytes-previous); err != nil {
			return record, err
		}
		return record, nil
	})
}
