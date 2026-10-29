package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const recordingColumns = "id,session_id,run_id,experiment_resource_id,kind,archive_class,path,summary_json,metadata_json,sort_at,created_at"

// recordingDeleteColumns adds the stored row charge; a delete releases exactly
// that persisted value rather than recomputing it.
const recordingDeleteColumns = recordingColumns + ",bytes"

// recordingSummaryColumns replaces the arbitrary metadata by an empty value: a
// list reads and returns only the fixed stored projection.
const recordingSummaryColumns = "id,session_id,run_id,experiment_resource_id,kind,archive_class,path,summary_json,x'',sort_at,created_at"

func scanRecording(row scanner, persistedBytes ...*int64) (model.Recording, error) {
	var v model.Recording
	var class string
	var summary, metadata []byte
	var sortAt, created int64
	fields := []any{&v.ID, &v.SessionID, &v.RunID, &v.ExperimentResourceID, &v.Kind, &class, &v.Path, &summary, &metadata, &sortAt, &created}
	if len(persistedBytes) > 0 {
		fields = append(fields, persistedBytes[0])
	}
	if err := row.Scan(fields...); err != nil {
		return v, err
	}
	v.ArchiveClass = model.RecordingArchiveClass(class)
	v.Metadata = rawOrNil(metadata)
	v.SortAt, v.CreatedAt = instant(sortAt), instant(created)
	if len(summary) == 0 || json.Unmarshal(summary, &v.Summary) != nil {
		return model.Recording{}, failure("data_loss", "stored recording summary is invalid")
	}
	return v, nil
}

func recordingBytes(v model.Recording, summary []byte) int64 {
	return int64(rowOverhead + len(v.ID) + len(v.SessionID) + len(v.RunID) + len(v.ExperimentResourceID) + len(v.Kind) + len(v.ArchiveClass) + len(v.Path) + len(summary) + len(v.Metadata))
}

// recordingArchiveClass is the deterministic archive surface of a recording.
func recordingArchiveClass(kind, path string) model.RecordingArchiveClass {
	switch {
	case kind == "image-gallery":
		return model.RecordingArchiveGallery
	case filepath.Ext(path) == ".bag":
		return model.RecordingArchiveROSBag
	case filepath.Ext(path) == ".mp4":
		return model.RecordingArchiveScreen
	default:
		return model.RecordingArchiveOther
	}
}

type recordingOwner struct {
	targetID       string
	experimentID   string
	experimentName string
	runMode        string
}

// resolveRecordingOwner reads the accepted owner inside the registration
// transaction. A missing, foreign-scope or mismatched owner fails closed.
func resolveRecordingOwner(ctx context.Context, tx *sql.Tx, scope string, q model.NewRecording) (recordingOwner, error) {
	var runTarget, runSession, runExperiment string
	err := tx.QueryRowContext(ctx, "SELECT target_id,session_id,source_experiment_id FROM runs WHERE scope=? AND id=?", scope, q.RunID).Scan(&runTarget, &runSession, &runExperiment)
	if errors.Is(err, sql.ErrNoRows) {
		return recordingOwner{}, failure("not_found", "recording run not found")
	}
	if err != nil {
		return recordingOwner{}, err
	}
	if q.SessionID == "" {
		if runSession != "" {
			return recordingOwner{}, failure("invalid_argument", "a recording of a session run requires its session identity")
		}
		return recordingOwner{targetID: runTarget, experimentID: runExperiment}, nil
	}
	var sessionTarget, sessionExperiment, sessionName, sessionMode string
	err = tx.QueryRowContext(ctx, "SELECT target_id,experiment_resource_id,experiment_name,run_mode FROM sessions WHERE scope=? AND id=?", scope, q.SessionID).Scan(&sessionTarget, &sessionExperiment, &sessionName, &sessionMode)
	if errors.Is(err, sql.ErrNoRows) {
		return recordingOwner{}, failure("not_found", "recording session not found")
	}
	if err != nil {
		return recordingOwner{}, err
	}
	if runSession != q.SessionID || runTarget != sessionTarget {
		return recordingOwner{}, failure("conflict", "recording session does not match its run owner")
	}
	if sessionExperiment == "" {
		return recordingOwner{}, failure("invalid_argument", "recording session has no experiment association")
	}
	return recordingOwner{targetID: sessionTarget, experimentID: sessionExperiment, experimentName: sessionName, runMode: sessionMode}, nil
}

// recordingMetadataSummary projects only the fixed summary fields from the
// registration metadata. Association fields are overwritten from the accepted
// owner afterwards, and unrelated keys are never promoted. A field of the wrong
// type is ignored rather than making an otherwise valid registration unusable.
func recordingMetadataSummary(raw json.RawMessage) model.RecordingSummary {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &fields) != nil {
		return model.RecordingSummary{}
	}
	return model.RecordingSummary{
		RecordingID: summaryText(fields, "recordingId"),
		RecordID:    summaryText(fields, "recordId"),
		BindingID:   summaryText(fields, "bindingId"),
		TimeSource:  summaryText(fields, "timeSource"),
		MediaType:   summaryText(fields, "mediaType"),
		DurationMS:  summaryInt64(fields, "durationMs"),
		Width:       summaryInt(fields, "width"),
		Height:      summaryInt(fields, "height"),
		Status:      summaryText(fields, "status"),
		HasPoster:   summaryBool(fields, "hasPoster"),
		StartedAt:   summaryInstant(fields, "startedAt"),
		EndedAt:     summaryInstant(fields, "endedAt"),
		FinishedAt:  summaryInstant(fields, "finishedAt"),
	}
}

func summaryRaw(fields map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	raw, ok := fields[key]
	if !ok {
		return nil, false
	}
	return raw, true
}

func summaryText(fields map[string]json.RawMessage, key string) string {
	raw, ok := summaryRaw(fields, key)
	if !ok {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}

func summaryInt64(fields map[string]json.RawMessage, key string) int64 {
	raw, ok := summaryRaw(fields, key)
	if !ok {
		return 0
	}
	var value int64
	if json.Unmarshal(raw, &value) != nil {
		return 0
	}
	return value
}

func summaryInt(fields map[string]json.RawMessage, key string) int {
	raw, ok := summaryRaw(fields, key)
	if !ok {
		return 0
	}
	var value int
	if json.Unmarshal(raw, &value) != nil {
		return 0
	}
	return value
}

func summaryBool(fields map[string]json.RawMessage, key string) bool {
	raw, ok := summaryRaw(fields, key)
	if !ok {
		return false
	}
	var value bool
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	return value
}

// summaryInstant accepts only RFC3339/RFC3339Nano. An absent, empty or
// otherwise invalid value sorts at the registration's CreatedAt instead.
func summaryInstant(fields map[string]json.RawMessage, key string) time.Time {
	value := summaryText(fields, key)
	if value == "" {
		return time.Time{}
	}
	instant, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return instant.UTC()
}

func addRecording(ctx context.Context, tx *sql.Tx, scope string, q model.NewRecording) (model.Recording, bool, error) {
	if !textKey(q.ID) || !textKey(q.RunID) || !textKey(q.Kind) {
		return model.Recording{}, false, failure("invalid_argument", "recording, run and kind identities are required")
	}
	if err := optionalKey("session", q.SessionID); err != nil {
		return model.Recording{}, false, err
	}
	if q.Path == "" || len(q.Path) > 4096 || !utf8.ValidString(q.Path) || strings.ContainsRune(q.Path, 0) {
		return model.Recording{}, false, failure("invalid_argument", "a recording path of at most 4096 valid UTF-8 bytes is required")
	}
	metadata, err := jsonField("metadata", q.Metadata, model.MaxRecordingMetadata, false, true)
	if err != nil {
		return model.Recording{}, false, err
	}
	v := model.Recording{ID: q.ID, SessionID: q.SessionID, RunID: q.RunID, Kind: q.Kind, Path: q.Path, Metadata: rawOrNil(metadata), CreatedAt: nowOr(q.At)}
	// The immutable registration is checked before any owner lookup: an exact
	// replay returns the stored facts even after the owning Run/Session history
	// has been pruned. Changing a registration fact is a conflict.
	existing, err := scanRecording(tx.QueryRowContext(ctx, "SELECT "+recordingColumns+" FROM recordings WHERE scope=? AND id=?", scope, q.ID))
	if err == nil {
		if existing.SessionID != v.SessionID || existing.RunID != v.RunID || existing.Kind != v.Kind || existing.Path != v.Path || string(existing.Metadata) != string(v.Metadata) {
			return model.Recording{}, false, failure("conflict", "recording identity already registered with other facts")
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.Recording{}, false, err
	}
	owner, err := resolveRecordingOwner(ctx, tx, scope, q)
	if err != nil {
		return model.Recording{}, false, err
	}
	summary := recordingMetadataSummary(metadata)
	// The accepted owner is authoritative; metadata cannot forge association,
	// target, experiment name or run mode.
	summary.ExperimentName, summary.RunMode, summary.TargetID = owner.experimentName, owner.runMode, owner.targetID
	stored, err := encode(summary)
	if err != nil {
		return model.Recording{}, false, err
	}
	// The list payload is this fixed projection, not the arbitrary metadata.
	// A typed field cannot turn a page into an equally oversized payload.
	if len(stored) > model.MaxRecordingSummaryBytes {
		return model.Recording{}, false, failure("resource_exhausted", "recording summary exceeds its byte limit")
	}
	v.ExperimentResourceID = owner.experimentID
	v.ArchiveClass = recordingArchiveClass(q.Kind, q.Path)
	v.Summary = summary
	v.SortAt = v.CreatedAt
	if !summary.StartedAt.IsZero() {
		v.SortAt = summary.StartedAt
	}
	bytes := recordingBytes(v, stored)
	if err = reserve(ctx, tx, scope, 1, bytes); err != nil {
		return model.Recording{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO recordings(scope,id,session_id,run_id,experiment_resource_id,kind,archive_class,path,summary_json,metadata_json,sort_at,created_at,bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", scope, v.ID, v.SessionID, v.RunID, v.ExperimentResourceID, v.Kind, string(v.ArchiveClass), v.Path, stored, []byte(metadata), nanos(v.SortAt), nanos(v.CreatedAt), bytes)
	return v, true, err
}

func getRecording(ctx context.Context, tx *sql.Tx, scope, id string) (model.Recording, error) {
	if !textKey(id) {
		return model.Recording{}, failure("invalid_argument", "recording identity required")
	}
	v, err := scanRecording(tx.QueryRowContext(ctx, "SELECT "+recordingColumns+" FROM recordings WHERE scope=? AND id=?", scope, id))
	if errors.Is(err, sql.ErrNoRows) {
		return v, failure("not_found", "recording not found")
	}
	return v, err
}

func validateRecordingDelete(q model.RecordingDelete) error {
	if !textKey(q.ID) || !textKey(q.RunID) || !textKey(q.Kind) {
		return failure("invalid_argument", "recording, run and kind identities are required")
	}
	if err := optionalKey("session", q.SessionID); err != nil {
		return err
	}
	if q.Path == "" || len(q.Path) > 4096 || !utf8.ValidString(q.Path) || strings.ContainsRune(q.Path, 0) {
		return failure("invalid_argument", "a recording path of at most 4096 valid UTF-8 bytes is required")
	}
	if q.CreatedAt.IsZero() {
		return failure("invalid_argument", "recording creation time is required")
	}
	return nil
}

// recordingRunDeletable checks the exact owning Run without reading Sessions.
// A missing Run is terminal history already pruned; registration required the
// accepted owner, so it does not block deletion of the stored recording.
func recordingRunDeletable(ctx context.Context, tx *sql.Tx, scope, runID string) error {
	var status string
	var finished int64
	err := tx.QueryRowContext(ctx, "SELECT status,finished_at FROM runs WHERE scope=? AND id=?", scope, runID).Scan(&status, &finished)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !model.RunStatus(status).Terminal() || finished == 0 {
		return failure("conflict", "recording run is not finished")
	}
	return nil
}

func getRecordingForDeletion(ctx context.Context, tx *sql.Tx, scope, id string) (model.Recording, error) {
	v, err := getRecording(ctx, tx, scope, id)
	if err != nil {
		return v, err
	}
	if err = recordingRunDeletable(ctx, tx, scope, v.RunID); err != nil {
		return v, err
	}
	return v, nil
}

func deleteRecording(ctx context.Context, tx *sql.Tx, scope string, q model.RecordingDelete) (model.Recording, error) {
	if err := validateRecordingDelete(q); err != nil {
		return model.Recording{}, err
	}
	var persisted int64
	v, err := scanRecording(tx.QueryRowContext(ctx, "SELECT "+recordingDeleteColumns+" FROM recordings WHERE scope=? AND id=?", scope, q.ID), &persisted)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Recording{}, failure("not_found", "recording not found")
	}
	if err != nil {
		return model.Recording{}, err
	}
	if v.RunID != q.RunID || v.SessionID != q.SessionID || v.Kind != q.Kind || v.Path != q.Path || !v.CreatedAt.Equal(q.CreatedAt) {
		return model.Recording{}, failure("conflict", "recording identity was reused with other facts")
	}
	if err = recordingRunDeletable(ctx, tx, scope, v.RunID); err != nil {
		return model.Recording{}, err
	}
	deleted, err := tx.ExecContext(ctx, "DELETE FROM recordings WHERE scope=? AND id=? AND run_id=? AND session_id=? AND kind=? AND path=? AND created_at=?", scope, v.ID, v.RunID, v.SessionID, v.Kind, v.Path, nanos(v.CreatedAt))
	if err != nil {
		return model.Recording{}, err
	}
	n, err := deleted.RowsAffected()
	if err != nil {
		return model.Recording{}, err
	}
	if n != 1 {
		return model.Recording{}, failure("conflict", "recording identity changed before deletion")
	}
	released, err := tx.ExecContext(ctx, "UPDATE core_data_usage SET rows=rows-1,bytes=bytes-? WHERE scope=? AND rows>=1 AND bytes>=?", persisted, scope, persisted)
	if err != nil {
		return model.Recording{}, err
	}
	n, err = released.RowsAffected()
	if err != nil {
		return model.Recording{}, err
	}
	if n != 1 {
		return model.Recording{}, failure("data_loss", "recording quota disagrees")
	}
	return v, nil
}

func recordingClass(value model.RecordingArchiveClass) (string, error) {
	switch value {
	case "":
		return "", nil
	case model.RecordingArchiveROSBag, model.RecordingArchiveScreen, model.RecordingArchiveGallery, model.RecordingArchiveOther:
		return string(value), nil
	default:
		return "", failure("invalid_argument", "unknown recording archive class")
	}
}

func recordingAscending(value model.RecordingOrder) (bool, error) {
	switch value {
	case "", model.RecordingOldestFirst:
		return true, nil
	case model.RecordingNewestFirst:
		return false, nil
	default:
		return false, failure("invalid_argument", "unknown recording order")
	}
}

func recordingQuery(scope string, f model.RecordingFilter) (query string, args []any, limit int, err error) {
	if limit, err = pageLimit(f.Limit); err != nil {
		return "", nil, 0, err
	}
	ascending, err := recordingAscending(f.Order)
	if err != nil {
		return "", nil, 0, err
	}
	class, err := recordingClass(f.ArchiveClass)
	if err != nil {
		return "", nil, 0, err
	}
	if f.DataFilesOnly {
		if f.ExperimentResourceID == "" || f.ArchiveClass != "" {
			return "", nil, 0, failure("invalid_argument", "a data-file selection requires exactly one experiment and no archive class")
		}
		if f.Kind == "image-gallery" {
			return "", nil, 0, failure("invalid_argument", "a data-file selection cannot select image galleries")
		}
	}
	if f.Kind != "" && f.ArchiveClass != "" && (f.Kind == "image-gallery") != (f.ArchiveClass == model.RecordingArchiveGallery) {
		return "", nil, 0, failure("invalid_argument", "recording kind and archive class disagree")
	}
	where, args := []string{"scope=?"}, []any{scope}
	if f.ExperimentResourceID != "" {
		if !textKey(f.ExperimentResourceID) {
			return "", nil, 0, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		// The nonempty check is the condition of the partial experiment indexes.
		where, args = append(where, "experiment_resource_id=?", "experiment_resource_id<>''"), append(args, f.ExperimentResourceID)
	}
	if f.SessionID != "" {
		if !textKey(f.SessionID) {
			return "", nil, 0, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		// The nonempty check is the condition of the partial session indexes.
		where, args = append(where, "session_id=?", "session_id<>''"), append(args, f.SessionID)
	}
	if f.RunID != "" {
		if !textKey(f.RunID) {
			return "", nil, 0, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		where, args = append(where, "run_id=?"), append(args, f.RunID)
	}
	if f.Kind != "" {
		if !textKey(f.Kind) {
			return "", nil, 0, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		where, args = append(where, "kind=?"), append(args, f.Kind)
	}
	if class != "" {
		where, args = append(where, "archive_class=?"), append(args, class)
	}
	if f.DataFilesOnly {
		// The condition of the partial data-file indexes.
		where = append(where, "archive_class<>'image-gallery'")
	}
	order, compare := "sort_at,id", ">"
	if !ascending {
		order, compare = "sort_at DESC,id DESC", "<"
	}
	if f.Cursor != "" {
		at, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return "", nil, 0, err
		}
		where, args = append(where, "(sort_at,id)"+compare+"(?,?)"), append(args, at, id)
	}
	return "SELECT " + recordingSummaryColumns + " FROM recordings WHERE " + strings.Join(where, " AND ") + " ORDER BY " + order + " LIMIT ?", append(args, limit+1), limit, nil
}

func listRecordings(ctx context.Context, tx *sql.Tx, scope string, f model.RecordingFilter) (model.RecordingPage, error) {
	query, args, limit, err := recordingQuery(scope, f)
	if err != nil {
		return model.RecordingPage{}, err
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return model.RecordingPage{}, err
	}
	defer rows.Close()
	var page model.RecordingPage
	for rows.Next() {
		v, err := scanRecording(rows)
		if err != nil {
			return page, err
		}
		if len(page.Recordings) == limit {
			last := page.Recordings[limit-1]
			page.Next = encodeCursor(nanos(last.SortAt), last.ID)
			break
		}
		page.Recordings = append(page.Recordings, v)
	}
	return page, rows.Err()
}

// AddRecording durably adds a recording to the index. Registering the same
// recording again returns it with created false. The accepted Run and optional
// Session must already exist in this scope.
func (s *Store) AddRecording(ctx context.Context, q model.NewRecording) (model.Recording, bool, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.Recording{}, false, err
	}
	var recording model.Recording
	var created bool
	err := s.db.WriteWithInput(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx, input *engine.WriteInput) (e error) {
		if e = input.Own(ctx, func() { q.Metadata = cloneBounded(q.Metadata, model.MaxRecordingMetadata) }); e != nil {
			return e
		}
		recording, created, e = addRecording(ctx, tx, s.scope, q)
		return e
	})
	if err != nil {
		return model.Recording{}, false, err
	}
	return recording, created, nil
}

func (s *Store) GetRecording(ctx context.Context, id string) (model.Recording, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Recording, error) {
		return getRecording(ctx, tx, scope, id)
	})
}

// GetRecordingForDeletion returns the selected full row and checks, in the same
// read transaction, that its owning Run is finished or already pruned. An open
// Run is a conflict. The API uses this before unlinking a physical file.
func (s *Store) GetRecordingForDeletion(ctx context.Context, id string) (model.Recording, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.Recording, error) {
		return getRecordingForDeletion(ctx, tx, scope, id)
	})
}

// DeleteRecording durably deletes exactly the guarded row, rechecks its owning
// Run lifecycle, and releases its stored row and byte charge once. The physical
// file is the caller's responsibility.
func (s *Store) DeleteRecording(ctx context.Context, q model.RecordingDelete) (model.Recording, error) {
	if err := canceledWrite(ctx); err != nil {
		return model.Recording{}, err
	}
	return write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (model.Recording, error) {
		return deleteRecording(ctx, tx, scope, q)
	})
}

// ListRecordings selects one exact page of the stored summary. The arbitrary
// metadata is never read; GetRecording returns it.
func (s *Store) ListRecordings(ctx context.Context, f model.RecordingFilter) (model.RecordingPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RecordingPage, error) {
		return listRecordings(ctx, tx, scope, f)
	})
}
