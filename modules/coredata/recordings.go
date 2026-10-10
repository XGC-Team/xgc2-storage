package coredata

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

const recordingColumns = "id,session_id,run_id,kind,path,metadata_json,created_at"

func scanRecording(row scanner) (model.Recording, error) {
	var v model.Recording
	var metadata []byte
	var created int64
	if err := row.Scan(&v.ID, &v.SessionID, &v.RunID, &v.Kind, &v.Path, &metadata, &created); err != nil {
		return v, err
	}
	v.Metadata, v.CreatedAt = rawOrNil(metadata), instant(created)
	return v, nil
}

func recordingBytes(v model.Recording) int64 {
	return int64(rowOverhead + len(v.ID) + len(v.SessionID) + len(v.RunID) + len(v.Kind) + len(v.Path) + len(v.Metadata))
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
	existing, err := scanRecording(tx.QueryRowContext(ctx, "SELECT "+recordingColumns+" FROM recordings WHERE scope=? AND id=?", scope, q.ID))
	if err == nil {
		// Registering the same recording again is a replay; a different one is a clash.
		if existing.SessionID != v.SessionID || existing.RunID != v.RunID || existing.Kind != v.Kind || existing.Path != v.Path || string(existing.Metadata) != string(v.Metadata) {
			return model.Recording{}, false, failure("conflict", "recording identity already registered with other facts")
		}
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return model.Recording{}, false, err
	}
	bytes := recordingBytes(v)
	if err = reserve(ctx, tx, scope, 1, bytes); err != nil {
		return model.Recording{}, false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO recordings VALUES(?,?,?,?,?,?,?,?,?)", scope, v.ID, v.SessionID, v.RunID, v.Kind, v.Path, []byte(metadata), nanos(v.CreatedAt), bytes)
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

func listRecordings(ctx context.Context, tx *sql.Tx, scope string, f model.RecordingFilter) (model.RecordingPage, error) {
	limit, err := pageLimit(f.Limit)
	if err != nil {
		return model.RecordingPage{}, err
	}
	where, args := []string{"scope=?"}, []any{scope}
	for column, value := range map[string]string{"session_id": f.SessionID, "run_id": f.RunID, "kind": f.Kind} {
		if value == "" {
			continue
		}
		if !textKey(value) {
			return model.RecordingPage{}, failure("invalid_argument", "filter values must be bounded identifiers")
		}
		where, args = append(where, column+"=?"), append(args, value)
	}
	if f.Cursor != "" {
		at, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return model.RecordingPage{}, err
		}
		where, args = append(where, "(created_at,id)>(?,?)"), append(args, at, id)
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+recordingColumns+" FROM recordings WHERE "+strings.Join(where, " AND ")+" ORDER BY created_at,id LIMIT ?", append(args, limit+1)...)
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
			page.Next = encodeCursor(nanos(last.CreatedAt), last.ID)
			break
		}
		page.Recordings = append(page.Recordings, v)
	}
	return page, rows.Err()
}

// AddRecording durably adds a recording to the index. Registering the same
// recording again returns it with created false.
func (s *Store) AddRecording(ctx context.Context, q model.NewRecording) (recording model.Recording, created bool, err error) {
	err = s.db.Write(ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) (e error) {
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

// ListRecordings pages through recordings oldest first.
func (s *Store) ListRecordings(ctx context.Context, f model.RecordingFilter) (model.RecordingPage, error) {
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RecordingPage, error) {
		return listRecordings(ctx, tx, scope, f)
	})
}
