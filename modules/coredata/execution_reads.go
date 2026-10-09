package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func readExecutionCommands(ctx context.Context, tx *sql.Tx, scope string, r model.CommandListRead) (model.CommandList, error) {
	out := model.CommandList{Receipts: []model.CommandReceipt{}}
	if (len(r.IDs) == 0) == (r.AcceptedAction == "") || len(r.IDs) > 5000 {
		return out, failure("invalid_argument", "one bounded command identity set or accepted action required")
	}
	if r.AcceptedAction != "" && !optionalIdentity(r.AcceptedAction, 128, true) {
		return out, failure("invalid_argument", "canonical accepted action required")
	}
	query := "SELECT command_id,idempotency_key,status,action,target,body FROM core_commands WHERE scope=?"
	args := []any{scope}
	if len(r.IDs) > 0 {
		marks := make([]string, len(r.IDs))
		for i, id := range r.IDs {
			if !optionalIdentity(id, 64, true) {
				return out, failure("invalid_argument", "canonical command IDs required")
			}
			marks[i] = "?"
			args = append(args, id)
		}
		query += " AND command_id IN (" + strings.Join(marks, ",") + ")"
	} else {
		query += " AND action=? AND status='accepted'"
		args = append(args, r.AcceptedAction)
	}
	query += " ORDER BY command_id LIMIT 5001"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, key, status, action, target string
		var raw []byte
		if err = rows.Scan(&id, &key, &status, &action, &target, &raw); err != nil {
			return out, err
		}
		var c model.CommandReceipt
		if err = json.Unmarshal(raw, &c); err != nil || c.CommandID != id || c.IdempotencyKey != key || c.Status != status || c.Action != action || c.Target != target {
			return out, failure("data_loss", "command list identity disagrees with body")
		}
		out.Receipts = append(out.Receipts, c)
		if len(out.Receipts) > 5000 {
			return out, failure("resource_exhausted", "accepted command recovery bound exceeded")
		}
	}
	return out, rows.Err()
}
func readExecutionEventPage(ctx context.Context, tx *sql.Tx, scope string, r model.JobEventPageRead) (model.JobEventPage, error) {
	out := model.JobEventPage{Events: []model.ExecutionEvent{}}
	if r.Page < 1 || r.Page > 1000000 || r.PageSize < 1 || r.PageSize > 100 || utf8.RuneCountInString(r.Query) > 128 {
		return out, failure("invalid_argument", "valid bounded job event page required")
	}
	if r.Level != "" && r.Level != "info" && r.Level != "warn" && r.Level != "error" && r.Level != "debug" {
		return out, failure("invalid_argument", "valid job event level required")
	}
	where := "scope=? AND entity_type='job' AND type LIKE 'job.%'"
	args := []any{scope}
	if r.Level != "" {
		where += " AND json_extract(CAST(body AS TEXT),'$.level')=?"
		args = append(args, r.Level)
	}
	if r.Query != "" {
		where += " AND (entity_id LIKE ? OR type LIKE ? OR json_extract(CAST(body AS TEXT),'$.command_id') LIKE ?)"
		like := "%" + strings.TrimSpace(r.Query) + "%"
		args = append(args, like, like, like)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM core_execution_events WHERE "+where, args...).Scan(&out.Total); err != nil {
		return out, err
	}
	query := "SELECT offset,entity_type,entity_id,seq,type,body FROM core_execution_events WHERE " + where + " ORDER BY offset DESC LIMIT ? OFFSET ?"
	args = append(args, r.PageSize, (r.Page-1)*r.PageSize)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var offset, seq int64
		var kind, id, typ string
		var raw []byte
		if err = rows.Scan(&offset, &kind, &id, &seq, &typ, &raw); err != nil {
			return out, err
		}
		var v model.ExecutionEvent
		if err = json.Unmarshal(raw, &v); err != nil || v.Offset != fmt.Sprint(offset) || v.Seq != fmt.Sprint(seq) || v.EntityType != kind || v.EntityID != id || v.Type != typ {
			return out, failure("data_loss", "job event page identity disagrees with body")
		}
		out.Events = append(out.Events, v)
	}
	return out, rows.Err()
}
