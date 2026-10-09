package coredata

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// ImportExecution borrows the explicit offline creation/deploy transaction.
// Original command IDs/status/result/times and event seq/offset remain facts;
// none are replayed as new commands or synthesized Named receipts.
func ImportExecution(ctx context.Context, tx *sql.Tx, scope string, in model.ExecutionImport) error {
	return atomicData(ctx, tx, func() error {
		var rows int
		if e := tx.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM core_commands WHERE scope=?)+(SELECT count(*) FROM core_execution_events WHERE scope=?)+(SELECT count(*) FROM core_event_offsets WHERE scope=?)+(SELECT count(*) FROM core_event_sequences WHERE scope=?)", scope, scope, scope, scope).Scan(&rows); e != nil {
			return e
		}
		if rows != 0 {
			return failure("failed_precondition", "execution import requires an empty target")
		}
		for _, v := range in.Commands {
			if e := validateCommand(v.CommandRequest); e != nil {
				return e
			}
			if v.CommandID == "" || v.RequestID == "" || v.CreatedAt.IsZero() || !encodedJSON(v.Result) || (v.Status != "accepted" && v.Status != "succeeded" && v.Status != "failed" && v.Status != "rejected") || (v.Status == "accepted" && v.CompletedAt != nil) || (v.Status != "accepted" && (v.CompletedAt == nil || v.CompletedAt.IsZero())) {
				return failure("data_loss", "invalid original command receipt")
			}
			body, e := encode(v)
			if e != nil {
				return e
			}
			if e = reserve(ctx, tx, scope, 1, int64(len(body))+1024); e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO core_commands VALUES(?,?,?,?,?,?,?)", scope, v.CommandID, v.IdempotencyKey, v.Status, v.Action, v.Target, body); e != nil {
				return e
			}
		}
		frontier, e := strconv.ParseInt(in.EventFrontier, 10, 64)
		if e != nil || frontier < 0 || strconv.FormatInt(frontier, 10) != in.EventFrontier {
			return failure("data_loss", "invalid original event frontier")
		}
		last := int64(0)
		seqs := map[string]int64{}
		for _, v := range in.Events {
			offset, e := strconv.ParseInt(v.Offset, 10, 64)
			if e != nil || offset <= last || offset > frontier || strconv.FormatInt(offset, 10) != v.Offset {
				return failure("data_loss", "invalid original event offset")
			}
			seq, e := strconv.ParseInt(v.Seq, 10, 64)
			if e != nil || seq < 1 || strconv.FormatInt(seq, 10) != v.Seq || !textKey(v.EntityType) || !textKey(v.EntityID) || !textKey(v.Type) || v.CreatedAt.IsZero() || !encodedJSON(v.Payload) || !optionalIdentity(v.CommandID, 64, false) {
				return failure("data_loss", "invalid original event")
			}
			key := v.EntityType + "\x00" + v.EntityID
			if prior := seqs[key]; prior > 0 && seq != prior+1 {
				return failure("data_loss", "original entity sequence is discontinuous")
			}
			seqs[key] = seq
			last = offset
			body, e := encode(v)
			if e != nil {
				return e
			}
			if e = reserve(ctx, tx, scope, 1, int64(len(body))+1024); e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO core_execution_events VALUES(?,?,?,?,?,?,?)", scope, offset, v.EntityType, v.EntityID, seq, v.Type, body); e != nil {
				return e
			}
		}
		if last > frontier {
			return failure("data_loss", "event frontier behind original events")
		}
		if e = reserve(ctx, tx, scope, 1, int64(1024)); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO core_event_offsets VALUES(?,?)", scope, frontier); e != nil {
			return e
		}
		for key, seq := range seqs {
			// Coordinates were checked above; split the exact pair without normalization.
			at := 0
			for key[at] != '\x00' {
				at++
			}
			if e = reserve(ctx, tx, scope, 1, 1024); e != nil {
				return e
			}
			if _, e = tx.ExecContext(ctx, "INSERT INTO core_event_sequences VALUES(?,?,?,?)", scope, key[:at], key[at+1:], seq); e != nil {
				return e
			}
		}
		return nil
	})
}
