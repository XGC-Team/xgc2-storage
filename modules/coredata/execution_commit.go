package coredata

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// commitExecution applies a finite data action in the owner's existing writer.
// The record algorithm, index codec and deployed class limits belong to engine;
// command/event facts and the outer durable receipt join that same transaction.
func commitExecution(ctx context.Context, tx *sql.Tx, scope string, r model.ExecutionCommit) (out model.ExecutionCommitted, err error) {
	if len(r.State) == 0 && len(r.RunQueue) == 0 && len(r.Events) == 0 && r.Command == nil && r.Completion == nil {
		return out, failure("invalid_argument", "nonempty execution data action required")
	}
	if len(r.RunQueue) > model.MaxExecutionStateMutations || len(r.State) > model.MaxExecutionStateMutations || len(r.Guards) > model.MaxExecutionStateMutations || len(r.Events) > model.MaxExecutionEvents {
		return out, failure("resource_exhausted", "execution data action count limit exceeded")
	}
	if err = checkSize(r); err != nil {
		return out, err
	}
	if r.Command != nil && r.Completion != nil && (r.Command.CommandID == "" || r.Command.CommandID != r.Completion.CommandID) {
		return out, failure("invalid_argument", "combined acceptance/completion requires the same explicit command identity")
	}
	if r.Command != nil {
		var receipt model.CommandReceipt
		acceptedAt := r.CommandAcceptedAt
		if acceptedAt.IsZero() {
			acceptedAt = time.Now().UTC()
		}
		receipt, out.CommandCreated, err = acceptCommandAt(ctx, tx, scope, *r.Command, acceptedAt)
		if err != nil {
			return out, err
		}
		out.Command = &receipt
	}
	if r.Completion != nil {
		prior, err := readCommand(ctx, tx, scope, model.CommandRead{ID: r.Completion.CommandID})
		if err != nil {
			return out, err
		}
		if !prior.Found {
			return out, failure("not_found", "command receipt not found")
		}
		receipt, err := completeCommand(ctx, tx, scope, *r.Completion)
		if err != nil {
			return out, err
		}
		out.Command = &receipt
		out.Replayed = prior.Receipt.Status != "accepted"
	} else if r.Command != nil && !out.CommandCreated {
		out.Replayed = true
	}
	// A business-command replay confirms the original receipt, not a fresh
	// action. In particular it must not repeat state changes or append events.
	if out.Replayed {
		return out, nil
	}
	// Point read guards fence the complete prepared decision without a global
	// scope CAS. Unrelated execution actions can commit independently.
	seen := make(map[string]bool, len(r.Guards))
	for _, guard := range r.Guards {
		key := guard.Collection + "\x00" + guard.Key
		if guard.Collection == "" || guard.Key == "" || seen[key] || !executionGuardVersion(guard.Version) {
			return out, failure("invalid_argument", "unique exact record read guards required")
		}
		seen[key] = true
		version, readErr := engine.ReadRecordVersion(ctx, tx, scope, guard.Collection, guard.Key)
		if readErr != nil {
			return out, readErr
		}
		if version != guard.Version {
			return out, failure("conflict", "execution decision source changed")
		}
	}
	if err = checkCommandAbsence(ctx, tx, scope, r.CommandAbsenceGuards); err != nil {
		return out, err
	}
	if err = configurationValidatePinnedTargets(ctx, tx, scope, r.ConfigurationPins); err != nil {
		return out, err
	}
	if err = configurationValidateMainPins(ctx, tx, scope, r.ConfigurationMainPins); err != nil {
		return out, err
	}
	state, err := prepareRunQueue(ctx, tx, scope, r.State, r.RunQueue)
	if err != nil {
		return out, err
	}
	if len(state) > 0 {
		out.State, err = engine.ApplyRecords(ctx, tx, scope, state)
		if err != nil {
			return out, err
		}
	}
	if len(r.RunQueue) > 0 {
		keys := make([]string, 0, len(r.RunQueue))
		seen := map[string]bool{}
		for _, q := range r.RunQueue {
			if !seen[q.RunID] {
				seen[q.RunID] = true
				keys = append(keys, q.RunID)
			}
		}
		actual, readErr := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: model.RunTasksCollection, Keys: keys})
		if readErr != nil {
			return out, readErr
		}
		positions := map[string]int{}
		for i, record := range out.State {
			if record.Collection == model.RunTasksCollection {
				positions[record.Key] = i
			}
		}
		for _, record := range actual.Records {
			record.Collection = actual.Collection
			if record.Missing || record.Deleted {
				continue
			}
			if i, ok := positions[record.Key]; ok {
				out.State[i] = record
			} else {
				out.State = append(out.State, record)
			}
		}
	}
	out.Events, err = appendExecutionEvents(ctx, tx, scope, r.Events)
	return out, err
}

func executionGuardVersion(raw string) bool {
	v, err := strconv.ParseUint(raw, 10, 64)
	return err == nil && strconv.FormatUint(v, 10) == raw
}

func checkCommandAbsence(ctx context.Context, tx *sql.Tx, scope string, guards []model.CommandAbsenceGuard) error {
	if len(guards) > model.MaxExecutionStateMutations {
		return failure("resource_exhausted", "command guard count limit exceeded")
	}
	seen := map[string]bool{}
	for _, guard := range guards {
		if !textKey(guard.Target) || !textKey(guard.Action) || len(guard.Statuses) < 1 || len(guard.Statuses) > 4 {
			return failure("invalid_argument", "exact command absence predicate required")
		}
		statuses := map[string]bool{}
		args := []any{scope, guard.Target, guard.Action}
		marks := make([]string, len(guard.Statuses))
		for i, status := range guard.Statuses {
			switch status {
			case "accepted", "succeeded", "failed", "rejected":
			default:
				return failure("invalid_argument", "invalid command guard status")
			}
			if statuses[status] {
				return failure("invalid_argument", "duplicate command guard status")
			}
			statuses[status] = true
			marks[i] = "?"
			args = append(args, status)
		}
		key := guard.Target + "\x00" + guard.Action + "\x00" + strings.Join(guard.Statuses, "\x00")
		if seen[key] {
			continue
		}
		seen[key] = true
		var exists int
		err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM core_commands WHERE scope=? AND target=? AND action=? AND status IN ("+strings.Join(marks, ",")+"))", args...).Scan(&exists)
		if err != nil {
			return err
		}
		if exists != 0 {
			return failure("conflict", "command-derived decision fence changed")
		}
	}
	return nil
}
