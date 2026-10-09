package coredata

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// commitExecution applies a finite data action in the owner's existing writer.
// The record algorithm, index codec and deployed class limits belong to engine;
// command/event facts and the outer durable receipt join that same transaction.
func commitExecution(ctx context.Context, tx *sql.Tx, scope string, r model.ExecutionCommit) (out model.ExecutionCommitted, err error) {
	if len(r.State) == 0 && len(r.Events) == 0 && r.Command == nil && r.Completion == nil {
		return out, failure("invalid_argument", "nonempty execution data action required")
	}
	if len(r.State) > model.MaxExecutionStateMutations || len(r.Guards) > model.MaxExecutionStateMutations || len(r.Events) > model.MaxExecutionEvents {
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
		rows, readErr := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: guard.Collection, Keys: []string{guard.Key}, IncludeDeleted: true})
		if readErr != nil {
			return out, readErr
		}
		if len(rows.Records) != 1 || rows.Records[0].Version != guard.Version {
			return out, failure("conflict", "execution decision source changed")
		}
	}
	if err = configurationValidatePinnedTargets(ctx, tx, scope, r.ConfigurationPins); err != nil {
		return out, err
	}
	if err = configurationValidateMainPins(ctx, tx, scope, r.ConfigurationMainPins); err != nil {
		return out, err
	}
	if len(r.State) > 0 {
		out.State, err = engine.ApplyRecords(ctx, tx, scope, r.State)
		if err != nil {
			return out, err
		}
	}
	out.Events, err = appendExecutionEvents(ctx, tx, scope, r.Events)
	return out, err
}

func executionGuardVersion(raw string) bool {
	v, err := strconv.ParseUint(raw, 10, 64)
	return err == nil && strconv.FormatUint(v, 10) == raw
}
