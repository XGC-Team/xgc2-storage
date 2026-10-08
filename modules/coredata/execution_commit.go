package coredata

import (
	"context"
	"database/sql"

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
	if len(r.State) > model.MaxExecutionStateMutations || len(r.Events) > model.MaxExecutionEvents {
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
		receipt, out.CommandCreated, err = acceptCommand(ctx, tx, scope, *r.Command)
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
	if len(r.State) > 0 {
		out.State, err = engine.ApplyRecords(ctx, tx, scope, r.State)
		if err != nil {
			return out, err
		}
	}
	out.Events, err = appendExecutionEvents(ctx, tx, scope, r.Events)
	return out, err
}
