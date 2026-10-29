package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// RunCreateFaultFact preserves the transaction-local evaluated candidate and
// an exact current identity at its still-owned writer cut. On a confirmed
// rollback, before-state and in-batch creation provenance provide that fact
// without a read; CurrentKnown is false only when it was not established.
type RunCreateFaultFact struct {
	Attempted    model.CreatedRun
	Current      model.Run
	CurrentKnown bool
	Present      bool
}

// RunFaultFact is a current, same-owner observation, not a COMMIT receipt.
// NonCommitted is set only when the physical owner confirmed a rollback.
type RunFaultFact struct {
	NonCommitted bool
	Creates      []RunCreateFaultFact
	Retention    model.RunRetentionEffect
}

// RunWriteFaultError retains the original write disposition and its exact
// post-join, pre-intervening-write revalidation. The original error remains
// unknown even when the current fact is available.
type RunWriteFaultError struct {
	cause error
	state *runFaultState
}

func (e *RunWriteFaultError) Error() string { return e.cause.Error() }
func (e *RunWriteFaultError) Unwrap() error { return e.cause }

func (e *RunWriteFaultError) WaitRevalidation(ctx context.Context) (RunFaultFact, error) {
	var disposition *engine.WriteError
	if !errors.As(e.cause, &disposition) {
		return RunFaultFact{}, failure("unavailable", "original writer disposition unavailable")
	}
	if err := disposition.WaitSettlement(ctx); err != nil {
		return RunFaultFact{}, err
	}
	return copyRunFaultFact(e.state.fact), nil
}

type runFaultState struct {
	scope        string
	policy       model.RunRetentionPolicy
	created      []model.CreatedRun
	finished     []string
	attempt      model.RunRetentionEffect
	fact         RunFaultFact
	tracksWrites bool
}

const runFaultFinishedQuery = `SELECT id,status,finished_at FROM runs WHERE scope=? AND id IN (SELECT value FROM json_each(?))`

func (s *runFaultState) probe(ctx context.Context, tx *sql.Tx, possibleCommit bool) error {
	if !possibleCommit {
		s.fact = s.noCommitFact()
		return nil
	}
	var out RunFaultFact
	if len(s.created) != 0 {
		ids := make([]string, 0, len(s.created))
		for _, candidate := range s.created {
			ids = append(ids, candidate.Run.ID)
		}
		encoded, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT "+runSummaryColumns+" FROM runs WHERE scope=? AND id IN (SELECT value FROM json_each(?))", s.scope, string(encoded))
		if err != nil {
			return err
		}
		current := make(map[string]model.Run, len(ids))
		for rows.Next() {
			run, e := scanRun(rows, model.RunIdentity)
			if e != nil {
				err = e
				break
			}
			current[run.ID] = run
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		out.Creates = make([]RunCreateFaultFact, 0, len(s.created))
		for _, candidate := range s.created {
			run, present := current[candidate.Run.ID]
			out.Creates = append(out.Creates, RunCreateFaultFact{Attempted: candidate, Current: run, CurrentKnown: true, Present: present})
		}
	}
	if len(s.finished) != 0 {
		encoded, err := json.Marshal(s.finished)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, runFaultFinishedQuery, s.scope, string(encoded))
		if err != nil {
			return err
		}
		finished := 0
		for rows.Next() {
			var id, status string
			var at int64
			if err = rows.Scan(&id, &status, &at); err != nil {
				break
			}
			if at > 0 {
				if !model.RunStatus(status).Terminal() {
					err = failure("data_loss", "finished Run has a nonterminal status")
					break
				}
				finished++
			}
		}
		if err == nil {
			err = rows.Err()
		}
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if finished != 0 && finished != len(s.finished) {
			return failure("data_loss", "atomic finished Run cut disagrees")
		}
		if finished != 0 {
			cut := retentionChanges{changedFinish: true, released: s.attempt.EligibilityReleased}
			out.Retention, err = retentionEffect(ctx, tx, s.scope, &cut, s.policy)
			if err != nil {
				return err
			}
		}
	}
	s.fact = out
	return nil
}

func (s *runFaultState) noCommitFact() RunFaultFact {
	out := RunFaultFact{NonCommitted: true}
	inserted := make(map[string]bool, len(s.created))
	for _, candidate := range s.created {
		id := candidate.Run.ID
		if candidate.Created {
			inserted[id] = true
		}
		fact := RunCreateFaultFact{Attempted: candidate, CurrentKnown: true}
		if !candidate.Created && !inserted[id] {
			fact.Present = true
			fact.Current = identityFaultRun(candidate.Run)
		}
		out.Creates = append(out.Creates, fact)
	}
	return out
}

func identityFaultRun(run model.Run) model.Run {
	run.Inputs, run.Trigger, run.SourceObservation = nil, nil, nil
	run.Error, run.ErrorSummary, run.Result, run.Nodes, run.CleanupErrors = nil, nil, nil, nil, nil
	run.CleanupFailed = 0
	run.Projection = model.RunIdentity
	return run
}

func copyRunFaultFact(fact RunFaultFact) RunFaultFact {
	fact.Creates = append([]RunCreateFaultFact(nil), fact.Creates...)
	for i := range fact.Creates {
		fact.Creates[i].Attempted.Run = copyFaultRun(fact.Creates[i].Attempted.Run)
		fact.Creates[i].Current = copyFaultRun(fact.Creates[i].Current)
	}
	return fact
}

func copyFaultRun(run model.Run) model.Run {
	run.Inputs = bytes.Clone(run.Inputs)
	run.Trigger = bytes.Clone(run.Trigger)
	run.SourceObservation = bytes.Clone(run.SourceObservation)
	run.Error = bytes.Clone(run.Error)
	run.ErrorSummary = bytes.Clone(run.ErrorSummary)
	run.Result = bytes.Clone(run.Result)
	run.Nodes = bytes.Clone(run.Nodes)
	run.CleanupErrors = bytes.Clone(run.CleanupErrors)
	return run
}

func runFaultError(err error, state *runFaultState) error {
	var outcome *engine.WriteError
	if state.tracksWrites && err != nil && errors.As(err, &outcome) && outcome.Disposition == engine.WriteUnknown {
		return &RunWriteFaultError{cause: err, state: state}
	}
	return err
}
