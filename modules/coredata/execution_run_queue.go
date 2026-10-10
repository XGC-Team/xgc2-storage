package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

type storedRunQueueTask struct {
	model.RunReconcileTask
	ClaimToken string `json:"claimToken"`
}

// The queue is merged by the existing single storage writer. Wakes do not CAS
// an already-ready parent, and independent Run creation does not prepare an
// update against a shared sequence outside the writer transaction.
func prepareRunQueue(ctx context.Context, tx *sql.Tx, scope string, state []api.Mutation, queue []model.RunQueueMutation) ([]api.Mutation, error) {
	if len(queue) == 0 {
		return state, nil
	}
	state = append([]api.Mutation(nil), state...)
	positions := map[string]int{}
	for i, m := range state {
		positions[m.Collection+"\x00"+m.Key] = i
	}
	type taskEntry struct {
		task    storedRunQueueTask
		record  api.Record
		changed bool
	}
	tasks := map[string]*taskEntry{}
	order := []string{}
	var sequence struct {
		Value     int64     `json:"value"`
		UpdatedAt time.Time `json:"updatedAt"`
	}
	var sequenceRecord api.Record
	sequenceLoaded, sequenceChanged := false, false
	point := func(collection, key string) (api.Record, error) {
		result, err := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: collection, Keys: []string{key}, IncludeDeleted: true})
		if err != nil {
			return api.Record{}, err
		}
		if len(result.Records) != 1 {
			return api.Record{}, failure("data_loss", "run queue point read missing")
		}
		return result.Records[0], nil
	}
	nextSequence := func(at time.Time) (int64, error) {
		if !sequenceLoaded {
			var err error
			sequenceRecord, err = point(model.RunSequencesCollection, "ready")
			if err != nil {
				return 0, err
			}
			if !sequenceRecord.Missing && !sequenceRecord.Deleted {
				if err = json.Unmarshal(sequenceRecord.Data, &sequence); err != nil {
					return 0, err
				}
			}
			sequenceLoaded = true
		}
		if sequence.Value < 0 || sequence.Value == math.MaxInt64 {
			return 0, failure("resource_exhausted", "run queue sequence exhausted")
		}
		sequence.Value++
		sequence.UpdatedAt = at
		sequenceChanged = true
		return sequence.Value, nil
	}
	for _, q := range queue {
		if !textKey(q.RunID) || q.At.IsZero() {
			return nil, failure("invalid_argument", "exact run queue identity/time required")
		}
		q.At = q.At.UTC()
		if !q.Complete {
			if q.Owner != "" || q.ClaimToken != "" || !q.NextAvailableAt.IsZero() || q.Error != "" {
				return nil, failure("invalid_argument", "wake must not carry a claim completion")
			}
			// A Run created/transitioned in this action is already in the finite state
			// set. Otherwise inspect only its lifecycle field at the exact validated PK.
			var status string
			if p, ok := positions[model.RunsCollection+"\x00"+q.RunID]; ok {
				if state[p].Delete {
					return nil, failure("not_found", "run queue Run not found")
				}
				var run struct {
					Status string `json:"status"`
				}
				if err := json.Unmarshal(state[p].Data, &run); err != nil {
					return nil, err
				}
				status = run.Status
			} else {
				version, err := engine.ReadRecordVersion(ctx, tx, scope, model.RunsCollection, q.RunID)
				if err != nil {
					return nil, err
				}
				if version == "0" {
					return nil, failure("not_found", "run queue Run not found")
				}
				var deleted bool
				if err = tx.QueryRowContext(ctx, "SELECT deleted,json_extract(data,'$.status') FROM records WHERE scope=? AND collection=? AND key=?", scope, model.RunsCollection, q.RunID).Scan(&deleted, &status); err != nil {
					return nil, err
				}
				if deleted {
					return nil, failure("not_found", "run queue Run not found")
				}
			}
			switch status {
			case "succeeded", "failed", "stopped", "canceled", "rejected":
				continue
			case "accepted", "queued", "running", "waiting", "stopping":
			default:
				return nil, failure("data_loss", "run queue Run lifecycle invalid")
			}
		}
		entry := tasks[q.RunID]
		if entry == nil {
			record, err := point(model.RunTasksCollection, q.RunID)
			if err != nil {
				return nil, err
			}
			entry = &taskEntry{record: record}
			if !record.Missing && !record.Deleted {
				if err = json.Unmarshal(record.Data, &entry.task); err != nil {
					return nil, err
				}
			}
			if p, ok := positions[model.RunTasksCollection+"\x00"+q.RunID]; ok {
				m := state[p]
				if m.Delete {
					return nil, failure("invalid_argument", "run queue mutation follows task deletion")
				}
				if err = json.Unmarshal(m.Data, &entry.task); err != nil {
					return nil, err
				}
				entry.changed = true
			}
			if entry.task.RunID != "" && (entry.task.RunID != q.RunID || entry.task.Revision < 1 || entry.task.State.Validate() != nil) {
				return nil, failure("data_loss", "run queue task identity mismatch")
			}
			tasks[q.RunID] = entry
			order = append(order, q.RunID)
		}
		task := &entry.task
		if q.Complete {
			if q.Owner == "" || q.ClaimToken == "" || q.NextAvailableAt.IsZero() || q.NextAvailableAt.Before(q.At) {
				return nil, failure("invalid_argument", "exact run queue completion fence required")
			}
			if task.State != model.RunReconcileTaskClaimed || task.ClaimOwner != q.Owner || task.ClaimToken != q.ClaimToken {
				return nil, failure("conflict", "run queue claim changed")
			}
			task.AvailableAt = q.NextAvailableAt.UTC()
			if task.DirtyGeneration > task.ClaimedGeneration {
				task.AvailableAt = q.At
			}
			value, err := nextSequence(q.At)
			if err != nil {
				return nil, err
			}
			task.ReadySequence = value
			task.LastError = q.Error
			if q.Error != "" {
				task.ConsecutiveErrors++
			} else {
				task.ConsecutiveErrors = 0
			}
			task.State = model.RunReconcileTaskReady
			task.ClaimedGeneration = 0
			task.ClaimOwner = ""
			task.ClaimToken = ""
			task.ClaimExpiresAt = nil
			task.UpdatedAt = q.At
			task.Revision++
			entry.changed = true
			continue
		}
		if task.RunID == "" {
			value, err := nextSequence(q.At)
			if err != nil {
				return nil, err
			}
			task.RunReconcileTask = model.RunReconcileTask{RunID: q.RunID, State: model.RunReconcileTaskReady, AvailableAt: q.At, ReadySequence: value, DirtyGeneration: 1, CreatedAt: q.At, UpdatedAt: q.At, Revision: 1}
			entry.changed = true
		} else if task.State == model.RunReconcileTaskClaimed || task.AvailableAt.After(q.At) {
			// Ready and already due is already an adequate wake. A claimed worker must
			// see a new generation even when its current deadline has not changed.
			task.DirtyGeneration++
			if task.State == model.RunReconcileTaskReady {
				task.AvailableAt = q.At
			}
			task.UpdatedAt = q.At
			task.Revision++
			entry.changed = true
		}
	}
	for _, id := range order {
		entry := tasks[id]
		if !entry.changed {
			continue
		}
		body, err := json.Marshal(entry.task)
		if err != nil {
			return nil, err
		}
		key := model.RunTasksCollection + "\x00" + id
		if p, ok := positions[key]; ok {
			state[p].Data = body
		} else {
			state = append(state, api.Mutation{Collection: model.RunTasksCollection, Key: id, ExpectedVersion: entry.record.Version, Data: body})
		}
	}
	if sequenceChanged {
		if _, ok := positions[model.RunSequencesCollection+"\x00ready"]; ok {
			return nil, failure("invalid_argument", "run queue sequence belongs to owner")
		}
		body, err := json.Marshal(sequence)
		if err != nil {
			return nil, err
		}
		state = append(state, api.Mutation{Collection: model.RunSequencesCollection, Key: "ready", ExpectedVersion: sequenceRecord.Version, Data: body})
	}
	if len(state) > model.MaxExecutionStateMutations {
		return nil, failure("resource_exhausted", "execution state mutation count limit exceeded")
	}
	return state, nil
}
