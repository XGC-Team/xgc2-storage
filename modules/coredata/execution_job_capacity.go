package coredata

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Independent Job completions merge their capacity changes in the same finite
// writer transaction. Core never prepares a CAS on a shared capacity snapshot.
func prepareJobCapacity(ctx context.Context, tx *sql.Tx, scope string, state []api.Mutation, admissions []model.JobAdmission) ([]api.Mutation, error) {
	limits := make(map[string]model.JobAdmission, len(admissions))
	for _, admission := range admissions {
		if !textKey(admission.RunID) || admission.GlobalLimit < 0 || admission.PerKindLimit < 0 {
			return nil, failure("invalid_argument", "exact nonnegative job admission required")
		}
		if _, exists := limits[admission.RunID]; exists {
			return nil, failure("invalid_argument", "duplicate job admission")
		}
		limits[admission.RunID] = admission
	}
	type lifecycle struct {
		Kind   string `json:"kind"`
		Status string `json:"status"`
	}
	active := func(v lifecycle) bool { return v.Status == "running" || v.Status == "cancel_requested" }
	type change struct {
		id, kind string
		delta    int
	}
	changes := make([]change, 0, len(admissions))
	for _, mutation := range state {
		if mutation.Collection == model.JobCapacityCollection {
			return nil, failure("invalid_argument", "job capacity is maintained by execution transitions")
		}
		if mutation.Collection != model.JobRunsCollection {
			continue
		}
		result, err := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: model.JobRunsCollection, Keys: []string{mutation.Key}, IncludeDeleted: true})
		if err != nil {
			return nil, err
		}
		if len(result.Records) != 1 {
			return nil, failure("data_loss", "job capacity source missing")
		}
		var before, after lifecycle
		old := result.Records[0]
		if old.Version != mutation.ExpectedVersion {
			return nil, failure("conflict", "job transition source changed")
		}
		if !old.Missing && !old.Deleted {
			if err := json.Unmarshal(old.Data, &before); err != nil {
				return nil, err
			}
		}
		if !mutation.Delete {
			if err := json.Unmarshal(mutation.Data, &after); err != nil {
				return nil, err
			}
		}
		if before.Kind != "" && after.Kind != "" && before.Kind != after.Kind {
			return nil, failure("invalid_argument", "job kind is immutable")
		}
		if active(before) == active(after) {
			continue
		}
		if active(after) {
			if _, ok := limits[mutation.Key]; !ok {
				return nil, failure("invalid_argument", "job admission limits required")
			}
			changes = append(changes, change{mutation.Key, after.Kind, 1})
		} else {
			changes = append(changes, change{mutation.Key, before.Kind, -1})
		}
	}
	if len(changes) == 0 {
		if len(limits) > 0 {
			return nil, failure("invalid_argument", "job admission requires a new running claim")
		}
		return state, nil
	}
	result, err := engine.ReadRecords(ctx, tx, scope, api.Query{Collection: model.JobCapacityCollection, Keys: []string{"active"}, IncludeDeleted: true})
	if err != nil {
		return nil, err
	}
	if len(result.Records) != 1 {
		return nil, failure("data_loss", "job capacity record missing")
	}
	record := result.Records[0]
	capacity := struct {
		Active map[string]int `json:"active"`
	}{Active: map[string]int{}}
	if !record.Missing && !record.Deleted {
		if err = json.Unmarshal(record.Data, &capacity); err != nil {
			return nil, err
		}
		if capacity.Active == nil {
			return nil, failure("data_loss", "invalid job capacity")
		}
	}
	total := 0
	for _, count := range capacity.Active {
		if count < 0 {
			return nil, failure("data_loss", "negative job capacity")
		}
		total += count
	}
	// Release existing occupants before admitting any replacements in this action.
	for _, change := range changes {
		if change.delta > 0 {
			continue
		}
		if capacity.Active[change.kind] <= 0 {
			return nil, failure("data_loss", "inconsistent job capacity")
		}
		capacity.Active[change.kind]--
		total--
	}
	for _, change := range changes {
		if change.delta < 0 {
			continue
		}
		limit := limits[change.id]
		if limit.GlobalLimit > 0 && total >= limit.GlobalLimit || limit.PerKindLimit > 0 && capacity.Active[change.kind] >= limit.PerKindLimit {
			return nil, failure("conflict", "job scheduler capacity unavailable")
		}
		capacity.Active[change.kind]++
		total++
		delete(limits, change.id)
	}
	if len(limits) > 0 {
		return nil, failure("invalid_argument", "unused job admission")
	}
	raw, err := json.Marshal(capacity)
	if err != nil {
		return nil, err
	}
	version := record.Version
	return append(state, api.Mutation{Collection: model.JobCapacityCollection, Key: "active", ExpectedVersion: version, Data: raw}), nil
}
