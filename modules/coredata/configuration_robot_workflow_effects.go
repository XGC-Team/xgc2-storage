package coredata

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The reference rows remain the authority. This derived bit is changed in the
// source mutation transaction and only for its exact previous or new head.
func configurationRobotReferenceLiveness(ctx context.Context, tx *sql.Tx, scope, domain, commit string, references []model.ConfigurationReference, live bool) error {
	if domain != "robot" || len(references) == 0 {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE core_references SET current_main_live=? WHERE scope=? AND domain=? AND commit_id=? AND current_main_live<>?`, live, scope, domain, commit, live)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != int64(len(references)) {
		return failure("data_loss", "current Robot reference liveness disagrees")
	}
	return nil
}

func robotWorkflowTargets(domain string, live bool, references []model.ConfigurationReference) map[string]bool {
	if domain != "robot" || !live || len(references) == 0 {
		return nil
	}
	var out map[string]bool
	for _, ref := range references {
		if strings.HasPrefix(ref.Slot, "workflow-binding:") && ref.TargetDomain == "automation" && ref.Mode == "tracking" && ref.TargetBranch == "main" && ref.TargetComponentID == "" && ref.TargetCommitID == "" {
			if out == nil {
				out = make(map[string]bool)
			}
			out[ref.TargetResourceID] = true
		}
	}
	return out
}

// A label changes only when membership changes or the visible Robot name does.
// Before and after references are already held by the mutation owner.
func robotWorkflowEffects(domain string, beforeLive, afterLive bool, beforeName, afterName string, before, after []model.ConfigurationReference) []string {
	if domain != "robot" {
		return nil
	}
	old, next := robotWorkflowTargets(domain, beforeLive, before), robotWorkflowTargets(domain, afterLive, after)
	if len(old) == 0 && len(next) == 0 {
		return nil
	}
	changed := make([]string, 0)
	for id := range old {
		if !next[id] || beforeName != afterName {
			changed = append(changed, id)
		}
	}
	for id := range next {
		if !old[id] {
			changed = append(changed, id)
		}
	}
	sort.Strings(changed)
	return changed
}

func mergeAffectedWorkflows(groups ...[]string) ([]string, error) {
	seen := map[string]bool{}
	for _, group := range groups {
		for _, id := range group {
			if !textKey(id) {
				return nil, failure("data_loss", "invalid affected Workflow identity")
			}
			seen[id] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func validAffectedWorkflows(ids []string) bool {
	for i, id := range ids {
		if !textKey(id) || i > 0 && ids[i-1] >= id {
			return false
		}
	}
	return true
}
