package coredata

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// Every input here is existing immutable owner data. This bounded reverse
// traversal stays inside the mutation transaction, so both sides of an unlink
// survive in its durable receipt. It follows only live current-main tracking
// references and reads only small association/reference metadata, never Specs.
const maxAffectedAgentWork = 16384

type affectedResource struct{ domain, id string }

func configurationAffectedAgents(ctx context.Context, tx *sql.Tx, scope, domain, id string) ([]string, error) {
	queue := []affectedResource{{domain, id}}
	visited := map[affectedResource]bool{}
	agents := map[string]bool{}
	work, bytes := 0, 0
	step := func(size int) error {
		work++
		bytes += size
		if work > maxAffectedAgentWork || bytes > model.MaxConfigurationDecodedBytes {
			return failure("resource_exhausted", "affected Agent dependency traversal exceeds bound")
		}
		return nil
	}
	addFacts := func(resource affectedResource, commit string, onlyOwner bool) error {
		var body []byte
		if err := tx.QueryRowContext(ctx, `SELECT body FROM core_snapshots WHERE scope=? AND domain=? AND id=? AND resource_id=?`, scope, resource.domain, commit, resource.id).Scan(&body); err != nil {
			return err
		}
		var c model.ConfigurationCommit
		if json.Unmarshal(body, &c) != nil || c.ID != commit || c.ResourceID != resource.id || !sha256Hex(c.AssociationDigest) || c.AssociationCount < 0 || c.AssociationCount > model.MaxConfigurationAssociations {
			return failure("data_loss", "invalid affected snapshot metadata")
		}
		facts, size, err := configurationAssociations(ctx, tx, scope, resource.domain, commit)
		if err != nil {
			return err
		}
		if err = configurationAssociationIntegrity(c, facts); err != nil {
			return err
		}
		if err = step(len(body) + size); err != nil {
			return err
		}
		for _, fact := range facts {
			// World-only association serves optional target-origin presentation;
			// it never installs slot or Robot workflows on an Agent.
			if fact.Kind == model.AgentAssociationWorld {
				continue
			}
			if onlyOwner && fact.Kind != model.AgentAssociationRobotOwner {
				continue
			}
			agents[fact.AgentID] = true
			if len(agents) > maxAffectedAgentWork {
				return failure("resource_exhausted", "affected Agent count exceeds bound")
			}
		}
		return nil
	}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resource := queue[0]
		queue = queue[1:]
		if visited[resource] {
			continue
		}
		visited[resource] = true
		if err := step(len(resource.domain) + len(resource.id)); err != nil {
			return nil, err
		}
		var commit string
		err := tx.QueryRowContext(ctx, `SELECT main_commit_id FROM core_resources WHERE scope=? AND domain=? AND id=? AND archived=0`, scope, resource.domain, resource.id).Scan(&commit)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if err == nil {
			if err = addFacts(resource, commit, false); err != nil {
				return nil, err
			}
			// Robot slot targets add their current physical owners. Do not parse
			// the Experiment document or treat an Agent as a config resource.
			rows, err := tx.QueryContext(ctx, `SELECT r.domain,r.id,r.main_commit_id FROM core_references f
JOIN core_resources r ON r.scope=f.scope AND r.domain=f.target_domain AND r.id=f.target_resource_id AND r.archived=0
WHERE f.scope=? AND f.domain=? AND f.commit_id=? AND substr(f.slot,1,6)='robot:' ORDER BY f.slot LIMIT ?`, scope, resource.domain, commit, maxAffectedAgentWork+1)
			if err != nil {
				return nil, err
			}
			type robotPin struct {
				resource affectedResource
				commit   string
			}
			var robots []robotPin
			for rows.Next() {
				var v robotPin
				if err = rows.Scan(&v.resource.domain, &v.resource.id, &v.commit); err != nil {
					break
				}
				if err = step(len(v.resource.domain) + len(v.resource.id) + len(v.commit)); err != nil {
					break
				}
				robots = append(robots, v)
			}
			if err == nil {
				err = rows.Err()
			}
			rows.Close()
			if err != nil {
				return nil, err
			}
			for _, v := range robots {
				if err = addFacts(v.resource, v.commit, true); err != nil {
					return nil, err
				}
			}
		}
		// Find only current-main owners which track this resource's main.
		// The existing target-leading index bounds each reverse graph edge.
		rows, err := tx.QueryContext(ctx, `SELECT r.domain,r.id FROM core_references f INDEXED BY core_reference_targets
JOIN core_snapshots s ON s.scope=f.scope AND s.domain=f.domain AND s.id=f.commit_id
JOIN core_resources r ON r.scope=s.scope AND r.domain=s.domain AND r.id=s.resource_id AND r.main_commit_id=s.id AND r.archived=0
WHERE f.scope=? AND f.target_domain=? AND f.target_resource_id=? AND f.target_commit_id=''
AND json_extract(CAST(f.body AS TEXT),'$.mode')='tracking' AND json_extract(CAST(f.body AS TEXT),'$.target_branch')='main' LIMIT ?`, scope, resource.domain, resource.id, maxAffectedAgentWork+1)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var next affectedResource
			if err = rows.Scan(&next.domain, &next.id); err != nil {
				break
			}
			if err = step(len(next.domain) + len(next.id)); err != nil {
				break
			}
			if !visited[next] {
				queue = append(queue, next)
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(agents))
	for id := range agents {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func mergeAffectedAgents(groups ...[]string) ([]string, error) {
	seen := map[string]bool{}
	bytes := 0
	for _, group := range groups {
		for _, id := range group {
			if !seen[id] {
				seen[id] = true
				bytes += len(id)
				if len(seen) > maxAffectedAgentWork || bytes > model.MaxConfigurationDecodedBytes {
					return nil, failure("resource_exhausted", "affected Agent receipt exceeds bound")
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Preserve exact opaque Agent IDs; the domain projector and registry own the
// policy. No normalization or case folding is performed in the index owner.
func validAffectedAgentIDs(ids []string) bool {
	if len(ids) > maxAffectedAgentWork {
		return false
	}
	for i, id := range ids {
		if id == "" || len(id) > 128 || !utf8.ValidString(id) || strings.ContainsAny(id, "\x00\r\n") || i > 0 && ids[i-1] >= id {
			return false
		}
	}
	return true
}

func configurationAgentSynchronizationChanged(before, after []byte) bool {
	var left, right map[string]json.RawMessage
	if json.Unmarshal(before, &left) != nil || json.Unmarshal(after, &right) != nil {
		return true
	}
	var a, b string
	if json.Unmarshal(left["_agentSyncDigest"], &a) != nil || json.Unmarshal(right["_agentSyncDigest"], &b) != nil || !sha256Hex(a) || !sha256Hex(b) {
		return true
	}
	return a != b
}
