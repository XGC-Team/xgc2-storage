package coredata

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// The local predicate excludes the two group guards (a live root and a queued
// successor Session). It is maintained on the exact Run whose owner, root-link
// or predecessor relation changed. The group indexes let those two guards be
// changed without visiting rows still protected by the other group guard.
const retentionPendingClear = `CASE WHEN r.pending_remote_count<>0 THEN 0
 WHEN EXISTS (SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.owner_run_id=r.id) THEN 0
 WHEN EXISTS (SELECT 1 FROM run_remote_pending p WHERE p.scope=r.scope AND p.root_run_id=r.id) THEN 0 ELSE 1 END`
const retentionLocal = `CASE WHEN (` + retentionPendingClear + `) THEN NOT EXISTS (SELECT 1 FROM runs successor WHERE successor.scope=r.scope AND successor.predecessor_run_id=r.id AND successor.predecessor_run_id<>'' AND successor.finished_at=0) ELSE 0 END`
const retentionLocalPlanned = `CASE WHEN (` + retentionPendingClear + `) THEN NOT EXISTS (SELECT 1 FROM runs successor WHERE successor.scope=r.scope AND successor.predecessor_run_id=r.id AND successor.predecessor_run_id<>'' AND successor.finished_at=0 AND (?15='' OR successor.id NOT IN (SELECT value FROM json_each(?15)))) ELSE 0 END`
const retentionRootClear = `NOT EXISTS (SELECT 1 FROM runs p WHERE p.scope=r.scope AND p.id=r.root_run_id AND p.finished_at=0)`
const retentionRootPlanned = `CASE WHEN ?14 THEN 1 ELSE (` + retentionRootClear + `) END`
const retentionSessionClear = `NOT EXISTS (SELECT 1 FROM sessions s WHERE s.scope=r.scope AND s.target_id=r.target_id AND s.predecessor_session_id=r.session_id AND s.predecessor_session_id<>'' AND s.status='queued')`

// Compute the local predicate once, inside the changed finish UPDATE. The
// materialized scalar also avoids group guard reads when the local guard fails.
const retentionFinish = `UPDATE runs AS r SET status=?1,termination=?2,error_json=?3,error_summary_json=?4,result_json=?5,nodes_json=?6,cleanup_errors_json=?7,cleanup_failed=?8,finished_at=?9,revision=revision+1,bytes=bytes+?10,
 (retention_local_ok,retention_eligible)=(WITH local(value) AS MATERIALIZED (SELECT CASE WHEN ?9>0 THEN (` + retentionLocalPlanned + `) ELSE 0 END) SELECT value,CASE WHEN value AND (` + retentionRootPlanned + `) THEN (` + retentionSessionClear + `) ELSE 0 END FROM local)
 WHERE scope=?11 AND id=?12 AND status=?13 RETURNING retention_eligible,predecessor_run_id,CASE WHEN ?8>0 THEN session_id ELSE '' END`

type retentionRootChange struct{ before, after bool }
type retentionChanges struct {
	delta                   int64
	changedFinish, released bool
	roots                   map[string]retentionRootChange
	local                   map[string]struct{}
	finishedIDs             *[]string
}

func (c *retentionChanges) created(id, predecessor string) {
	if c.roots == nil {
		c.roots = make(map[string]retentionRootChange)
	}
	change, ok := c.roots[id]
	if !ok {
		change.before = false
	}
	change.after = true
	c.roots[id] = change
	c.touch(predecessor)
}
func (c *retentionChanges) finished(id, predecessor string, eligible int64) {
	c.changedFinish = true
	if c.finishedIDs != nil {
		*c.finishedIDs = append(*c.finishedIDs, id)
	}
	c.delta += eligible
	if eligible != 0 {
		c.released = true
	}
	if c.roots == nil {
		c.roots = make(map[string]retentionRootChange)
	}
	change, ok := c.roots[id]
	if !ok {
		change.before = true
	}
	change.after = false
	c.roots[id] = change
	c.touch(predecessor)
}
func (c *retentionChanges) touch(id string) {
	if id == "" {
		return
	}
	if c.local == nil {
		c.local = make(map[string]struct{})
	}
	c.local[id] = struct{}{}
}
func (c *retentionChanges) add(n int64) {
	c.delta += n
	if n > 0 {
		c.released = true
	}
}

// reconcileLocal visits a single changed identity. Missing roots and
// predecessors are permitted by the authoritative model.
func (c *retentionChanges) reconcileLocal(ctx context.Context, tx *sql.Tx, scope, id string) error {
	var oldLocal, oldEligible, nextLocal, nextEligible int64
	q := `WITH fact AS MATERIALIZED (
       SELECT r.scope,r.id,r.root_run_id,r.target_id,r.session_id,r.retention_local_ok,r.retention_eligible,
       CASE WHEN r.finished_at>0 THEN (` + retentionLocal + `) ELSE 0 END AS next_local
       FROM runs r WHERE r.scope=? AND r.id=?)
       SELECT retention_local_ok,retention_eligible,next_local,
       CASE WHEN next_local AND (` + retentionRootClear + `) THEN (` + retentionSessionClear + `) ELSE 0 END FROM fact r`
	err := tx.QueryRowContext(ctx, q, scope, id).Scan(&oldLocal, &oldEligible, &nextLocal, &nextEligible)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if oldLocal == nextLocal && oldEligible == nextEligible {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE runs SET retention_local_ok=?,retention_eligible=? WHERE scope=? AND id=?`, nextLocal, nextEligible, scope, id); err != nil {
		return err
	}
	c.add(nextEligible - oldEligible)
	return nil
}

// A newly inserted remote relation is itself sufficient to make its root's
// local predicate false. Capture the old eligible bit from the same keyed
// update; do not reread the other guards or rewrite an already protected root.
func (c *retentionChanges) invalidateRemoteRoot(ctx context.Context, tx *sql.Tx, scope, id string) error {
	const q = `WITH old AS MATERIALIZED (SELECT id,retention_eligible FROM runs WHERE scope=?1 AND id=?2 AND retention_local_ok=1)
 UPDATE runs SET retention_local_ok=0,retention_eligible=0 WHERE scope=?1 AND id IN (SELECT id FROM old)
 RETURNING (SELECT retention_eligible FROM old)`
	var wasEligible int64
	err := tx.QueryRowContext(ctx, q, scope, id).Scan(&wasEligible)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	c.add(-wasEligible)
	return nil
}

// Explicit per-column keyset seeks skip a complete protected Session group.
// A tuple comparison visits its duplicate entries in SQLite despite the index.
const retentionRootRelease = `WITH RECURSIVE groups(j) AS (
 SELECT (SELECT json_array(target_id,session_id) FROM runs INDEXED BY runs_retention_root_group WHERE scope=?1 AND root_run_id=?2 AND retention_local_ok=1 AND retention_eligible=0 AND finished_at>0 ORDER BY target_id,session_id LIMIT 1)
 UNION ALL
 SELECT coalesce(
 (SELECT json_array(target_id,session_id) FROM runs INDEXED BY runs_retention_root_group WHERE scope=?1 AND root_run_id=?2 AND retention_local_ok=1 AND retention_eligible=0 AND finished_at>0 AND target_id=json_extract(groups.j,'$[0]') AND session_id>json_extract(groups.j,'$[1]') ORDER BY session_id LIMIT 1),
 (SELECT json_array(target_id,session_id) FROM runs INDEXED BY runs_retention_root_group WHERE scope=?1 AND root_run_id=?2 AND retention_local_ok=1 AND retention_eligible=0 AND finished_at>0 AND target_id>json_extract(groups.j,'$[0]') ORDER BY target_id,session_id LIMIT 1))
 FROM groups WHERE groups.j IS NOT NULL
), allowed(target,session) AS MATERIALIZED (
 SELECT json_extract(j,'$[0]'),json_extract(j,'$[1]') FROM groups WHERE j IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.scope=?1 AND s.target_id=json_extract(j,'$[0]') AND s.predecessor_session_id=json_extract(j,'$[1]') AND s.predecessor_session_id<>'' AND s.status='queued')
), affected(id) AS MATERIALIZED (
 SELECT r.id FROM allowed CROSS JOIN runs r INDEXED BY runs_retention_root_group
 WHERE r.scope=?1 AND r.root_run_id=?2 AND r.retention_local_ok=1 AND r.retention_eligible=0 AND r.finished_at>0 AND r.target_id=allowed.target AND r.session_id=allowed.session
)
UPDATE runs SET retention_eligible=1 WHERE scope=?1 AND id IN (SELECT id FROM affected)`

// Session release uses the inverse grouping: seek distinct root IDs and check
// each live-root guard once before visiting the rows it can actually release.
const retentionSessionRelease = `WITH RECURSIVE groups(root) AS (
 SELECT (SELECT root_run_id FROM runs INDEXED BY runs_retention_session_group WHERE scope=?1 AND target_id=?2 AND session_id=?3 AND session_id<>'' AND retention_local_ok=1 AND retention_eligible=0 AND finished_at>0 ORDER BY root_run_id LIMIT 1)
 UNION ALL
 SELECT (SELECT root_run_id FROM runs INDEXED BY runs_retention_session_group WHERE scope=?1 AND target_id=?2 AND session_id=?3 AND session_id<>'' AND retention_local_ok=1 AND retention_eligible=0 AND finished_at>0 AND root_run_id>groups.root ORDER BY root_run_id LIMIT 1) FROM groups WHERE root IS NOT NULL
), allowed(root) AS MATERIALIZED (
 SELECT root FROM groups WHERE root IS NOT NULL AND NOT EXISTS (SELECT 1 FROM runs p WHERE p.scope=?1 AND p.id=groups.root AND p.finished_at=0)
), affected(id) AS MATERIALIZED (
 SELECT r.id FROM allowed CROSS JOIN runs r INDEXED BY runs_retention_session_group
 WHERE r.scope=?1 AND r.target_id=?2 AND r.session_id=?3 AND r.session_id<>'' AND r.retention_local_ok=1 AND r.retention_eligible=0 AND r.finished_at>0 AND r.root_run_id=allowed.root
)
UPDATE runs SET retention_eligible=1 WHERE scope=?1 AND id IN (SELECT id FROM affected)`

func (c *retentionChanges) root(ctx context.Context, tx *sql.Tx, scope, id string, live bool) error {
	var res sql.Result
	var err error
	if live {
		res, err = tx.ExecContext(ctx, `UPDATE runs INDEXED BY runs_retention_root_group SET retention_eligible=0 WHERE scope=? AND root_run_id=? AND retention_local_ok=1 AND retention_eligible=1 AND finished_at>0`, scope, id)
	} else {
		res, err = tx.ExecContext(ctx, retentionRootRelease, scope, id)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if live {
		n = -n
	}
	c.add(n)
	return nil
}
func (c *retentionChanges) session(ctx context.Context, tx *sql.Tx, scope, target, session string, queued bool) error {
	if session == "" {
		return nil
	}
	var res sql.Result
	var err error
	if queued {
		res, err = tx.ExecContext(ctx, `UPDATE runs INDEXED BY runs_retention_session_group SET retention_eligible=0 WHERE scope=? AND target_id=? AND session_id=? AND session_id<>'' AND retention_local_ok=1 AND retention_eligible=1 AND finished_at>0`, scope, target, session)
	} else {
		res, err = tx.ExecContext(ctx, retentionSessionRelease, scope, target, session)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if queued {
		n = -n
	}
	c.add(n)
	return nil
}
func (c *retentionChanges) finish(ctx context.Context, tx *sql.Tx, scope string) error {
	ids := make([]string, 0, len(c.local))
	for id := range c.local {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := c.reconcileLocal(ctx, tx, scope, id); err != nil {
			return err
		}
	}
	ids = ids[:0]
	for id := range c.roots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ch := c.roots[id]
		if ch.before != ch.after {
			if err := c.root(ctx, tx, scope, id, ch.after); err != nil {
				return err
			}
		}
	}
	if c.delta == 0 {
		return nil
	}
	r, err := tx.ExecContext(ctx, `UPDATE core_data_usage SET retention_eligible_count=retention_eligible_count+? WHERE scope=? AND retention_eligible_count+? BETWEEN 0 AND rows`, c.delta, scope, c.delta)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return failure("data_loss", "retention eligibility accounting disagrees")
	}
	return nil
}

func validateRetentionPolicy(p model.RunRetentionPolicy) error {
	if p.KeepNewest < 0 {
		return failure("invalid_argument", "retention count must be nonnegative")
	}
	if !p.OlderThan.IsZero() && !time.Unix(0, nanos(p.OlderThan)).Equal(p.OlderThan) {
		return failure("invalid_argument", "retention cutoff is outside the stored timestamp range")
	}
	return nil
}
func retentionEffect(ctx context.Context, tx *sql.Tx, scope string, c *retentionChanges, p model.RunRetentionPolicy) (model.RunRetentionEffect, error) {
	out := model.RunRetentionEffect{ChangedFinish: c.changedFinish, EligibilityReleased: c.released}
	if !(c.changedFinish || c.released) || p.KeepNewest == 0 && p.OlderThan.IsZero() {
		return out, nil
	}
	if p.KeepNewest > 0 {
		var count int64
		if err := tx.QueryRowContext(ctx, `SELECT retention_eligible_count FROM core_data_usage WHERE scope=?`, scope).Scan(&count); err != nil {
			return model.RunRetentionEffect{}, err
		}
		if count > int64(p.KeepNewest) {
			out.HasVictim = true
			return out, nil
		}
		if count == 0 {
			return out, nil
		}
	}
	if !p.OlderThan.IsZero() {
		var oldest int64
		err := tx.QueryRowContext(ctx, pruneOldest, scope).Scan(&oldest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return model.RunRetentionEffect{}, err
		}
		out.HasVictim = err == nil && oldest < nanos(p.OlderThan)
	}
	return out, nil
}
