package engine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	workCheckpoint uint32 = 1 << iota
	workExpire
)

// maintenanceRetry is how long failed maintenance waits before it tries again,
// so that a transient failure does not switch the work off until the next event.
var maintenanceRetry = 30 * time.Second

// maintenance runs the owner's background work. It is event driven: a commit
// arms a checkpoint, a stored receipt arms its own expiry, and an idle
// database wakes nothing. A checkpoint also makes relaxed commits durable.
type maintenance struct {
	store *Store
	kick  chan struct{}
	work  atomic.Uint32
	stop  context.CancelFunc
	done  chan struct{}
	once  sync.Once

	mu              sync.Mutex
	checkpointTimer *time.Timer
	expiryTimer     *time.Timer
	expiryAt        int64 // Unix second the expiry timer is armed for; 0 when none
}

func (s *Store) startMaintenance(ctx context.Context) {
	runCtx, stop := context.WithCancel(context.Background())
	m := &maintenance{store: s, kick: make(chan struct{}, 1), stop: stop, done: make(chan struct{})}
	s.maintenance = m
	if at, ok := s.nextExpiry(ctx); ok {
		m.armExpiry(at)
	}
	go m.run(runCtx)
}

func (s *Store) stopMaintenance() {
	m := s.maintenance
	if m == nil {
		return
	}
	m.once.Do(func() {
		m.stop()
		m.mu.Lock()
		for _, t := range []*time.Timer{m.checkpointTimer, m.expiryTimer} {
			if t != nil {
				t.Stop()
			}
		}
		m.mu.Unlock()
		<-m.done
	})
}

func (m *maintenance) run(ctx context.Context) {
	defer close(m.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.kick:
		}
		work := m.work.Swap(0)
		if work&workCheckpoint != 0 && m.report(ctx, m.checkpoint(ctx)) {
			m.scheduleCheckpoint(maintenanceRetry)
		}
		if work&workExpire != 0 && m.report(ctx, m.expire(ctx)) {
			m.armExpiry(time.Now().Add(maintenanceRetry).Unix())
		}
	}
}

func (m *maintenance) request(work uint32) {
	m.work.Or(work)
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// report counts and delivers a failure and tells whether the work should be
// retried. A failure while the owner shuts down is expected and ignored.
func (m *maintenance) report(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	m.store.maintenanceErrs.Add(1)
	if m.store.config.OnMaintenanceError != nil {
		m.store.config.OnMaintenanceError(err)
	}
	return true
}

// afterCommit decides whether the WAL needs attention. A WAL over the
// threshold is checkpointed at once; otherwise one checkpoint is scheduled a
// fixed delay after the first commit that follows the previous one.
func (m *maintenance) afterCommit() {
	if m.store.walSize() >= m.store.config.CheckpointBytes {
		m.request(workCheckpoint)
		return
	}
	m.scheduleCheckpoint(m.store.config.CheckpointDelay)
}

// scheduleCheckpoint arms the one checkpoint timer unless it is armed already.
func (m *maintenance) scheduleCheckpoint(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.checkpointTimer == nil {
		m.checkpointTimer = time.AfterFunc(delay, func() {
			m.mu.Lock()
			m.checkpointTimer = nil
			m.mu.Unlock()
			m.request(workCheckpoint)
		})
	}
}

func (m *maintenance) checkpoint(ctx context.Context) error {
	_, err := m.store.Checkpoint(ctx)
	return err
}

// armExpiry schedules the next receipt pruning at the given expiry time. A
// later time never replaces an earlier one that is already armed.
func (m *maintenance) armExpiry(at int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.expiryAt != 0 && at >= m.expiryAt {
		return
	}
	if m.expiryTimer != nil {
		m.expiryTimer.Stop()
	}
	m.expiryAt = at
	// A receipt is expired once its time is strictly in the past.
	m.expiryTimer = time.AfterFunc(max(time.Until(time.Unix(at+1, 0)), 0), func() { m.request(workExpire) })
}

func (m *maintenance) expire(ctx context.Context) error {
	m.mu.Lock()
	m.expiryAt = 0
	m.mu.Unlock()
	for ctx.Err() == nil {
		n, err := m.store.PruneExpiredReceipts(ctx, time.Now(), 256)
		if err != nil {
			return err
		}
		if n < 256 {
			break
		}
	}
	if at, ok := m.store.nextExpiry(ctx); ok {
		m.armExpiry(at)
	}
	return nil
}

// nextExpiry reports the earliest receipt expiry, if any receipt exists.
func (s *Store) nextExpiry(ctx context.Context) (int64, bool) {
	var at sql.NullInt64
	if err := s.rdb.QueryRowContext(ctx, "SELECT min(expires) FROM receipts").Scan(&at); err != nil || !at.Valid {
		return 0, false
	}
	return at.Int64, true
}

// PruneExpiredReceipts deletes at most limit receipts that expired before the
// given time and releases their quota counters. It is relaxed: losing a prune
// to a power failure only repeats it.
func (s *Store) PruneExpiredReceipts(ctx context.Context, before time.Time, limit int) (deleted int64, err error) {
	defer func() { err = classify(err) }()
	if limit < 1 || limit > 1000 || before.After(time.Now()) {
		return 0, fail("invalid_argument", "bounded past-expiry maintenance required")
	}
	err = s.write(ctx, Relaxed, false, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, "DELETE FROM receipts WHERE rowid IN (SELECT rowid FROM receipts WHERE expires<? ORDER BY expires LIMIT ?) RETURNING scope", before.Unix(), limit)
		if err != nil {
			return err
		}
		perScope := map[string]int64{}
		for rows.Next() {
			var scope string
			if err = rows.Scan(&scope); err != nil {
				rows.Close()
				return err
			}
			perScope[scope]++
			deleted++
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
		for scope, n := range perScope {
			if _, err = tx.ExecContext(ctx, "UPDATE scopes SET receipts=max(receipts-?,0) WHERE scope=?", n, scope); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.receiptsPruned.Add(uint64(deleted))
	return deleted, nil
}
