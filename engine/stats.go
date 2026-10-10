package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Stats are cumulative counters since Open plus the current file sizes. They
// are for diagnostics and benchmarks; the owner logs nothing per call.
type Stats struct {
	DatabaseID    string `json:"database_id"`
	SQLiteVersion string `json:"sqlite_version"`
	DatabaseBytes int64  `json:"database_bytes"`
	WALBytes      int64  `json:"wal_bytes"`
	SHMBytes      int64  `json:"shm_bytes"`
	FreeBytes     uint64 `json:"free_bytes"`
	// WritersQueued counts writers admitted or waiting for the single writer.
	WritersQueued  int `json:"writers_queued"`
	WriterCapacity int `json:"writer_capacity"`
	ReadersActive  int `json:"readers_active"`
	ReaderCapacity int `json:"reader_capacity"`
	// Calls counts admitted calls. Conflicts counts rejected compare-and-set
	// guards. Timeouts counts calls whose deadline expired while they waited
	// for admission.
	Calls     uint64 `json:"calls"`
	Conflicts uint64 `json:"conflicts"`
	Timeouts  uint64 `json:"timeouts"`
	// QueueWaitNS is the total time calls waited for admission and SQLTimeNS
	// the total time they held a connection.
	QueueWaitNS uint64 `json:"queue_wait_ns"`
	SQLTimeNS   uint64 `json:"sql_time_ns"`
	// Commits by class. Fsyncs counts the durability barriers the owner asked
	// for: one per durable commit and two per checkpoint that moved frames.
	// SQLite may add an internal sync for the first frame after a WAL reset.
	CommitsDurable    uint64 `json:"commits_durable"`
	CommitsRelaxed    uint64 `json:"commits_relaxed"`
	Fsyncs            uint64 `json:"fsyncs"`
	Checkpoints       uint64 `json:"checkpoints"`
	ReceiptsPruned    uint64 `json:"receipts_pruned"`
	MaintenanceErrors uint64 `json:"maintenance_errors"`
}

func (s *Store) Stats() (Stats, error) {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed.Load() {
		return Stats{}, fail("unavailable", "store closed")
	}
	out := Stats{DatabaseID: s.dbid, SQLiteVersion: s.engine, WritersQueued: len(s.queue), WriterCapacity: cap(s.queue), ReadersActive: len(s.reads), ReaderCapacity: cap(s.reads),
		Calls: s.calls.Load(), Conflicts: s.conflicts.Load(), Timeouts: s.timeouts.Load(), QueueWaitNS: s.waitNS.Load(), SQLTimeNS: s.sqlNS.Load(),
		CommitsDurable: s.durable.Load(), CommitsRelaxed: s.relaxed.Load(), Fsyncs: s.fsyncs.Load(), Checkpoints: s.checkpoints.Load(),
		ReceiptsPruned: s.receiptsPruned.Load(), MaintenanceErrors: s.maintenanceErrs.Load()}
	for suffix, target := range map[string]*int64{"": &out.DatabaseBytes, "-wal": &out.WALBytes, "-shm": &out.SHMBytes} {
		var st unix.Stat_t
		if e := unix.Fstatat(s.owner.parent, s.owner.name+suffix, &st, unix.AT_SYMLINK_NOFOLLOW); e == nil {
			*target = st.Size
		}
	}
	var fs unix.Statfs_t
	if e := unix.Fstatfs(s.owner.parent, &fs); e != nil {
		return out, e
	}
	out.FreeBytes = fs.Bavail * uint64(fs.Bsize)
	return out, nil
}

// committed accounts one successful writer commit and lets maintenance react.
func (s *Store) committed(d Durability) {
	if d == Durable {
		s.durable.Add(1)
		s.fsyncs.Add(1)
	} else {
		s.relaxed.Add(1)
	}
	if s.maintenance != nil {
		s.maintenance.afterCommit()
	}
}

type CheckpointResult struct {
	Busy              int `json:"busy"`
	LogPages          int `json:"log_pages"`
	CheckpointedPages int `json:"checkpointed_pages"`
}

func (s *Store) Checkpoint(ctx context.Context) (out CheckpointResult, err error) {
	defer func() { err = classify(err) }()
	ctx, release, err := s.admit(ctx, true)
	if err != nil {
		return out, err
	}
	defer release()
	err = s.writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&out.Busy, &out.LogPages, &out.CheckpointedPages)
	if err != nil {
		return
	}
	s.checkpoints.Add(1)
	if out.CheckpointedPages > 0 {
		// The checkpoint syncs the WAL, copies the frames and syncs the database.
		s.fsyncs.Add(2)
	}
	// PASSIVE backfills frames but leaves the allocated WAL file in place. At
	// the physical admission watermark, that file would otherwise prevent the
	// next writer from resetting it. Attempt one non-waiting reset under this
	// same owner admission and deadline; a pinned reader leaves Busy set and
	// preserves pressure until a later maintenance call can reclaim the file.
	if s.walSize() >= s.config.MaxWALBytes {
		err = s.writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&out.Busy, &out.LogPages, &out.CheckpointedPages)
	}
	return
}

func (s *Store) Integrity(ctx context.Context) error {
	return s.Read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var result string
		if err := tx.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
			return err
		}
		if result != "ok" {
			return fail("corrupt", result)
		}
		return nil
	})
}

type BackupReceipt struct {
	DatabaseID string `json:"database_id"`
	Bytes      int64  `json:"bytes"`
	CreatedAt  string `json:"created_at"`
}

// Backup is an explicit administrative grant, never an ordinary data RPC.
// The destination is no-overwrite and in an owned private directory.
func (s *Store) Backup(ctx context.Context, destination string) (receipt BackupReceipt, err error) {
	defer func() { err = classify(err) }()
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || filepath.Ext(destination) != ".db" || strings.ContainsAny(destination, "\x00?#") {
		return receipt, fail("invalid_argument", "canonical backup .db grant required")
	}
	ctx, release, err := s.admit(ctx, true)
	if err != nil {
		return receipt, err
	}
	defer release()
	parent, e := openPrivateDirectory(filepath.Dir(destination))
	if e != nil {
		return receipt, e
	}
	defer unix.Close(parent)
	if e = unix.Flock(parent, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return receipt, fail("conflict", "backup directory is already owned")
	}
	defer unix.Flock(parent, unix.LOCK_UN)
	// Duplicate the descriptor so os.File cannot close the grant's original fd.
	dup, e := unix.Dup(parent)
	if e != nil {
		return receipt, e
	}
	dir := os.NewFile(uintptr(dup), fmt.Sprintf("/proc/self/fd/%d", parent))
	defer dir.Close()
	entries, e := dir.ReadDir(129)
	if e != nil && e != io.EOF {
		return receipt, e
	}
	if len(entries) >= 128 {
		return receipt, fail("resource_exhausted", "backup directory count limit reached")
	}
	var total int64
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil {
			return receipt, e
		}
		if !info.Mode().IsRegular() {
			return receipt, fail("permission_denied", "backup directory must contain regular outputs only")
		}
		total += info.Size()
	}
	if total >= 2*s.config.MaxDBBytes {
		return receipt, fail("resource_exhausted", "backup directory byte limit reached")
	}
	if err = s.diskCheck(); err != nil {
		return receipt, err
	}
	var pageCount, pageSize int64
	if err = s.writer.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount); err != nil {
		return receipt, err
	}
	if err = s.writer.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return receipt, err
	}
	var destinationFS unix.Statfs_t
	if err = unix.Fstatfs(parent, &destinationFS); err != nil {
		return receipt, err
	}
	if destinationFS.Bavail*uint64(destinationFS.Bsize) < uint64(pageCount*pageSize+s.config.MinFreeBytes) {
		return receipt, fail("resource_exhausted", "backup destination free-space budget exhausted")
	}
	temp, e := os.CreateTemp(fmt.Sprintf("/proc/self/fd/%d", parent), ".storage-backup-*.db")
	if e != nil {
		return receipt, e
	}
	name := filepath.Base(temp.Name())
	temp.Close()
	unix.Unlinkat(parent, name, 0)
	defer unix.Unlinkat(parent, name, 0)
	tempPath := fmt.Sprintf("/proc/self/fd/%d/%s", parent, name)
	if _, err = s.writer.ExecContext(ctx, "VACUUM INTO ?", tempPath); err != nil {
		return receipt, err
	}
	fd, e := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if e != nil {
		return receipt, e
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		return receipt, e
	}
	if st.Size > s.config.MaxDBBytes || total+st.Size > 2*s.config.MaxDBBytes {
		return receipt, fail("resource_exhausted", "backup byte budget exceeded")
	}
	if e = unix.Fchmod(fd, 0600); e != nil {
		return receipt, e
	}
	if e = unix.Fsync(fd); e != nil {
		return receipt, e
	}
	candidate, e := sql.Open("sqlite", tempPath+"?mode=ro&immutable=1")
	if e != nil {
		return receipt, e
	}
	defer candidate.Close()
	var result, id string
	if e = candidate.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); e != nil || result != "ok" {
		return receipt, fail("corrupt", "backup integrity check failed")
	}
	if e = candidate.QueryRowContext(ctx, "SELECT database_id FROM storage_meta WHERE id=1").Scan(&id); e != nil || id != s.dbid {
		return receipt, fail("failed_precondition", "backup identity mismatch")
	}
	if e = unix.Linkat(parent, name, parent, filepath.Base(destination), 0); e != nil {
		return receipt, e
	}
	if e = unix.Unlinkat(parent, name, 0); e != nil {
		return receipt, e
	}
	if e = unix.Fsync(parent); e != nil {
		return receipt, e
	}
	return BackupReceipt{DatabaseID: s.dbid, Bytes: st.Size, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}

func openPrivateDirectory(path string) (int, error) {
	fd, e := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if e != nil {
		return -1, e
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if err != nil {
			return -1, err
		}
		fd = next
	}
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0700 {
		unix.Close(fd)
		return -1, fail("permission_denied", "owned private directory required")
	}
	return fd, nil
}

// EncodeStats is useful for an explicit operator diagnostic, not DB access.
func (s *Store) EncodeStats() ([]byte, error) {
	stats, e := s.Stats()
	if e != nil {
		return nil, e
	}
	return json.Marshal(stats)
}
