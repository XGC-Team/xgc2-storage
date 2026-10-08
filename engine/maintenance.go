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

type Stats struct {
	DatabaseID     string `json:"database_id"`
	SQLiteVersion  string `json:"sqlite_version"`
	DatabaseBytes  int64  `json:"database_bytes"`
	WALBytes       int64  `json:"wal_bytes"`
	SHMBytes       int64  `json:"shm_bytes"`
	FreeBytes      uint64 `json:"free_bytes"`
	WriterAdmitted int    `json:"writer_admitted"`
	WriterCapacity int    `json:"writer_capacity"`
	ReadersActive  int    `json:"readers_active"`
	ReaderCapacity int    `json:"reader_capacity"`
	Calls          uint64 `json:"calls"`
	Conflicts      uint64 `json:"conflicts"`
	Overloads      uint64 `json:"overloads"`
	QueueWaitNS    uint64 `json:"queue_wait_ns"`
	SQLTimeNS      uint64 `json:"sql_time_ns"`
}

func (s *Store) Stats() (Stats, error) {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed.Load() {
		return Stats{}, fail("unavailable", "store closed")
	}
	out := Stats{DatabaseID: s.dbid, SQLiteVersion: s.engine, WriterAdmitted: len(s.writers), WriterCapacity: cap(s.writers), ReadersActive: len(s.reads), ReaderCapacity: cap(s.reads), Calls: s.calls.Load(), Conflicts: s.conflicts.Load(), Overloads: s.overloads.Load(), QueueWaitNS: s.waitNS.Load(), SQLTimeNS: s.sqlNS.Load()}
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

type CheckpointResult struct {
	Busy              int `json:"busy"`
	LogPages          int `json:"log_pages"`
	CheckpointedPages int `json:"checkpointed_pages"`
}

func (s *Store) Checkpoint(ctx context.Context) (out CheckpointResult, err error) {
	defer func() { err = classify(err) }()
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return out, err
	}
	defer release()
	err = s.writer.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&out.Busy, &out.LogPages, &out.CheckpointedPages)
	return
}
func (s *Store) PruneExpiredReceipts(ctx context.Context, before time.Time, limit int) (deleted int64, err error) {
	defer func() { err = classify(err) }()
	if limit < 1 || limit > 1000 || before.After(time.Now()) {
		return 0, fail("invalid_argument", "bounded past-expiry maintenance required")
	}
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return 0, err
	}
	defer release()
	result, err := s.writer.ExecContext(ctx, "DELETE FROM receipts WHERE rowid IN (SELECT rowid FROM receipts WHERE expires<? ORDER BY expires LIMIT ?)", before.Unix(), limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
func (s *Store) Integrity(ctx context.Context) error {
	ctx, release, e := s.beginCall(ctx, false)
	if e != nil {
		return classify(e)
	}
	defer release()
	var result string
	if e = s.reader.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); e != nil {
		return classify(e)
	}
	if result != "ok" {
		return fail("corrupt", result)
	}
	return nil
}

type BackupReceipt struct {
	DatabaseID   string `json:"database_id"`
	ManifestHash string `json:"manifest_hash"`
	Bytes        int64  `json:"bytes"`
	CreatedAt    string `json:"created_at"`
}

// Backup is an explicit administrative grant, never an ordinary data RPC.
// The destination is no-overwrite and in an owned private directory.
func (s *Store) Backup(ctx context.Context, destination string) (receipt BackupReceipt, err error) {
	defer func() { err = classify(err) }()
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || filepath.Ext(destination) != ".db" || strings.ContainsAny(destination, "\x00?#") {
		return receipt, fail("invalid_argument", "canonical backup .db grant required")
	}
	ctx, release, err := s.beginCall(ctx, true)
	if err != nil {
		return receipt, err
	}
	defer release()
	parent, e := openPrivateDirectory(filepath.Dir(destination))
	if e != nil {
		return receipt, e
	}
	defer unix.Close(parent)
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
	if len(entries) > 128 {
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
	var result, id, digest string
	if e = candidate.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); e != nil || result != "ok" {
		return receipt, fail("corrupt", "backup integrity check failed")
	}
	if e = candidate.QueryRowContext(ctx, "SELECT database_id,manifest_hash FROM storage_meta WHERE id=1").Scan(&id, &digest); e != nil || id != s.dbid || digest != hash(s.config.Manifest) {
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
	receipt = BackupReceipt{DatabaseID: s.dbid, ManifestHash: digest, Bytes: st.Size, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	return receipt, nil
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
