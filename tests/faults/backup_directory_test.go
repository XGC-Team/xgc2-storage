//go:build linux

package faults_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
)

func verifyBackupCandidate(t *testing.T, path string, source api.BatchRequest, receipt api.Receipt) {
	t.Helper()
	// A stable engine owner.lock is intentionally retained after Close. Restore
	// this complete closed backup in another private directory so its ownership
	// files do not become inputs to the publication-directory output budget.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restorePath := filepath.Join(privateDir(t), "restored.db")
	if err = os.WriteFile(restorePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	restored := open(t, restorePath, false, nil)
	verifyBusiness(t, read(t, restored), source, receipt)
	retained, err := restored.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: source.RequestID})
	if err != nil || !reflect.DeepEqual(retained, receipt) {
		t.Fatalf("nonempty-directory backup lost retained receipt: %v", err)
	}
	if err = restored.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFaultBackupNonemptyDirectoryAndCountRace(t *testing.T) {
	s := open(t, filepath.Join(privateDir(t), "fixture.db"), true, nil)
	source := request(read(t, s).Token, "backup-directory-business")
	commit, err := s.Batch(deadline(t), source)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("second-distinct-output", func(t *testing.T) {
		dir := privateDir(t)
		first, second := filepath.Join(dir, "first.db"), filepath.Join(dir, "second.db")
		if _, err := s.Backup(deadline(t), first); err != nil {
			t.Fatal(err)
		}
		firstHash := fileHash(t, first)
		backup, err := s.Backup(deadline(t), second)
		if err != nil || backup.DatabaseID != commit.Token.DatabaseID {
			t.Fatalf("regular nonempty directory rejected new backup: %+v %v", backup, err)
		}
		if fileHash(t, first) != firstHash {
			t.Fatal("second backup changed existing output bytes")
		}
		verifyBackupCandidate(t, second, source, commit)
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 2 || fileHash(t, first) != firstHash {
			t.Fatalf("second backup left unexpected directory artifacts: %v", err)
		}
		evidence(t, map[string]any{"first_output_unchanged": true, "second_distinct_backup_restored_business_and_receipt": true,
			"directory_outputs": len(entries), "backup_receipt": backup})
	})
	t.Run("127-output-two-caller-boundary", func(t *testing.T) {
		dir := privateDir(t)
		existing := map[string]string{}
		for i := 0; i < 127; i++ {
			name := filepath.Join(dir, fmt.Sprintf("existing-%03d.db", i))
			if err := os.WriteFile(name, []byte(fmt.Sprintf("test-owned-regular-output-%d", i)), 0600); err != nil {
				t.Fatal(err)
			}
			existing[name] = fileHash(t, name)
		}
		type outcome struct {
			path    string
			receipt engine.BackupReceipt
			err     error
		}
		start := make(chan struct{})
		results := make(chan outcome, 2)
		ctx := deadline(t)
		for i := 0; i < 2; i++ {
			path := filepath.Join(dir, fmt.Sprintf("new-%d.db", i))
			go func() {
				<-start
				receipt, err := s.Backup(ctx, path)
				results <- outcome{path, receipt, err}
			}()
		}
		close(start)
		var success, rejected int
		var candidate string
		var unexpected error
		for i := 0; i < 2; i++ {
			result := <-results
			if result.err == nil {
				success++
				candidate = result.path
				if result.receipt.DatabaseID != commit.Token.DatabaseID {
					unexpected = fmt.Errorf("backup identity differs from source")
				}
			} else if backupCountLimit(result.err) {
				rejected++
			} else {
				unexpected = result.err
			}
		}
		if unexpected != nil || success != 1 || rejected != 1 {
			t.Fatalf("127-output backup admission raced: success=%d rejected=%d unexpected=%v", success, rejected, unexpected)
		}
		verifyBackupCandidate(t, candidate, source, commit)
		if _, err := s.Backup(deadline(t), filepath.Join(dir, "must-not-exist.db")); !backupCountLimit(err) {
			t.Fatalf("128-output boundary permitted further publication: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 128 {
			t.Fatalf("backup boundary leaked outputs or temporary files: count=%d err=%v", len(entries), err)
		}
		for path, sha := range existing {
			if fileHash(t, path) != sha {
				t.Fatal("backup changed preexisting regular output")
			}
		}
		stats, err := idleStats(t, s)
		if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 {
			t.Fatalf("backup count race leaked admission: %+v %v", stats, err)
		}
		evidence(t, map[string]any{"initial_nonzero_regular_outputs": 127, "callers": 2, "successful_publications": success,
			"count_limit_rejections": rejected, "final_outputs": len(entries), "existing_bytes_unchanged": true,
			"new_business_and_receipt_restored": true, "temporary_files_absent": true,
			"same_store_writer_gate_boundary": true, "count_failure_message": "backup directory count limit reached", "final_admission": stats})
	})
}

func backupCountLimit(err error) bool {
	var domain *api.Error
	return errors.As(err, &domain) && domain.Code == "resource_exhausted" && domain.Message == "backup directory count limit reached"
}
