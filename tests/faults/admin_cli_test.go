//go:build linux

package faults_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
)

func admin(t *testing.T, dir, operation string, extra ...string) ([]byte, error) {
	t.Helper()
	binary := os.Getenv("FAULT_STORAGE_ADMIN_BIN")
	args := []string{operation, "--db", filepath.Join(dir, "fixture.db"), "--manifest", filepath.Join(dir, "manifest.json")}
	args = append(args, extra...)
	return exec.CommandContext(deadline(t), binary, args...).CombinedOutput()
}

func TestFaultAdministrativeCLINewBusinessRecovery(t *testing.T) {
	dir := privateDir(t)
	d := start(t, dir, true, "")
	req := request(nativeRead(t, d).Token, "admin-cli-source")
	commit, err := d.client.Batch(deadline(t), req)
	if err != nil {
		t.Fatal(err)
	}
	// Administrative commands must not become a second live SQLite owner.
	for _, operation := range []string{"check", "backup"} {
		extra := []string{}
		if operation == "backup" {
			extra = []string{"--destination", filepath.Join(privateDir(t), "must-not-exist.db")}
		}
		raw, err := admin(t, dir, operation, extra...)
		if err == nil || !strings.Contains(string(raw), "database already owned") {
			t.Fatalf("administrative %s acquired the live database: %s %v", operation, raw, err)
		}
	}
	verifyBusiness(t, nativeRead(t, d), req, commit)
	d.kill(t)
	raw, err := admin(t, dir, "check")
	if err != nil {
		t.Fatalf("offline integrity after SIGKILL failed: %s %v", raw, err)
	}
	var integrity map[string]string
	if err = json.Unmarshal(raw, &integrity); err != nil || integrity["integrity"] != "ok" {
		t.Fatalf("administrative integrity report is not verified: %s %v", raw, err)
	}
	raw, err = admin(t, dir, "stats")
	if err != nil {
		t.Fatalf("offline stats failed: %s %v", raw, err)
	}
	var stats engine.Stats
	if err = json.Unmarshal(raw, &stats); err != nil || stats.DatabaseID != commit.Token.DatabaseID || !strings.HasPrefix(stats.SQLiteVersion, "3.") {
		t.Fatalf("administrative stats lost identity or engine version: %s %v", raw, err)
	}
	restoredDir := privateDir(t)
	destination := filepath.Join(restoredDir, "fixture.db")
	raw, err = admin(t, dir, "backup", "--destination", destination)
	if err != nil {
		t.Fatalf("administrative consistent backup failed: %s %v", raw, err)
	}
	var backup engine.BackupReceipt
	if err = json.Unmarshal(raw, &backup); err != nil || backup.DatabaseID != commit.Token.DatabaseID || backup.Bytes == 0 {
		t.Fatalf("backup CLI did not publish a valid identity/receipt: %s %v", raw, err)
	}
	digest := fileHash(t, destination)
	if raw, err = admin(t, dir, "backup", "--destination", destination); err == nil || fileHash(t, destination) != digest {
		t.Fatalf("administrative backup overwrote an existing candidate: %s %v", raw, err)
	}
	configFiles(t, restoredDir)
	if raw, err = admin(t, restoredDir, "check"); err != nil {
		t.Fatalf("administrative restore candidate failed integrity: %s %v", raw, err)
	}
	// Activation here is the documented explicit private file grant, served by
	// a fresh real process; there is no legacy import or overwrite operation.
	restored := start(t, restoredDir, false, "")
	verifyBusiness(t, nativeRead(t, restored), req, commit)
	retained, err := restored.client.Receipt(deadline(t), "admin-resolve", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(retained, commit) {
		t.Fatalf("CLI backup lost service receipt: %+v %v", retained, err)
	}
	evidence(t, map[string]any{"administrative_check_stats_backup_tested": true,
		"live_owner_check_and_backup_rejected": true, "backup_receipt": backup, "backup_sha256": digest,
		"new_business_and_commit_receipt_served_from_explicit_candidate": true,
		"deployment_restore_activation_window_tested":                    false})
}

func TestFaultAdministrativeCLIInvalidOperationNoMutation(t *testing.T) {
	for _, operation := range []string{"unknown-operation", "backup"} {
		t.Run(operation, func(t *testing.T) {
			source := open(t, filepath.Join(privateDir(t), "fixture.db"), true, nil)
			req := request(read(t, source).Token, "invalid-admin-source")
			if _, err := source.Batch(deadline(t), req); err != nil {
				t.Fatal(err)
			}
			candidate := privateDir(t)
			path := filepath.Join(candidate, "fixture.db")
			if _, err := source.Backup(deadline(t), path); err != nil {
				t.Fatal(err)
			}
			configFiles(t, candidate)
			before := fileHash(t, path)
			raw, err := admin(t, candidate, operation)
			if err == nil {
				t.Fatalf("invalid administrative request succeeded: %s", raw)
			}
			after := fileHash(t, path)
			evidence(t, map[string]any{"operation": operation, "rejected": true, "diagnostic": strings.TrimSpace(string(raw)),
				"candidate_before_sha256": before, "candidate_after_sha256": after, "candidate_unchanged": before == after})
			if before != after {
				t.Fatalf("invalid administrative operation changed a backup candidate before argument rejection: %s", raw)
			}
		})
	}
}
