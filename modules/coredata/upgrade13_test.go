package coredata_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
)

// This fixture was created through the actual schema-13 Storage implementation
// at 68d01b015284d1348420b22c913ff3c01260d560, using its public CreateResource,
// OpenSession, CreateRun and ReserveRemoteRun operations. It has one bound and
// one historically unbound pending child, and no private deployment data.
func realThirteenFixture(t *testing.T) (engine.Config, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open("testdata/coredata13-real.db.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	unzip, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer unzip.Close()
	path := filepath.Join(dir, "owner.db")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(out, unzip)
	if err := errors.Join(copyErr, out.Close()); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(dir, "backups")
	if err := os.Mkdir(backupDir, 0700); err != nil {
		t.Fatal(err)
	}
	return engine.Config{Path: path, Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}}, backupDir
}

func oldThirteenOwner(t *testing.T, cfg engine.Config) *engine.Store {
	t.Helper()
	old := coredata.Module()
	old.Version = 13
	cfg.Modules = []engine.Module{old}
	db, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Compare every original column and row in the actual accepted business
// tables. The new Run field is separately checked through the typed API.
func originalFacts(t *testing.T, path string) map[string][]string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	out := make(map[string][]string)
	for _, table := range []string{"core_configuration_domains", "core_resources", "core_branches", "core_snapshots", "core_changes", "sessions", "runs", "run_remote_pending"} {
		rows, err := db.Query("PRAGMA table_info(" + table + ")")
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for rows.Next() {
			var seq, notnull, pk int
			var name, typ string
			var def any
			if err := rows.Scan(&seq, &name, &typ, &notnull, &def, &pk); err != nil {
				t.Fatal(err)
			}
			if name != "resource_recovery_json" {
				columns = append(columns, `"`+strings.ReplaceAll(name, `"`, `""`)+`"`)
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		if len(columns) == 0 {
			t.Fatalf("fixture missing table %s", table)
		}
		data, err := db.Query("SELECT " + strings.Join(columns, ",") + " FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		for data.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := data.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			var parts []string
			for _, value := range values {
				parts = append(parts, fmt.Sprintf("%T:%x", value, value))
			}
			out[table] = append(out[table], strings.Join(parts, "|"))
		}
		if err := errors.Join(data.Err(), data.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(out[table])
	}
	return out
}

func assertPrecondition(t *testing.T, err error, code string) {
	t.Helper()
	var failure *api.Error
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("wanted %s: %v", code, err)
	}
}

func TestOfflineUpgradeRealThirteenPreservesAcceptedFacts(t *testing.T) {
	cfg, backups := realThirteenFixture(t)
	ctx := t.Context()
	before := originalFacts(t, cfg.Path)
	if len(before["core_resources"]) != 1 || len(before["sessions"]) != 1 || len(before["runs"]) != 1 || len(before["run_remote_pending"]) != 2 {
		t.Fatalf("old real fixture did not contain its expected records: %+v", before)
	}
	if db, err := engine.Open(ctx, cfg); err == nil {
		db.Close()
		t.Fatal("ordinary startup silently accepted schema 13")
	} else {
		assertPrecondition(t, err, "failed_precondition")
	}
	destination := filepath.Join(backups, "before.db")
	receipt, err := coredata.Upgrade13To14(ctx, cfg, destination)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.From != 13 || receipt.To != 14 || receipt.DatabaseID == "" || receipt.Backup.DatabaseID != receipt.DatabaseID || receipt.Backup.Bytes == 0 {
		t.Fatalf("incomplete explicit upgrade receipt: %+v", receipt)
	}
	if got := originalFacts(t, destination); !reflect.DeepEqual(got, before) {
		t.Fatal("pre-upgrade backup changed accepted facts")
	}
	if got := originalFacts(t, cfg.Path); !reflect.DeepEqual(got, before) {
		t.Fatal("upgrade changed accepted Config, Session, Run or pending relations")
	}

	db, err := engine.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("normal current-schema reopen: %v", err)
	}
	scope := api.Scope{Namespace: "core", User: "operator", Workspace: "station"}
	store, err := coredata.New(db, scope)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.GetRun(ctx, "kept-run")
	if err != nil || run.ID != "kept-run" || run.SessionID != "kept-session" {
		t.Fatalf("run: %+v %v", run, err)
	}
	if state, found, err := store.GetRunResourceRecovery(ctx, "kept-run"); err != nil || found || state.Joined {
		t.Fatalf("old empty evidence did not remain unknown: %+v %v %v", state, found, err)
	}
	page, err := store.ListPendingRemoteRuns(ctx, "agent", "", 10)
	if err != nil || len(page.Links) != 2 || page.Links[0].BootID != "old-agent-boot" || page.Links[1].BootID != "" {
		t.Fatalf("pending exact identities: %+v %v", page, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	repeat := filepath.Join(backups, "repeat.db")
	if _, err := coredata.Upgrade13To14(ctx, cfg, repeat); err == nil {
		t.Fatal("repeat upgrade accepted a 14 database")
	} else {
		assertPrecondition(t, err, "failed_precondition")
	}
	if _, err := os.Stat(repeat); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("repeat created a backup: %v", err)
	}
}

func TestOfflineUpgradeRefusesRunningOwnerAndWrongVersion(t *testing.T) {
	t.Run("owned", func(t *testing.T) {
		cfg, backups := realThirteenFixture(t)
		db := oldThirteenOwner(t, cfg)
		defer db.Close()
		if _, err := coredata.Upgrade13To14(t.Context(), cfg, filepath.Join(backups, "busy.db")); err == nil {
			t.Fatal("running owner was not excluded")
		} else {
			assertPrecondition(t, err, "conflict")
		}
		if _, err := os.Stat(filepath.Join(backups, "busy.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("busy owner created a backup: %v", err)
		}
	})
	t.Run("wrong-version", func(t *testing.T) {
		cfg, backups := realThirteenFixture(t)
		db := oldThirteenOwner(t, cfg)
		if err := db.Write(t.Context(), engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "UPDATE schema_versions SET version=12 WHERE module='coredata'")
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		before := originalFacts(t, cfg.Path)
		if _, err := coredata.Upgrade13To14(t.Context(), cfg, filepath.Join(backups, "wrong.db")); err == nil {
			t.Fatal("accepted version 12")
		} else {
			assertPrecondition(t, err, "failed_precondition")
		}
		if got := originalFacts(t, cfg.Path); !reflect.DeepEqual(got, before) {
			t.Fatal("wrong version changed facts")
		}
		if _, err := os.Stat(filepath.Join(backups, "wrong.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("wrong version created a backup: %v", err)
		}
	})
	t.Run("backup-exists", func(t *testing.T) {
		cfg, backups := realThirteenFixture(t)
		before := originalFacts(t, cfg.Path)
		backup := filepath.Join(backups, "existing.db")
		if err := os.WriteFile(backup, []byte("kept-backup"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := coredata.Upgrade13To14(t.Context(), cfg, backup); err == nil {
			t.Fatal("upgrade overwrote an existing backup")
		}
		if got, err := os.ReadFile(backup); err != nil || string(got) != "kept-backup" {
			t.Fatalf("existing backup changed: %q %v", got, err)
		}
		if got := originalFacts(t, cfg.Path); !reflect.DeepEqual(got, before) {
			t.Fatal("refused backup changed accepted facts")
		}
		old := oldThirteenOwner(t, cfg)
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("different-manifest", func(t *testing.T) {
		cfg, backups := realThirteenFixture(t)
		before := originalFacts(t, cfg.Path)
		cfg.Manifest.Namespaces[0].Collections[0].Indexes = []api.Index{{ID: "different", Fields: []string{"field"}}}
		backup := filepath.Join(backups, "wrong-manifest.db")
		if _, err := coredata.Upgrade13To14(t.Context(), cfg, backup); err == nil {
			t.Fatal("upgrade accepted different collection declarations")
		} else {
			assertPrecondition(t, err, "failed_precondition")
		}
		if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("different manifest created backup: %v", err)
		}
		if got := originalFacts(t, cfg.Path); !reflect.DeepEqual(got, before) {
			t.Fatal("different manifest changed accepted facts")
		}
	})
}

func TestOfflineUpgradeCrashRollsBackTheSameThirteenDatabase(t *testing.T) {
	if os.Getenv("XGC_TEST_UPGRADE_CHILD") == "1" {
		cfg := engine.Config{Path: os.Getenv("XGC_TEST_DB"), Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}}
		_, err := engine.UpgradeCoreData13To14(context.Background(), cfg, os.Getenv("XGC_TEST_BACKUP"), func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE runs ADD COLUMN resource_recovery_json BLOB NOT NULL DEFAULT x''"); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "column-written-before-commit")
			select {}
		})
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg, backups := realThirteenFixture(t)
	before := originalFacts(t, cfg.Path)
	backup := filepath.Join(backups, "crash-before.db")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(exe, "-test.run=^TestOfflineUpgradeCrashRollsBackTheSameThirteenDatabase$")
	child.Env = append(os.Environ(), "XGC_TEST_UPGRADE_CHILD=1", "XGC_TEST_DB="+cfg.Path, "XGC_TEST_BACKUP="+backup)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); done <- line }()
	select {
	case line := <-done:
		if strings.TrimSpace(line) != "column-written-before-commit" {
			child.Process.Kill()
			child.Wait()
			t.Fatalf("child did not reach transaction cut: %q", line)
		}
	case <-time.After(10 * time.Second):
		child.Process.Kill()
		child.Wait()
		t.Fatal("child did not reach transaction cut")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if got := originalFacts(t, cfg.Path); !reflect.DeepEqual(got, before) {
		t.Fatal("interrupted transaction changed accepted facts")
	}
	if got := originalFacts(t, backup); !reflect.DeepEqual(got, before) {
		t.Fatal("crash lost the verified pre-change backup")
	}
	old := oldThirteenOwner(t, cfg)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := engine.Open(t.Context(), cfg); err == nil {
		db.Close()
		t.Fatal("interrupted 13 database became current")
	}
	if _, err := coredata.Upgrade13To14(t.Context(), cfg, filepath.Join(backups, "retry.db")); err != nil {
		t.Fatalf("legitimate retry after rollback: %v", err)
	}
	db, err := engine.Open(t.Context(), cfg)
	if err != nil {
		t.Fatalf("ordinary reopen after retry: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
