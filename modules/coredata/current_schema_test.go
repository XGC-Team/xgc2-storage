package coredata_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestOnlyCurrentCoreSchemaOpensWithoutChangingAcceptedFacts(t *testing.T) {
	for _, version := range []int{1, 2, 3, 4, 5, coredata.SchemaVersion + 1} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			f := openStore(t)
			if _, _, err := f.core.CreateRun(f.ctx, newRun("accepted-root")); err != nil {
				t.Fatal(err)
			}
			if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "UPDATE schema_versions SET version=? WHERE module=?", version, model.Module)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(f.config.Path)
			if err != nil {
				t.Fatal(err)
			}
			config := f.config
			config.Create = false
			if opened, err := engine.Open(f.ctx, config); errCode(err) != "failed_precondition" {
				if opened != nil {
					opened.Close()
				}
				t.Fatalf("noncurrent schema opened: %v", err)
			}
			after, err := os.ReadFile(config.Path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused schema changed accepted facts: %v", err)
			}
			backups, err := filepath.Glob(config.Path + ".before-*")
			if err != nil || len(backups) != 0 {
				t.Fatalf("refusal started migration work: %v %v", backups, err)
			}
		})
	}
}
