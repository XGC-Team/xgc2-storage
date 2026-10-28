package coredata

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAgentLinkWritesUseOnlyExactIndexedIdentityAndOwnership(t *testing.T) {
	var selected [2]int
	for trial, others := range []int{0, 64} {
		rec := &headerSQLRecorder{}
		db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "agent-links.db") + "?_pragma=foreign_keys(1)", rec: rec})
		db.SetMaxOpenConns(1)
		ctx := t.Context()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = Initialize(ctx, tx); err != nil {
			t.Fatal(err)
		}
		for i := range others {
			r := model.AgentLinkRecord{AgentID: fmt.Sprintf("other-%02d", i), Enrollment: "known", PublicKeyFingerprint: fmt.Sprintf("%064x", i+1), AgentLinkFacts: model.AgentLinkFacts{CapabilityManifest: []byte(`{}`)}}
			if _, err = agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, 0, r); err != nil {
				t.Fatal(err)
			}
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		r := model.AgentLinkRecord{AgentID: "selected", Enrollment: "known", PublicKeyFingerprint: strings.Repeat("f", 64), AgentLinkFacts: model.AgentLinkFacts{CapabilityManifest: []byte(`{"x":1}`)}}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec.statements, rec.bytes = nil, 0
		created, err := agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, 0, r)
		if err != nil || !created.Changed || len(rec.statements) != 2 || rec.bytes != 0 {
			t.Fatalf("create: %+v statements=%v bytes=%d err=%v", created, rec.statements, rec.bytes, err)
		}
		if !strings.Contains(rec.statements[1], "INDEXED BY core_agent_links_fingerprint") {
			t.Fatalf("fingerprint query not indexed: %s", rec.statements[1])
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		r.DisplayName = "renamed"
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec.statements, rec.bytes = nil, 0
		updated, err := agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, 1, r)
		if err != nil || !updated.Changed || len(rec.statements) != 1 {
			t.Fatalf("same-pin update: %+v statements=%v err=%v", updated, rec.statements, err)
		}
		selected[trial] = rec.bytes
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec.statements, rec.bytes = nil, 0
		noop, err := agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, 2, r)
		if err != errAgentLinkNoChange || noop.Changed || len(rec.statements) != 1 {
			t.Fatalf("noop: %+v statements=%v err=%v", noop, rec.statements, err)
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		for _, query := range []struct {
			sql   string
			args  []any
			index string
		}{
			{`SELECT version FROM core_agent_links WHERE scope=? AND agent_id=?`, []any{testScope, "selected"}, "sqlite_autoindex_core_agent_links_1"},
			{`SELECT agent_id FROM core_agent_links INDEXED BY core_agent_links_fingerprint WHERE scope=? AND fingerprint=?`, []any{testScope, strings.Repeat("f", 64)}, "core_agent_links_fingerprint"},
		} {
			plans, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query.sql, query.args...)
			if err != nil {
				t.Fatal(err)
			}
			var detail string
			for plans.Next() {
				var a, b, c int
				var part string
				if err = plans.Scan(&a, &b, &c, &part); err != nil {
					t.Fatal(err)
				}
				detail += part
			}
			plans.Close()
			if !strings.Contains(detail, "SEARCH") || !strings.Contains(detail, query.index) || strings.Contains(detail, "SCAN core_agent_links") {
				t.Fatalf("unindexed query: %s", detail)
			}
		}
		db.Close()
	}
	if selected[0] != selected[1] {
		t.Fatalf("unrelated agents changed selected driver bytes: %v", selected)
	}
	t.Logf("selected same-pin update driver bytes 0/64 unrelated agents=%d/%d; exact row query only; create uses row and fingerprint indexes", selected[0], selected[1])
}

func TestAgentLinkSameSizeChangeDoesNotRewriteQuota(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "same-size.db") + "?_pragma=foreign_keys(1)", rec: rec})
	defer db.Close()
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err != nil {
		t.Fatal(err)
	}
	record := model.AgentLinkRecord{AgentID: "agent", Enrollment: "known", AgentLinkFacts: model.AgentLinkFacts{DisplayName: "aaaaa", CapabilityManifest: []byte(`{}`)}}
	if _, err = agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, 0, record); err != nil {
		t.Fatal(err)
	}
	observed := false
	for version := int64(1); version <= 20; version++ {
		var beforeChanges, beforeBytes int64
		if err = tx.QueryRowContext(ctx, `SELECT total_changes()`).Scan(&beforeChanges); err != nil {
			t.Fatal(err)
		}
		if err = tx.QueryRowContext(ctx, `SELECT agent_link_bytes FROM core_data_usage WHERE scope=?`, testScope).Scan(&beforeBytes); err != nil {
			t.Fatal(err)
		}
		if version%2 == 1 {
			record.DisplayName = "bbbbb"
		} else {
			record.DisplayName = "aaaaa"
		}
		out, err := agentLinkWrite(ctx, tx, testScope, model.AgentLinkIdentity{}, version, record)
		if err != nil || !out.Changed {
			t.Fatalf("update %d=%+v %v", version, out, err)
		}
		var afterChanges, afterBytes int64
		if err = tx.QueryRowContext(ctx, `SELECT total_changes()`).Scan(&afterChanges); err != nil {
			t.Fatal(err)
		}
		if err = tx.QueryRowContext(ctx, `SELECT agent_link_bytes FROM core_data_usage WHERE scope=?`, testScope).Scan(&afterBytes); err != nil {
			t.Fatal(err)
		}
		wantWrites := int64(2)
		if beforeBytes == afterBytes {
			wantWrites = 1
			observed = true
		}
		if afterChanges-beforeChanges != wantWrites {
			t.Fatalf("unchanged bytes=%v wrote %d rows, want %d", beforeBytes == afterBytes, afterChanges-beforeChanges, wantWrites)
		}
		if observed {
			break
		}
	}
	if !observed {
		t.Fatal("fixture never produced a same-size timestamp encoding")
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
