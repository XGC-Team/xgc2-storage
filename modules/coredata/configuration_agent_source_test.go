package coredata

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAgentSourceIndexPagesLifecycleAndIntegrity(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	robot := configurationCreateForTest(t, domain, "robot", nil)
	robot.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "target"}}
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, robot)
	ref := model.ConfigurationReference{Slot: "robot:slot1", Mode: "tracking", TargetDomain: domain.Key, TargetResourceID: "robot", TargetBranch: "main"}
	experiment := configurationCreateForTest(t, domain, "experiment", []model.ConfigurationReference{ref})
	experiment.Snapshot.Associations = []model.ConfigurationAgentAssociation{
		{Kind: model.AgentAssociationOnboardSlot, AgentID: "target", SlotID: "slot1", RobotResourceID: "robot"},
		{Kind: model.AgentAssociationSlotHost, AgentID: "target", SlotID: "slot1"},
	}
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, experiment)
	readAgent := func(agent string, after *model.ConfigurationAgentSourceCursor, limit int) model.ConfigurationAgentSourcePage {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		page, err := configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: agent, Limit: limit, After: after})
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	first := readAgent("target", nil, 1)
	if len(first.Items) != 1 || first.Next == nil {
		t.Fatalf("first page: %+v", first)
	}
	second := readAgent("target", first.Next, 1)
	third := readAgent("target", second.Next, 1)
	if len(second.Items) != 1 || second.Next == nil || len(third.Items) != 1 || third.Next != nil {
		t.Fatalf("continuations: %+v %+v", second, third)
	}
	if first.Items[0].Header.Head.Commit.ID != "experiment-v1" || second.Items[0].Header.Head.Commit.ID != "experiment-v1" || third.Items[0].Header.Head.Commit.ID != "robot-v1" {
		t.Fatal("missing or reordered exact facts")
	}
	tx, _ := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err := configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "other", Limit: 1, After: first.Next})
	tx.Rollback()
	configurationCode(t, err, "invalid_argument")
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err = configurationAgentSources(ctx, tx, "other-scope", model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "target", Limit: 1, After: first.Next})
	tx.Rollback()
	configurationCode(t, err, "invalid_argument")
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	incoming, err := configurationRobotIncoming(ctx, tx, testScope, model.ConfigurationRobotIncomingRead{Domain: domain, RobotDomain: domain, RobotID: "robot", Limit: 1})
	tx.Rollback()
	if err != nil || len(incoming.Items) != 1 || incoming.Items[0].Slot != "robot:slot1" || incoming.Items[0].Header.Head.Commit.ID != "experiment-v1" {
		t.Fatalf("reverse reference: %+v %v", incoming, err)
	}

	// An exact complete read verifies the same fact digest; payload and indexes
	// are separate, so discovery remains cheap even if an unused body is bad.
	execSQL(t, db, ctx, `UPDATE core_snapshots SET payload=? WHERE id=?`, []byte(`broken`), "robot-v1")
	if len(readAgent("target", nil, 10).Items) != 3 {
		t.Fatal("discovery read unrelated document bytes")
	}
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err = configurationRead(ctx, tx, testScope, model.ConfigurationResourceRead{Domain: domain, ResourceID: "robot", Branch: "main"})
	tx.Rollback()
	configurationCode(t, err, "data_loss")
	// Restore the exact bytes, then advance the Experiment's main. Its old
	// facts remain immutable, but must not appear as current candidates.
	execSQL(t, db, ctx, `UPDATE core_snapshots SET payload=? WHERE id=?`, robot.Snapshot.Payload, "robot-v1")
	commit := configurationCommitForTest(t, domain, created, "next", "b", true, nil, experiment.Snapshot.Manifest)
	commit.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationSlotHost, AgentID: "new-target", SlotID: "slot1"}}
	configurationWrite(t, db, ctx, model.ResourceCommitOperation, commit)
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err = configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "target", Limit: 1, After: first.Next})
	tx.Rollback()
	configurationCode(t, err, "conflict")
	if got := readAgent("target", nil, 10); len(got.Items) != 1 || got.Items[0].Header.Head.Resource.ID != "robot" {
		t.Fatalf("stale facts leaked: %+v", got)
	}
	if got := readAgent("new-target", nil, 10); len(got.Items) != 1 || got.Items[0].Header.Head.Commit.ID != commit.CommitID {
		t.Fatalf("new main missing: %+v", got)
	}
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	incoming, err = configurationRobotIncoming(ctx, tx, testScope, model.ConfigurationRobotIncomingRead{Domain: domain, RobotDomain: domain, RobotID: "robot", Limit: 1})
	tx.Rollback()
	if err != nil || len(incoming.Items) != 0 {
		t.Fatalf("stale reverse reference: %+v %v", incoming, err)
	}
	// Fact loss or alteration is detected when the selected immutable snapshot
	// is opened; discovery does not claim an empty lookup audits the database.
	execSQL(t, db, ctx, `DELETE FROM core_configuration_agent_index WHERE commit_id=?`, commit.CommitID)
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err = configurationRead(ctx, tx, testScope, model.ConfigurationResourceRead{Domain: domain, ResourceID: "experiment", Branch: "main"})
	tx.Rollback()
	configurationCode(t, err, "data_loss")
}

func TestAgentSourceIndexUsesSelectedTargetOnly(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "agents.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err == nil {
		err = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: domain.RegistryDigest}})
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var directBytes, reverseBytes int
	for _, n := range []int{1, 2048} {
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("asset-%03d-%03d", n, i)
			q := configurationCreateForTest(t, domain, id, nil)
			if n == 1 {
				q.Snapshot.Payload = []byte(`{"identity":{"active":true},"body":{"graph":"` + strings.Repeat("x", 768<<10) + `"}}`)
			}
			q.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "unrelated-" + id}}
			if n == 1 {
				q.Snapshot.Associations[0].AgentID = "selected"
			}
			configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
		}
		if n == 1 {
			linked := configurationCreateForTest(t, domain, "linked-experiment", []model.ConfigurationReference{{Slot: "robot:slot", Mode: "tracking", TargetDomain: domain.Key, TargetResourceID: "asset-001-000", TargetBranch: "main"}})
			configurationWrite(t, db, ctx, model.ResourceCreateOperation, linked)
		}
		rec.statements, rec.bytes = nil, 0
		tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		page, err := configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "selected", Limit: 8})
		tx.Rollback()
		if err != nil || len(page.Items) != 1 || len(rec.statements) != 3 || rec.bytes > 8192 {
			t.Fatalf("%d unrelated: %+v SQL=%d bytes=%d err=%v", n, page, len(rec.statements), rec.bytes, err)
		}
		query := strings.ToLower(rec.statements[2])
		for _, forbidden := range []string{"s.payload", "s.manifest", "core_references"} {
			if strings.Contains(query, forbidden) {
				t.Fatalf("discovery read %s", forbidden)
			}
		}
		if n == 1 {
			directBytes = rec.bytes
		} else if rec.bytes != directBytes {
			t.Fatalf("%d unrelated rows changed direct selected bytes: %d vs %d", n, rec.bytes, directBytes)
		}
		rec.statements, rec.bytes = nil, 0
		tx, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		reverse, err := configurationRobotIncoming(ctx, tx, testScope, model.ConfigurationRobotIncomingRead{Domain: domain, RobotDomain: domain, RobotID: "asset-001-000", Limit: 8})
		tx.Rollback()
		if err != nil || len(reverse.Items) != 1 || len(rec.statements) != 4 || rec.bytes > 8192 {
			t.Fatalf("%d unrelated reverse query=%+v SQL=%d bytes=%d err=%v", n, reverse, len(rec.statements), rec.bytes, err)
		}
		if n == 1 {
			reverseBytes = rec.bytes
		} else if rec.bytes != reverseBytes {
			t.Fatalf("%d unrelated rows changed reverse selected bytes: %d vs %d", n, rec.bytes, reverseBytes)
		}
	}
	for _, test := range []struct {
		query, index string
		args         []any
	}{
		{agentSourceQuery, "core_configuration_agent_target", []any{testScope, "selected", domain.Key, false, "", "", "", "", 9}},
		{robotIncomingQuery, "core_reference_targets", []any{testScope, domain.Key, "robot", domain.Key, "", "", "", 9}},
	} {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+test.query, test.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var a, b, c int
			var detail string
			if err = rows.Scan(&a, &b, &c, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail + "; ")
		}
		rows.Close()
		if !strings.Contains(plan.String(), test.index+" (") || strings.Contains(plan.String(), "SCAN a") {
			t.Fatalf("unbounded target discovery: %s", plan.String())
		}
	}
}

func TestAgentSourcePageRejectsSelectedDriverByteOverflow(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("selected-%02d", i)
		q := configurationCreateForTest(t, domain, id, nil)
		q.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "selected"}}
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	// Unknown corrupt metadata is not reflected in the public head after JSON
	// decoding, but its actual selected driver bytes are still charged.
	execSQL(t, db, ctx, `UPDATE core_resources SET body=json_set(CAST(body AS TEXT),'$.padding',?) WHERE scope=? AND domain=?`, strings.Repeat("x", 240<<10), testScope, domain.Key)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	page, err := configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "selected", Limit: 80})
	tx.Rollback()
	configurationCode(t, err, "resource_exhausted")
	if page.Items != nil || page.Next != nil {
		t.Fatal("selected-byte overflow returned a partial page")
	}
}

func TestAgentWorkflowDiscoveryOmitsWorldOnlyHeaders(t *testing.T) {
	rec := &headerSQLRecorder{}
	db := sql.OpenDB(headerCountingConnector{dsn: "file:" + filepath.Join(t.TempDir(), "world-only.db") + "?_pragma=foreign_keys(1)", rec: rec})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	domain := model.ConfigurationDomainGuard{Key: "test", SchemaIdentity: "catalog-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("a", 64)}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = Initialize(ctx, tx); err == nil {
		err = declareDomains(ctx, tx, []model.ConfigurationDomainDeclaration{{Key: domain.Key, SchemaIdentity: domain.SchemaIdentity, SchemaVersion: 1, RegistryDigest: domain.RegistryDigest}})
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("world-%02d", i)
		q := configurationCreateForTest(t, domain, id, nil)
		q.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationWorld, AgentID: "world-agent"}}
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, q)
	}
	read := func(exclude bool) (model.ConfigurationAgentSourcePage, int) {
		t.Helper()
		rec.statements, rec.bytes = nil, 0
		tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			t.Fatal(e)
		}
		out, e := configurationAgentSources(ctx, tx, testScope, model.ConfigurationAgentSourceRead{Domain: domain, AgentID: "world-agent", Limit: 32, ExcludeWorld: exclude})
		tx.Rollback()
		if e != nil {
			t.Fatal(e)
		}
		return out, rec.bytes
	}
	all, withWorld := read(false)
	workflow, withoutWorld := read(true)
	if len(all.Items) != 16 || len(workflow.Items) != 0 || workflow.Next != nil || withoutWorld >= withWorld/4 {
		t.Fatalf("world=%d/%d selected bytes=%d/%d", len(all.Items), len(workflow.Items), withWorld, withoutWorld)
	}
}

func TestRobotIncomingContinuationIsTargetScopedAndGenerationFenced(t *testing.T) {
	db, ctx, domain := configurationFixture(t)
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, domain, "robot", nil))
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("experiment-%d", i)
		ref := model.ConfigurationReference{Slot: "robot:slot", Mode: "tracking", TargetDomain: domain.Key, TargetResourceID: "robot", TargetBranch: "main"}
		configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, domain, id, []model.ConfigurationReference{ref}))
	}
	read := func(q model.ConfigurationRobotIncomingRead) (model.ConfigurationRobotIncomingPage, error) {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return configurationRobotIncoming(ctx, tx, testScope, q)
	}
	q := model.ConfigurationRobotIncomingRead{Domain: domain, RobotDomain: domain, RobotID: "robot", Limit: 1}
	page, err := read(q)
	if err != nil || len(page.Items) != 1 || page.Next == nil {
		t.Fatalf("first page %+v %v", page, err)
	}
	q.After = page.Next
	q.RobotID = "other"
	_, err = read(q)
	configurationCode(t, err, "invalid_argument")
	q.RobotID = "robot"
	second, err := read(q)
	if err != nil || len(second.Items) != 1 || second.Next != nil || second.Items[0].Header.Head.Commit.ID == page.Items[0].Header.Head.Commit.ID {
		t.Fatalf("second page %+v %v", second, err)
	}
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, domain, "unrelated", nil))
	_, err = read(q)
	configurationCode(t, err, "conflict")
}
