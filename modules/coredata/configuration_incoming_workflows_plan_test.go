package coredata

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestWorkflowRobotBindingsUsesSelectedCoveringReferenceIndex(t *testing.T) {
	db, ctx, _ := configurationFixture(t)
	args := []any{testScope, `["selected"]`, testScope}
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+workflowRobotLinksQuery, args...)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "SEARCH core_references USING INDEX core_reference_robot_workflow_inverse") ||
		!strings.Contains(joined, "target_resource_id=?") {
		t.Fatalf("inverse selection is not a target-index seek:\n%s", joined)
	}
	var root int
	if err := db.QueryRowContext(ctx, "SELECT rootpage FROM sqlite_master WHERE name='core_references'").Scan(&root); err != nil {
		t.Fatal(err)
	}
	program, err := db.QueryContext(ctx, "EXPLAIN "+workflowRobotLinksQuery, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer program.Close()
	tableCursors := map[int]bool{}
	var bodyReads []string
	for program.Next() {
		var address, p1, p2, p3, p5 int
		var opcode string
		var p4, comment sql.NullString
		if err := program.Scan(&address, &opcode, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
			t.Fatal(err)
		}
		if opcode == "OpenRead" && p2 == root {
			tableCursors[p1] = true
		}
		if opcode == "Column" && tableCursors[p1] {
			bodyReads = append(bodyReads, fmt.Sprintf("%d:%d", p1, p2))
		}
	}
	if err := program.Err(); err != nil {
		t.Fatal(err)
	}
	if len(tableCursors) == 0 || len(bodyReads) != 0 {
		t.Fatalf("selected query reads core_references table columns %v (cursors %v)", bodyReads, tableCursors)
	}
	t.Logf("selected inverse query uses target-leading partial covering index; SQLite reference-table column reads=0; plan: %s", joined)
}

func TestWorkflowRobotBindingsOneReadSnapshotAcrossMove(t *testing.T) {
	db, ctx, _ := configurationFixture(t)
	db.SetMaxOpenConns(2)
	workflow := model.ConfigurationDomainGuard{Key: "automation", SchemaIdentity: "automation-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("b", 64)}
	robot := model.ConfigurationDomainGuard{Key: "robot", SchemaIdentity: "robot-v1", SchemaVersion: 1, RegistryDigest: strings.Repeat("c", 64)}
	declare, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = declareDomains(ctx, declare, []model.ConfigurationDomainDeclaration{
		{Key: workflow.Key, SchemaIdentity: workflow.SchemaIdentity, SchemaVersion: 1, RegistryDigest: workflow.RegistryDigest, MainVisibility: true},
		{Key: robot.Key, SchemaIdentity: robot.SchemaIdentity, SchemaVersion: 1, RegistryDigest: robot.RegistryDigest, MainVisibility: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err = declare.Commit(); err != nil {
		t.Fatal(err)
	}
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, workflow, "left", nil))
	configurationWrite(t, db, ctx, model.ResourceCreateOperation, configurationCreateForTest(t, workflow, "right", nil))
	ref := model.ConfigurationReference{Slot: "workflow-binding:start", Mode: "tracking", TargetDomain: "automation", TargetResourceID: "left", TargetBranch: "main"}
	create := configurationCreateForTest(t, robot, "robot", []model.ConfigurationReference{ref})
	created := configurationWrite(t, db, ctx, model.ResourceCreateOperation, create)
	read, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	// Force the SQLite snapshot before the competing committed move.
	if _, err = configurationDomain(ctx, read, workflow); err != nil {
		t.Fatal(err)
	}
	ref.TargetResourceID = "right"
	move := configurationCommitForTest(t, robot, created, "robot-move", "b", true, []model.ConfigurationReference{ref}, create.Snapshot.Manifest)
	configurationWrite(t, db, ctx, model.ResourceCommitOperation, move)
	q := model.ConfigurationWorkflowRobotRead{WorkflowDomain: workflow, RobotDomain: robot, WorkflowIDs: []string{"left", "right"}}
	before, err := configurationWorkflowRobotBindings(ctx, read, testScope, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Links) != 1 || before.Links[0].WorkflowID != "left" || before.Links[0].CommitID != created.Result.Head.Commit.ID || before.Robots[0].CommitID != created.Result.Head.Commit.ID {
		t.Fatalf("mixed pre-move cut: %+v", before)
	}
	if err = read.Commit(); err != nil {
		t.Fatal(err)
	}
	newRead, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer newRead.Rollback()
	after, err := configurationWorkflowRobotBindings(ctx, newRead, testScope, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Links) != 1 || after.Links[0].WorkflowID != "right" || after.Links[0].CommitID != move.CommitID || after.Robots[0].CommitID != move.CommitID {
		t.Fatalf("mixed post-move cut: %+v", after)
	}
}
