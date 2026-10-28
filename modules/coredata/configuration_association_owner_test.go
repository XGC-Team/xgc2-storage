package coredata_test

import (
	"reflect"
	"testing"

	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestAgentAssociationsReopenAndFailBeforePublication(t *testing.T) {
	f := openStore(t)
	q := f.create(t, "asset")
	q.Snapshot.Associations = []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "board"}}
	created, err := f.core.CreateResource(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil || generation < 1 {
		t.Fatalf("generation=%d %v", generation, err)
	}
	f.reopen(t)
	page, err := f.core.ReadAgentSources(f.ctx, model.ConfigurationAgentSourceRead{Domain: f.domain, AgentID: "board", Limit: 4})
	if err != nil || page.Generation != generation || len(page.Items) != 1 || page.Items[0].Header.Head.Commit.ID != created.Result.Head.Commit.ID {
		t.Fatalf("reopen lost association: %+v %v", page, err)
	}
	selected := f.read(t, "asset", "main", "")
	if !reflect.DeepEqual(selected.Associations, q.Snapshot.Associations) || selected.Head.Commit.AssociationCount != 1 {
		t.Fatalf("reopen lost immutable fact set: %+v", selected)
	}
	// Receipt replay cannot advance the mutation fence or duplicate facts.
	replayed, err := f.core.CreateResource(f.ctx, q)
	if err != nil || !replayed.Replayed {
		t.Fatalf("replay: %+v %v", replayed, err)
	}
	after, err := f.core.ReadConfigurationGeneration(f.ctx)
	if err != nil || after != generation || f.count(t, "core_configuration_agent_index") != 1 {
		t.Fatal("replay changed generation or facts")
	}
	base := f.read(t, "asset", "main", "")
	for _, tc := range []struct {
		name  string
		facts []model.ConfigurationAgentAssociation
		code  string
	}{
		{"missing", nil, "invalid_argument"},
		{"duplicate", []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationRobotOwner, AgentID: "board"}, {Kind: model.AgentAssociationRobotOwner, AgentID: "board"}}, "invalid_argument"},
		{"unsorted", []model.ConfigurationAgentAssociation{{Kind: model.AgentAssociationWorld, AgentID: "z"}, {Kind: model.AgentAssociationRobotOwner, AgentID: "board"}}, "invalid_argument"},
		{"too-many", make([]model.ConfigurationAgentAssociation, model.MaxConfigurationAssociations+1), "resource_exhausted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commit := f.commit(t, base, "bad-"+tc.name, "a") // unchanged RootDigest exercises noop admission too
			commit.Snapshot.Associations = tc.facts
			_, err := f.core.CommitResource(f.ctx, commit)
			requireCode(t, err, tc.code)
			current, e := f.core.ReadConfigurationGeneration(f.ctx)
			if e != nil || current != generation || f.count(t, "core_configuration_agent_index") != 1 || f.count(t, "core_snapshots") != 1 {
				t.Fatalf("rejected projection changed durable state: %d %v", current, e)
			}
		})
	}
}
