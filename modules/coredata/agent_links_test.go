package coredata_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func agentRecord(id, fingerprint string) model.AgentLinkRecord {
	return model.AgentLinkRecord{AgentID: id, Enrollment: "known", PublicKeyFingerprint: fingerprint, AgentLinkFacts: model.AgentLinkFacts{
		CapabilityManifest: json.RawMessage(`{"host":{"arch":"amd64"},"n":9007199254740993}`), Capabilities: []string{"execute"},
	}}
}

func absentAgent(identity model.AgentLinkIdentity, id string) model.AgentLinkGuard {
	return model.AgentLinkGuard{Identity: identity, AgentID: id, Version: "0"}
}

func TestAgentLinkExactGuardsNoopAndDurableProjection(t *testing.T) {
	f := openStore(t)
	initial, err := f.core.ListAgentLinks(f.ctx)
	if err != nil || len(initial.Records) != 0 || initial.Identity.DatabaseID != f.db.DatabaseID() || initial.Identity.Scope != f.scope || initial.Identity.ModuleSchemaVersion != coredata.SchemaVersion {
		t.Fatalf("initial=%+v %v", initial, err)
	}
	record := agentRecord("agent", strings.Repeat("a", 64))
	q := model.AgentLinkWrite{Expected: absentAgent(initial.Identity, record.AgentID), Record: record}
	before := stats(t, f)
	created, err := f.core.WriteAgentLink(f.ctx, q)
	if err != nil || !created.Changed || created.Stored.Guard.Version != "1" || created.Stored.Guard.Identity != initial.Identity || created.Stored.Guard.AgentID != record.AgentID || created.Stored.CreatedAt.IsZero() || !created.Stored.CreatedAt.Equal(created.Stored.UpdatedAt) {
		t.Fatalf("create=%+v %v", created, err)
	}
	if d, r := commits(t, f, before); d != 1 || r != 0 {
		t.Fatalf("create commits durable=%d relaxed=%d", d, r)
	}
	// Returned nested buffers and slices must never alias caller inputs or storage.
	created.Stored.Record.CapabilityManifest[0] = '['
	created.Stored.Record.Capabilities[0] = "mutated"
	if record.CapabilityManifest[0] != '{' || record.Capabilities[0] != "execute" {
		t.Fatal("result aliased caller input")
	}
	listed, err := f.core.ListAgentLinks(f.ctx)
	if err != nil || len(listed.Records) != 1 || listed.Records[0].Record.CapabilityManifest[0] != '{' || listed.Records[0].Record.Capabilities[0] != "execute" {
		t.Fatalf("durable list=%+v %v", listed, err)
	}
	stored := listed.Records[0]
	before = stats(t, f)
	noop, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: stored.Guard, Record: stored.Record})
	if err != nil || noop.Changed || !reflect.DeepEqual(noop.Stored, stored) {
		t.Fatalf("noop=%+v %v", noop, err)
	}
	if d, r := commits(t, f, before); d != 0 || r != 0 {
		t.Fatalf("noop made a commit durable=%d relaxed=%d", d, r)
	}
	// Different JSON member order and escaped spelling are the same canonical
	// capability fact and must not manufacture a new durable update.
	reordered := stored.Record
	reordered.CapabilityManifest = []byte(`{"n":9007199254740993,"host":{"arch":"\u0061md64"}}`)
	before = stats(t, f)
	canonicalNoop, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: stored.Guard, Record: reordered})
	if err != nil || canonicalNoop.Changed || !reflect.DeepEqual(canonicalNoop.Stored, stored) {
		t.Fatalf("canonical no-op=%+v %v", canonicalNoop, err)
	}
	if d, r := commits(t, f, before); d != 0 || r != 0 {
		t.Fatalf("canonical no-op made a commit durable=%d relaxed=%d", d, r)
	}
	if _, err := f.core.WriteAgentLink(f.ctx, q); !errors.Is(err, coredata.ErrAgentLinkVersionConflict) || errCode(err) != "conflict" {
		t.Fatalf("stale identical candidate did not conflict: %v", err)
	}
	updated := stored.Record
	updated.Enrollment = "revoked"
	changed, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: stored.Guard, Record: updated})
	if err != nil || !changed.Changed || changed.Stored.Guard.Version != "2" || changed.Stored.Record.PublicKeyFingerprint != record.PublicKeyFingerprint || !changed.Stored.CreatedAt.Equal(stored.CreatedAt) {
		t.Fatalf("revoke=%+v %v", changed, err)
	}
	forgot := agentRecord("agent", "")
	forgot.Capabilities = nil
	forgotten, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: changed.Stored.Guard, Record: forgot})
	if err != nil || forgotten.Stored.Guard.Version != "3" || !forgotten.Stored.CreatedAt.Equal(stored.CreatedAt) {
		t.Fatalf("forget=%+v %v", forgotten, err)
	}
	f.reopen(t)
	reopened, err := f.core.ListAgentLinks(f.ctx)
	if err != nil || len(reopened.Records) != 1 || !reflect.DeepEqual(reopened.Records[0], forgotten.Stored) {
		t.Fatalf("reopen=%+v %v", reopened, err)
	}
}

func TestAgentLinkConcurrentExactVersionAndFingerprintOwnership(t *testing.T) {
	for _, sameID := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_id_%t", sameID), func(t *testing.T) {
			f := openStore(t)
			initial, err := f.core.ListAgentLinks(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			a := agentRecord("agent-a", strings.Repeat("a", 64))
			b := agentRecord("agent-b", strings.Repeat("a", 64))
			if sameID {
				b.AgentID = a.AgentID
				b.PublicKeyFingerprint = strings.Repeat("b", 64)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			var results [2]model.AgentLinkWriteResult
			var errs [2]error
			for i, record := range []model.AgentLinkRecord{a, b} {
				wg.Add(1)
				go func(i int, record model.AgentLinkRecord) {
					defer wg.Done()
					<-start
					results[i], errs[i] = f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(initial.Identity, record.AgentID), Record: record})
				}(i, record)
			}
			close(start)
			wg.Wait()
			wins, losses := 0, 0
			for i, err := range errs {
				if err == nil {
					if !results[i].Changed {
						t.Fatal("winner did not change")
					}
					wins++
					continue
				}
				want := coredata.ErrAgentLinkFingerprintConflict
				if sameID {
					want = coredata.ErrAgentLinkVersionConflict
				}
				if !errors.Is(err, want) || errCode(err) != "conflict" || !reflect.DeepEqual(results[i], model.AgentLinkWriteResult{}) {
					t.Fatalf("loser=%+v %v", results[i], err)
				}
				losses++
			}
			if wins != 1 || losses != 1 {
				t.Fatalf("winners=%d losers=%d", wins, losses)
			}
			list, err := f.core.ListAgentLinks(f.ctx)
			if err != nil || len(list.Records) != 1 {
				t.Fatalf("durable winners=%+v %v", list, err)
			}
		})
	}
}

func TestAgentLinkScopeIdentityAndSparsePins(t *testing.T) {
	f := openStore(t)
	foreignDatabase := openStore(t)
	foreignList, err := foreignDatabase.core.ListAgentLinks(foreignDatabase.ctx)
	if err != nil {
		t.Fatal(err)
	}
	otherScope := f.scope
	otherScope.Workspace = "other"
	other, err := coredata.New(f.db, otherScope)
	if err != nil {
		t.Fatal(err)
	}
	first, err := f.core.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := other.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"known-a", "known-b"} {
		q := model.AgentLinkWrite{Expected: absentAgent(first.Identity, id), Record: agentRecord(id, "")}
		if _, err := f.core.WriteAgentLink(f.ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	pin := strings.Repeat("c", 64)
	for _, pair := range []struct {
		s        *coredata.Store
		identity model.AgentLinkIdentity
	}{{f.core, first.Identity}, {other, second.Identity}} {
		r := agentRecord("pinned", pin)
		if _, err := pair.s.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(pair.identity, r.AgentID), Record: r}); err != nil {
			t.Fatal(err)
		}
	}
	q := model.AgentLinkWrite{Expected: absentAgent(first.Identity, "foreign"), Record: agentRecord("foreign", "")}
	foreignGuard := q
	foreignGuard.Expected = absentAgent(foreignList.Identity, q.Record.AgentID)
	if _, err := f.core.WriteAgentLink(f.ctx, foreignGuard); errCode(err) != "failed_precondition" {
		t.Fatalf("guard from a different actual database accepted: %v", err)
	}
	for name, mutate := range map[string]func(*model.AgentLinkGuard){
		"database": func(g *model.AgentLinkGuard) { g.Identity.DatabaseID = "another" },
		"scope":    func(g *model.AgentLinkGuard) { g.Identity.Scope.Workspace = "other" },
		"epoch":    func(g *model.AgentLinkGuard) { g.Identity.ModuleSchemaVersion-- },
		"agent":    func(g *model.AgentLinkGuard) { g.AgentID = "another" },
	} {
		bad := q
		mutate(&bad.Expected)
		if _, err := f.core.WriteAgentLink(f.ctx, bad); errCode(err) != "failed_precondition" {
			t.Fatalf("%s guard accepted: %v", name, err)
		}
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if out, err := f.core.WriteAgentLink(ctx, q); errCode(err) != "cancelled" || !reflect.DeepEqual(out, model.AgentLinkWriteResult{}) {
		t.Fatalf("canceled write=%+v %v", out, err)
	}
	list, err := f.core.ListAgentLinks(f.ctx)
	if err != nil || len(list.Records) != 3 {
		t.Fatalf("unexpected list=%+v %v", list, err)
	}
}

func TestAgentLinkNormalIndependentWritesIgnoreAuditRevisions(t *testing.T) {
	for _, count := range []int{1, 2, 10} {
		t.Run(fmt.Sprintf("agents_%d", count), func(t *testing.T) {
			f := openStore(t)
			list, err := f.core.ListAgentLinks(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := f.db.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "marker", Keys: []string{"audit"}}}})
			if err != nil {
				t.Fatal(err)
			}
			before := stats(t, f)
			if _, err := f.db.Batch(f.ctx, api.BatchRequest{Scope: f.scope, Expected: snap.Token, RequestID: "audit", Mutations: []api.Mutation{{Collection: "marker", Key: "audit", ExpectedVersion: "0", Data: json.RawMessage(`{"event":"kept"}`)}}}); err != nil {
				t.Fatal(err)
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			errs := make([]error, count)
			for i := range count {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					id := fmt.Sprintf("agent-%02d", i)
					record := agentRecord(id, fmt.Sprintf("%064x", i+1))
					_, errs[i] = f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(list.Identity, id), Record: record})
				}(i)
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			after := stats(t, f)
			if after.Conflicts != before.Conflicts || after.CommitsDurable-before.CommitsDurable != uint64(count+1) {
				t.Fatalf("conflicts %d->%d durable delta=%d", before.Conflicts, after.Conflicts, after.CommitsDurable-before.CommitsDurable)
			}
			listed, err := f.core.ListAgentLinks(f.ctx)
			if err != nil || len(listed.Records) != count {
				t.Fatalf("list=%+v %v", listed, err)
			}
			audit, err := f.db.Snapshot(f.ctx, api.SnapshotRequest{Scope: f.scope, Queries: []api.Query{{Collection: "marker", Keys: []string{"audit"}}}})
			if err != nil || audit.Results[0].Records[0].Missing {
				t.Fatalf("audit lost: %+v %v", audit, err)
			}
			t.Logf("%d independent intents: one startup list, %d typed durable commits, zero unrelated conflicts; audit committed", count, count)
		})
	}
}

func TestAgentLinkInputValidationAndTransactionalQuota(t *testing.T) {
	f := openStore(t)
	list, err := f.core.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	valid := agentRecord("agent", strings.Repeat("d", 64))
	q := model.AgentLinkWrite{Expected: absentAgent(list.Identity, valid.AgentID), Record: valid}
	for name, mutate := range map[string]func(*model.AgentLinkWrite){
		"duplicate_manifest_key": func(q *model.AgentLinkWrite) { q.Record.CapabilityManifest = []byte(`{"x":1,"x":2}`) },
		"nested_duplicate":       func(q *model.AgentLinkWrite) { q.Record.CapabilityManifest = []byte(`{"x":{"k":1,"k":2}}`) },
		"invalid_manifest":       func(q *model.AgentLinkWrite) { q.Record.CapabilityManifest = []byte(`[]`) },
		"invalid_fingerprint":    func(q *model.AgentLinkWrite) { q.Record.PublicKeyFingerprint = "not-a-hash" },
		"enrolled_without_pin":   func(q *model.AgentLinkWrite) { q.Record.Enrollment = "enrolled"; q.Record.PublicKeyFingerprint = "" },
		"noncanonical_version":   func(q *model.AgentLinkWrite) { q.Expected.Version = "00" },
	} {
		bad := q
		mutate(&bad)
		if _, err := f.core.WriteAgentLink(f.ctx, bad); errCode(err) != "invalid_argument" {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	oversized := q
	oversized.Record.CapabilityManifest = bytes.Repeat([]byte("x"), model.MaxAgentLinkRecordBytes+1)
	if _, err := f.core.WriteAgentLink(f.ctx, oversized); errCode(err) != "resource_exhausted" {
		t.Fatalf("unbounded input: %v", err)
	}
	oversized = q
	oversized.Record.DisplayName = string(bytes.Repeat([]byte{0xff}, model.MaxAgentLinkRecordBytes+1))
	if _, err := f.core.WriteAgentLink(f.ctx, oversized); errCode(err) != "resource_exhausted" {
		t.Fatalf("oversized text was inspected before its size bound: %v", err)
	}
	created, err := f.core.WriteAgentLink(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	var rows, charged, totalRows, totalBytes int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT agent_link_rows,agent_link_bytes,rows,bytes FROM core_data_usage WHERE scope=?`, engine.ScopeID(f.scope)).Scan(&rows, &charged, &totalRows, &totalBytes)
	}); err != nil {
		t.Fatal(err)
	}
	// Reconstruct the old generic collection's exact canonical JSON accounting.
	raw, err := json.Marshal(struct {
		model.AgentLinkRecord
		CreatedAt, UpdatedAt any `json:"-"`
	}{AgentLinkRecord: created.Stored.Record})
	if err != nil {
		t.Fatal(err)
	}
	var full map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err = dec.Decode(&full); err != nil {
		t.Fatal(err)
	}
	full["createdAt"], full["updatedAt"] = created.Stored.CreatedAt, created.Stored.UpdatedAt
	canonical, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 || charged != int64(len(canonical)+len(valid.AgentID)) || totalRows != 0 || totalBytes != 0 {
		t.Fatalf("quota rows=%d bytes=%d expected=%d relational=%d/%d", rows, charged, len(canonical)+len(valid.AgentID), totalRows, totalBytes)
	}
	changedRecord := created.Stored.Record
	changedRecord.DisplayName = "unicode 王 <& \" \\\n"
	changedRecord.CapabilityManifest = []byte(`{"escaped":"\u0061","html":"<>&","n":9007199254740993}`)
	changedRecord.Capabilities = []string{"alpha", "<beta>"}
	changed, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: created.Stored.Guard, Record: changedRecord})
	if err != nil || !changed.Changed {
		t.Fatalf("escaped fact update=%+v %v", changed, err)
	}
	raw, err = json.Marshal(struct {
		model.AgentLinkRecord
		CreatedAt, UpdatedAt any `json:"-"`
	}{AgentLinkRecord: changed.Stored.Record})
	if err != nil {
		t.Fatal(err)
	}
	full = nil
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err = dec.Decode(&full); err != nil {
		t.Fatal(err)
	}
	full["createdAt"], full["updatedAt"] = changed.Stored.CreatedAt, changed.Stored.UpdatedAt
	canonical, err = json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT agent_link_rows,agent_link_bytes,rows,bytes FROM core_data_usage WHERE scope=?`, engine.ScopeID(f.scope)).Scan(&rows, &charged, &totalRows, &totalBytes)
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || charged != int64(len(canonical)+len(valid.AgentID)) || totalRows != 0 || totalBytes != 0 {
		t.Fatalf("updated quota rows=%d bytes=%d expected=%d relational=%d/%d", rows, charged, len(canonical)+len(valid.AgentID), totalRows, totalBytes)
	}
	// Deliberate quota fault: a refusal rolls the whole attempted create back.
	f.exec(t, `UPDATE core_data_usage SET agent_link_rows=? WHERE scope=?`, model.MaxAgentLinkRecords, engine.ScopeID(f.scope))
	blocked := agentRecord("blocked", "")
	if _, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(list.Identity, blocked.AgentID), Record: blocked}); errCode(err) != "resource_exhausted" {
		t.Fatalf("quota fault=%v", err)
	}
	if got, err := f.core.ListAgentLinks(f.ctx); err != nil || len(got.Records) != 1 {
		t.Fatalf("quota failure created a row: %+v %v", got, err)
	}
}

func TestAgentLinkListRejectsMalformedCurrentFactsAndPhysicalPinCollision(t *testing.T) {
	f := openStore(t)
	list, err := f.core.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := agentRecord("first", strings.Repeat("a", 64))
	second := agentRecord("second", strings.Repeat("b", 64))
	for _, record := range []model.AgentLinkRecord{first, second} {
		if _, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(list.Identity, record.AgentID), Record: record}); err != nil {
			t.Fatal(err)
		}
	}
	// The actual unique index is an independent backstop, even if a caller
	// bypasses the typed operation in this isolated corruption fixture.
	if err := f.db.Write(f.ctx, engine.Durable, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE core_agent_links SET fingerprint=? WHERE scope=? AND agent_id=?`, first.PublicKeyFingerprint, engine.ScopeID(f.scope), second.AgentID)
		return err
	}); errCode(err) != "conflict" {
		t.Fatalf("physical uniqueness not enforced: %v", err)
	}
	validFacts := `{"protocolMajor":0,"protocolMinor":0,"displayName":"","buildIdentity":"","advertisedManagementEndpoint":"","persistentAccessToken":"","capabilityManifest":{},"capabilities":[]}`
	for name, bad := range map[string]string{
		"duplicate_field":  strings.Replace(validFacts, `"displayName":""`, `"displayName":"","displayName":""`, 1),
		"missing_field":    strings.Replace(validFacts, `,"buildIdentity":""`, "", 1),
		"unknown_field":    strings.Replace(validFacts, `"protocolMajor":0`, `"unknown":1,"protocolMajor":0`, 1),
		"null_text":        strings.Replace(validFacts, `"displayName":""`, `"displayName":null`, 1),
		"null_capability":  strings.Replace(validFacts, `"capabilities":[]`, `"capabilities":[null]`, 1),
		"duplicate_nested": strings.Replace(validFacts, `"capabilityManifest":{}`, `"capabilityManifest":{"x":1,"x":2}`, 1),
	} {
		f.exec(t, `UPDATE core_agent_links SET facts_json=? WHERE scope=? AND agent_id=?`, []byte(bad), engine.ScopeID(f.scope), first.AgentID)
		if _, err := f.core.ListAgentLinks(f.ctx); errCode(err) != "data_loss" {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}

func TestAgentLinkWriteRollsBackQuotaWithoutMisclassifyingStorageFault(t *testing.T) {
	f := openStore(t)
	initial, err := f.core.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `CREATE TRIGGER agent_link_failure BEFORE INSERT ON core_agent_links BEGIN SELECT RAISE(ABORT,'injected insertion failure'); END`)
	record := agentRecord("agent", strings.Repeat("e", 64))
	out, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(initial.Identity, record.AgentID), Record: record})
	if err == nil || errors.Is(err, coredata.ErrAgentLinkFingerprintConflict) || errors.Is(err, coredata.ErrAgentLinkVersionConflict) || !reflect.DeepEqual(out, model.AgentLinkWriteResult{}) {
		t.Fatalf("unrelated insertion failure misclassified: %+v %v", out, err)
	}
	var rows, bytes int64
	if err := f.db.Read(f.ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT coalesce(sum(agent_link_rows),0),coalesce(sum(agent_link_bytes),0) FROM core_data_usage WHERE scope=?`, engine.ScopeID(f.scope)).Scan(&rows, &bytes)
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 0 || bytes != 0 {
		t.Fatalf("failed insert retained quota: %d/%d", rows, bytes)
	}
	after, err := f.core.ListAgentLinks(f.ctx)
	if err != nil || len(after.Records) != 0 {
		t.Fatalf("failed insert retained owner: %+v %v", after, err)
	}
}

func TestAgentLinkRejectsOversizedInputBeforeWriterAdmission(t *testing.T) {
	f := openStore(t)
	initial, err := f.core.ListAgentLinks(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	held, release := make(chan struct{}), make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- f.db.Write(f.ctx, engine.Durable, func(context.Context, *sql.Tx) error { close(held); <-release; return nil })
	}()
	<-held
	record := agentRecord("agent", "")
	record.CapabilityManifest = bytes.Repeat([]byte("x"), model.MaxAgentLinkRecordBytes+1)
	type outcome struct {
		result model.AgentLinkWriteResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		out, err := f.core.WriteAgentLink(f.ctx, model.AgentLinkWrite{Expected: absentAgent(initial.Identity, record.AgentID), Record: record})
		done <- outcome{out, err}
	}()
	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		close(release)
		<-writerDone
		t.Fatal("oversized input waited for writer admission")
	}
	close(release)
	if writerErr := <-writerDone; writerErr != nil {
		t.Fatal(writerErr)
	}
	if errCode(got.err) != "resource_exhausted" || !reflect.DeepEqual(got.result, model.AgentLinkWriteResult{}) {
		t.Fatalf("oversized input waited for or entered writer: %+v %v", got.result, got.err)
	}
}
