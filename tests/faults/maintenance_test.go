//go:build linux

package faults_test

import (
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

func TestFaultReceiptMaintenancePreservesBusiness(t *testing.T) {
	t.Run("bounded-prune", func(t *testing.T) {
		s := open(t, filepath.Join(privateDir(t), "fixture.db"), true, func(c *engine.Config) {
			c.Manifest.Namespaces[0].ReceiptTTLSeconds = 1
		})
		token := read(t, s).Token
		var last api.Receipt
		var expiry time.Time
		for i := 0; i < 260; i++ {
			req := api.BatchRequest{Scope: scope, Expected: token, RequestID: "prune-" + strconv.Itoa(i),
				Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: token.Revision,
					Data: json.RawMessage(`{"operation":"bounded-prune","counter":9223372036854775806}`)}}}
			var err error
			last, err = s.Batch(deadline(t), req)
			if err != nil {
				t.Fatal(err)
			}
			token = last.Token
			expiry, err = time.Parse(time.RFC3339Nano, last.ExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
		}
		// SQL expiry is in whole seconds and pruning uses a strict past boundary.
		time.Sleep(time.Until(expiry.Add(time.Second)))
		before := read(t, s)
		count, err := s.PruneExpiredReceipts(deadline(t), time.Now(), 256)
		if err != nil || count != 256 {
			t.Fatalf("bounded prune changed its finite work limit: count=%d err=%v", count, err)
		}
		remaining := 0
		for i := 0; i < 260; i++ {
			_, err := s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: "prune-" + strconv.Itoa(i)})
			if err == nil {
				remaining++
			} else if code(err) != "not_found" {
				t.Fatal(err)
			}
		}
		if remaining != 4 {
			t.Fatalf("bounded prune retained %d receipts, want 4", remaining)
		}
		count, err = s.PruneExpiredReceipts(deadline(t), time.Now(), 256)
		if err != nil || count != 4 || !reflect.DeepEqual(read(t, s), before) {
			t.Fatalf("prune lost business state/revision: count=%d err=%v", count, err)
		}
		if err = s.Integrity(deadline(t)); err != nil {
			t.Fatal(err)
		}
		evidence(t, map[string]any{"expired_receipts": 260, "first_prune": 256, "second_prune": 4,
			"business_values_and_revision_unchanged": true, "revision": last.Token.Revision})
	})
	t.Run("native-idle-expiry", func(t *testing.T) {
		dir := privateDir(t)
		m := coreConfig("", false).Manifest
		m.Namespaces[0].ReceiptTTLSeconds = 3
		m.Namespaces[0].MaxReceipts = 2
		d := startWithManifest(t, dir, true, "", m)
		initial := nativeRead(t, d)
		seed := request(initial.Token, "expiry-business")
		seed.Mutations = append(seed.Mutations,
			api.Mutation{Collection: "runs", Key: "parent", ExpectedVersion: "0", Data: faultPreparationBody(false)},
			api.Mutation{Collection: "invocations", Key: "invocation", ExpectedVersion: "0", Data: faultPreparationBody(true)},
			api.Mutation{Collection: "definitions", Key: "pin", ExpectedVersion: "0", Data: json.RawMessage(`{"immutable":true}`)})
		guard, err := d.client.Batch(deadline(t), seed)
		if err != nil {
			t.Fatal(err)
		}
		group := model.GroupPrepare{ID: "expiry-group", ParentID: "parent", InvocationID: "invocation", GroupKey: "members", Body: json.RawMessage(`{"policy":"all"}`),
			ParentGuard:     model.RecordGuard{Collection: "runs", Key: "parent", Version: guard.Token.Revision},
			InvocationGuard: model.RecordGuard{Collection: "invocations", Key: "invocation", Version: guard.Token.Revision},
			PinGuard:        model.RecordGuard{Collection: "definitions", Key: "pin", Version: guard.Token.Revision},
			Members: []model.GroupMember{{ItemKey: "item", ChildID: "child", EventID: "event", Parameters: []byte(`{ "html":"<tag>", "counter":9223372036854775806 }`),
				Event: json.RawMessage(`{"type":"prepared"}`), Link: json.RawMessage(`{"parent":"parent"}`), Body: json.RawMessage(`{"phase":"prepared"}`)}}}
		faultGroupCondition(&group)
		payload, _ := json.Marshal(group)
		namedReq := api.NamedRequest{Scope: scope, DatabaseID: initial.Token.DatabaseID, Schema: initial.Token.Schema,
			Module: coredata.Spec().ID, Operation: "group.prepare", RequestID: "expiry-named", Payload: payload}
		prepared, err := d.client.Named(deadline(t), namedReq)
		if err != nil || prepared.Receipt == nil {
			t.Fatalf("named seed: %v", err)
		}
		retained, err := d.client.NamedResult(deadline(t), "expiry-retained-named", api.ReceiptRequest{Scope: scope, RequestID: namedReq.RequestID})
		if err != nil || !reflect.DeepEqual(retained, prepared) {
			t.Fatalf("named replay result missing before declared expiry: %+v %v", retained, err)
		}
		retainedBatch, err := d.client.Receipt(deadline(t), "expiry-retained-doc", api.ReceiptRequest{Scope: scope, RequestID: seed.RequestID})
		if err != nil || !reflect.DeepEqual(retainedBatch, guard) {
			t.Fatalf("document receipt missing before declared expiry: %+v %v", retainedBatch, err)
		}
		third := api.BatchRequest{Scope: scope, Expected: prepared.Receipt.Token, RequestID: "expiry-quota",
			Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: guard.Token.Revision,
				Data: json.RawMessage(`{"operation":"run-1","phase":"resumed"}`)}}}
		if _, err = d.client.Batch(deadline(t), third); code(err) != "resource_exhausted" {
			t.Fatalf("receipt quota did not reject third commit: %v", err)
		}
		before := nativeRead(t, d)
		// No writes trigger cleanup. The production worker must release both
		// receipt and named-result retention within this finite idle window.
		limit := time.Now().Add(7 * time.Second)
		for {
			_, batchErr := d.client.Receipt(deadline(t), "expiry-resolve-doc", api.ReceiptRequest{Scope: scope, RequestID: seed.RequestID})
			_, namedErr := d.client.NamedResult(deadline(t), "expiry-resolve-named", api.ReceiptRequest{Scope: scope, RequestID: namedReq.RequestID})
			if code(batchErr) == "not_found" && code(namedErr) == "not_found" {
				break
			}
			if (batchErr != nil && code(batchErr) != "not_found") || (namedErr != nil && code(namedErr) != "not_found") || time.Now().After(limit) {
				t.Fatalf("native idle cleanup failed: batch=%v named=%v", batchErr, namedErr)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !reflect.DeepEqual(nativeRead(t, d), before) {
			t.Fatal("idle receipt expiry deleted documents or changed revision")
		}
		d.kill(t)
		dsn := (&url.URL{Scheme: "file", Path: filepath.Join(dir, "fixture.db"), RawQuery: "mode=ro&_pragma=query_only(1)"}).String()
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		for _, query := range []string{"SELECT COUNT(*) FROM receipts", "SELECT COUNT(*) FROM named_results"} {
			var count int
			if err = db.QueryRowContext(deadline(t), query).Scan(&count); err != nil || count != 0 {
				db.Close()
				t.Fatalf("expiry left retained replay rows: %s count=%d err=%v", query, count, err)
			}
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		d = startWithManifest(t, dir, false, "", m)
		if !reflect.DeepEqual(nativeRead(t, d), before) {
			t.Fatal("business state after expiry failed SIGKILL recovery")
		}
		readPayload, _ := json.Marshal(model.GroupRead{ID: group.ID})
		namedReq.Operation, namedReq.RequestID, namedReq.Payload = "group.snapshot", "expiry-group-read", readPayload
		out, err := d.client.Named(deadline(t), namedReq)
		if err != nil {
			t.Fatal(err)
		}
		var saved model.GroupSnapshot
		if err = json.Unmarshal(out.Result, &saved); err != nil || saved.MemberCount != 1 || !reflect.DeepEqual(saved.Members, group.Members) || !equalJSON(t, saved.Body, group.Body) {
			t.Fatalf("receipt pruning deleted relational business facts: %+v %v", saved, err)
		}
		commit, err := d.client.Batch(deadline(t), third)
		if err != nil || commit.Token.Revision != "3" {
			t.Fatalf("expired receipt capacity was not reusable: %+v %v", commit, err)
		}
		evidence(t, map[string]any{"production_idle_expiry": true, "ttl_seconds": 3, "quota": 2,
			"receipt_and_named_replay_rows_removed": true, "document_and_relational_business_preserved_after_sigkill": true,
			"byte_exact_encoded_json_parameters": true, "receipt_capacity_reused": true})
	})
}
