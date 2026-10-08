//go:build linux

package faults_test

import (
	"errors"
	"testing"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

func TestFaultNativeHTTPBoundReferenceNoEffect(t *testing.T) {
	d := start(t, privateDir(t), true, "")
	initial := nativeRead(t, d)
	for _, field := range []string{"target", "instance"} {
		t.Run(field, func(t *testing.T) {
			wrong := d.ref
			if field == "target" {
				wrong.TargetID += "-different"
			} else {
				wrong.InstanceID += "-different"
			}
			consumer, err := client.New(d.caller, wrong)
			if err != nil {
				t.Fatalf("fixture reference is invalid before caller binding: %v", err)
			}
			req := request(initial.Token, "wrong-reference-"+field)
			_, err = consumer.Batch(deadline(t), req)
			var failure *xrpc.CallError
			if code(err) != "invalid_argument" || !errors.As(err, &failure) || failure.Disposition != xrpc.NotSent {
				t.Fatalf("bound caller did not reject complete mismatched reference locally: %v", err)
			}
			unchanged := nativeRead(t, d)
			if unchanged.Token != initial.Token {
				t.Fatal("mismatched reference changed the real daemon revision")
			}
			for _, result := range unchanged.Results {
				for _, record := range result.Records {
					if !record.Missing {
						t.Fatal("mismatched reference published business data")
					}
				}
			}
			if _, err = d.client.Receipt(deadline(t), "wrong-reference-receipt", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID}); code(err) != "not_found" {
				t.Fatalf("mismatched reference published a receipt: %v", err)
			}
			evidence(t, map[string]any{"mismatched_field": field, "disposition": failure.Disposition,
				"business_and_revision_unchanged": true, "receipt_absent": true})
		})
	}
	correct := request(initial.Token, "correct-bound-reference")
	commit, err := d.client.Batch(deadline(t), correct)
	if err != nil {
		t.Fatalf("rejected reference poisoned the bound client: %v", err)
	}
	verifyBusiness(t, nativeRead(t, d), correct, commit)
}
