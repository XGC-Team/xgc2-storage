// xgc2-configuration-import is an explicit one-time storage-owner command.
// It creates a new database; the server's normal startup never loads old data.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
	"io"
	"os"
	"reflect"
	"time"
)

// This offline file boundary includes retained history; it is never an RPC limit.
const maxOfflineArchiveBytes int64 = 8 << 30

func readFile(path string, out any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return e
	}
	if info.Size() > maxOfflineArchiveBytes {
		return errors.New("offline archive exceeds finite file budget")
	}
	d := json.NewDecoder(io.LimitReader(f, maxOfflineArchiveBytes+1))
	d.DisallowUnknownFields()
	if e = d.Decode(out); e != nil {
		return e
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return errors.New("one bounded input value required")
	}
	return nil
}
func sameDocument(a, b []byte) bool {
	decode := func(raw []byte) (any, error) {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var v any
		e := d.Decode(&v)
		return v, e
	}
	left, le := decode(a)
	right, re := decode(b)
	return le == nil && re == nil && reflect.DeepEqual(left, right)
}

func run() error {
	archive := flag.String("archive", "", "private exported configuration archive")
	path := flag.String("database", "", "new target database path")
	manifestFile := flag.String("manifest", "", "explicit deployment manifest file")
	scopeFile := flag.String("scope", "", "explicit target scope file")
	domainsFile := flag.String("domains", "", "current compiled deployment domain declarations")
	flag.Parse()
	if *archive == "" || *path == "" || *manifestFile == "" || *scopeFile == "" || *domainsFile == "" {
		return errors.New("--archive --database --manifest --scope --domains are required")
	}
	var in model.ConfigurationImport
	var manifest api.Manifest
	var scope api.Scope
	var domains []model.ConfigurationDomainDeclaration
	for _, r := range []struct {
		path string
		out  any
	}{{*archive, &in}, {*manifestFile, &manifest}, {*scopeFile, &scope}, {*domainsFile, &domains}} {
		if e := readFile(r.path, r.out); e != nil {
			return e
		}
	}
	if !reflect.DeepEqual(domains, in.Domains) {
		return errors.New("archive domains disagree with current explicit deployment")
	}
	scopeRaw, e := json.Marshal(scope)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	spec := coredata.Spec()
	declared := false
	for _, n := range manifest.Namespaces {
		if n.ID == scope.Namespace && n.Schema == coredata.Schema {
			for _, m := range n.Modules {
				if m.ID == spec.ID && reflect.DeepEqual(m, spec) {
					declared = true
				}
			}
		}
	}
	if !declared {
		return errors.New("target manifest must explicitly contain current coredata module")
	}
	cfg := engine.Config{Path: *path, Create: true, Manifest: manifest, MaxDBBytes: 8 << 30, Modules: []engine.DataModule{{Spec: spec, Initialize: coredata.Initialize, Execute: coredata.Execute, Deploy: func(ctx context.Context, tx *sql.Tx) error {
		if e := coredata.DeclareConfigurationDomains(ctx, tx, domains); e != nil {
			return e
		}
		if e := coredata.ImportConfiguration(ctx, tx, string(scopeRaw), in); e != nil {
			return e
		}
		if in.Execution != nil {
			return coredata.ImportExecution(ctx, tx, string(scopeRaw), *in.Execution)
		}
		return nil
	}}}}
	store, e := engine.Open(ctx, cfg)
	if e != nil {
		return e
	}
	defer store.Close()
	// Bulk historical events were committed by the explicit owner deploy. Use
	// its existing checkpoint before the next offline write admission.
	if checkpoint, err := store.Checkpoint(ctx); err != nil {
		return err
	} else if checkpoint.Busy != 0 {
		return errors.New("offline checkpoint is pinned")
	}
	if e = store.ImportRecords(ctx, scope, in.Records); e != nil {
		return e
	}
	// Checkpoint the offline bulk transaction before ordinary read admission.
	if checkpoint, err := store.Checkpoint(ctx); err != nil {
		return err
	} else if checkpoint.Busy != 0 {
		return errors.New("offline checkpoint is pinned")
	}
	// Read through the same public engine operations used by the installed
	// native owner, after the creation/import transaction has committed.
	var marker string
	for _, n := range manifest.Namespaces {
		if n.ID == scope.Namespace && len(n.Collections) > 0 {
			marker = n.Collections[0].ID
			break
		}
	}
	if marker == "" {
		return errors.New("explicit target collection required for initial token")
	}
	snap, e := store.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: marker, Keys: []string{"offline-token"}}}})
	if e != nil {
		return e
	}
	guards := map[string]model.ConfigurationDomainGuard{}
	for _, d := range in.Domains {
		guards[d.Key] = model.ConfigurationDomainGuard{Key: d.Key, SchemaIdentity: d.SchemaIdentity, SchemaVersion: d.SchemaVersion, RegistryDigest: d.RegistryDigest}
	}
	resources := map[string]model.ConfigurationResource{}
	branches := map[string]model.ConfigurationBranch{}
	for _, v := range in.Resources {
		resources[v.Row.ID] = v.Row
	}
	for _, v := range in.Branches {
		branches[v.Row.ID] = v.Row
	}
	maxWire, refs := 0, 0
	for i, v := range in.Snapshots {
		request, e := json.Marshal(model.ConfigurationResourceRead{Domain: guards[v.Domain], ResourceID: v.Commit.ResourceID, CommitID: v.Commit.ID, IncludeArchived: true})
		if e != nil {
			return e
		}
		result, e := store.Named(ctx, api.NamedRequest{Scope: scope, DatabaseID: snap.Token.DatabaseID, Schema: coredata.Schema, Module: spec.ID, Operation: model.ResourceSnapshotOperation, RequestID: fmt.Sprintf("offline-read-%d", i), Payload: request})
		if e != nil {
			return fmt.Errorf("readback %s: %w", v.Commit.ID, e)
		}
		var got model.ConfigurationResourceSnapshot
		if e = json.Unmarshal(result.Result, &got); e != nil {
			return e
		}
		if got.Head.Resource != resources[v.Commit.ResourceID] || got.Head.Branch != branches[v.Commit.BranchID] || got.Head.Commit != v.Commit || !bytes.Equal(got.Payload, v.Payload) || !bytes.Equal(got.Manifest, v.Manifest) || !reflect.DeepEqual(got.References, v.References) {
			return errors.New("immutable configuration did not round trip byte-exactly")
		}
		raw, e := json.Marshal(result)
		if e != nil {
			return e
		}
		maxWire = max(maxWire, len(raw))
		refs += len(got.References)
	}
	for offset := 0; offset < len(in.Records); {
		collection := in.Records[offset].Collection
		count, size := 0, 0
		keys := []string{}
		for offset+count < len(in.Records) && count < api.MaxRows {
			v := in.Records[offset+count]
			if v.Collection != collection {
				break
			}
			bytes := len(v.Key) + len(v.Data) + 128
			if count > 0 && size+bytes > api.MaxResponseBytes/2 {
				break
			}
			size += bytes
			keys = append(keys, v.Key)
			count++
		}
		read, e := store.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: collection, Keys: keys}}})
		if e != nil {
			return e
		}
		if len(read.Results) != 1 || len(read.Results[0].Records) != count {
			return errors.New("imported record bounded read missing")
		}
		for i, got := range read.Results[0].Records {
			v := in.Records[offset+i]
			if got.Key != v.Key || got.Missing || got.Deleted || got.Version != "1" || !sameDocument(got.Data, v.Data) {
				return errors.New("imported record did not round trip")
			}
		}
		offset += count
	}

	commandsVerified, eventsVerified := 0, 0
	if history := in.Execution; history != nil {
		named := func(op string, input, output any) error {
			body, e := json.Marshal(input)
			if e != nil {
				return e
			}
			result, e := store.Named(ctx, api.NamedRequest{Scope: scope, DatabaseID: snap.Token.DatabaseID, Schema: coredata.Schema, Module: spec.ID, Operation: op, RequestID: fmt.Sprintf("offline-history-%d-%d", commandsVerified, eventsVerified), Payload: body})
			if e != nil {
				return e
			}
			return json.Unmarshal(result.Result, output)
		}
		// Each list is finite; use the existing owner's bounded public read ports.
		for start := 0; start < len(history.Commands); start += 1000 {
			end := min(start+1000, len(history.Commands))
			expected := map[string]model.CommandReceipt{}
			read := model.CommandListRead{}
			for _, v := range history.Commands[start:end] {
				read.IDs = append(read.IDs, v.CommandID)
				expected[v.CommandID] = v
			}
			var got model.CommandList
			if e := named(model.ExecutionCommandListOperation, read, &got); e != nil {
				return e
			}
			if len(got.Receipts) != len(expected) {
				return errors.New("original command set did not round trip")
			}
			for _, v := range got.Receipts {
				if !reflect.DeepEqual(v, expected[v.CommandID]) {
					return errors.New("original command receipt changed")
				}
				commandsVerified++
			}
		}
		after := "0"
		for eventsVerified < len(history.Events) {
			var page model.EventPage
			if e := named(model.ExecutionEventReadOperation, model.EventRead{AfterOffset: after, Through: history.EventFrontier, Limit: 1000}, &page); e != nil {
				return e
			}
			if page.Cursor.LatestOffset != history.EventFrontier || page.Cursor.StreamID == history.LegacyStreamID || len(page.Events) == 0 {
				return errors.New("imported event frontier or new authority identity disagrees")
			}
			for _, v := range page.Events {
				if eventsVerified >= len(history.Events) || !reflect.DeepEqual(v, history.Events[eventsVerified]) {
					return errors.New("original event seq/offset/payload changed")
				}
				eventsVerified++
			}
			after = page.NextOffset
		}
	}

	if e = store.Integrity(ctx); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"sourceSHA256": in.SourceSHA256, "recordsVerified": len(in.Records), "commandsVerified": commandsVerified, "eventsVerified": eventsVerified, "namespaces": len(in.Namespaces), "resources": len(in.Resources), "branches": len(in.Branches), "commitsVerified": len(in.Snapshots), "referencesVerified": refs, "changesImported": len(in.Changes), "maxNamedResponseBytes": maxWire, "integrity": "ok"})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
