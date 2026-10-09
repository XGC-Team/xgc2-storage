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

func readFile(path string, out any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, coredata.MaxScopeBytes+1))
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	cfg := engine.Config{Path: *path, Create: true, Manifest: manifest, Modules: []engine.DataModule{{Spec: spec, Initialize: coredata.Initialize, Execute: coredata.Execute, Deploy: func(ctx context.Context, tx *sql.Tx) error {
		if e := coredata.DeclareConfigurationDomains(ctx, tx, domains); e != nil {
			return e
		}
		return coredata.ImportConfiguration(ctx, tx, string(scopeRaw), in)
	}}}}
	store, e := engine.Open(ctx, cfg)
	if e != nil {
		return e
	}
	defer store.Close()
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
	if e = store.Integrity(ctx); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"sourceSHA256": in.SourceSHA256, "namespaces": len(in.Namespaces), "resources": len(in.Resources), "branches": len(in.Branches), "commitsVerified": len(in.Snapshots), "referencesVerified": refs, "changesImported": len(in.Changes), "maxNamedResponseBytes": maxWire, "integrity": "ok"})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
