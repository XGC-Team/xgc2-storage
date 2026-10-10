package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/registry"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("storage-admin: check | stats | checkpoint | backup | prune-receipts | update-deployment")
	}
	operation := os.Args[1]
	switch operation {
	case "check", "stats", "checkpoint", "backup", "prune-receipts", "update-deployment":
	default:
		return errors.New("storage-admin: unknown operation")
	}
	flags := flag.NewFlagSet(operation, flag.ContinueOnError)
	var path, manifest, oldManifest, destination string
	var limit int
	var maxDBBytes int64
	flags.StringVar(&path, "db", "", "explicit offline database grant")
	flags.Int64Var(&maxDBBytes, "max-db-bytes", 1<<30, "finite owner database capacity in bytes")
	flags.StringVar(&manifest, "manifest", "", "exact deployed manifest or update-deployment target")
	flags.StringVar(&oldManifest, "old-manifest", "", "exact old manifest required by update-deployment")
	flags.StringVar(&destination, "destination", "", "absent backup destination in private managed directory")
	flags.IntVar(&limit, "limit", 256, "bounded expired receipt cleanup count (1..1000)")
	if e := flags.Parse(os.Args[2:]); e != nil {
		return e
	}
	if path == "" || manifest == "" {
		return errors.New("storage-admin: db and manifest required")
	}
	if flags.NArg() != 0 {
		return errors.New("storage-admin: unexpected positional arguments")
	}
	if (operation == "update-deployment") != (oldManifest != "") {
		return errors.New("storage-admin: --old-manifest is required only by update-deployment")
	}
	if operation == "backup" && (!filepath.IsAbs(destination) || filepath.Clean(destination) != destination || filepath.Ext(destination) != ".db" || strings.ContainsAny(destination, "\x00?#")) {
		return errors.New("storage-admin: canonical absolute backup .db destination required")
	}
	if operation == "prune-receipts" && (limit < 1 || limit > 1000) {
		return errors.New("storage-admin: receipt cleanup limit must be 1..1000")
	}
	f, e := os.Open(manifest)
	if e != nil {
		return e
	}
	m, e := engine.DecodeManifest(f)
	f.Close()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if operation == "update-deployment" {
		f, e := os.Open(oldManifest)
		if e != nil {
			return e
		}
		old, e := engine.DecodeManifest(f)
		f.Close()
		if e != nil {
			return e
		}
		result, e := engine.UpdateDeployment(ctx, path, old, m, registry.Compiled())
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	store, e := engine.Open(ctx, engine.Config{Path: path, Manifest: m, Modules: registry.Compiled(), MaxDBBytes: maxDBBytes})
	if e != nil {
		return e
	}
	defer store.Close()
	var result any
	switch operation {
	case "check":
		if e = store.Integrity(ctx); e != nil {
			return e
		}
		result = map[string]string{"integrity": "ok"}
	case "stats":
		result, e = store.Stats()
	case "checkpoint":
		result, e = store.Checkpoint(ctx)
	case "backup":
		result, e = store.Backup(ctx, destination)
	case "prune-receipts":
		result, e = store.PruneExpiredReceipts(ctx, time.Now(), limit)
	}
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
