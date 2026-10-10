package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/host"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	var path, manifest, grantsFile, socket, grpcSocket, target, refOut, identityOut string
	var create bool
	var maxDBBytes int64
	var readers, writerQueue int
	var callTimeout time.Duration
	flag.StringVar(&path, "db", "", "explicit managed database file grant (existing by default)")
	flag.Int64Var(&maxDBBytes, "max-db-bytes", 1<<30, "finite owner database capacity in bytes")
	flag.StringVar(&manifest, "manifest", "", "reviewed deployment manifest")
	flag.StringVar(&grantsFile, "grants", "", "private mode0600 owner grant JSON array")
	flag.StringVar(&socket, "http-socket", "", "private XRPC HTTP Unix endpoint")
	flag.StringVar(&grpcSocket, "grpc-socket", "", "private XRPC gRPC Unix endpoint")
	flag.StringVar(&target, "target-id", "", "local target identity")
	flag.StringVar(&refOut, "ref-out", "", "private runtime file for bound ServiceRef array")
	flag.StringVar(&identityOut, "identity-out", "", "private runtime file for this owner's actual database identity")
	flag.IntVar(&readers, "readers", 0, "concurrent read connections (default 4, at most 16)")
	flag.IntVar(&writerQueue, "writer-queue", 0, "writers admitted at once; later callers wait for their deadline (default 64)")
	flag.DurationVar(&callTimeout, "call-timeout", 0, "longest time one call may run (default 30s)")
	flag.BoolVar(&create, "create", false, "explicitly initialize absent DB; never replace existing")
	flag.Parse()
	if flag.NArg() != 0 {
		return errors.New("storage: unexpected positional arguments")
	}
	if path == "" || manifest == "" || grantsFile == "" || target == "" || refOut == "" || (socket == "" && grpcSocket == "") {
		return errors.New("storage: db/manifest/grants/target-id/ref-out and a socket required")
	}
	file, e := os.Open(manifest)
	if e != nil {
		return e
	}
	m, e := engine.DecodeManifest(file)
	file.Close()
	if e != nil {
		return e
	}
	st, e := os.Lstat(grantsFile)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return errors.New("storage: grants must be regular mode0600")
	}
	grantFile, e := os.Open(grantsFile)
	if e != nil {
		return e
	}
	raw, e := io.ReadAll(io.LimitReader(grantFile, (1<<20)+1))
	grantFile.Close()
	if e != nil {
		return e
	}
	if len(raw) > 1<<20 {
		return errors.New("storage: grant configuration exceeds 1 MiB")
	}
	var grants []server.Grant
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if e = decoder.Decode(&grants); e != nil {
		return e
	}
	var tail any
	if e = decoder.Decode(&tail); e != io.EOF {
		return errors.New("storage: one owner grant array required")
	}
	if e = server.ValidateGrants(grants); e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	diagnostics, e := xrpc.NewDiagnostics(xrpc.DiagnosticOptions{Sink: os.Stderr, MaxQueuedRecords: 128, MaxRecordBytes: 4096})
	if e != nil {
		return e
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = diagnostics.Close(shutdown)
	}()
	owner, e := host.Open(ctx, host.Config{Path: path, Create: create, Manifest: m, Readers: readers, WriterQueue: writerQueue,
		CallBudget: callTimeout, MaxDBBytes: maxDBBytes, Diagnostics: diagnostics})
	if e != nil {
		return e
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = owner.Close(shutdown)
	}()
	exposure, e := owner.Serve(ctx, host.ServeConfig{TargetID: target, HTTPSocket: socket, GRPCSocket: grpcSocket, Grants: grants})
	if e != nil {
		return e
	}
	if identityOut != "" {
		if e = publishPrivateJSON(identityOut, map[string]string{"database_id": owner.DatabaseID()}); e != nil {
			return e
		}
	}
	if e = publishPrivateJSON(refOut, exposure.References()); e != nil {
		return e
	}
	select {
	case <-ctx.Done():
	case <-exposure.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return owner.Close(shutdown)
}

func publishPrivateJSON(path string, value any) error {
	parent := filepath.Dir(path)
	st, e := os.Lstat(parent)
	if e != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return errors.New("storage: ref directory must be private")
	}
	if st, e = os.Lstat(path); e == nil && (!st.Mode().IsRegular() || st.Mode().Perm() != 0600) {
		return errors.New("storage: unsafe existing reference file")
	}
	f, e := os.CreateTemp(parent, ".storage-ref-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return e
	}
	e = json.NewEncoder(f).Encode(value)
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	dir, e := os.Open(parent)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
