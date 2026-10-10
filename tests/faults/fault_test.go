//go:build linux

// These black-box tests never patch the engine or reuse a user's database.
package faults_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/server"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/grpcx"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const grant = "fault-validation-only-0123456789abcdef"

var scope = api.Scope{Namespace: "business", User: "test-user", Workspace: "test-workspace"}

func manifest() api.Manifest {
	var collections []api.Collection
	for _, id := range []string{"state", "events", "product_receipts", "layouts", "preferences", "configuration"} {
		collections = append(collections, api.Collection{ID: id, MaxRecordBytes: 3 << 20,
			MaxRecords: 2048, MaxBytes: 32 << 20, Retention: "fixture-owner", Recovery: "consistent-backup",
			Indexes: []api.Index{{ID: "operation", Fields: []string{"operation"}, Unique: true}}})
	}
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: scope.Namespace,
		Owner: "fault-validation", Schema: "business.v1", MaxScopes: 4, MaxReceipts: 1024,
		ReceiptTTLSeconds: 3600, Collections: collections}}}
}

func privateDir(t *testing.T) string {
	t.Helper()
	// A subtest's full name can exceed Linux's 108-byte Unix socket bound.
	// Allocate a short private path; it is still owned and reaped by this test.
	dir, err := os.MkdirTemp("", "fault-db-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func open(t *testing.T, path string, create bool, adjust func(*engine.Config)) *engine.Store {
	t.Helper()
	config := engine.Config{Path: path, Create: create, Manifest: manifest(), Readers: 2,
		WriterQueue: 4, CallBudget: 5 * time.Second, MaxDBBytes: 16 << 20, MaxWALBytes: 8 << 20}
	if adjust != nil {
		adjust(&config)
	}
	s, err := engine.Open(deadline(t), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func request(token api.Token, id string) api.BatchRequest {
	// This is a new-schema business transaction: state/event/product receipt,
	// layout/active preference, and configuration commit together.
	values := []string{
		`{"operation":"run-1","phase":"prepared","counter":9223372036854775806}`,
		`{"operation":"run-1","sequence":1,"event":"prepared"}`,
		`{"operation":"run-1","status":"committed"}`,
		`{"operation":"run-1","camera":{"x":1.25,"frame":"map"},"title":"视角"}`,
		`{"operation":"run-1","active":"primary","theme":"dark"}`,
		`{"operation":"run-1","nodes":[{"id":"a","value":false}],"asset":{"owner":"fixture","asset_id":"a","sha256":"abc","bytes":8}}`,
	}
	var mutations []api.Mutation
	for i, collection := range []string{"state", "events", "product_receipts", "layouts", "preferences", "configuration"} {
		mutations = append(mutations, api.Mutation{Collection: collection, Key: "primary", ExpectedVersion: "0", Data: json.RawMessage(values[i])})
	}
	return api.BatchRequest{Scope: scope, Expected: token, RequestID: id, Mutations: mutations}
}

func queries() []api.Query {
	var out []api.Query
	for _, c := range manifest().Namespaces[0].Collections {
		out = append(out, api.Query{Collection: c.ID, Keys: []string{"primary"}})
	}
	return out
}

func read(t *testing.T, s *engine.Store) api.SnapshotResponse {
	t.Helper()
	out, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope, Queries: queries()})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func code(err error) string {
	if err == nil {
		return ""
	}
	var domain *api.Error
	if errors.As(err, &domain) {
		return domain.Code
	}
	return xrpc.Code(err)
}

func evidence(t *testing.T, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(raw))
	if dir := os.Getenv("FAULT_EVIDENCE_DIR"); dir != "" {
		name := strings.ReplaceAll(t.Name(), "/", "-") + ".json"
		if err := os.WriteFile(filepath.Join(dir, name), append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func fileHash(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type daemon struct {
	cmd       *exec.Cmd
	transport *httpx.Client
	client    *client.Client
	ref       xrpc.ServiceRef
	lines     <-chan string
	log       *os.File
	stopped   bool
	grpc      *grpcx.Profile
	caller    xrpc.Caller
}

func configFiles(t *testing.T, dir string) {
	configFilesFor(t, dir, manifest())
}

func configFilesFor(t *testing.T, dir string, m api.Manifest) {
	t.Helper()
	for name, value := range map[string]any{"manifest.json": m,
		"grants.json": []server.Grant{{Token: grant, Namespace: scope.Namespace, User: scope.User, Workspace: scope.Workspace}}} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func daemonArgs(dir string, create bool) []string {
	args := []string{"-db", filepath.Join(dir, "fixture.db"), "-manifest", filepath.Join(dir, "manifest.json"),
		"-grants", filepath.Join(dir, "grants.json"), "-target-id", "fault-fixture",
		"-http-socket", filepath.Join(dir, "rpc.sock"), "-ref-out", filepath.Join(dir, "refs.json")}
	if create {
		args = append(args, "-create")
	}
	return args
}

func start(t *testing.T, dir string, create bool, held string) *daemon {
	t.Helper()
	return startWithManifest(t, dir, create, held, manifest())
}

func startWithManifest(t *testing.T, dir string, create bool, held string, m api.Manifest) *daemon {
	return startProfileManifest(t, dir, create, held, m, xrpc.HTTP)
}

func startProfileManifest(t *testing.T, dir string, create bool, held string, m api.Manifest, profile string) *daemon {
	t.Helper()
	binary := os.Getenv("FAULT_STORAGE_BIN")
	if binary == "" {
		t.Skip("run scripts/fault-validate.py to build the isolated production daemon; native conformance was not tested")
	}
	configFilesFor(t, dir, m)
	if err := os.Remove(filepath.Join(dir, "refs.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	d := &daemon{}
	if held == "" {
		args := daemonArgs(dir, create)
		if profile == xrpc.GRPC {
			for i, arg := range args {
				if arg == "-http-socket" {
					args[i] = "-grpc-socket"
				}
			}
		}
		d.cmd = exec.Command(binary, args...)
	} else {
		d.cmd = exec.Command(os.Args[0], "-test.run=^TestFaultChild$", "-test.v")
		d.cmd.Env = append(os.Environ(), "FAULT_CHILD=held", "FAULT_CHILD_DIR="+dir, "FAULT_CHILD_HOLD="+held)
	}
	if d.cmd.Env == nil {
		d.cmd.Env = os.Environ()
	}
	d.cmd.Env = append(d.cmd.Env, "GOMAXPROCS=2")
	log, err := os.CreateTemp(dir, "child-*.log")
	if err != nil {
		t.Fatal(err)
	}
	d.log = log
	d.cmd.Stderr = log
	stdout, err := d.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 16)
	d.lines = lines
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "HELD ") {
				lines <- scanner.Text()
			}
		}
	}()
	if err = d.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { d.kill(t) })
	limit := time.Now().Add(8 * time.Second)
	for time.Now().Before(limit) {
		raw, err := os.ReadFile(filepath.Join(dir, "refs.json"))
		if err == nil {
			var refs []xrpc.ServiceRef
			if json.Unmarshal(raw, &refs) == nil && len(refs) == 1 {
				d.ref = refs[0]
				requestBytes, responseBytes := api.MaxRequestBytes, api.MaxResponseBytes
				if d.ref.Profile == xrpc.HTTP {
					d.transport, err = httpx.New(httpx.Config{LocalTargetID: d.ref.TargetID, Service: d.ref,
						MaxRequestBytes: int64(requestBytes), MaxResponseBytes: int64(responseBytes),
						MaxConnections: 4, MaxInFlight: 16})
					if err != nil {
						t.Fatal(err)
					}
					d.caller = client.HTTPCaller{Transport: d.transport, Grant: grant}
				} else {
					d.grpc = grpcx.NewProfile(grpcx.DialOptions{LocalTargetID: d.ref.TargetID,
						MaxRequestBytes: requestBytes, MaxResponseBytes: responseBytes,
						Metadata: metadata.Pairs("authorization", "Bearer "+grant)}, protoregistry.GlobalFiles)
					d.caller = d.grpc
				}
				d.client, err = client.New(d.caller, d.ref)
				if err != nil {
					t.Fatal(err)
				}
				return d
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	raw, _ := os.ReadFile(log.Name())
	t.Fatalf("private daemon did not publish its reference: %s", raw)
	return nil
}

func (d *daemon) kill(t *testing.T) {
	t.Helper()
	if d.stopped {
		return
	}
	d.stopped = true
	if err := d.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	_ = d.cmd.Wait()
	if d.transport != nil {
		d.transport.Close()
	}
	if d.grpc != nil {
		d.grpc.Close()
	}
	_ = d.log.Close()
}

func nativeRead(t *testing.T, d *daemon) api.SnapshotResponse {
	t.Helper()
	out, err := d.client.Snapshot(deadline(t), "read-business", api.SnapshotRequest{Scope: scope, Queries: queries()})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func verifyBusiness(t *testing.T, saved api.SnapshotResponse, req api.BatchRequest, receipt api.Receipt) {
	t.Helper()
	if saved.Token != receipt.Token || len(saved.Results) != len(req.Mutations) {
		t.Fatalf("business snapshot/receipt mismatch: %+v %+v", saved.Token, receipt.Token)
	}
	for i, mutation := range req.Mutations {
		result := saved.Results[i]
		if result.Collection != mutation.Collection || len(result.Records) != 1 {
			t.Fatalf("missing atomic collection %s", mutation.Collection)
		}
		r := result.Records[0]
		// Compare decoded canonical JSON with UseNumber to retain integer precision.
		decode := func(raw []byte) any {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.UseNumber()
			var out any
			if err := d.Decode(&out); err != nil {
				t.Fatal(err)
			}
			return out
		}
		if r.Missing || r.Deleted || r.Version != receipt.Token.Revision || !reflect.DeepEqual(decode(r.Data), decode(mutation.Data)) {
			t.Fatalf("atomic business value lost: %s %+v", mutation.Collection, r)
		}
	}
}

func TestFaultKillAcknowledgedCommitAndOwnerConflict(t *testing.T) {
	dir := privateDir(t)
	d := start(t, dir, true, "")
	initial := nativeRead(t, d)
	req := request(initial.Token, "acknowledged")
	receipt, err := d.client.Batch(deadline(t), req)
	if err != nil || receipt.Durability != "sqlite-full" || receipt.Token.Revision != "1" {
		t.Fatalf("durable commit: %+v %v", receipt, err)
	}
	// A separate real service cannot acquire this temporary database's owner.
	other := exec.CommandContext(deadline(t), os.Getenv("FAULT_STORAGE_BIN"), daemonArgs(dir, false)...)
	raw, err := other.CombinedOutput()
	if err == nil || !strings.Contains(string(raw), "database already owned") {
		t.Fatalf("second process owner was not rejected: %s %v", raw, err)
	}
	previous := d.ref
	d.kill(t)
	d = start(t, dir, false, "")
	if d.ref.InstanceID == previous.InstanceID {
		t.Fatal("process restart reused the old instance identity")
	}
	saved := nativeRead(t, d)
	verifyBusiness(t, saved, req, receipt)
	after, err := d.client.Receipt(deadline(t), "receipt-after-kill", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(after, receipt) {
		t.Fatalf("committed receipt changed across SIGKILL: %+v %v", after, err)
	}
	again, err := d.client.Batch(deadline(t), req)
	if err != nil || !reflect.DeepEqual(again, receipt) {
		t.Fatalf("identical replay was not the original receipt: %+v %v", again, err)
	}
	changed := req
	changed.Mutations = append([]api.Mutation(nil), req.Mutations...)
	changed.Mutations[0].Data = json.RawMessage(`{"operation":"different"}`)
	if _, err = d.client.Batch(deadline(t), changed); code(err) != "conflict" {
		t.Fatalf("changed identity replay accepted: %v", err)
	}
	oldTransport, err := httpx.New(httpx.Config{LocalTargetID: previous.TargetID, Service: previous})
	if err != nil {
		t.Fatal(err)
	}
	defer oldTransport.Close()
	oldClient, err := client.New(client.HTTPCaller{Transport: oldTransport, Grant: grant}, previous)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = oldClient.Snapshot(deadline(t), "stale-instance", api.SnapshotRequest{Scope: scope, Queries: queries()}); code(err) != "conflict" {
		t.Fatalf("stale process binding was accepted: %v", err)
	}
	evidence(t, map[string]any{"signal": "SIGKILL", "receipt": receipt, "restart_instance_changed": true,
		"second_owner_rejected": true, "changed_replay_rejected": true, "stale_instance_rejected": true})
}

func TestFaultKillBeforeAndAfterCommitReply(t *testing.T) {
	for _, boundary := range []string{"before", "after"} {
		t.Run(boundary, func(t *testing.T) {
			dir := privateDir(t)
			d := start(t, dir, true, boundary)
			req := request(nativeRead(t, d).Token, "held-commit")
			result := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				_, err := d.client.Batch(ctx, req)
				result <- err
			}()
			select {
			case line := <-d.lines:
				if line != "HELD "+boundary {
					t.Fatalf("unexpected private boundary signal: %q", line)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("real handler never reached the held boundary")
			}
			d.kill(t)
			err := <-result
			var failure *xrpc.CallError
			if !errors.As(err, &failure) || failure.Disposition != xrpc.OutcomeUnknown {
				t.Fatalf("lost response must preserve uncertainty: %v", err)
			}
			d = start(t, dir, false, "")
			saved := nativeRead(t, d)
			receipt, err := d.client.Receipt(deadline(t), "resolve-held", api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
			if boundary == "after" {
				if err != nil {
					t.Fatal(err)
				}
				verifyBusiness(t, saved, req, receipt)
			} else {
				if code(err) != "not_found" || saved.Token.Revision != "0" {
					t.Fatalf("unexecuted call left a revision/receipt: %+v %v", saved.Token, err)
				}
				for _, result := range saved.Results {
					if !result.Records[0].Missing {
						t.Fatal("pre-execution crash left a partial business transaction")
					}
				}
			}
			evidence(t, map[string]any{"boundary": boundary, "signal": "SIGKILL", "disposition": failure.Disposition,
				"revision_after_restart": saved.Token.Revision, "receipt_error": code(err)})
		})
	}
}

func TestFaultDiskFullAtomicRollbackAndRecovery(t *testing.T) {
	path := filepath.Join(privateDir(t), "fixture.db")
	limited := func(c *engine.Config) { c.MaxDBBytes = 1 << 20 }
	s := open(t, path, true, limited)
	seed := request(read(t, s).Token, "seed-business")
	receipt, err := s.Batch(deadline(t), seed)
	if err != nil {
		t.Fatal(err)
	}
	before := read(t, s)
	large := api.BatchRequest{Scope: scope, Expected: receipt.Token, RequestID: "full-transaction"}
	for i := 0; i < 160; i++ {
		data, _ := json.Marshal(map[string]string{"payload": strings.Repeat("x", 8192), "operation": fmt.Sprintf("fill-%d", i)})
		large.Mutations = append(large.Mutations, api.Mutation{Collection: "state", Key: fmt.Sprintf("fill-%03d", i), ExpectedVersion: "0", Data: data})
	}
	_, err = s.Batch(deadline(t), large)
	if code(err) != "disk_full" || !(strings.Contains(err.Error(), "SQLITE_FULL") || strings.Contains(err.Error(), "(13)")) {
		t.Fatalf("must reach real SQLite page exhaustion, not admission quota: %v", err)
	}
	failure := err.Error()
	if !reflect.DeepEqual(read(t, s), before) {
		t.Fatal("disk-full changed committed business facts/revision")
	}
	check, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "state", Limit: 2048}}})
	if err != nil || len(check.Results[0].Records) != 1 {
		t.Fatalf("partial full transaction leaked: %+v %v", check, err)
	}
	if _, err = s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: large.RequestID}); code(err) != "not_found" {
		t.Fatalf("failed write produced a committed receipt: %v", err)
	}
	if err = s.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Stats()
	if err != nil || stats.WritersQueued != 0 || stats.ReadersActive != 0 || stats.DatabaseBytes > 1<<20 {
		t.Fatalf("disk-full leaked resources or page limit: %+v %v", stats, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, false, limited)
	verifyBusiness(t, read(t, s), seed, receipt)
	// Recovery is observable through a fresh successful business update.
	update := api.BatchRequest{Scope: scope, Expected: receipt.Token, RequestID: "after-full",
		Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: "1", Data: json.RawMessage(`{"operation":"run-1","phase":"resumed"}`)}}}
	if _, err = s.Batch(deadline(t), update); err != nil {
		t.Fatalf("writer did not recover after failed commit: %v", err)
	}
	evidence(t, map[string]any{"injection": "SQLite max_page_count=256, MaxDBBytes=1MiB", "failure": failure,
		"stats_after_failure": stats, "rollback_verified": true, "restart_write_succeeded": true,
		"physical_filesystem_enospc_tested": false})
}

func TestFaultCompetingCASAndProcessResources(t *testing.T) {
	dir := privateDir(t)
	d := start(t, dir, true, "")
	listener := listenerSocket(t, d)
	initial := nativeRead(t, d)
	const callers = 16
	barrier := make(chan struct{})
	type result struct {
		receipt api.Receipt
		err     error
	}
	results := make(chan result, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			<-barrier
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			r, err := d.client.Batch(ctx, request(initial.Token, fmt.Sprintf("competitor-%d", i)))
			results <- result{r, err}
		}(i)
	}
	close(barrier)
	success, conflicts := 0, 0
	var winner api.Receipt
	for i := 0; i < callers; i++ {
		r := <-results
		if r.err == nil {
			success++
			winner = r.receipt
		} else if code(r.err) == "conflict" {
			conflicts++
		} else {
			t.Fatalf("unexpected competing write outcome: %v", r.err)
		}
	}
	if success != 1 || conflicts != callers-1 {
		t.Fatalf("CAS lost its single winner: success=%d conflicts=%d", success, conflicts)
	}
	verifyBusiness(t, nativeRead(t, d), request(initial.Token, winner.RequestID), winner)
	for i := 0; i < callers; i++ {
		id := fmt.Sprintf("competitor-%d", i)
		_, err := d.client.Receipt(deadline(t), "verify-"+id, api.ReceiptRequest{Scope: scope, RequestID: id})
		if id != winner.RequestID && code(err) != "not_found" {
			t.Fatalf("loser %s published a receipt: %v", id, err)
		}
	}
	// Observe native process resources after warm-up, during bounded repeated
	// work and after close. These are safety caps, not shared-host benchmarks.
	base := processResources(t, d.cmd.Process.Pid)
	peak := base
	for round := 0; round < 8; round++ {
		var group sync.WaitGroup
		errs := make(chan error, callers)
		for i := 0; i < callers; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err := d.client.Snapshot(ctx, fmt.Sprintf("churn-%d-%d", round, i), api.SnapshotRequest{Scope: scope, Queries: queries()})
				errs <- err
			}(i)
		}
		done := make(chan struct{})
		go func() { group.Wait(); close(done) }()
		for {
			current := processResources(t, d.cmd.Process.Pid)
			peak.FDs = max(peak.FDs, current.FDs)
			peak.Threads = max(peak.Threads, current.Threads)
			peak.RSSKiB = max(peak.RSSKiB, current.RSSKiB)
			if current.FDs > 128 || current.Threads > 32 || current.RSSKiB > 192*1024 {
				t.Fatalf("native bounded smoke resource cap exceeded: %+v", current)
			}
			select {
			case <-done:
				goto finished
			case <-time.After(time.Millisecond):
			}
		}
	finished:
		close(errs)
		for err := range errs {
			if err != nil && code(err) != "resource_exhausted" {
				t.Fatal(err)
			}
		}
	}
	// Concurrent reads lazily populate the bounded SQL reader pool. Compare
	// closure against this warmed pool, and audit accepted sockets separately.
	beforeClose := processResources(t, d.cmd.Process.Pid)
	d.transport.Close()
	var final resources
	limit := time.Now().Add(2 * time.Second)
	for {
		final = processResources(t, d.cmd.Process.Pid)
		if socketFDs(t, d.cmd.Process.Pid) == 1 || time.Now().After(limit) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	remainingSockets := socketLinks(t, d.cmd.Process.Pid)
	if len(remainingSockets) != 1 || !remainingSockets[listener] || final.FDs > beforeClose.FDs {
		t.Fatalf("closing pooled client leaked service FDs: warm=%+v final=%+v", beforeClose, final)
	}
	evidence(t, map[string]any{"callers": callers, "success": success, "conflicts": conflicts,
		"resource_baseline": base, "resource_observed_max": peak, "resource_after_client_close": final,
		"resource_before_client_close": beforeClose, "remaining_socket_fds": 1,
		"remaining_socket_is_original_listener": true,
		"caps":                                  resources{FDs: 128, Threads: 32, RSSKiB: 192 * 1024}, "gomaxprocs": 2,
		"benchmark_claim": false})
}

type resources struct {
	FDs     int `json:"fds"`
	Threads int `json:"threads"`
	RSSKiB  int `json:"rss_kib"`
}

func processResources(t *testing.T, pid int) resources {
	t.Helper()
	base := fmt.Sprintf("/proc/%d", pid)
	fds, err := os.ReadDir(base + "/fd")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(base + "/status")
	if err != nil {
		t.Fatal(err)
	}
	out := resources{FDs: len(fds)}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			n, _ := strconv.Atoi(fields[1])
			switch fields[0] {
			case "Threads:":
				out.Threads = n
			case "VmRSS:":
				out.RSSKiB = n
			}
		}
	}
	return out
}

func TestFaultBackupNewBusinessRestoreAndNegativeControls(t *testing.T) {
	sourcePath := filepath.Join(privateDir(t), "fixture.db")
	s := open(t, sourcePath, true, nil)
	req := request(read(t, s).Token, "backup-business")
	receipt, err := s.Batch(deadline(t), req)
	if err != nil {
		t.Fatal(err)
	}
	verifyBusiness(t, read(t, s), req, receipt)
	tombstoneCreate, err := s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: receipt.Token, RequestID: "obsolete-create",
		Mutations: []api.Mutation{{Collection: "configuration", Key: "obsolete", ExpectedVersion: "0", Data: json.RawMessage(`{"operation":"obsolete"}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	tombstoneDelete, err := s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: tombstoneCreate.Token, RequestID: "obsolete-delete",
		Mutations: []api.Mutation{{Collection: "configuration", Key: "obsolete", ExpectedVersion: tombstoneCreate.Token.Revision, Delete: true}}})
	if err != nil {
		t.Fatal(err)
	}
	saved := read(t, s)
	stats, err := s.Stats()
	if err != nil || stats.WALBytes == 0 {
		t.Fatalf("backup must include uncheckpointed committed WAL: %+v %v", stats, err)
	}
	// Negative control: a bare live main-file copy loses these small WAL commits.
	// Both paths are private fixtures; this deliberately incorrect copy is never
	// offered as a backup or activated as a user database.
	barePath := filepath.Join(privateDir(t), "bare-main.db")
	mainOnly, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(barePath, mainOnly, 0600); err != nil {
		t.Fatal(err)
	}
	bare := open(t, barePath, false, nil)
	if reflect.DeepEqual(read(t, bare), saved) {
		t.Fatal("backup fixture no longer distinguishes missing committed WAL")
	}
	_ = bare.Close()
	dir := privateDir(t)
	backup := filepath.Join(dir, "snapshot.db")
	backupReceipt, err := s.Backup(deadline(t), backup)
	if err != nil {
		t.Fatal(err)
	}
	digest := fileHash(t, backup)
	if _, err = s.Backup(deadline(t), backup); err == nil || fileHash(t, backup) != digest {
		t.Fatalf("no-overwrite backup negative control failed: %v", err)
	}
	// Change the source after backup; restoring must recover the chosen commit.
	if _, err = s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: saved.Token, RequestID: "later-source-change",
		Mutations: []api.Mutation{{Collection: "preferences", Key: "primary", ExpectedVersion: "1", Data: json.RawMessage(`{"operation":"run-1","active":"later"}`)}}}); err != nil {
		t.Fatal(err)
	}
	restored := open(t, backup, false, nil)
	if err = restored.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read(t, restored), saved) {
		t.Fatal("new business backup lost values, versions, revision or scope")
	}
	deleted, err := restored.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "configuration", Keys: []string{"obsolete"}, IncludeDeleted: true}}})
	if err != nil || len(deleted.Results[0].Records) != 1 || !deleted.Results[0].Records[0].Deleted || deleted.Results[0].Records[0].Version != tombstoneDelete.Token.Revision {
		t.Fatalf("backup lost tombstone/ABA version: %+v %v", deleted, err)
	}
	if _, err = restored.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: saved.Token, RequestID: "forbidden-aba",
		Mutations: []api.Mutation{{Collection: "configuration", Key: "obsolete", ExpectedVersion: "0", Data: json.RawMessage(`{"operation":"revived"}`)}}}); code(err) != "conflict" {
		t.Fatalf("restored tombstone allowed ABA recreation: %v", err)
	}
	got, err := restored.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(got, receipt) {
		t.Fatalf("backup lost the commit receipt: %+v %v", got, err)
	}
	indexed, err := restored.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope,
		Queries: []api.Query{{Collection: "layouts", Index: "operation", Equal: []json.RawMessage{json.RawMessage(`"run-1"`)}, Limit: 8}}})
	if err != nil || len(indexed.Results[0].Records) != 1 || indexed.Results[0].Records[0].Key != "primary" {
		t.Fatalf("backup lost registered lookup data: %+v %v", indexed, err)
	}
	if _, err = restored.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: "later-source-change"}); code(err) != "not_found" {
		t.Fatalf("backup included a later commit: %v", err)
	}
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
	corruptPath := filepath.Join(privateDir(t), "corrupt.db")
	raw, err := os.ReadFile(backup)
	if err != nil {
		t.Fatal(err)
	}
	copy(raw[:16], []byte("NOT A SQLITE DB!"))
	if err = os.WriteFile(corruptPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if candidate, err := engine.Open(deadline(t), engine.Config{Path: corruptPath, Manifest: manifest()}); err == nil {
		candidate.Close()
		t.Fatal("corrupt candidate silently opened or rebuilt")
	}
	evidence(t, map[string]any{"backup": backupReceipt, "backup_file_sha256": digest, "business_collections": 6,
		"receipt_and_index_restored": true, "tombstone_and_aba_restored": true, "later_source_commit_excluded": true,
		"corrupt_candidate_rejected": true,
		"activation_cli_tested":      false, "legacy_schema_import_tested": false})
}

func TestFaultReadonlyStartupNoMutation(t *testing.T) {
	if os.Getenv("FAULT_STORAGE_BIN") == "" {
		t.Skip("run scripts/fault-validate.py for the isolated readonly startup matrix")
	}
	for _, mode := range []string{"file-mode", "landlock", "readonly-mount"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "readonly-mount" && os.Getenv("FAULT_CONTAINER_IMAGE") == "" {
				t.Skip("use --container-image with a cached image for actual readonly mount injection")
			}
			dir := privateDir(t)
			path := filepath.Join(dir, "fixture.db")
			s := open(t, path, true, nil)
			if _, err := s.Batch(deadline(t), request(read(t, s).Token, "readonly-source")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before := fileHash(t, path)
			configFiles(t, dir)
			var cmd *exec.Cmd
			if mode == "file-mode" {
				if err := os.Chmod(path, 0400); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(path, 0600) })
				cmd = exec.CommandContext(deadline(t), os.Getenv("FAULT_STORAGE_BIN"), daemonArgs(dir, false)...)
			} else if mode == "landlock" {
				cmd = exec.CommandContext(deadline(t), os.Args[0], "-test.run=^TestFaultChild$", "-test.v")
				cmd.Env = append(os.Environ(), "FAULT_CHILD=readonly", "FAULT_CHILD_DIR="+dir)
			} else {
				args := containerArgs(t, os.Getenv("FAULT_STORAGE_BIN"))
				args = append(args, "--mount", "type=bind,src="+dir+",dst=/fixture,readonly",
					os.Getenv("FAULT_CONTAINER_IMAGE"), "/runner")
				args = append(args, daemonArgs("/fixture", false)...)
				cmd = exec.CommandContext(deadline(t), "docker", args...)
			}
			raw, err := cmd.CombinedOutput()
			message := strings.ToLower(string(raw))
			if err == nil || !(strings.Contains(message, "permission denied") || (mode == "readonly-mount" && strings.Contains(message, "read-only file system"))) {
				t.Fatalf("OS readonly/write-denial did not reject startup: %s %v", raw, err)
			}
			if fileHash(t, path) != before {
				t.Fatal("write-denied startup changed existing business data")
			}
			if _, err = os.Stat(filepath.Join(dir, "refs.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed startup published a usable service reference")
			}
			if err = os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			recovered := open(t, path, false, nil)
			if read(t, recovered).Token.Revision != "1" {
				t.Fatal("readonly recovery lost the committed revision")
			}
			evidence(t, map[string]any{"injection": mode, "startup_rejected": true, "database_unchanged": true,
				"diagnostic": strings.TrimSpace(string(raw)), "runtime_sqlite_readonly_tested": false})
		})
	}
}

// Every container is private, bounded, offline, and has only test-owned mounts.
// No image pull, shared mount mutation, privileged container or station restart.
func containerArgs(t *testing.T, binary string) []string {
	t.Helper()
	name := fmt.Sprintf("sol20-fault-%d-%d", os.Getpid(), time.Now().UnixNano())
	if ledger := os.Getenv("FAULT_CONTAINER_LEDGER"); ledger != "" {
		f, err := os.OpenFile(ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(map[string]string{"name": name})
		_, writeErr := f.Write(append(raw, '\n'))
		syncErr, closeErr := f.Sync(), f.Close()
		if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "docker", "rm", "-f", name).Run()
	})
	return []string{"run", "--rm", "--pull=never", "--name", name, "--read-only", "--network=none",
		"--cap-drop=ALL", "--pids-limit=64", "--cpus=1", "--memory=256m", "--memory-swap=256m",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"--mount", "type=bind,src=" + binary + ",dst=/runner,readonly"}
}

func TestFaultPhysicalFilesystemFullPrivateTmpfs(t *testing.T) {
	if os.Getenv("FAULT_CONTAINER_IMAGE") == "" {
		t.Skip("use --container-image for isolated physical ENOSPC injection")
	}
	args := containerArgs(t, os.Getenv("FAULT_CONTAINER_TEST_BIN"))
	args = append(args, "--tmpfs", fmt.Sprintf("/fixture:rw,size=2m,mode=0700,uid=%d,gid=%d", os.Getuid(), os.Getgid()),
		"--env", "GOMAXPROCS=2", "--env", "FAULT_CHILD=physical-full", "--env", "FAULT_CHILD_DIR=/fixture",
		os.Getenv("FAULT_CONTAINER_IMAGE"), "/runner", "-test.run=^TestFaultChild$", "-test.v")
	raw, err := exec.CommandContext(deadline(t), "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("private physical-full test failed: %s %v", raw, err)
	}
	if !strings.Contains(string(raw), "PHYSICAL_FULL_VERIFIED") {
		t.Fatalf("container did not verify physical-full postconditions: %s", raw)
	}
	evidence(t, map[string]any{"injection": "private 2MiB tmpfs, engine page/WAL caps larger than filesystem",
		"physical_filesystem_enospc_tested": true, "diagnostic": strings.TrimSpace(string(raw))})
}

func TestFaultCommittedReplayUnderDiskWatermark(t *testing.T) {
	path := filepath.Join(privateDir(t), "fixture.db")
	s := open(t, path, true, nil)
	req := request(read(t, s).Token, "retained-replay")
	committed, err := s.Batch(deadline(t), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path, false, func(c *engine.Config) { c.MinFreeBytes = 1<<63 - 1 })
	retained, err := s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: req.RequestID})
	if err != nil || !reflect.DeepEqual(retained, committed) {
		t.Fatalf("receipt read failed under watermark: %+v %v", retained, err)
	}
	again, err := s.Batch(deadline(t), req)
	evidence(t, map[string]any{"retained_receipt": true, "replay_error": code(err), "same_receipt": reflect.DeepEqual(again, committed)})
	if err != nil || !reflect.DeepEqual(again, committed) {
		t.Fatalf("already committed identity must resolve without new disk admission: %+v %v", again, err)
	}
}

func TestFaultBackupCountAdmissionBoundary(t *testing.T) {
	s := open(t, filepath.Join(privateDir(t), "fixture.db"), true, nil)
	dir := privateDir(t)
	for i := 0; i < 128; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("existing-%03d.db", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Backup(deadline(t), filepath.Join(dir, "overflow.db"))
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	evidence(t, map[string]any{"existing_outputs": 128, "outputs_after_attempt": len(entries), "error": code(err)})
	if code(err) != "resource_exhausted" || len(entries) != 128 {
		t.Fatalf("backup count admission exceeded 128 outputs: entries=%d error=%v", len(entries), err)
	}
}

// TestFaultChild is a private subprocess entry point, not a mock storage engine.
// The after boundary calls the real production handler and buffers only its wire
// reply. SQLite has already committed when the parent receives HELD after.
func TestFaultChild(t *testing.T) {
	mode := os.Getenv("FAULT_CHILD")
	if mode == "" {
		return
	}
	dir := os.Getenv("FAULT_CHILD_DIR")
	if dir == "" || !filepath.IsAbs(dir) {
		t.Fatal("private fixture directory required")
	}
	if mode == "physical-full" {
		s := open(t, filepath.Join(dir, "fixture.db"), true, func(c *engine.Config) { c.MinFreeBytes = 1 << 20 })
		seed := request(read(t, s).Token, "physical-source")
		commit, err := s.Batch(deadline(t), seed)
		if err != nil {
			t.Fatal(err)
		}
		before, err := s.Stats()
		if err != nil || before.FreeBytes <= 1<<20 || before.FreeBytes > 2<<20 {
			t.Fatalf("physical tmpfs injection was not isolated/admissible: %+v %v", before, err)
		}
		full := api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "physical-full"}
		for i := 0; i < 240; i++ {
			data, _ := json.Marshal(map[string]string{"payload": strings.Repeat("x", 12*1024), "operation": fmt.Sprintf("physical-%d", i)})
			full.Mutations = append(full.Mutations, api.Mutation{Collection: "state", Key: fmt.Sprintf("physical-%03d", i), ExpectedVersion: "0", Data: data})
		}
		_, err = s.Batch(deadline(t), full)
		if code(err) != "disk_full" || !strings.Contains(err.Error(), "(13)") {
			t.Fatalf("physical full must be SQLite 13, not the watermark: %v", err)
		}
		failure := err.Error()
		verifyBusiness(t, read(t, s), seed, commit)
		all, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "state", Limit: 2048}}})
		if err != nil || len(all.Results[0].Records) != 1 {
			t.Fatalf("physical-full leaked partial data: %+v %v", all, err)
		}
		if _, err = s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: full.RequestID}); code(err) != "not_found" {
			t.Fatalf("physical-full retained failed receipt: %v", err)
		}
		if err = s.Integrity(deadline(t)); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		s = open(t, filepath.Join(dir, "fixture.db"), false, func(c *engine.Config) { c.MinFreeBytes = 1 << 20 })
		verifyBusiness(t, read(t, s), seed, commit)
		if _, err = s.Batch(deadline(t), api.BatchRequest{Scope: scope, Expected: commit.Token, RequestID: "physical-resume",
			Mutations: []api.Mutation{{Collection: "state", Key: "primary", ExpectedVersion: "1", Data: json.RawMessage(`{"operation":"run-1","phase":"resumed"}`)}}}); err != nil {
			t.Fatalf("physical-full writer did not recover after restart: %v", err)
		}
		fmt.Printf("PHYSICAL_FULL_VERIFIED %s\n", failure)
		return
	}
	if mode == "readonly" {
		runtime.LockOSThread()
		// Deny new write opens, then exec so every thread of the production daemon
		// inherits the restriction. Read access and already-open stdout remain.
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		attr := struct{ Handled uint64 }{unix.LANDLOCK_ACCESS_FS_WRITE_FILE}
		fd, _, errno := syscall.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0)
		if errno != 0 {
			t.Fatalf("Landlock unavailable (no readonly pass claimed): %v", errno)
		}
		_, _, errno = syscall.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, fd, 0, 0)
		unix.Close(int(fd))
		if errno != 0 {
			t.Fatal(errno)
		}
		binary := os.Getenv("FAULT_STORAGE_BIN")
		if err := syscall.Exec(binary, append([]string{binary}, daemonArgs(dir, false)...), os.Environ()); err != nil {
			t.Fatal(err)
		}
		return
	}
	if mode != "held" {
		t.Fatal("unknown private child mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "fixture.db"), Create: true, Manifest: manifest()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	handler, err := server.HTTP(s, []server.Grant{{Token: grant, Namespace: scope.Namespace, User: scope.User, Workspace: scope.Workspace}})
	if err != nil {
		t.Fatal(err)
	}
	boundary := os.Getenv("FAULT_CHILD_HOLD")
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/batch" {
			handler.ServeHTTP(w, r)
			return
		}
		if boundary == "before" {
			fmt.Println("HELD before")
			<-r.Context().Done()
			return
		}
		buffer := httptest.NewRecorder()
		handler.ServeHTTP(buffer, r)
		if buffer.Code != 200 {
			for name, values := range buffer.Header() {
				w.Header()[name] = values
			}
			w.WriteHeader(buffer.Code)
			_, _ = w.Write(buffer.Body.Bytes())
			return
		}
		fmt.Println("HELD after")
		<-r.Context().Done()
	})
	lease, err := unixlease.Reserve(ctx, filepath.Join(dir, "rpc.sock"), unixlease.Options{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := lease.Listen()
	if err != nil {
		t.Fatal(err)
	}
	ref := xrpc.ServiceRef{TargetID: "fault-fixture", Service: api.Service, APIVersion: api.Version,
		InstanceID: "private-held-instance", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: filepath.Join(dir, "rpc.sock")}}
	host, err := httpx.Serve(listener, lease, wrapper, httpx.HostOptions{InstanceID: ref.InstanceID,
		MaxBodyBytes: api.MaxRequestBytes, MaxInFlight: 16, MaxConnections: 16, MaxCallTime: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Shutdown(ctx) }()
	raw, _ := json.Marshal([]xrpc.ServiceRef{ref})
	if err = os.WriteFile(filepath.Join(dir, "refs.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	t.Fatal("held child should have been killed by its own parent")
}
