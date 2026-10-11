//go:build linux

package faults_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/api"
	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A durable commit is on stable storage when it returns; a relaxed one is in
// the WAL and survives the death of the process, and only a power failure may
// take it. This test kills the process, which both classes must survive: every
// commit the child acknowledged is present after SIGKILL, every transaction is
// whole, and the database is consistent. The power-loss difference is covered
// by the benchmark (one sync per durable commit) and the commit accounting.

type child struct {
	cmd   *exec.Cmd
	mu    sync.Mutex
	lines []string
	wake  chan struct{}
}

func startChild(t *testing.T, mode, dir string) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFaultChild$", "-test.v")
	cmd.Env = append(os.Environ(), "FAULT_CHILD="+mode, "FAULT_CHILD_DIR="+dir, "GOMAXPROCS=2")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: cmd, wake: make(chan struct{}, 1)}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "ACK ") {
				c.mu.Lock()
				c.lines = append(c.lines, scanner.Text())
				c.mu.Unlock()
				select {
				case c.wake <- struct{}{}:
				default:
				}
			}
		}
	}()
	return c
}

// acknowledged waits for n acknowledgements, kills the child with SIGKILL and
// returns what it acknowledged before it died.
func (c *child) killAfter(t *testing.T, n int) []string {
	t.Helper()
	limit := time.After(20 * time.Second)
	for {
		c.mu.Lock()
		count := len(c.lines)
		c.mu.Unlock()
		if count >= n {
			break
		}
		select {
		case <-c.wake:
		case <-limit:
			t.Fatalf("child acknowledged only %d of %d commits", count, n)
		}
	}
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = c.cmd.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

// commitsChild commits atomic pairs forever, alternating durable and relaxed
// batches, and acknowledges each commit on stdout only after it returned.
func commitsChild(t *testing.T, dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "fixture.db"), Create: true, Manifest: manifest(), MaxDBBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	token := api.Token{}
	versions := map[string]string{"state": "0", "events": "0"}
	for i := 1; ; i++ {
		if i == 1 {
			first, e := s.Snapshot(ctx, api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "state", Keys: []string{"pair"}}}})
			if e != nil {
				t.Fatal(e)
			}
			token = first.Token
		}
		relaxed := i%3 != 0
		body := json.RawMessage(fmt.Sprintf(`{"operation":"crash-%d","counter":%d}`, i, i))
		request := api.BatchRequest{Scope: scope, Expected: token, Relaxed: relaxed, Mutations: []api.Mutation{
			{Collection: "state", Key: "pair", ExpectedVersion: versions["state"], Data: body},
			{Collection: "events", Key: "pair", ExpectedVersion: versions["events"], Data: body}}}
		class := "relaxed"
		if !relaxed {
			request.RequestID = fmt.Sprintf("durable-%d", i)
			class = "durable"
		}
		receipt, e := s.Batch(ctx, request)
		if e != nil {
			t.Fatal(e)
		}
		token = receipt.Token
		versions["state"], versions["events"] = receipt.Token.Revision, receipt.Token.Revision
		fmt.Printf("ACK %d %s %s\n", i, class, receipt.Token.Revision)
	}
}

func TestFaultKilledProcessKeepsDurableAndRelaxedBatches(t *testing.T) {
	for round, acked := range []int{25, 60, 140} {
		t.Run(strconv.Itoa(acked), func(t *testing.T) {
			dir := privateDir(t)
			c := startChild(t, "commits", dir)
			lines := c.killAfter(t, acked)
			var last int
			durable := map[string]bool{}
			classes := map[string]int{}
			for _, line := range lines {
				var n int
				var class, revision string
				if _, err := fmt.Sscanf(line, "ACK %d %s %s", &n, &class, &revision); err != nil {
					t.Fatal(line, err)
				}
				last = n
				classes[class]++
				if class == "durable" {
					durable[fmt.Sprintf("durable-%d", n)] = true
				}
			}
			if classes["durable"] == 0 || classes["relaxed"] == 0 {
				t.Fatalf("both commit classes must be exercised: %v", classes)
			}
			s := open(t, filepath.Join(dir, "fixture.db"), false, nil)
			if err := s.Integrity(deadline(t)); err != nil {
				t.Fatal(err)
			}
			got, err := s.Snapshot(deadline(t), api.SnapshotRequest{Scope: scope, Queries: []api.Query{{Collection: "state", Keys: []string{"pair"}}, {Collection: "events", Keys: []string{"pair"}}}})
			if err != nil {
				t.Fatal(err)
			}
			state, events := got.Results[0].Records[0], got.Results[1].Records[0]
			if state.Missing || events.Missing || string(state.Data) != string(events.Data) || state.Version != events.Version {
				t.Fatalf("a transaction was torn: %+v %+v", state, events)
			}
			var stored struct{ Counter int }
			if err = json.Unmarshal(state.Data, &stored); err != nil {
				t.Fatal(err)
			}
			// Everything acknowledged is there; at most the one commit that was
			// in flight when the process died may have completed without its
			// acknowledgement.
			if stored.Counter < last || stored.Counter > last+1 {
				t.Fatalf("round %d: acknowledged commit %d, stored %d", round, last, stored.Counter)
			}
			if got.Token.Revision != strconv.Itoa(stored.Counter) || state.Version != got.Token.Revision {
				t.Fatalf("revision %s does not follow the commit count %d", got.Token.Revision, stored.Counter)
			}
			for id := range durable {
				if _, err = s.Receipt(deadline(t), api.ReceiptRequest{Scope: scope, RequestID: id}); err != nil {
					t.Fatalf("acknowledged durable request %s lost its receipt: %v", id, err)
				}
			}
			// Request-less relaxed batches left no receipt behind.
			stats, _ := s.Stats()
			t.Logf("killed after %d acknowledged commits (%v), stored %d, WAL %d bytes", len(lines), classes, stored.Counter, stats.WALBytes)
		})
	}
}

// coreChild drives the Core data module the way Core does: accept a Run
// durably, mark it running relaxed, and finish every third one durably.
func coreChild(t *testing.T, dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s, err := engine.Open(ctx, engine.Config{Path: filepath.Join(dir, "core.db"), Create: true, Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}, MaxDBBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	core, err := coredata.New(s, coreScope)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; ; i++ {
		id := fmt.Sprintf("run-%d", i)
		if _, _, err = core.CreateRun(ctx, model.NewRun{ID: id, TargetID: "local", WorkflowResourceID: "wf", WorkflowCommitID: "v1", DefinitionDigest: "d", ActionID: "run",
			Inputs: json.RawMessage(`{"i":` + strconv.Itoa(i) + `}`), Trigger: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("ACK created %s\n", id)
		if err = core.UpdateRunStatus(ctx, model.RunStatusUpdate{ID: id, Status: model.RunRunning}); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("ACK running %s\n", id)
		if i%3 == 0 {
			if _, err = core.FinishRun(ctx, model.RunFinish{ID: id, Status: model.RunSucceeded, Result: json.RawMessage(`{"ok":true}`)}); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("ACK finished %s\n", id)
		}
	}
}

var coreScope = api.Scope{Namespace: "core", User: "operator", Workspace: "station"}

func coreManifest() api.Manifest {
	return api.Manifest{Format: "storage-v1", Namespaces: []api.Namespace{{ID: "core", Owner: "fault", Schema: model.Schema, MaxScopes: 2, MaxReceipts: 16, ReceiptTTLSeconds: 3600, Modules: []string{model.Module},
		Collections: []api.Collection{{ID: "marker", MaxRecordBytes: 1024, MaxRecords: 4, MaxBytes: 4096, Retention: "fault", Recovery: "fault"}}}}}
}

func TestFaultKilledProcessKeepsAcknowledgedRunFacts(t *testing.T) {
	dir := privateDir(t)
	c := startChild(t, "core", dir)
	lines := c.killAfter(t, 90)
	created, running, finished := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, line := range lines {
		var kind, id string
		if _, err := fmt.Sscanf(line, "ACK %s %s", &kind, &id); err != nil {
			t.Fatal(line, err)
		}
		map[string]map[string]bool{"created": created, "running": running, "finished": finished}[kind][id] = true
	}
	s, err := engine.Open(deadline(t), engine.Config{Path: filepath.Join(dir, "core.db"), Manifest: coreManifest(), Modules: []engine.Module{coredata.Module()}, MaxDBBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	core, err := coredata.New(s, coreScope)
	if err != nil {
		t.Fatal(err)
	}
	for id := range created {
		run, err := core.GetRun(deadline(t), id)
		if err != nil {
			t.Fatalf("durably accepted run %s was lost: %v", id, err)
		}
		// A relaxed transition survives the process; so does everything after it.
		if running[id] && run.Status == model.RunQueued {
			t.Fatalf("acknowledged transition of %s was lost", id)
		}
		if finished[id] && run.Status != model.RunSucceeded {
			t.Fatalf("acknowledged outcome of %s was lost: %s", id, run.Status)
		}
	}
	open, err := core.ListRuns(deadline(t), model.RunFilter{Statuses: []model.RunStatus{model.RunQueued, model.RunRunning}, Limit: 1000, OmitPayloads: true})
	if err != nil {
		t.Fatal(err)
	}
	// Core's boot repair: whatever the dead process left open ends interrupted.
	n, err := core.InterruptOpenRuns(deadline(t), time.Now())
	if err != nil || n != len(open.Runs) {
		t.Fatalf("boot repair interrupted %d, expected the %d open runs: %v", n, len(open.Runs), err)
	}
	if err = s.Integrity(deadline(t)); err != nil {
		t.Fatal(err)
	}
	t.Logf("killed after %d acknowledgements: %d accepted, %d running, %d finished, %d interrupted at boot", len(lines), len(created), len(running), len(finished), n)
}
