//go:build linux

package faults_test

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/XGC-Team/xgc2-storage/client"
	"github.com/XGC-Team/xgc2-storage/registry"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

func socketFDs(t *testing.T, pid int) int {
	return len(socketLinks(t, pid))
}

func socketLinks(t *testing.T, pid int) map[string]bool {
	t.Helper()
	dir := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	links := map[string]bool{}
	for _, entry := range entries {
		name, err := os.Readlink(filepath.Join(dir, entry.Name()))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err == nil && strings.HasPrefix(name, "socket:[") {
			links[name] = true
		}
	}
	return links
}

func listenerSocket(t *testing.T, d *daemon) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(d.cmd.Process.Pid), "net", "unix"))
	if err != nil {
		t.Fatal(err)
	}
	owned := socketLinks(t, d.cmd.Process.Pid)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 8 && fields[3] == "00010000" {
			link := "socket:[" + fields[6] + "]"
			if !owned[link] {
				continue
			}
			path := fields[7]
			// SDK binds in the pinned directory namespace; the kernel records
			// /proc/self/fd/N/rpc.sock rather than its advertised public path.
			if strings.HasPrefix(path, "/proc/self/fd/") {
				path, err = filepath.EvalSymlinks(strings.Replace(path, "/proc/self/fd/", "/proc/"+strconv.Itoa(d.cmd.Process.Pid)+"/fd/", 1))
				if err != nil {
					t.Fatal(err)
				}
			}
			if path == d.ref.Endpoint.Address {
				return link
			}
		}
	}
	t.Fatal("private native Unix listener inode not found")
	return ""
}

func TestFaultNativeHTTPConnectionAdmissionAndCancellation(t *testing.T) {
	d := start(t, privateDir(t), true, "")
	initial := nativeRead(t, d)
	d.transport.Close()
	// Wait for the initial pooled connection to close before counting sockets.
	time.Sleep(30 * time.Millisecond)
	baseSockets := socketFDs(t, d.cmd.Process.Pid)
	base := processResources(t, d.cmd.Process.Pid)
	var slow []net.Conn
	t.Cleanup(func() {
		for _, connection := range slow {
			_ = connection.Close()
		}
	})
	for i := 0; i < 4; i++ {
		connection, err := net.DialTimeout("unix", d.ref.Endpoint.Address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		slow = append(slow, connection)
		if _, err = connection.Write([]byte("POST /v1/snapshot HTTP/1.1\r\nHost: unix\r\n")); err != nil {
			t.Fatal(err)
		}
	}
	limit := time.Now().Add(time.Second)
	for socketFDs(t, d.cmd.Process.Pid) != baseSockets+4 {
		if time.Now().After(limit) {
			t.Fatal("four slow header connections were not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	fifth, err := net.DialTimeout("unix", d.ref.Endpoint.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer fifth.Close()
	_ = fifth.SetDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err = fifth.Write([]byte("GET / HTTP/1.1\r\nHost: unix\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	var byteBuffer [1]byte
	_, err = fifth.Read(byteBuffer[:])
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("fifth connection bypassed finite native admission: %v", err)
	}
	if got := socketFDs(t, d.cmd.Process.Pid); got != baseSockets+4 {
		t.Fatalf("native host exceeded four accepted sockets: sockets=%d base=%d", got, baseSockets)
	}
	during := processResources(t, d.cmd.Process.Pid)
	_ = fifth.Close()
	for _, connection := range slow {
		_ = connection.Close()
	}
	limit = time.Now().Add(2 * time.Second)
	for socketFDs(t, d.cmd.Process.Pid) != baseSockets {
		if time.Now().After(limit) {
			t.Fatal("cancelled slow connections leaked accepted sockets")
		}
		time.Sleep(time.Millisecond)
	}
	after := processResources(t, d.cmd.Process.Pid)
	requestBytes, responseBytes := registry.TransportBounds(manifest())
	d.transport, err = httpx.New(httpx.Config{LocalTargetID: d.ref.TargetID, Service: d.ref,
		MaxConnections: 4, MaxInFlight: 16, MaxRequestBytes: int64(requestBytes), MaxResponseBytes: int64(responseBytes)})
	if err != nil {
		t.Fatal(err)
	}
	d.client, err = client.New(client.HTTPCaller{Transport: d.transport, Grant: grant}, d.ref)
	if err != nil {
		t.Fatal(err)
	}
	if nativeRead(t, d).Token != initial.Token {
		t.Fatal("slow-header cancellation changed business revision")
	}
	req := request(initial.Token, "after-admission-cancel")
	commit, err := d.client.Batch(deadline(t), req)
	if err != nil || commit.Token.Revision != "1" {
		t.Fatalf("native admission did not recover a real commit: %+v %v", commit, err)
	}
	verifyBusiness(t, nativeRead(t, d), req, commit)
	if during.FDs > 128 || during.Threads > 32 || during.RSSKiB > 192*1024 || after.FDs > base.FDs+1 {
		t.Fatalf("bounded admission resources leaked: base=%+v during=%+v after=%+v", base, during, after)
	}
	evidence(t, map[string]any{"profile": "http-unix", "host_connection_limit": 4,
		"accepted_slow_header_connections": 4, "fifth_accept_waited": true, "cancelled_connections_released": true,
		"resource_baseline": base, "resource_during_admission": during, "resource_after_cancel": after,
		"new_business_commit_after_cancel": true, "host_inflight_and_grpc_caps_tested": false})
}
