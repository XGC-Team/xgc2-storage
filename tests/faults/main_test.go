//go:build linux

package faults_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestMain builds the production daemon and the administrative CLI from this
// source tree, so that the black-box tests run the same executables that ship.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "storage-fault-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)
	for variable, command := range map[string]string{"FAULT_STORAGE_BIN": "xgc2-storage", "FAULT_STORAGE_ADMIN_BIN": "storage-admin"} {
		if os.Getenv(variable) != "" {
			continue
		}
		binary := filepath.Join(dir, command)
		build := exec.Command("go", "build", "-trimpath", "-o", binary, "../../cmd/"+command)
		build.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building %s: %v\n%s", command, err, out)
			return 1
		}
		os.Setenv(variable, binary)
	}
	return m.Run()
}
