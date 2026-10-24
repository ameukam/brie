// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

func launcherFixture(t *testing.T, mode string) *FirecrackerLauncher {
	t.Helper()
	// Keep the runtime socket below sockaddr_un's path limit.
	dir, err := os.MkdirTemp("", "brie-launch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "fake-firecracker")
	script := fmt.Sprintf("#!/bin/sh\nexec '%s' -test.run=^TestFirecrackerChild$ -- --mode=%s \"$@\"\n",
		strings.ReplaceAll(executable, "'", "'\\''"), mode)
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	launcher, err := NewFirecrackerLauncher(binary, filepath.Join(dir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	return launcher
}

func TestFirecrackerChild(t *testing.T) {
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator == -1 {
		return
	}
	flags := flag.NewFlagSet("fake-firecracker", flag.ContinueOnError)
	mode := flags.String("mode", "", "")
	socket := flags.String("api-sock", "", "")
	id := flags.String("id", "", "")
	if err := flags.Parse(os.Args[separator+1:]); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("BRIE_TEST_PARENT_ONLY") != "" {
		t.Fatal("inherited parent environment")
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(*socket), "child.pid"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	if *mode == "exit" {
		t.Fatal("injected startup failure")
	}
	if *mode == "no-socket" {
		time.Sleep(time.Minute)
		return
	}
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/" {
				identity := *id
				state := "Not started"
				switch *mode {
				case "wrong-id":
					identity = "another-execution"
				case "running":
					state = "Running"
				}
				if err := json.NewEncoder(w).Encode(firecracker.InstanceInfo{
					AppName: "Firecracker", ID: identity, State: state, VMMVersion: "test",
				}); err != nil {
					t.Error(err)
				}
				return
			}
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	if err := server.Serve(listener); err != nil {
		t.Fatal(err)
	}
}

func TestFirecrackerLauncherLifecycle(t *testing.T) {
	t.Setenv("BRIE_TEST_PARENT_ONLY", "must-not-be-inherited")
	launcher := launcherFixture(t, "normal")
	config := testConfig()
	config.OperationTimeout = 5 * time.Second
	config.JournalDir = filepath.Join(t.TempDir(), "journal")
	o, err := New(config, launcher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := o.Stop(ctx); err != nil {
			t.Error(err)
		}
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	if err := o.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	client, err := firecracker.New(launcher.SocketPath(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Info(t.Context()); err != nil {
		t.Fatalf("canceling start context terminated the VMM: %v", err)
	}
	if err := o.Boot(t.Context(), client, validBootConfig()); err != nil {
		t.Fatal(err)
	}
	assertJournalState(t, o.journal.path, Running, BootStartAcknowledged)
	data, err := os.ReadFile(o.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.ProcessPID <= 0 || record.ProcessPID != readChildPID(t, launcher.dir) {
		t.Fatalf("journal did not record the owned PID: %+v", record)
	}
	if err := o.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertChildReaped(t, record.ProcessPID)
	if err := o.Start(t.Context()); err == nil {
		t.Fatal("exited owner restarted the VMM")
	}
	for _, path := range []string{launcher.dir, filepath.Join(launcher.dir, "vmm.log")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("runtime resource is not private: %s", path)
		}
	}
}

func TestFirecrackerLauncherStartupFailuresReapChild(t *testing.T) {
	for _, mode := range []string{"exit", "no-socket", "wrong-id", "running"} {
		t.Run(mode, func(t *testing.T) {
			launcher := launcherFixture(t, mode)
			config := testConfig()
			config.OperationTimeout = 5 * time.Second
			ctx := t.Context()
			if mode == "no-socket" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
			}
			process, err := launcher.Start(ctx, config)
			if err == nil || process != nil {
				t.Fatalf("Start() = %v, %v; want no process and startup error", process, err)
			}
			if mode == "no-socket" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Start() = %v; want deadline exceeded", err)
			}
			assertChildReaped(t, readChildPID(t, launcher.dir))
			if _, err := launcher.Start(t.Context(), config); err == nil {
				t.Fatal("reused an interrupted runtime directory")
			}
		})
	}
}

func TestFirecrackerLauncherValidation(t *testing.T) {
	valid := launcherFixture(t, "normal")
	link := filepath.Join(filepath.Dir(valid.binary), "symlink")
	if err := os.Symlink(valid.binary, link); err != nil {
		t.Fatal(err)
	}
	notExecutable := filepath.Join(filepath.Dir(valid.binary), "not-executable")
	if err := os.WriteFile(notExecutable, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		binary string
		dir    string
	}{
		{name: "relative binary", binary: "firecracker", dir: valid.dir},
		{name: "missing binary", binary: filepath.Join(t.TempDir(), "missing"), dir: valid.dir},
		{name: "directory binary", binary: t.TempDir(), dir: valid.dir},
		{name: "symlink binary", binary: link, dir: valid.dir},
		{name: "not executable", binary: notExecutable, dir: valid.dir},
		{name: "NUL binary", binary: "/bad\x00path", dir: valid.dir},
		{name: "relative runtime", binary: valid.binary, dir: "relative"},
		{name: "NUL runtime", binary: valid.binary, dir: "/bad\x00path"},
		{name: "long socket", binary: valid.binary, dir: "/" + strings.Repeat("a", 108)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewFirecrackerLauncher(tt.binary, tt.dir); err == nil {
				t.Fatal("invalid launcher configuration accepted")
			}
		})
	}
}

func TestFirecrackerProcessStop(t *testing.T) {
	launcher := launcherFixture(t, "normal")
	config := testConfig()
	config.OperationTimeout = 5 * time.Second
	process, err := launcher.Start(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Stop(context.Background()); err != nil {
			t.Error(err)
		}
		if err := process.Wait(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := process.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Stop() = %v", err)
	}
	client, err := firecracker.New(launcher.SocketPath(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Info(t.Context()); err != nil {
		t.Fatalf("canceled stop killed the process: %v", err)
	}
	if err := process.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := process.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestFirecrackerProcessUnexpectedExit(t *testing.T) {
	launcher := launcherFixture(t, "normal")
	config := testConfig()
	config.OperationTimeout = 5 * time.Second
	process, err := launcher.Start(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	child := process.(*firecrackerProcess)
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err == nil {
		t.Fatal("unexpected kill was reported as clean termination")
	}
	if err := process.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() after exit = %v", err)
	}
	assertChildReaped(t, child.PID())
}

func TestFirecrackerLauncherDoesNotReuseDirectory(t *testing.T) {
	launcher := launcherFixture(t, "normal")
	if err := os.Mkdir(launcher.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Start(t.Context(), testConfig()); err == nil {
		t.Fatal("reused existing runtime directory")
	}
	if _, err := os.Stat(filepath.Join(launcher.dir, "child.pid")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing directory was mutated: %v", err)
	}
}

func TestFirecrackerLauncherCanceledBeforeStart(t *testing.T) {
	launcher := launcherFixture(t, "normal")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := launcher.Start(ctx, testConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start() = %v; want canceled", err)
	}
	if _, err := os.Stat(launcher.dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled start created resources: %v", err)
	}
}

func TestFirecrackerLauncherExecFailure(t *testing.T) {
	launcher := launcherFixture(t, "normal")
	if err := os.WriteFile(launcher.binary, []byte("not an executable format"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Start(t.Context(), testConfig()); err == nil {
		t.Fatal("invalid executable returned success")
	}
}

func readChildPID(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func assertChildReaped(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child %d still exists after cleanup: %v", pid, err)
	}
}
