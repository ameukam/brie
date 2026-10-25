// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

func serviceConfig(t *testing.T, mode string) ServiceConfig {
	t.Helper()
	launcher := launcherFixture(t, mode)
	return ServiceConfig{
		Owner:  Config{SandboxUID: "sandbox", PodUID: "pod", OperationTimeout: 3 * time.Second},
		Binary: launcher.binary, Directory: filepath.Join(filepath.Dir(launcher.dir), "service"),
		Boot: BootConfig{
			Machine:    firecracker.MachineConfig{VCPUCount: 1, MemoryMiB: 128},
			BootSource: firecracker.BootSource{KernelImagePath: "/kernel"},
			RootDrive:  firecracker.RootDrive{PathOnHost: "/rootfs", ReadOnly: true},
		},
	}
}

func serviceHTTP(socket string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}
}

func serviceRequest(t *testing.T, client *http.Client, method, path string, identity bool, body string) (int, Observation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://owner"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if identity {
		req.Header.Set("X-Brie-Sandbox-UID", "sandbox")
		req.Header.Set("X-Brie-Pod-UID", "pod")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	var observation Observation
	if err := json.NewDecoder(resp.Body).Decode(&observation); err != nil {
		t.Fatal(err)
	}
	if observation.Version != 1 || observation.PodUID != "pod" || observation.SandboxUID != "sandbox" {
		t.Fatalf("incorrect owner identity: %+v", observation)
	}
	return resp.StatusCode, observation
}

func waitService(t *testing.T, config ServiceConfig) {
	t.Helper()
	client := serviceHTTP(filepath.Join(config.Directory, "control.sock"))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://owner/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err == nil {
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status endpoint: %s", resp.Status)
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("owner service unavailable: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func startService(t *testing.T, config ServiceConfig) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, config, log.New(io.Discard, "", 0)) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("owner service did not stop")
		}
	})
	waitService(t, config)
}

func TestOwnerServiceProtocol(t *testing.T) {
	config := serviceConfig(t, "normal")
	startService(t, config)
	client := serviceHTTP(filepath.Join(config.Directory, "control.sock"))
	for _, tt := range []struct {
		name, method, path, body string
		identity                 bool
		code                     int
	}{
		{name: "unknown path", method: "POST", path: "/v2/start", code: 404},
		{name: "wrong method", method: "GET", path: "/v1/start", code: 405},
		{name: "missing identity", method: "POST", path: "/v1/start", code: 409},
		{name: "body rejected", method: "POST", path: "/v1/start", body: "{}", identity: true, code: 400},
		{name: "query rejected", method: "GET", path: "/v1/status?start=true", code: 400},
		{name: "stop before start", method: "POST", path: "/v1/stop", identity: true, code: 409},
		{name: "boot before start", method: "POST", path: "/v1/boot", identity: true, code: 409},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, result := serviceRequest(t, client, tt.method, tt.path, tt.identity, tt.body)
			if code != tt.code || result.Error == "" || result.Process != Idle {
				t.Fatalf("response: %d %+v", code, result)
			}
		})
	}
	for _, path := range []string{"/v1/start", "/v1/boot", "/v1/stop", "/v1/stop"} {
		code, result := serviceRequest(t, client, "POST", path, true, "")
		if code != http.StatusOK || result.Error != "" {
			t.Fatalf("%s: %d %+v", path, code, result)
		}
	}
	code, result := serviceRequest(t, client, "POST", "/v1/start", true, "")
	if code != http.StatusConflict || result.Process != Exited {
		t.Fatalf("relaunch accepted: %d %+v", code, result)
	}
	for _, name := range []string{"", "journal", "control.sock"} {
		info, err := os.Stat(filepath.Join(config.Directory, name))
		if err != nil || info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("non-private service artifact %s: %v, %v", name, info, err)
		}
	}
}

func TestOwnerServiceValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*ServiceConfig)
	}{
		{name: "relative directory", change: func(c *ServiceConfig) { c.Directory = "relative" }},
		{name: "journal override", change: func(c *ServiceConfig) { c.Owner.JournalDir = "/journal" }},
		{name: "invalid boot", change: func(c *ServiceConfig) { c.Boot.Machine.VCPUCount = 0 }},
		{name: "long socket", change: func(c *ServiceConfig) { c.Directory = "/" + strings.Repeat("a", 110) }},
		{name: "missing executable", change: func(c *ServiceConfig) { c.Binary = "/missing-firecracker" }},
		{name: "missing identity", change: func(c *ServiceConfig) { c.Owner.PodUID = "" }},
		{name: "invalid timeout", change: func(c *ServiceConfig) { c.Owner.OperationTimeout = 0 }},
		{name: "existing directory", change: func(c *ServiceConfig) { c.Directory = filepath.Dir(c.Directory) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := serviceConfig(t, "normal")
			tt.change(&config)
			if err := Serve(t.Context(), config, log.New(io.Discard, "", 0)); err == nil {
				t.Fatal("invalid service accepted")
			}
		})
	}
	config := serviceConfig(t, "normal")
	if err := Serve(t.Context(), config, nil); err == nil {
		t.Fatal("nil logger accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Serve(ctx, config, log.New(io.Discard, "", 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled service: %v", err)
	}
}

func TestOwnerServiceChild(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--owner-service" {
		return
	}
	var config ServiceConfig
	if err := json.Unmarshal([]byte(os.Args[len(os.Args)-1]), &config); err != nil {
		t.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Serve(ctx, config, log.New(os.Stderr, "", 0)); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerServiceSurvivesClients(t *testing.T) {
	config := serviceConfig(t, "normal")
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestOwnerServiceChild$", "--", "--owner-service", string(data))
	output, err := os.CreateTemp(filepath.Dir(config.Directory), "owner-output-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			t.Error(err)
		}
	}()
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	waited := false
	t.Cleanup(func() {
		if waited {
			return
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				data, readErr := os.ReadFile(output.Name())
				t.Errorf("owner child: %v, log=%s, read=%v", err, data, readErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("owner subprocess did not exit")
			if err := cmd.Process.Kill(); err != nil {
				t.Error(err)
			}
			if err := <-done; err != nil {
				t.Logf("killed owner subprocess: %v", err)
			}
		}
	})
	waitService(t, config)
	caller := exec.CommandContext(t.Context(), executable, "-test.run=^TestOwnerClientChild$", "--", "--owner-client", config.Directory)
	if output, err := caller.CombinedOutput(); err != nil {
		t.Fatalf("owner client subprocess: %v\n%s", err, output)
	}

	pidData, err := os.ReadFile(filepath.Join(config.Directory, "vmm", "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidData))
	if err != nil {
		t.Fatal(err)
	}
	client := serviceHTTP(filepath.Join(config.Directory, "control.sock"))
	code, observation := serviceRequest(t, client, "GET", "/v1/status", false, "")
	if code != 200 || observation.Process != Running || observation.Boot != BootStartAcknowledged ||
		syscall.Kill(pid, 0) != nil {
		t.Fatalf("lost VM after client reconnect: %d %+v", code, observation)
	}
	code, observation = serviceRequest(t, client, "POST", "/v1/start", true, "")
	if code != 409 || observation.Process != Running {
		t.Fatalf("duplicate launch: %d %+v", code, observation)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		waited = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("owner subprocess did not shut down")
	}
	if syscall.Kill(pid, 0) == nil {
		t.Fatal("owner shutdown did not reap the VMM")
	}
}

func TestOwnerClientChild(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "--owner-client" {
		return
	}
	client := serviceHTTP(filepath.Join(os.Args[len(os.Args)-1], "control.sock"))
	defer client.CloseIdleConnections()
	for _, path := range []string{"/v1/start", "/v1/boot"} {
		code, observation := serviceRequest(t, client, "POST", path, true, "")
		if code != http.StatusOK || observation.Error != "" {
			t.Fatalf("%s: %d %+v", path, code, observation)
		}
	}

}

func TestOwnerServiceDisconnectedMutation(t *testing.T) {
	config := serviceConfig(t, "normal")
	config.Owner.JournalDir = filepath.Join(filepath.Dir(config.Directory), "journal")
	process := newProcess(t)
	started, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	o, err := New(config.Owner, launchFunc(func(ctx context.Context, _ Config) (Process, error) {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return process, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(filepath.Dir(config.Directory), "control.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveOwner(ctx, listener, o, nil, config, log.New(io.Discard, "", 0)) }()
	t.Cleanup(func() {
		unblock.Do(func() { close(release) })
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("service failed to shut down")
		}
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	conn, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(conn, "POST /v1/start HTTP/1.1\r\nHost: owner\r\nX-Brie-Sandbox-UID: sandbox\r\nX-Brie-Pod-UID: pod\r\n\r\n")
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("mutation was not accepted")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	client := serviceHTTP(socket)
	code, observation := serviceRequest(t, client, "POST", "/v1/start", true, "")
	if code != http.StatusServiceUnavailable || observation.Process != Starting {
		t.Fatalf("concurrent launch accepted: %d %+v", code, observation)
	}
	unblock.Do(func() { close(release) })
	deadline := time.Now().Add(5 * time.Second)
	for o.Status().State == Starting && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	code, observation = serviceRequest(t, client, "GET", "/v1/status", false, "")
	if code != http.StatusOK || observation.Process != Running || observation.Error != "" {
		t.Fatalf("disconnect canceled accepted launch: %d %+v", code, observation)
	}
}
