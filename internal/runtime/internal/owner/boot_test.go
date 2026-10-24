// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

type apiRequest struct {
	method string
	path   string
	body   map[string]any
}

func bootClient(t *testing.T, handler http.Handler) *firecracker.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "brie-owner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	listener, err := net.Listen("unix", filepath.Join(dir, "api.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	if err := server.Listener.Close(); err != nil {
		t.Fatal(err)
	}
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	client, err := firecracker.New(listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func startOwner(t *testing.T) (*Owner, *fakeProcess) {
	t.Helper()
	process := newProcess(t)
	o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
		return process, nil
	}))
	if err := o.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return o, process
}

func validBootConfig() BootConfig {
	return BootConfig{
		Machine: firecracker.MachineConfig{VCPUCount: 2, MemoryMiB: 1024},
		BootSource: firecracker.BootSource{
			KernelImagePath: "/var/lib/brie/kernel", BootArgs: "console=ttyS0 root=/dev/vda ro",
		},
		RootDrive: firecracker.RootDrive{PathOnHost: "/var/lib/brie/rootfs.ext4", ReadOnly: true},
	}
}

func expectedBootRequests() []apiRequest {
	return []apiRequest{
		{method: http.MethodPut, path: "/machine-config", body: map[string]any{"vcpu_count": float64(2), "mem_size_mib": float64(1024)}},
		{method: http.MethodPut, path: "/boot-source", body: map[string]any{"kernel_image_path": "/var/lib/brie/kernel", "boot_args": "console=ttyS0 root=/dev/vda ro"}},
		{method: http.MethodPut, path: "/drives/rootfs", body: map[string]any{
			"drive_id": "rootfs", "path_on_host": "/var/lib/brie/rootfs.ext4", "is_root_device": true,
			"is_read_only": true, "cache_type": "Writeback", "io_engine": "Sync",
		}},
		{method: http.MethodPut, path: "/actions", body: map[string]any{"action_type": "InstanceStart"}},
	}
}

func TestBootOrdersConfigurationBeforeStart(t *testing.T) {
	var mu sync.Mutex
	var calls []apiRequest
	client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		var body map[string]any
		if err := json.Unmarshal(data, &body); err != nil {
			t.Error(err)
		}
		mu.Lock()
		calls = append(calls, apiRequest{method: r.Method, path: r.URL.Path, body: body})
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	o, process := startOwner(t)
	if err := o.Boot(t.Context(), client, validBootConfig()); err != nil {
		t.Fatal(err)
	}
	if got := o.BootStatus(); got.State != BootStartAcknowledged || got.Err != nil {
		t.Fatalf("BootStatus() = %+v", got)
	}
	assertJournalState(t, o.journal.path, Running, BootStartAcknowledged)
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(calls, expectedBootRequests()) {
		t.Fatalf("Firecracker requests = %#v", calls)
	}
	if o.Status().State != Running || process.stopCalls.Load() != 0 {
		t.Fatalf("API acknowledgment changed process state: owner=%+v", o.Status())
	}
}

func TestBootFailuresAreNotRetried(t *testing.T) {
	for _, failurePath := range []string{
		"/machine-config", "/boot-source", "/drives/rootfs", "/actions",
	} {
		t.Run(failurePath, func(t *testing.T) {
			var mu sync.Mutex
			var calls []apiRequest
			client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				var body map[string]any
				if err := json.Unmarshal(data, &body); err != nil {
					t.Error(err)
				}
				mu.Lock()
				calls = append(calls, apiRequest{method: r.Method, path: r.URL.Path, body: body})
				mu.Unlock()
				if r.URL.Path == failurePath {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = fmt.Fprint(w, `{"fault_message":"uncertain mutation"}`)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			o, _ := startOwner(t)
			err := o.Boot(t.Context(), client, validBootConfig())
			if err == nil || !strings.Contains(err.Error(), "uncertain mutation") {
				t.Fatalf("Boot() = %v, want API error", err)
			}
			if status := o.BootStatus(); status.State != BootUnknown || status.Err == nil {
				t.Fatalf("BootStatus() = %+v, want unknown", status)
			}
			assertJournalState(t, o.journal.path, Running, BootUnknown)
			mu.Lock()
			before := len(calls)
			got := append([]apiRequest(nil), calls...)
			mu.Unlock()
			if err := o.Boot(t.Context(), client, validBootConfig()); err == nil {
				t.Fatal("uncertain boot was retried")
			}
			expected := expectedBootRequests()[:before]
			if !reflect.DeepEqual(got, expected) {
				t.Fatalf("requests before failure = %#v; want %#v", got, expected)
			}
			mu.Lock()
			after := len(calls)
			mu.Unlock()
			if after != before {
				t.Fatalf("retry sent more requests: before=%d after=%d", before, after)
			}
		})
	}
}

func TestBootLostReplyIsNotRetried(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/drives/rootfs" {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	o, _ := startOwner(t)
	if err := o.Boot(t.Context(), client, validBootConfig()); err == nil {
		t.Fatal("lost mutation response returned success")
	}
	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if want := []string{"/machine-config", "/boot-source", "/drives/rootfs"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("requests = %v; want %v", got, want)
	}
	if o.BootStatus().State != BootUnknown {
		t.Fatalf("lost reply state = %+v", o.BootStatus())
	}
	assertJournalState(t, o.journal.path, Running, BootUnknown)
}

func TestBootValidatesEntireProfileBeforeMutation(t *testing.T) {
	var calls atomic.Int32
	client := bootClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	o, _ := startOwner(t)
	for _, tt := range []struct {
		name   string
		change func(*BootConfig)
	}{
		{name: "machine", change: func(c *BootConfig) { c.Machine.VCPUCount = 0 }},
		{name: "kernel path", change: func(c *BootConfig) { c.BootSource.KernelImagePath = "relative" }},
		{name: "boot args", change: func(c *BootConfig) { c.BootSource.BootArgs = "bad\x00args" }},
		{name: "root drive", change: func(c *BootConfig) { c.RootDrive.PathOnHost = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := validBootConfig()
			tt.change(&config)
			if err := o.Boot(t.Context(), client, config); err == nil {
				t.Fatal("invalid boot profile accepted")
			}
			if o.BootStatus().State != BootUnconfigured {
				t.Fatalf("validation changed state: %+v", o.BootStatus())
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid profiles sent %d API requests", calls.Load())
	}
}

func TestBootRequiresRunningOwnerAndClient(t *testing.T) {
	idle := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
		return newProcess(t), nil
	}))
	client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := idle.Boot(t.Context(), client, validBootConfig()); err == nil {
		t.Fatal("idle owner booted")
	}
	running, _ := startOwner(t)
	if err := running.Boot(t.Context(), nil, validBootConfig()); err == nil {
		t.Fatal("nil client accepted")
	}
	if err := running.Boot(t.Context(), client, validBootConfig()); err != nil {
		t.Fatal(err)
	}
}

func TestBootConflictDoesNotSendAnotherSequence(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	o, _ := startOwner(t)
	var results sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		results.Add(1)
		go func() {
			defer results.Done()
			errs <- o.Boot(t.Context(), client, validBootConfig())
		}()
	}
	results.Wait()
	close(errs)
	successes, failures := 0, 0
	for err := range errs {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes=%d failures=%d", successes, failures)
	}
	mu.Lock()
	got := append([]string(nil), paths...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"/machine-config", "/boot-source", "/drives/rootfs", "/actions"}) {
		t.Fatalf("concurrent requests interleaved or repeated: %v", got)
	}
}

func TestProcessExitDuringBootPreventsLaterMutations(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	process := newProcess(t)
	var o *Owner
	client := bootClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls = append(calls, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		if r.URL.Path == "/machine-config" {
			process.exit(errors.New("VMM exited"))
			deadline := time.Now().Add(time.Second)
			for o.Status().State != Exited && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}
	}))
	o = newOwner(t, launchFunc(func(context.Context, Config) (Process, error) { return process, nil }))
	if err := o.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := o.Boot(t.Context(), client, validBootConfig()); err == nil {
		t.Fatal("boot continued after owned VMM exited")
	}
	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if !reflect.DeepEqual(got, []string{"/machine-config"}) {
		t.Fatalf("requests after VMM exit: %v", got)
	}
	if o.BootStatus().State != BootUnknown {
		t.Fatalf("status = %+v", o.BootStatus())
	}
	assertJournalState(t, o.journal.path, Exited, BootUnknown)
}
