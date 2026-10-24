// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package firecracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const validInfo = `{"app_name":"Firecracker","id":"test-vm","state":"Not started","vmm_version":"1.17.0"}`

func socketServer(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	// Short paths avoid sockaddr_un limits even for long subtest names.
	dir, err := os.MkdirTemp("", "brie-fc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(dir); err != nil {
			t.Error(err)
		}
	})
	path := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", path)
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
	return path
}

func testClient(t *testing.T, path string, timeout time.Duration) *Client {
	t.Helper()
	client, err := New(path, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestNew(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		path    string
		timeout time.Duration
	}{
		{name: "empty path", timeout: time.Second},
		{name: "relative path", path: "api.sock", timeout: time.Second},
		{name: "NUL path", path: "/tmp/api\x00.sock", timeout: time.Second},
		{name: "zero timeout", path: "/tmp/api.sock"},
		{name: "negative timeout", path: "/tmp/api.sock", timeout: -time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if client, err := New(tt.path, tt.timeout); err == nil || client != nil {
				t.Fatalf("New() = %v, %v; want nil client and error", client, err)
			}
		})
	}
}

func TestInfo(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body string
		pass bool
	}{
		{name: "valid", body: validInfo, pass: true},
		{name: "unknown field", body: strings.TrimSuffix(validInfo, "}") + `,"future":true}`, pass: true},
		{name: "exact response limit", body: validInfo + strings.Repeat(" ", maxResponseBytes-len(validInfo)), pass: true},
		{name: "oversized response", body: validInfo + strings.Repeat(" ", maxResponseBytes+1-len(validInfo))},
		{name: "malformed", body: "{"},
		{name: "empty"},
		{name: "null", body: "null"},
		{name: "missing metadata", body: `{"state":"Running"}`},
		{name: "missing state", body: strings.ReplaceAll(validInfo, `"state":"Not started",`, "")},
		{name: "unknown state", body: strings.ReplaceAll(validInfo, "Not started", "Unknown")},
		{name: "wrong field type", body: strings.ReplaceAll(validInfo, `"test-vm"`, "123")},
		{name: "trailing JSON", body: validInfo + `{}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := socketServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/" || r.Header.Get("Accept") != "application/json" {
					t.Errorf("unexpected request: %s %s headers=%v", r.Method, r.URL, r.Header)
				}
				if _, err := fmt.Fprint(w, tt.body); err != nil {
					t.Error(err)
				}
			})
			info, err := testClient(t, path, time.Second).Info(t.Context())
			if (err == nil) != tt.pass {
				t.Fatalf("Info() = %+v, %v; want pass=%t", info, err, tt.pass)
			}
			if tt.pass && info != (InstanceInfo{
				AppName: "Firecracker", ID: "test-vm", State: "Not started", VMMVersion: "1.17.0",
			}) {
				t.Errorf("unexpected info: %+v", info)
			}
			if !tt.pass && info != (InstanceInfo{}) {
				t.Errorf("failed request returned success-shaped metadata: %+v", info)
			}
		})
	}
}

func TestConfigureMachine(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		config MachineConfig
		pass   bool
	}{
		{name: "minimal", config: MachineConfig{VCPUCount: 1, MemoryMiB: 1}, pass: true},
		{name: "normal", config: MachineConfig{VCPUCount: 2, MemoryMiB: 256}, pass: true},
		{name: "max CPU", config: MachineConfig{VCPUCount: 32, MemoryMiB: 512}, pass: true},
		{name: "odd CPU without SMT", config: MachineConfig{VCPUCount: 3, MemoryMiB: 128}, pass: true},
		{name: "zero CPU", config: MachineConfig{MemoryMiB: 128}},
		{name: "negative CPU", config: MachineConfig{VCPUCount: -1, MemoryMiB: 128}},
		{name: "too many CPUs", config: MachineConfig{VCPUCount: 33, MemoryMiB: 128}},
		{name: "zero memory", config: MachineConfig{VCPUCount: 1}},
		{name: "negative memory", config: MachineConfig{VCPUCount: 1, MemoryMiB: -1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			path := socketServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPut || r.URL.Path != "/machine-config" ||
					r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("unexpected request: %s %s headers=%v", r.Method, r.URL, r.Header)
				}
				var got map[string]int64
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Error(err)
				}
				if len(got) != 2 || got["vcpu_count"] != int64(tt.config.VCPUCount) ||
					got["mem_size_mib"] != tt.config.MemoryMiB {
					t.Errorf("unexpected machine config: %v", got)
				}
				w.WriteHeader(http.StatusNoContent)
			})
			err := testClient(t, path, time.Second).ConfigureMachine(t.Context(), tt.config)
			if (err == nil) != tt.pass {
				t.Fatalf("ConfigureMachine() = %v; want pass=%t", err, tt.pass)
			}
			wantCalls := int32(0)
			if tt.pass {
				wantCalls = 1
			}
			if got := calls.Load(); got != wantCalls {
				t.Errorf("calls = %d; want %d", got, wantCalls)
			}
		})
	}
}

func TestAPIError(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		status int
		body   string
		fault  string
	}{
		{name: "bad request", status: 400, body: `{"fault_message":"already running"}`, fault: "already running"},
		{name: "server error", status: 500, body: `{"fault_message":"internal failure"}`, fault: "internal failure"},
		{name: "malformed fault", status: 400, body: "not JSON"},
		{name: "missing fault", status: 400, body: `{}`},
		{name: "redirect", status: 307, body: `{"fault_message":"redirect refused"}`, fault: "redirect refused"},
		{name: "wrong success status", status: 202, body: `{"fault_message":"not completed"}`, fault: "not completed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			path := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "http://example.invalid/must-not-follow")
				w.WriteHeader(tt.status)
				if _, err := fmt.Fprint(w, tt.body); err != nil {
					t.Error(err)
				}
			})
			client := testClient(t, path, time.Second)
			for _, mutate := range []bool{false, true} {
				var err error
				if mutate {
					err = client.ConfigureMachine(t.Context(), MachineConfig{VCPUCount: 1, MemoryMiB: 128})
				} else {
					_, err = client.Info(t.Context())
				}
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != tt.status || apiErr.FaultMessage != tt.fault {
					t.Fatalf("error = %v; want APIError status=%d fault=%q", err, tt.status, tt.fault)
				}
				if !strings.Contains(err.Error(), fmt.Sprint(tt.status)) {
					t.Errorf("error omits HTTP status: %v", err)
				}
			}
			if calls.Load() != 2 {
				t.Errorf("requests were retried or redirected: %d calls", calls.Load())
			}
		})
	}
}

func TestUnavailableSocket(t *testing.T) {
	t.Parallel()
	client := testClient(t, filepath.Join(t.TempDir(), "missing.sock"), time.Second)
	if _, err := client.Info(t.Context()); err == nil {
		t.Fatal("missing socket returned success")
	} else {
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			t.Fatalf("dial error not preserved: %v", err)
		}
	}
}

func TestRequestTimeout(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			path := socketServer(t, func(w http.ResponseWriter, r *http.Request) {
				if phase == "body" {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			client := testClient(t, path, 100*time.Millisecond)
			if _, err := client.Info(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout error = %v; want DeadlineExceeded", err)
			}
		})
	}
}

func TestCancellationAndQueue(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	var calls atomic.Int32
	path := socketServer(t, func(_ http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-r.Context().Done()
	})
	client := testClient(t, path, 2*time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Info(ctx)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first request did not reach server")
	}
	queued, cancelQueued := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelQueued()
	if _, err := client.Info(queued); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue deadline error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("more than one request reached server: %d", calls.Load())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error = %v; want Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request did not honor cancellation")
	}
}

func TestTruncatedResponse(t *testing.T) {
	t.Parallel()
	path := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		if _, err := fmt.Fprint(w, "{}"); err != nil {
			t.Error(err)
		}
	})
	if _, err := testClient(t, path, time.Second).Info(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "read Firecracker GET / response") {
		t.Fatalf("truncated response error = %v", err)
	}
}

func TestDroppedMutationReply(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	path := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	client := testClient(t, path, time.Second)
	if err := client.ConfigureMachine(t.Context(), MachineConfig{VCPUCount: 1, MemoryMiB: 128}); err == nil {
		t.Fatal("dropped mutation reply returned success")
	}
	if calls.Load() != 1 {
		t.Fatalf("mutation retried after lost reply: %d calls", calls.Load())
	}
}

func TestResponseHeaderLimit(t *testing.T) {
	t.Parallel()
	path := socketServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Oversized", strings.Repeat("x", maxResponseBytes+1))
		w.WriteHeader(http.StatusOK)
	})
	if _, err := testClient(t, path, time.Second).Info(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "response headers exceeded") {
		t.Fatalf("oversized response headers error = %v; want header limit error", err)
	}
}

func TestQueueTimeout(t *testing.T) {
	t.Parallel()
	client := testClient(t, filepath.Join(t.TempDir(), "unused.sock"), 20*time.Millisecond)
	client.slot <- struct{}{}
	defer func() { <-client.slot }()
	if _, err := client.Info(t.Context()); !errors.Is(err, context.DeadlineExceeded) ||
		!strings.Contains(err.Error(), "waiting for request slot") {
		t.Fatalf("queue timeout error = %v; want request timeout before dialing", err)
	}
}

func FuzzDecodeInfo(f *testing.F) {
	for _, seed := range []string{validInfo, "", "null", "{}", "{", validInfo + "{}"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		info, err := decodeInfo([]byte(body))
		if err != nil {
			if info != (InstanceInfo{}) {
				t.Fatalf("invalid response returned metadata: %+v", info)
			}
			return
		}
		if info.AppName == "" || info.ID == "" || info.VMMVersion == "" {
			t.Fatalf("accepted incomplete metadata: %+v", info)
		}
		switch info.State {
		case "Not started", "Running", "Paused":
		default:
			t.Fatalf("accepted unknown state: %q", info.State)
		}
	})
}
