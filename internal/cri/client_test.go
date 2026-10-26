// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fakeRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	version func(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error)
	status  func(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error)
}

func (s *fakeRuntime) Version(ctx context.Context, request *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	return s.version(ctx, request)
}

func (s *fakeRuntime) Status(ctx context.Context, request *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	return s.status(ctx, request)
}

func startRuntime(t *testing.T, fake *fakeRuntime) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, fake)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return socket
}

func newClient(t *testing.T, socket string, timeout time.Duration) *Client {
	t.Helper()
	client, err := New(socket, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}

func goodVersion() *runtimeapi.VersionResponse {
	return &runtimeapi.VersionResponse{
		Version: "0.1.0", RuntimeName: "containerd",
		RuntimeVersion: "2.4.2", RuntimeApiVersion: "v1",
	}
}

func goodStatus() *runtimeapi.RuntimeStatus {
	return &runtimeapi.RuntimeStatus{Conditions: []*runtimeapi.RuntimeCondition{
		{Type: runtimeapi.RuntimeReady, Status: true},
		{Type: runtimeapi.NetworkReady, Status: true},
	}}
}

func TestNew(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		socket  string
		timeout time.Duration
		valid   bool
	}{
		{name: "lazy", socket: "/no-runtime/cri.sock", timeout: time.Second, valid: true},
		{name: "longest", socket: "/" + strings.Repeat("x", 106), timeout: time.Second, valid: true},
		{name: "empty", timeout: time.Second},
		{name: "relative", socket: "cri.sock", timeout: time.Second},
		{name: "uri", socket: "unix:///run/containerd.sock", timeout: time.Second},
		{name: "nul", socket: "/run/a\x00b", timeout: time.Second},
		{name: "utf8", socket: "/run/\xff", timeout: time.Second},
		{name: "too long", socket: "/" + strings.Repeat("x", 107), timeout: time.Second},
		{name: "zero timeout", socket: "/run/cri.sock"},
		{name: "negative timeout", socket: "/run/cri.sock", timeout: -time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, err := New(tt.socket, tt.timeout)
			if (err == nil) != tt.valid {
				t.Fatalf("New() error = %v, want valid=%t", err, tt.valid)
			}
			if client != nil {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"valid", "version", "name", "runtime", "api", "unsupported"} {
		t.Run(field, func(t *testing.T) {
			response := goodVersion()
			switch field {
			case "version":
				response.Version = ""
			case "name":
				response.RuntimeName = ""
			case "runtime":
				response.RuntimeVersion = ""
			case "api":
				response.RuntimeApiVersion = ""
			case "unsupported":
				response.RuntimeApiVersion = "v1alpha2"
			}
			socket := startRuntime(t, &fakeRuntime{
				version: func(_ context.Context, request *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
					if request.GetVersion() != "0.1.0" {
						t.Errorf("requested version = %q", request.GetVersion())
					}
					return response, nil
				},
			})
			got, err := newClient(t, socket, time.Second).Version(t.Context())
			if field != "valid" {
				if err == nil || got != nil {
					t.Fatalf("invalid Version() = %v, %v", got, err)
				}
			} else if err != nil || !proto.Equal(got, response) {
				t.Fatalf("Version() = %v, %v; want %v", got, err, response)
			}
		})
	}
}

func TestStatus(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(*runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus
		valid  bool
	}{
		{name: "healthy", valid: true},
		{name: "unhealthy", valid: true, change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions[0].Status = false
			s.Conditions[0].Reason = "RuntimeNotReady"
			s.Conditions[0].Message = "initializing"
			s.Conditions[1].Status = false
			return s
		}},
		{name: "extension", valid: true, change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions = append(s.Conditions, &runtimeapi.RuntimeCondition{Type: "FutureCondition", Status: true})
			return s
		}},
		{name: "absent", change: func(*runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus { return nil }},
		{name: "empty", change: func(*runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus { return &runtimeapi.RuntimeStatus{} }},
		{name: "network missing", change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions = s.Conditions[:1]
			return s
		}},
		{name: "runtime missing", change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions = s.Conditions[1:]
			return s
		}},
		{name: "duplicate", change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions = append(s.Conditions, s.Conditions[0])
			return s
		}},
		{name: "nil condition", change: func(s *runtimeapi.RuntimeStatus) *runtimeapi.RuntimeStatus {
			s.Conditions = append(s.Conditions, nil)
			return s
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := goodStatus()
			if tt.change != nil {
				response = tt.change(response)
			}
			socket := startRuntime(t, &fakeRuntime{
				status: func(_ context.Context, request *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
					if request.GetVerbose() {
						t.Error("Status requested verbose diagnostics")
					}
					return &runtimeapi.StatusResponse{Status: response}, nil
				},
			})
			got, err := newClient(t, socket, time.Second).Status(t.Context())
			if !tt.valid {
				if err == nil || got != nil {
					t.Fatalf("invalid Status() = %v, %v", got, err)
				}
			} else if err != nil || !proto.Equal(got, response) {
				t.Fatalf("Status() = %v, %v; want %v", got, err, response)
			}
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unavailable, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			var calls atomic.Int32
			failure := status.Error(code, "runtime refused request")
			socket := startRuntime(t, &fakeRuntime{
				version: func(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
					calls.Add(1)
					return nil, failure
				},
				status: func(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
					calls.Add(1)
					return nil, failure
				},
			})
			client := newClient(t, socket, time.Second)
			for _, call := range callsFor(client) {
				if err := call(t.Context()); status.Code(err) != code {
					t.Fatalf("error = %v, want code %s", err, code)
				}
			}
			if calls.Load() != 2 {
				t.Errorf("runtime calls = %d, want 2 without retries", calls.Load())
			}
		})
	}
}

func callsFor(c *Client) []func(context.Context) error {
	return []func(context.Context) error{
		func(ctx context.Context) error { _, err := c.Version(ctx); return err },
		func(ctx context.Context) error { _, err := c.Status(ctx); return err },
	}
}

func TestBounds(t *testing.T) {
	t.Parallel()
	socket := startRuntime(t, &fakeRuntime{
		version: func(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			response := goodVersion()
			response.RuntimeVersion = strings.Repeat("x", maxMessageBytes)
			return response, nil
		},
		status: func(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
			return &runtimeapi.StatusResponse{
				Status: goodStatus(), Info: map[string]string{"oversized": strings.Repeat("x", maxMessageBytes)},
			}, nil
		},
	})
	for _, call := range callsFor(newClient(t, socket, time.Second)) {
		if err := call(t.Context()); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("oversized response error = %v, want ResourceExhausted", err)
		}
	}
}

func TestMessageLimit(t *testing.T) {
	t.Parallel()
	for _, size := range []int{maxMessageBytes - 1, maxMessageBytes, maxMessageBytes + 1} {
		response := goodVersion()
		response.RuntimeVersion = strings.Repeat("x", maxMessageBytes)
		response.RuntimeVersion = strings.Repeat("x", maxMessageBytes+size-proto.Size(response))
		if got := proto.Size(response); got != size {
			t.Fatalf("test response size = %d, want %d", got, size)
		}
		socket := startRuntime(t, &fakeRuntime{
			version: func(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
				return response, nil
			},
		})
		got, err := newClient(t, socket, time.Second).Version(t.Context())
		if size <= maxMessageBytes {
			if err != nil || !proto.Equal(got, response) {
				t.Fatalf("response of %d bytes rejected: %v", size, err)
			}
		} else if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("response of %d bytes: %v, want ResourceExhausted", size, err)
		}
	}
}

func TestDeadlines(t *testing.T) {
	t.Parallel()
	socket := startRuntime(t, &fakeRuntime{
		version: func(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
		status: func(ctx context.Context, _ *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
	})
	for _, tt := range []struct {
		name    string
		timeout time.Duration
		caller  time.Duration
		cancel  bool
	}{
		{name: "client timeout", timeout: 40 * time.Millisecond, caller: 3 * time.Second},
		{name: "caller deadline", timeout: 3 * time.Second, caller: 40 * time.Millisecond},
		{name: "canceled", timeout: time.Second, caller: time.Second, cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, call := range callsFor(newClient(t, socket, tt.timeout)) {
				ctx, cancel := context.WithTimeout(t.Context(), tt.caller)
				want := context.DeadlineExceeded
				if tt.cancel {
					cancel()
					want = context.Canceled
				}
				err := call(ctx)
				cancel()
				if !errors.Is(err, want) {
					t.Fatalf("error = %v, want %v", err, want)
				}
			}
		})
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"Version", "Status"} {
		t.Run(method, func(t *testing.T) {
			entered := make(chan struct{})
			wait := func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				return status.FromContextError(ctx.Err()).Err()
			}
			socket := startRuntime(t, &fakeRuntime{
				version: func(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
					return nil, wait(ctx)
				},
				status: func(ctx context.Context, _ *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
					return nil, wait(ctx)
				},
			})
			client := newClient(t, socket, 3*time.Second)
			call := callsFor(client)[0]
			if method == "Status" {
				call = callsFor(client)[1]
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- call(ctx) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not enter runtime")
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) || status.Code(err) != codes.Canceled {
				t.Fatalf("in-flight cancellation error = %v, want Canceled", err)
			}
		})
	}
}

func TestQueueTimeout(t *testing.T) {
	t.Parallel()
	client := newClient(t, "/no-runtime/cri.sock", 40*time.Millisecond)
	client.slot <- struct{}{}
	defer func() { <-client.slot }()
	for _, call := range callsFor(client) {
		if err := call(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("client timeout while queued = %v, want deadline", err)
		}
	}
}

func TestQueue(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	unblock := make(chan struct{})
	var statusCalls atomic.Int32
	socket := startRuntime(t, &fakeRuntime{
		version: func(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			close(entered)
			select {
			case <-unblock:
				return goodVersion(), nil
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		},
		status: func(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
			statusCalls.Add(1)
			return &runtimeapi.StatusResponse{Status: goodStatus()}, nil
		},
	})
	client := newClient(t, socket, 3*time.Second)
	done := make(chan error, 1)
	go func() { _, err := client.Version(t.Context()); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Version did not enter runtime")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	if _, err := client.Status(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued Status error = %v, want deadline", err)
	}
	if statusCalls.Load() != 0 {
		t.Fatal("queued Status reached runtime while Version was active")
	}
	close(unblock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := client.Status(t.Context()); err != nil {
		t.Fatalf("Status after releasing slot: %v", err)
	}
}

func TestTransport(t *testing.T) {
	t.Parallel()
	for _, serve := range []bool{false, true} {
		name := "missing"
		if serve {
			name = "stalled"
		}
		t.Run(name, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "cri.sock")
			if serve {
				listener, err := net.Listen("unix", socket)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
			}
			client := newClient(t, socket, 40*time.Millisecond)
			for _, call := range callsFor(client) {
				err := call(t.Context())
				if serve {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("stalled transport error = %v, want deadline", err)
					}
				} else if status.Code(err) != codes.Unavailable {
					t.Fatalf("missing transport error = %v, want Unavailable", err)
				}
			}
		})
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	socket := startRuntime(t, &fakeRuntime{
		version: func(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			close(entered)
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		},
	})
	client, err := New(socket, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := client.Version(t.Context()); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		_ = client.Close()
		t.Fatal("Version did not enter runtime")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("in-flight close error = %v, want Canceled", err)
	}
	for _, call := range callsFor(client) {
		if err := call(t.Context()); status.Code(err) != codes.Canceled {
			t.Fatalf("closed client error = %v, want Canceled", err)
		}
	}
}

func FuzzNew(f *testing.F) {
	for _, socket := range []string{"", "relative", "/run/containerd/containerd.sock", "/a\x00", "/a\xff"} {
		f.Add(socket, int64(time.Second))
	}
	f.Fuzz(func(t *testing.T, socket string, timeout int64) {
		client, err := New(socket, time.Duration(timeout))
		if err != nil {
			if client != nil {
				t.Fatal("invalid configuration returned a client")
			}
			return
		}
		if client.timeout <= 0 {
			t.Fatal("accepted nonpositive timeout")
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
