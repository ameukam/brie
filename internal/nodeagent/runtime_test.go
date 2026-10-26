// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type observationRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	healthy bool
	fail    string
	entered chan struct{}
}

func (s *observationRuntime) Version(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	if s.entered != nil {
		close(s.entered)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if s.fail == "version" {
		return nil, status.Error(codes.Unavailable, "version unavailable")
	}
	return &runtimeapi.VersionResponse{
		Version: "0.1.0", RuntimeName: "containerd", RuntimeVersion: "test", RuntimeApiVersion: "v1",
	}, nil
}

func (s *observationRuntime) Status(_ context.Context, request *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	if s.fail == "status" {
		return nil, status.Error(codes.Unavailable, "status unavailable")
	}
	if request.Verbose {
		return nil, status.Error(codes.InvalidArgument, "unexpected verbose request")
	}
	return &runtimeapi.StatusResponse{Status: &runtimeapi.RuntimeStatus{
		Conditions: []*runtimeapi.RuntimeCondition{
			{Type: runtimeapi.RuntimeReady, Status: s.healthy, Reason: "test", Message: "line\nbreak"},
			{Type: runtimeapi.NetworkReady, Status: s.healthy},
		},
	}}, nil
}

func observationSocket(t *testing.T, runtime *observationRuntime) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, runtime)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	return socket
}

func TestObserveRuntime(t *testing.T) {
	t.Parallel()
	for _, healthy := range []bool{false, true} {
		socket := observationSocket(t, &observationRuntime{healthy: healthy})
		var output bytes.Buffer
		if err := observeRuntime(t.Context(), socket, &output); err != nil {
			t.Fatal(err)
		}
		wantStatus := "status=false"
		if healthy {
			wantStatus = "status=true"
		}
		for _, want := range []string{`runtime="containerd"`, `api="v1"`, `"RuntimeReady"`,
			`"NetworkReady"`, wantStatus, `message="line\nbreak"`} {
			if !strings.Contains(output.String(), want) {
				t.Fatalf("missing %q in %q", want, output.String())
			}
		}
		if strings.Count(output.String(), "\n") != 3 {
			t.Fatalf("unescaped runtime output: %q", output.String())
		}
	}
}

type observationWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *observationWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if strings.Contains(string(p), "workload execution") {
		w.cancel()
	}
	return n, err
}

func TestRuntimeObservationIntegration(t *testing.T) {
	t.Parallel()
	for _, registration := range []bool{false, true} {
		for _, healthy := range []bool{false, true} {
			socket := observationSocket(t, &observationRuntime{healthy: healthy})
			config := Config{NodeName: "worker-1", RuntimeSocket: socket}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			output := &observationWriter{cancel: cancel}
			client := fake.NewClientset(testNode())
			var err error
			if registration {
				err = register(ctx, config, client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), output)
			} else {
				err = Run(ctx, config, output)
			}
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), "startup observation only") {
				t.Fatalf("CRI was skipped: %q", output.String())
			}
			if registration {
				node, err := client.CoreV1().Nodes().Get(t.Context(), "worker-1", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !node.Spec.Unschedulable || len(node.Spec.Taints) != 1 ||
					node.Status.Conditions[0].Status != corev1.ConditionFalse {
					t.Fatalf("runtime observation removed scheduling guards: %+v", node)
				}
				for _, resources := range []corev1.ResourceList{node.Status.Capacity, node.Status.Allocatable} {
					for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourcePods} {
						value, ok := resources[name]
						if !ok || !value.IsZero() {
							t.Fatalf("resource %s = %v, want zero", name, value)
						}
					}
				}
			}
		}
	}
}

func TestRuntimeObservationFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"version", "status", "invalid socket", "missing socket"} {
		t.Run(failure, func(t *testing.T) {
			socket := observationSocket(t, &observationRuntime{fail: failure})
			if failure == "invalid socket" {
				socket = "relative.sock"
			}
			if failure == "missing socket" {
				socket = filepath.Join(t.TempDir(), "missing")
			}
			for _, registration := range []bool{false, true} {
				var output bytes.Buffer
				client := fake.NewClientset()
				config := Config{NodeName: "worker-1", RuntimeSocket: socket}
				var err error
				if registration {
					err = register(t.Context(), config, client.CoreV1().Nodes(),
						client.CoordinationV1().Leases(leaseNamespace), &output)
				} else {
					err = Run(t.Context(), config, &output)
				}
				if err == nil || output.Len() != 0 || len(client.Actions()) != 0 {
					t.Fatalf("startup continued: err=%v output=%q actions=%v", err, output.String(), client.Actions())
				}
			}
		})
	}
}

func TestRuntimeObservationShutdown(t *testing.T) {
	t.Parallel()
	for _, registration := range []bool{false, true} {
		entered := make(chan struct{})
		socket := observationSocket(t, &observationRuntime{entered: entered})
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			config := Config{NodeName: "worker-1", RuntimeSocket: socket}
			if registration {
				client := fake.NewClientset()
				done <- register(ctx, config, client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), io.Discard)
			} else {
				done <- Run(ctx, config, io.Discard)
			}
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("CRI call did not start")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("cancellation during observation: %v", err)
		}
	}
}

func TestRuntimeObservationDeadline(t *testing.T) {
	t.Parallel()
	for _, registration := range []bool{false, true} {
		socket := observationSocket(t, &observationRuntime{entered: make(chan struct{})})
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		config := Config{NodeName: "worker-1", RuntimeSocket: socket}
		var err error
		if registration {
			client := fake.NewClientset()
			err = register(ctx, config, client.CoreV1().Nodes(), client.CoordinationV1().Leases(leaseNamespace), io.Discard)
		} else {
			err = Run(ctx, config, io.Discard)
		}
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline during observation: %v", err)
		}
	}
}

type conditionWriter struct{ calls int }

func (w *conditionWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls > 1 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func TestRuntimeObservationWriteError(t *testing.T) {
	t.Parallel()
	socket := observationSocket(t, &observationRuntime{})
	for _, writer := range []io.Writer{failingWriter{err: io.ErrClosedPipe}, &conditionWriter{}} {
		if err := observeRuntime(t.Context(), socket, writer); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("write error = %v", err)
		}
	}
}
