// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package probe

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type fakeExecer struct {
	mu       sync.Mutex
	exitCode int32
	err      error
	calls    int
	command  []string
}

func (f *fakeExecer) ExecSync(_ context.Context, _ string, command []string, _ int64) (*cri.ExecSyncResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.command = command
	if f.err != nil {
		return nil, f.err
	}
	return &cri.ExecSyncResult{ExitCode: f.exitCode}, nil
}

func (f *fakeExecer) set(exitCode int32, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exitCode, f.err = exitCode, err
}

func (f *fakeExecer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func fastManager(execer Execer, options Options) *Manager {
	m := NewManager(execer, options)
	m.second = 5 * time.Millisecond
	return m
}

func probeSpec(handler corev1.ProbeHandler, success, failure int32) *corev1.Probe {
	return &corev1.Probe{ProbeHandler: handler, PeriodSeconds: 1, TimeoutSeconds: 1, SuccessThreshold: success, FailureThreshold: failure}
}

func execHandler() corev1.ProbeHandler {
	return corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/check"}}}
}

func waitStatus(t *testing.T, m *Manager, uid string, condition func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, found := m.Status(uid)
		if found && condition(current) {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %+v (found %v), condition not met", current, found)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestExecReadinessAndLiveness(t *testing.T) {
	t.Parallel()
	execer := &fakeExecer{}
	var notified atomic.Int32
	var observed sync.Map
	m := fastManager(execer, Options{
		Notify: func(string) { notified.Add(1) },
		Observe: func(kind Kind, handler string, result Result) {
			observed.Store(string(kind)+"/"+handler+"/"+string(result), true)
		},
	})
	defer m.Close()
	err := m.Add(Target{
		PodUID: "uid", ContainerID: "c1",
		Readiness: probeSpec(execHandler(), 2, 1), Liveness: probeSpec(execHandler(), 1, 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	current, _ := m.Status("uid")
	if current.Ready || !current.HasReadiness {
		t.Fatalf("initial status = %+v; readiness must start false", current)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
	execer.set(1, nil)
	failed := waitStatus(t, m, "uid", func(s Status) bool { return !s.Ready && s.LivenessFailed })
	if !strings.Contains(failed.Message, "code 1") {
		t.Fatalf("message = %q", failed.Message)
	}
	// Liveness failure latches: Brie never restarts, so the caller fails the Pod.
	execer.set(0, nil)
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
	if current, _ := m.Status("uid"); !current.LivenessFailed {
		t.Fatal("liveness failure was cleared")
	}
	if notified.Load() < 3 {
		t.Fatalf("notifications = %d", notified.Load())
	}
	if _, found := observed.Load("liveness/exec/failure"); !found {
		t.Fatal("liveness failure not observed")
	}
}

func TestProbeErrorsAreNotLivenessEvidence(t *testing.T) {
	t.Parallel()
	execer := &fakeExecer{err: status.Error(codes.Unavailable, "runtime down")}
	m := fastManager(execer, Options{})
	defer m.Close()
	if err := m.Add(Target{PodUID: "uid", ContainerID: "c1", Liveness: probeSpec(execHandler(), 1, 1), Readiness: probeSpec(execHandler(), 1, 1)}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(Status) bool { return execer.callCount() >= 6 })
	current, _ := m.Status("uid")
	if current.LivenessFailed || current.Ready {
		t.Fatalf("status after runtime errors = %+v; want not ready and not liveness-failed", current)
	}
	// A timed-out exec is a failure, as in kubelet.
	execer.set(0, status.Error(codes.DeadlineExceeded, "timeout"))
	waitStatus(t, m, "uid", func(s Status) bool { return s.LivenessFailed && strings.Contains(s.Message, "timed out") })
}

func TestSuspendPreservesEvidence(t *testing.T) {
	t.Parallel()
	execer := &fakeExecer{}
	m := fastManager(execer, Options{})
	defer m.Close()
	if err := m.Add(Target{PodUID: "uid", ContainerID: "c1", Readiness: probeSpec(execHandler(), 1, 1), Liveness: probeSpec(execHandler(), 1, 2)}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
	if err := m.Suspend("uid", "Paused"); err != nil {
		t.Fatal(err)
	}
	current, _ := m.Status("uid")
	if current.Ready || current.Suspended != "Paused" {
		t.Fatalf("suspended status = %+v", current)
	}
	calls := execer.callCount()
	execer.set(1, nil)
	time.Sleep(50 * time.Millisecond)
	if got := execer.callCount(); got > calls+2 {
		t.Fatalf("probes kept running while suspended: %d -> %d", calls, got)
	}
	if current, _ := m.Status("uid"); current.LivenessFailed || current.Ready {
		t.Fatalf("suspended status changed: %+v", current)
	}
	if err := m.Resume("uid"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.LivenessFailed && s.Suspended == "" })
	for _, err := range []error{m.Suspend("absent", "Paused"), m.Resume("absent"), m.Suspend("uid", "")} {
		if err == nil {
			t.Fatal("invalid suspension accepted")
		}
	}
}

func serverPort(t *testing.T, server *httptest.Server) (string, int32) {
	t.Helper()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Hostname(), int32(port)
}

func TestHTTPGetProbe(t *testing.T) {
	t.Parallel()
	var code atomic.Int32
	code.Store(http.StatusOK)
	var sawHeader atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader.Store(r.Header.Get("X-Check") == "1" && r.URL.Path == "/ready" && r.Header.Get("User-Agent") == "kube-probe/brie")
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://192.0.2.1/elsewhere", http.StatusFound)
			return
		}
		w.WriteHeader(int(code.Load()))
	}))
	defer server.Close()
	host, port := serverPort(t, server)
	m := fastManager(nil, Options{})
	defer m.Close()
	readiness := probeSpec(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
		Path: "/ready", Port: intstr.FromString("http"), HTTPHeaders: []corev1.HTTPHeader{{Name: "X-Check", Value: "1"}},
	}}, 1, 1)
	err := m.Add(Target{
		PodUID: "uid", PodIP: host, ContainerID: "c1", Readiness: readiness,
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: port}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
	if !sawHeader.Load() {
		t.Fatal("probe request did not carry path, header, and user agent")
	}
	code.Store(http.StatusServiceUnavailable)
	notReady := waitStatus(t, m, "uid", func(s Status) bool { return !s.Ready })
	if !strings.Contains(notReady.Message, "HTTP 503") {
		t.Fatalf("message = %q", notReady.Message)
	}
	// A redirect to another host is not followed and counts as success.
	redirect := probeSpec(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/redirect", Port: intstr.FromInt32(port)}}, 1, 1)
	if err := m.Add(Target{PodUID: "uid", PodIP: host, ContainerID: "c1", Readiness: redirect}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
}

func TestTCPSocketProbe(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := int32(listener.Addr().(*net.TCPAddr).Port)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	m := fastManager(nil, Options{})
	defer m.Close()
	liveness := probeSpec(corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)}}, 1, 2)
	readiness := probeSpec(corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)}}, 1, 1)
	if err := m.Add(Target{PodUID: "uid", PodIP: "127.0.0.1", ContainerID: "c1", Liveness: liveness, Readiness: readiness}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.Ready })
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m, "uid", func(s Status) bool { return s.LivenessFailed && !s.Ready })
}

func TestInitialDelay(t *testing.T) {
	t.Parallel()
	execer := &fakeExecer{}
	m := fastManager(execer, Options{})
	defer m.Close()
	spec := probeSpec(execHandler(), 1, 1)
	spec.InitialDelaySeconds = 3600
	if err := m.Add(Target{PodUID: "uid", ContainerID: "c1", Readiness: spec}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if execer.callCount() != 0 {
		t.Fatal("probe ran before initialDelaySeconds")
	}
	if current, _ := m.Status("uid"); current.Ready {
		t.Fatal("ready before any probe")
	}
}

func TestAddValidation(t *testing.T) {
	t.Parallel()
	m := fastManager(&fakeExecer{}, Options{MaxPods: 1})
	defer m.Close()
	tcp := probeSpec(corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(80)}}, 1, 1)
	for _, tt := range []struct {
		name   string
		target Target
		want   string
	}{
		{"missing uid", Target{ContainerID: "c"}, "Pod UID"},
		{"missing container", Target{PodUID: "u"}, "container ID"},
		{"missing ip", Target{PodUID: "u", ContainerID: "c", Readiness: tcp}, "Pod IP"},
	} {
		if err := m.Add(tt.target); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
	if err := m.Add(Target{PodUID: "a", ContainerID: "c", Readiness: probeSpec(execHandler(), 1, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Add(Target{PodUID: "b", ContainerID: "c", Readiness: probeSpec(execHandler(), 1, 1)}); err == nil {
		t.Fatal("MaxPods not enforced")
	}
	// A target without probes removes the Pod.
	if err := m.Add(Target{PodUID: "a", ContainerID: "c"}); err != nil {
		t.Fatal(err)
	}
	if _, found := m.Status("a"); found {
		t.Fatal("probe-less target still tracked")
	}
	m.Close()
	if err := m.Add(Target{PodUID: "a", ContainerID: "c", Readiness: probeSpec(execHandler(), 1, 1)}); err == nil {
		t.Fatal("closed manager accepted a target")
	}
}

func TestProbeOnceEdges(t *testing.T) {
	t.Parallel()
	m := NewManager(nil, Options{MaxConcurrent: 1})
	ctx := context.Background()
	target := Target{PodIP: "127.0.0.1", Ports: []corev1.ContainerPort{{Name: "web", ContainerPort: 1}}}
	for _, tt := range []struct {
		name string
		spec *corev1.Probe
		want Result
		msg  string
	}{
		{"no execer", probeSpec(execHandler(), 1, 1), Error, "not configured"},
		{"no handler", probeSpec(corev1.ProbeHandler{}, 1, 1), Error, "unsupported"},
		{"undeclared port", probeSpec(corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("x")}}, 1, 1), Error, "undeclared"},
		{"range", probeSpec(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(0)}}, 1, 1), Error, "range"},
		{"refused", probeSpec(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("web"), Scheme: corev1.URISchemeHTTPS}}, 1, 1), Failure, "request failed"},
		{"bad path", probeSpec(corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(1), Path: "/%zz"}}, 1, 1), Error, "invalid"},
	} {
		result, message := m.probeOnce(ctx, target, tt.spec)
		if result != tt.want || !strings.Contains(message, tt.msg) {
			t.Errorf("%s: %s %q, want %s %q", tt.name, result, message, tt.want, tt.msg)
		}
	}
	if handlerName(&corev1.Probe{}) != "other" {
		t.Fatal("handler name")
	}
	m.slots <- struct{}{}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if result, _ := m.probeOnce(cancelled, target, probeSpec(execHandler(), 1, 1)); result != Error {
		t.Fatal("cancelled probe did not report an error")
	}
	m.execer = &fakeExecer{err: errors.New("boom")}
	<-m.slots
	if result, msg := m.probeOnce(ctx, target, probeSpec(execHandler(), 1, 1)); result != Error || !strings.Contains(msg, "could not run") {
		t.Fatalf("exec error = %s %q", result, msg)
	}
}
