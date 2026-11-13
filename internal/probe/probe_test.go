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
	if got := execer.callCount(); got != calls {
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

// gatedExecer blocks ExecSync for containers in blocked until release is
// closed, then answers with exit code 0 for a container's first call and
// laterExitCode afterwards. Cancellation ends a blocked call only when
// honorCancel is set.
type gatedExecer struct {
	blocked       map[string]bool
	honorCancel   bool
	laterExitCode int32
	release       chan struct{}
	started       chan string

	mu    sync.Mutex
	calls map[string]int
}

func newGatedExecer(honorCancel bool, blocked ...string) *gatedExecer {
	g := &gatedExecer{
		blocked: map[string]bool{}, honorCancel: honorCancel, release: make(chan struct{}),
		started: make(chan string, 64), calls: map[string]int{},
	}
	for _, id := range blocked {
		g.blocked[id] = true
	}
	return g
}

func (g *gatedExecer) ExecSync(ctx context.Context, containerID string, _ []string, _ int64) (*cri.ExecSyncResult, error) {
	g.mu.Lock()
	g.calls[containerID]++
	first := g.calls[containerID] == 1
	g.mu.Unlock()
	select {
	case g.started <- containerID:
	default:
	}
	if g.blocked[containerID] {
		cancelled := ctx.Done()
		if !g.honorCancel {
			cancelled = nil
		}
		select {
		case <-g.release:
		case <-cancelled:
			return nil, ctx.Err()
		}
	}
	if !first {
		return &cri.ExecSyncResult{ExitCode: g.laterExitCode}, nil
	}
	return &cri.ExecSyncResult{}, nil
}

func (g *gatedExecer) callCount(containerID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[containerID]
}

func (g *gatedExecer) awaitStart(t *testing.T, containerID string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case id := <-g.started:
			if id == containerID {
				return
			}
		case <-timeout:
			t.Fatalf("no probe started in %s", containerID)
		}
	}
}

// slowSpec keeps probe timeouts far beyond the test so only cancellation
// ends a blocked probe.
func slowSpec() *corev1.Probe {
	spec := probeSpec(execHandler(), 1, 1)
	spec.TimeoutSeconds = 60000
	return spec
}

// TestConcurrentAddLeavesNoUntrackedWorkers is a regression test: concurrent
// Adds for one UID must stop every replaced worker set, so no probe runs after
// Close.
func TestConcurrentAddLeavesNoUntrackedWorkers(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		adds  int
		round int
	}{
		{name: "two adds", adds: 2, round: 50},
		{name: "many adds", adds: 16, round: 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for range tt.round {
				execer := &fakeExecer{}
				m := fastManager(execer, Options{})
				start := make(chan struct{})
				var group sync.WaitGroup
				for i := range tt.adds {
					group.Add(1)
					go func() {
						defer group.Done()
						<-start
						spec := probeSpec(execHandler(), 1, 1)
						if err := m.Add(Target{PodUID: "uid", ContainerID: "c" + strconv.Itoa(i), Readiness: spec, Liveness: spec}); err != nil {
							t.Error(err)
						}
					}()
				}
				close(start)
				group.Wait()
				m.Close()
				calls := execer.callCount()
				time.Sleep(20 * time.Millisecond)
				if got := execer.callCount(); got != calls {
					t.Fatalf("probes ran after Close: %d -> %d calls", calls, got)
				}
			}
		})
	}
}

// TestSuspendQuiescesProbes is a regression test: after Suspend returns, a
// probe queued for a concurrency slot never starts, and an in-flight probe
// result never changes the Pod's status.
func TestSuspendQuiescesProbes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "queued for a slot", run: func(t *testing.T) {
			execer := newGatedExecer(true, "holder")
			m := fastManager(execer, Options{MaxConcurrent: 1})
			defer m.Close()
			if err := m.Add(Target{PodUID: "holder", ContainerID: "holder", Readiness: slowSpec()}); err != nil {
				t.Fatal(err)
			}
			execer.awaitStart(t, "holder")
			if err := m.Add(Target{PodUID: "queued", ContainerID: "queued", Readiness: probeSpec(execHandler(), 1, 1)}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond) // let the queued probe wait for the slot
			if err := m.Suspend("queued", "Paused"); err != nil {
				t.Fatal(err)
			}
			close(execer.release)
			time.Sleep(50 * time.Millisecond)
			if got := execer.callCount("queued"); got != 0 {
				t.Fatalf("queued probe ran %d times after Suspend", got)
			}
			if current, _ := m.Status("queued"); current.Ready {
				t.Fatalf("suspended status = %+v", current)
			}
		}},
		{name: "in flight", run: func(t *testing.T) {
			execer := newGatedExecer(true, "c1")
			m := fastManager(execer, Options{})
			defer m.Close()
			if err := m.Add(Target{PodUID: "uid", ContainerID: "c1", Readiness: slowSpec()}); err != nil {
				t.Fatal(err)
			}
			execer.awaitStart(t, "c1")
			if err := m.Suspend("uid", "Paused"); err != nil {
				t.Fatal(err)
			}
			calls := execer.callCount("c1")
			close(execer.release)
			time.Sleep(50 * time.Millisecond)
			if got := execer.callCount("c1"); got != calls {
				t.Fatalf("probes ran after Suspend: %d -> %d", calls, got)
			}
			if current, _ := m.Status("uid"); current.Ready || current.Suspended != "Paused" {
				t.Fatalf("in-flight result changed suspended status: %+v", current)
			}
		}},
		{name: "stale result after resume", run: func(t *testing.T) {
			execer := newGatedExecer(false, "c1")
			execer.laterExitCode = 1
			var m *Manager
			var everReady atomic.Bool
			m = fastManager(execer, Options{Notify: func(uid string) {
				if current, _ := m.Status(uid); current.Ready {
					everReady.Store(true)
				}
			}})
			defer m.Close()
			if err := m.Add(Target{PodUID: "uid", ContainerID: "c1", Readiness: slowSpec()}); err != nil {
				t.Fatal(err)
			}
			execer.awaitStart(t, "c1")
			suspended := make(chan error, 1)
			go func() { suspended <- m.Suspend("uid", "Paused") }()
			waitStatus(t, m, "uid", func(s Status) bool { return s.Suspended == "Paused" })
			if err := m.Resume("uid"); err != nil {
				t.Fatal(err)
			}
			if err := <-suspended; err != nil {
				t.Fatal(err)
			}
			// The pre-suspension probe now succeeds; later probes fail. The
			// stale success must be discarded.
			close(execer.release)
			time.Sleep(30 * time.Millisecond)
			if current, _ := m.Status("uid"); current.Ready || everReady.Load() {
				t.Fatalf("pre-suspension result made the resumed Pod ready: %+v", current)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
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
