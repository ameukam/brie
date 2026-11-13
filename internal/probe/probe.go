// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package probe runs readiness and liveness probes of the restricted Pod
// profile (DESIGN 12.4): exec probes through CRI ExecSync, and httpGet and
// tcpSocket probes against the Pod IP. The manager is keyed by Pod UID and can
// suspend a Pod's probes while a recorded lifecycle-extension state is valid.
//
// The manager only observes. It never restarts, stops, or resumes anything:
// under restartPolicy Never, a liveness failure is reported so the caller can
// fail the Pod.
package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Kind is readiness or liveness.
type Kind string

// Probe kinds.
const (
	Readiness Kind = "readiness"
	Liveness  Kind = "liveness"
)

// Result is one probe outcome. Error means the probe could not be run, which
// is not evidence about the application.
type Result string

// Probe results.
const (
	Success Result = "success"
	Failure Result = "failure"
	Error   Result = "error"
)

// Execer is the CRI subset exec probes use. *cri.Client implements it.
type Execer interface {
	ExecSync(ctx context.Context, containerID string, command []string, timeoutSeconds int64) (*cri.ExecSyncResult, error)
}

// Target is one running container to probe.
type Target struct {
	PodUID      string
	PodIP       string
	ContainerID string
	// StartedAt anchors initialDelaySeconds; zero means the time of Add.
	StartedAt time.Time
	Ports     []corev1.ContainerPort
	Readiness *corev1.Probe
	Liveness  *corev1.Probe
}

// Status is the probe state of one Pod.
type Status struct {
	// HasReadiness reports whether a readiness probe is configured.
	HasReadiness bool
	// Ready is true only after successThreshold consecutive readiness
	// successes, and false while suspended.
	Ready bool
	// LivenessFailed latches after failureThreshold consecutive liveness failures.
	LivenessFailed bool
	// Suspended is the recorded extension state while probes are suspended.
	Suspended string
	// Message describes the last failure without probe output.
	Message string
}

// Options bounds the manager. Zero values select the defaults.
type Options struct {
	// MaxPods bounds probed Pods. Default 110.
	MaxPods int
	// MaxConcurrent bounds probes running at once. Default 16.
	MaxConcurrent int
	// Observe receives each probe outcome.
	Observe func(kind Kind, handler string, result Result)
	// Notify is called after a Pod's Status changes. It must not block.
	Notify func(podUID string)
}

// Manager runs probes per Pod UID.
type Manager struct {
	execer  Execer
	options Options
	slots   chan struct{}
	client  *http.Client
	dialer  *net.Dialer
	// second is the unit of probe timing fields; tests shorten it.
	second time.Duration

	mu     sync.Mutex
	pods   map[string]*podProbes
	closed bool
}

type podProbes struct {
	target Target
	// ctx and cancel bound the Pod's probe workers until Remove or Close.
	ctx    context.Context
	cancel context.CancelFunc
	done   sync.WaitGroup
	status Status
	// The fields below are guarded by Manager.mu. generation changes on every
	// Suspend and Resume; a result from an earlier generation is discarded.
	// attempt bounds the probes of the current generation; Suspend cancels it.
	suspended     bool
	generation    uint64
	attempt       context.Context
	cancelAttempt context.CancelFunc
	// active counts probes started and not yet finished; idle is signalled
	// when it drops to zero.
	active   int
	idle     *sync.Cond
	counters map[Kind]*counter
}

type counter struct{ successes, failures int32 }

// NewManager creates a manager. execer may be nil when no exec probes run.
func NewManager(execer Execer, options Options) *Manager {
	if options.MaxPods <= 0 {
		options.MaxPods = 110
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = 16
	}
	if options.Observe == nil {
		options.Observe = func(Kind, string, Result) {}
	}
	if options.Notify == nil {
		options.Notify = func(string) {}
	}
	dialer := &net.Dialer{}
	return &Manager{
		execer: execer, options: options, slots: make(chan struct{}, options.MaxConcurrent),
		dialer: dialer, second: time.Second, pods: make(map[string]*podProbes),
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: nil, DialContext: dialer.DialContext, DisableKeepAlives: true,
				// Like kubelet, HTTPS probes do not verify the Pod's certificate.
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
			},
			CheckRedirect: func(request *http.Request, via []*http.Request) error {
				if len(via) >= 10 || request.URL.Host != via[0].URL.Host {
					return http.ErrUseLastResponse
				}
				return nil
			},
		},
	}
}

// Add starts probing a Pod, replacing any earlier target for its UID.
func (m *Manager) Add(target Target) error {
	if target.PodUID == "" || target.ContainerID == "" {
		return errors.New("probe target requires Pod UID and container ID")
	}
	if target.Readiness == nil && target.Liveness == nil {
		m.Remove(target.PodUID)
		return nil
	}
	if _, err := netip.ParseAddr(target.PodIP); err != nil && needsAddress(target) {
		return errors.New("network probes require a Pod IP")
	}
	if target.StartedAt.IsZero() {
		target.StartedAt = time.Now()
	}
	// The tracked entry is replaced atomically, so concurrent Adds for one UID
	// leave exactly one entry and every replaced entry is stopped below.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("probe manager closed")
	}
	previous := m.pods[target.PodUID]
	if previous == nil && len(m.pods) >= m.options.MaxPods {
		m.mu.Unlock()
		return fmt.Errorf("probe manager tracks at most %d Pods", m.options.MaxPods)
	}
	ctx, cancel := context.WithCancel(context.Background())
	attempt, cancelAttempt := context.WithCancel(ctx)
	pod := &podProbes{
		target: target, ctx: ctx, cancel: cancel, attempt: attempt, cancelAttempt: cancelAttempt,
		idle: sync.NewCond(&m.mu), counters: map[Kind]*counter{Readiness: {}, Liveness: {}},
		status: Status{HasReadiness: target.Readiness != nil},
	}
	m.pods[target.PodUID] = pod
	for kind, spec := range map[Kind]*corev1.Probe{Readiness: target.Readiness, Liveness: target.Liveness} {
		if spec == nil {
			continue
		}
		pod.done.Add(1)
		go m.run(pod, kind, spec)
	}
	m.mu.Unlock()
	stop(previous)
	return nil
}

// stop cancels a Pod's probe workers and waits for them to exit.
func stop(pod *podProbes) {
	if pod != nil {
		pod.cancel()
		pod.done.Wait()
	}
}

func needsAddress(target Target) bool {
	for _, spec := range []*corev1.Probe{target.Readiness, target.Liveness} {
		if spec != nil && spec.Exec == nil {
			return true
		}
	}
	return false
}

// Remove stops a Pod's probes and forgets its status.
func (m *Manager) Remove(podUID string) {
	m.mu.Lock()
	pod := m.pods[podUID]
	delete(m.pods, podUID)
	m.mu.Unlock()
	stop(pod)
}

// Close stops every probe.
func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	uids := make([]string, 0, len(m.pods))
	for uid := range m.pods {
		uids = append(uids, uid)
	}
	m.mu.Unlock()
	for _, uid := range uids {
		m.Remove(uid)
	}
}

// Suspend stops running a Pod's probes while state is the recorded extension
// state. It cancels probes that are queued for a concurrency slot or running
// and returns once they have finished, unless a Resume intervenes; their
// results are discarded. Readiness becomes false; liveness evidence is
// preserved, not reset.
func (m *Manager) Suspend(podUID, state string) error {
	if state == "" {
		return errors.New("suspension requires a recorded extension state")
	}
	m.mu.Lock()
	pod, found := m.pods[podUID]
	if !found {
		m.mu.Unlock()
		return errors.New("pod is not probed")
	}
	pod.suspended = true
	pod.generation++
	pod.cancelAttempt()
	pod.status.Suspended, pod.status.Ready = state, false
	pod.counters[Readiness].successes = 0
	for generation := pod.generation; pod.active > 0 && pod.generation == generation; {
		pod.idle.Wait()
	}
	m.mu.Unlock()
	m.options.Notify(podUID)
	return nil
}

// Resume restarts probing after suspension. Readiness must succeed again.
func (m *Manager) Resume(podUID string) error {
	m.mu.Lock()
	pod, found := m.pods[podUID]
	if !found {
		m.mu.Unlock()
		return errors.New("pod is not probed")
	}
	if pod.suspended {
		pod.generation++
		pod.attempt, pod.cancelAttempt = context.WithCancel(pod.ctx)
		// A Suspend still waiting for the previous generation returns now.
		pod.idle.Broadcast()
	}
	pod.suspended, pod.status.Suspended = false, ""
	m.mu.Unlock()
	m.options.Notify(podUID)
	return nil
}

// Status returns a Pod's probe status.
func (m *Manager) Status(podUID string) (Status, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pod, found := m.pods[podUID]
	if !found {
		return Status{}, false
	}
	return pod.status, true
}

func (m *Manager) run(pod *podProbes, kind Kind, spec *corev1.Probe) {
	defer pod.done.Done()
	delay := time.Until(pod.target.StartedAt.Add(time.Duration(spec.InitialDelaySeconds) * m.second))
	period := time.Duration(max(1, spec.PeriodSeconds)) * m.second
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	for {
		select {
		case <-pod.ctx.Done():
			return
		case <-timer.C:
		}
		m.mu.Lock()
		suspended, generation, attempt := pod.suspended, pod.generation, pod.attempt
		if !suspended {
			pod.active++
		}
		m.mu.Unlock()
		if !suspended {
			result, message := m.probeOnce(attempt, pod.target, spec)
			m.mu.Lock()
			pod.active--
			if pod.active == 0 {
				pod.idle.Broadcast()
			}
			m.mu.Unlock()
			if pod.ctx.Err() != nil {
				return
			}
			if attempt.Err() == nil {
				m.options.Observe(kind, handlerName(spec), result)
				m.record(pod, generation, kind, spec, result, message)
			}
		}
		timer.Reset(period)
	}
}

// record applies a result unless the Pod was suspended or resumed since the
// probe started.
func (m *Manager) record(pod *podProbes, generation uint64, kind Kind, spec *corev1.Probe, result Result, message string) {
	m.mu.Lock()
	if pod.suspended || pod.generation != generation {
		m.mu.Unlock()
		return
	}
	before := pod.status
	count := pod.counters[kind]
	switch {
	case result == Success:
		count.successes, count.failures = count.successes+1, 0
	case result == Failure || kind == Readiness:
		// An unrun readiness probe withholds traffic; an unrun liveness probe
		// is not evidence of failure and never fails the Pod.
		count.successes, count.failures = 0, count.failures+1
		pod.status.Message = message
	}
	switch kind {
	case Readiness:
		if count.successes >= max(1, spec.SuccessThreshold) {
			pod.status.Ready = true
		}
		if count.failures >= max(1, spec.FailureThreshold) {
			pod.status.Ready = false
		}
	case Liveness:
		if count.failures >= max(1, spec.FailureThreshold) {
			pod.status.LivenessFailed = true
		}
	}
	changed := before != pod.status
	uid := pod.target.PodUID
	m.mu.Unlock()
	if changed {
		m.options.Notify(uid)
	}
}

func handlerName(spec *corev1.Probe) string {
	switch {
	case spec.Exec != nil:
		return "exec"
	case spec.HTTPGet != nil:
		return "httpGet"
	case spec.TCPSocket != nil:
		return "tcpSocket"
	}
	return "other"
}

func (m *Manager) probeOnce(ctx context.Context, target Target, spec *corev1.Probe) (Result, string) {
	select {
	case m.slots <- struct{}{}:
		defer func() { <-m.slots }()
	case <-ctx.Done():
		return Error, "probe cancelled"
	}
	timeout := time.Duration(max(1, spec.TimeoutSeconds)) * m.second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	switch {
	case spec.Exec != nil:
		return m.execProbe(ctx, target, spec)
	case spec.HTTPGet != nil:
		return m.httpProbe(ctx, target, spec.HTTPGet)
	case spec.TCPSocket != nil:
		return m.tcpProbe(ctx, target, spec.TCPSocket)
	}
	return Error, "unsupported probe handler"
}

func (m *Manager) execProbe(ctx context.Context, target Target, spec *corev1.Probe) (Result, string) {
	if m.execer == nil {
		return Error, "exec probes are not configured"
	}
	result, err := m.execer.ExecSync(ctx, target.ContainerID, spec.Exec.Command, int64(max(1, spec.TimeoutSeconds)))
	switch {
	case err == nil && result.ExitCode == 0:
		return Success, ""
	case err == nil:
		return Failure, fmt.Sprintf("exec probe exited with code %d", result.ExitCode)
	case errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded:
		return Failure, "exec probe timed out"
	}
	return Error, "exec probe could not run"
}

func resolvePort(port intstr.IntOrString, ports []corev1.ContainerPort) (int32, error) {
	if port.Type == intstr.Int {
		if port.IntVal < 1 || port.IntVal > 65535 {
			return 0, errors.New("probe port out of range")
		}
		return port.IntVal, nil
	}
	for _, declared := range ports {
		if declared.Name != "" && declared.Name == port.StrVal {
			return declared.ContainerPort, nil
		}
	}
	return 0, errors.New("probe names an undeclared port")
}

func (m *Manager) httpProbe(ctx context.Context, target Target, spec *corev1.HTTPGetAction) (Result, string) {
	port, err := resolvePort(spec.Port, target.Ports)
	if err != nil {
		return Error, err.Error()
	}
	scheme := "http"
	if spec.Scheme == corev1.URISchemeHTTPS {
		scheme = "https"
	}
	path := spec.Path
	if path == "" {
		path = "/"
	}
	address := net.JoinHostPort(target.PodIP, strconv.Itoa(int(port)))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+address+path, nil)
	if err != nil {
		return Error, "invalid httpGet probe request"
	}
	request.Header.Set("User-Agent", "kube-probe/brie")
	request.Header.Set("Accept", "*/*")
	for _, header := range spec.HTTPHeaders {
		request.Header.Add(header.Name, header.Value)
	}
	response, err := m.client.Do(request)
	if err != nil {
		return Failure, "httpGet probe request failed"
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 10*1024))
	_ = response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 400 {
		return Success, ""
	}
	return Failure, fmt.Sprintf("httpGet probe returned HTTP %d", response.StatusCode)
}

func (m *Manager) tcpProbe(ctx context.Context, target Target, spec *corev1.TCPSocketAction) (Result, string) {
	port, err := resolvePort(spec.Port, target.Ports)
	if err != nil {
		return Error, err.Error()
	}
	conn, err := m.dialer.DialContext(ctx, "tcp", net.JoinHostPort(target.PodIP, strconv.Itoa(int(port))))
	if err != nil {
		return Failure, "tcpSocket probe connection failed"
	}
	_ = conn.Close()
	return Success, ""
}
