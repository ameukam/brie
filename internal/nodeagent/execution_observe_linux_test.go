// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/utils/ptr"
)

// hookCalls records executor hook calls for tests.
type hookCalls struct {
	mu       sync.Mutex
	calls    []string
	journals []JournalState
	probes   []string
}

func (h *hookCalls) add(call string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, call)
}

func (h *hookCalls) list() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.calls)
}

func (h *hookCalls) hooks() ExecutionHooks {
	return ExecutionHooks{
		Admission: func(pod *corev1.Pod, reason, _ string) { h.add("admission " + pod.Name + " " + reason) },
		Journal: func(state JournalState) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.journals = append(h.journals, state)
		},
		ImagePullFailed: func(pod *corev1.Pod, backoff bool, code string) {
			kind := "failed"
			if backoff {
				kind = "backoff"
			}
			h.add("pull " + pod.Name + " " + kind + " " + code)
		},
		ExecutionBlocked: func(pod *corev1.Pod, reason, _ string) { h.add("blocked " + pod.Name + " " + reason) },
		CleanupBlocked:   func(pod *corev1.Pod, message string) { h.add("cleanup " + pod.Name + " " + message) },
		ProbeResult: func(kind, handler, result string) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.probes = append(h.probes, kind+" "+handler+" "+result)
		},
		LivenessFailed: func(pod *corev1.Pod, _ string) { h.add("liveness " + pod.Name) },
	}
}

// probeExecer answers exec probes with a configurable exit code. With
// failContainer set, only probes of that container fail, with exit code 1.
type probeExecer struct {
	mu            sync.Mutex
	exit          int32
	failContainer string
}

func (p *probeExecer) ExecSync(_ context.Context, containerID string, _ []string, _ int64) (*cri.ExecSyncResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failContainer != "" && containerID == p.failContainer {
		return &cri.ExecSyncResult{ExitCode: 1}, nil
	}
	return &cri.ExecSyncResult{ExitCode: p.exit}, nil
}

func (p *probeExecer) set(exit int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exit = exit
}

func execProbe(initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/check", "tenant-secret-arg"}}},
		InitialDelaySeconds: initialDelay, TimeoutSeconds: 1, PeriodSeconds: 1, SuccessThreshold: 1, FailureThreshold: 1,
	}
}

func TestExecutionHooksReportAdmissionDecisions(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(*executor, *corev1.Pod)
		probes bool
		want   string
	}{
		{name: "admitted", want: AdmissionAdmitted},
		{name: "unsupported", change: func(_ *executor, pod *corev1.Pod) { pod.Spec.HostNetwork = true }, want: "UnsupportedPod"},
		{name: "overhead mismatch", change: func(_ *executor, pod *corev1.Pod) {
			pod.Spec.Overhead[corev1.ResourceCPU] = resource.MustParse("200m")
		}, want: "RuntimeClassOverheadMismatch"},
		{name: "budget", change: func(e *executor, _ *corev1.Pod) { e.config.CPU = "1" }, want: "InsufficientResources"},
		{name: "probes without the probe increment", change: func(_ *executor, pod *corev1.Pod) {
			pod.Spec.Containers[0].ReadinessProbe = execProbe(0)
		}, want: "UnsupportedPod"},
		{name: "probes with the probe increment", probes: true, change: func(_ *executor, pod *corev1.Pod) {
			pod.Spec.Containers[0].ReadinessProbe = execProbe(0)
		}, want: AdmissionAdmitted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pod := executionPod("app")
			e, _ := newExecutor(t)
			calls := &hookCalls{}
			e.hooks = calls.hooks()
			if tt.probes {
				e.probes = newProbeManager(&probeExecer{}, e.config.MaxPods, e.hooks)
				t.Cleanup(e.probes.Close)
			}
			if tt.change != nil {
				tt.change(e, pod)
			}
			if _, err := e.api.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			reconcilePasses(t, e, 3)
			admissions := slices.DeleteFunc(calls.list(), func(call string) bool { return !strings.HasPrefix(call, "admission ") })
			if want := []string{"admission app " + tt.want}; !slices.Equal(admissions, want) {
				t.Fatalf("admission decisions = %v, want %v", admissions, want)
			}
			got, err := e.api.CoreV1().Pods(pod.Namespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if tt.want != AdmissionAdmitted && (got.Status.Phase != corev1.PodFailed || got.Status.Reason != tt.want) {
				t.Fatalf("refusal status = %s/%s, want Failed/%s", got.Status.Phase, got.Status.Reason, tt.want)
			}
		})
	}
}

func TestExecutionJournalHookReportsReservationsAndPendingAge(t *testing.T) {
	t.Parallel()
	e, _ := newExecutor(t)
	calls := &hookCalls{}
	e.hooks = calls.hooks()
	e.journal.saved = e.journalSaved
	old, older := time.Unix(2000, 0).UTC(), time.Unix(1000, 0).UTC()
	hash := strings.Repeat("a", 64)
	record := func(uid, stage, pendingStage string, since time.Time) *executionRecord {
		return &executionRecord{UID: uid, Namespace: "default", Name: uid, Hash: hash, Stage: stage,
			SandboxID: "sandbox-" + uid, ContainerID: "container-" + uid, CPU: 100, Memory: 1024,
			PendingStage: pendingStage, PendingSince: since}
	}
	e.journal.Records = map[string]*executionRecord{
		"reserved-new":    record("reserved-new", "reserved", "reserved", old),
		"reserved-old":    record("reserved-old", "reserved", "reserved", older),
		"sandbox-unknown": record("sandbox-unknown", "sandbox-pending", "sandbox-pending", time.Time{}),
		"sandbox-known":   record("sandbox-known", "sandbox-pending", "sandbox-pending", older),
		"start-entered":   record("start-entered", "start-pending", "", time.Time{}),
		"running":         record("running", "running", "start-pending", older),
		"finished":        record("finished", "finished", "", time.Time{}),
	}
	before := time.Now()
	if err := e.journal.save(); err != nil {
		t.Fatal(err)
	}
	if len(calls.journals) != 1 {
		t.Fatalf("journal hook calls = %d", len(calls.journals))
	}
	state := calls.journals[0]
	if state.Records != 7 || state.Bytes <= 0 || state.ReservedExecutions != 6 ||
		state.ReservedCPUMillicores != 600 || state.ReservedMemoryBytes != 6*1024 {
		t.Fatalf("journal state = %+v", state)
	}
	for _, tt := range []struct {
		stage string
		check func(time.Time) bool
	}{
		{"reserved", func(at time.Time) bool { return at.Equal(older) }},
		// An unknown entry time is reported as unknown, never as a fresh time.
		{"sandbox-pending", time.Time.IsZero},
		{"start-pending", func(at time.Time) bool { return !at.Before(before) }},
	} {
		if at, found := state.PendingSince[tt.stage]; !found || !tt.check(at) {
			t.Errorf("pending %s = %v (found %v)", tt.stage, at, found)
		}
	}
	if _, found := state.PendingSince["container-pending"]; found || len(state.PendingSince) != 3 {
		t.Fatalf("pending stages = %v", state.PendingSince)
	}
	if running := e.journal.Records["running"]; running.PendingStage != "" || !running.PendingSince.IsZero() {
		t.Fatalf("pending time kept after leaving the stage: %+v", running)
	}
	// Leaving the stage and re-entering one restamps it; an unchanged stage keeps its time.
	e.journal.Records["reserved-old"].Stage = "sandbox-pending"
	if err := e.journal.save(); err != nil {
		t.Fatal(err)
	}
	if got := e.journal.Records["reserved-old"].PendingSince; got.Before(before) {
		t.Fatalf("new pending stage not stamped: %v", got)
	}
	if got := e.journal.Records["reserved-new"].PendingSince; !got.Equal(old) {
		t.Fatalf("unchanged pending stage restamped: %v", got)
	}
}

func TestExecutionJournalPendingTimes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		record map[string]any
		valid  bool
	}{
		{name: "older journal pending record", record: map[string]any{"Stage": "reserved"}, valid: true},
		{name: "recorded pending time", valid: true,
			record: map[string]any{"Stage": "reserved", "PendingStage": "reserved", "PendingSince": "2026-11-13T10:00:00Z"}},
		{name: "stage mismatch", record: map[string]any{"Stage": "reserved", "PendingStage": "sandbox-pending"}},
		{name: "time without stage", record: map[string]any{"Stage": "reserved", "PendingSince": "2026-11-13T10:00:00Z"}},
		{name: "pending time on a running record", record: map[string]any{
			"Stage": "running", "SandboxID": "s", "ContainerID": "c", "PendingStage": "running"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := executionConfig(t)
			if err := os.MkdirAll(config.StateDir, 0700); err != nil {
				t.Fatal(err)
			}
			payload := versionOneJournal(config)
			payload["Version"] = 2
			record := map[string]any{"UID": "pending", "Namespace": "default", "Name": "pending",
				"Hash": strings.Repeat("a", 64), "CPU": 100, "Memory": 1024}
			for key, value := range tt.record {
				record[key] = value
			}
			payload["Records"] = map[string]any{"pending": record}
			writeJournalPayload(t, config.StateDir, payload)
			journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
			if !tt.valid {
				if err == nil {
					_ = journal.close()
					t.Fatal("invalid pending stage time accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = journal.close() }()
			got := journal.Records["pending"]
			if got.PendingStage != "reserved" {
				t.Fatalf("pending stage = %+v", got)
			}
			// A record read without an entry time keeps it unknown across saves.
			if err := journal.save(); err != nil {
				t.Fatal(err)
			}
			if _, recorded := tt.record["PendingSince"]; recorded == got.PendingSince.IsZero() {
				t.Fatalf("pending time = %v", got.PendingSince)
			}
		})
	}
}

func TestExecutionHooksReportImagePullFailures(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	calls := &hookCalls{}
	e.hooks = calls.hooks()
	service.pullError = status.Error(codes.NotFound, "registry.example/tenant-image: not found")
	reconcilePasses(t, e, 4)
	pulls := slices.DeleteFunc(calls.list(), func(call string) bool { return !strings.HasPrefix(call, "pull ") })
	if want := []string{"pull app failed NotFound", "pull app backoff NotFound"}; !slices.Equal(pulls, want) {
		t.Fatalf("pull hooks = %v, want %v", pulls, want)
	}
	for _, tt := range []struct {
		err  error
		want string
	}{
		{status.Error(codes.Unavailable, "x"), "Unavailable"},
		{context.DeadlineExceeded, "DeadlineExceeded"},
		{os.ErrClosed, "Unknown"},
	} {
		if got := pullCode(tt.err); got != tt.want {
			t.Errorf("pullCode(%v) = %s, want %s", tt.err, got, tt.want)
		}
	}
	if got := e.pulls.failureCode("never-pulled"); got != "Unknown" {
		t.Fatalf("failure code without a pull = %s", got)
	}
}

func TestExecutionHooksReportExecutionBlocked(t *testing.T) {
	t.Parallel()
	e, _ := newExecutor(t, executionPod("app"))
	calls := &hookCalls{}
	e.hooks = calls.hooks()
	reconcilePasses(t, e, 5)
	pod, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers[0].Image = "example.test/app@sha256:" + strings.Repeat("b", 64)
	if _, err := e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	reconcilePasses(t, e, 3)
	blocked := slices.DeleteFunc(calls.list(), func(call string) bool { return !strings.HasPrefix(call, "blocked ") })
	// The condition is published once; unchanged passes report nothing new.
	if want := []string{"blocked app ExecutionConfigChanged"}; !slices.Equal(blocked, want) {
		t.Fatalf("blocked hooks = %v, want %v", blocked, want)
	}
}

func TestExecutionHooksReportCleanupBlocked(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		stage    string
		evidence string
		want     string
	}{
		{name: "no deletion evidence", stage: "running", want: "positive deletion evidence"},
		{name: "unresolved sandbox creation", stage: "sandbox-pending", evidence: "42", want: "unresolved sandbox creation"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, _ := newExecutor(t)
			calls := &hookCalls{}
			e.hooks = calls.hooks()
			record := &executionRecord{UID: "gone-uid", Namespace: "team", Name: "gone", Hash: strings.Repeat("a", 64),
				Stage: tt.stage, CPU: 100, Memory: 1024}
			if err := e.commit(record, func() { e.journal.Records[record.UID] = record }); err != nil {
				t.Fatal(err)
			}
			if err := e.cleanupDeleted(t.Context(), e.runtime, record, &podInventory{}, tt.evidence); err == nil {
				t.Fatal("cleanup proceeded without evidence")
			}
			got := calls.list()
			if len(got) != 1 || !strings.HasPrefix(got[0], "cleanup gone ") || !strings.Contains(got[0], tt.want) {
				t.Fatalf("cleanup hooks = %v, want one containing %q", got, tt.want)
			}
			if pod := recordPod(record); pod.Namespace != "team" || pod.UID != "gone-uid" {
				t.Fatalf("record reference = %+v", pod.ObjectMeta)
			}
		})
	}
}

func podCondition(pod *corev1.Pod, kind corev1.PodConditionType) corev1.ConditionStatus {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == kind {
			return condition.Status
		}
	}
	return ""
}

// waitProbeStatus polls the probe manager until check holds.
func waitProbeStatus(t *testing.T, e *executor, uid string, check func(bool, bool) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, found := e.probes.Status(uid)
		if found && check(status.Ready, status.LivenessFailed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe status = %+v (found %v)", status, found)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExecutionReadinessWaitsForProbes(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		readiness *corev1.Probe
		liveness  *corev1.Probe
		probes    bool
		exit      int32
		wait      bool
		want      corev1.ConditionStatus
	}{
		{name: "readiness succeeds", readiness: execProbe(0), probes: true, wait: true, want: corev1.ConditionTrue},
		{name: "readiness fails", readiness: execProbe(0), probes: true, exit: 1, want: corev1.ConditionFalse},
		{name: "readiness not yet run", readiness: execProbe(3600), probes: true, want: corev1.ConditionFalse},
		{name: "no probe manager", readiness: execProbe(0), want: corev1.ConditionFalse},
		{name: "no readiness probe", liveness: execProbe(0), probes: true, want: corev1.ConditionTrue},
		{name: "no probes", probes: true, want: corev1.ConditionTrue},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pod := executionPod("app")
			pod.Spec.Containers[0].ReadinessProbe, pod.Spec.Containers[0].LivenessProbe = tt.readiness, tt.liveness
			e, _ := newExecutor(t, pod)
			calls := &hookCalls{}
			e.hooks = calls.hooks()
			// The Pod is admitted with the probe increment; "no probe manager"
			// then runs without one, as after a restart without --enable-probes.
			e.probes = newProbeManager(&probeExecer{exit: tt.exit}, e.config.MaxPods, e.hooks)
			t.Cleanup(e.probes.Close)
			// The first pass only completes reconstruction; the second admits.
			reconcilePasses(t, e, 2)
			if !tt.probes {
				e.probes.Close()
				e.probes = nil
			}
			reconcilePasses(t, e, 4)
			if tt.wait {
				waitProbeStatus(t, e, string(pod.UID), func(ready, _ bool) bool { return ready })
			}
			if tt.exit != 0 {
				waitProbeStatus(t, e, string(pod.UID), func(_, _ bool) bool {
					calls.mu.Lock()
					defer calls.mu.Unlock()
					return slices.Contains(calls.probes, "readiness exec failure")
				})
			}
			reconcilePasses(t, e, 1)
			got, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != corev1.PodRunning || podCondition(got, corev1.PodReady) != tt.want ||
				podCondition(got, corev1.ContainersReady) != tt.want || got.Status.ContainerStatuses[0].Ready != (tt.want == corev1.ConditionTrue) {
				t.Fatalf("phase %s, Ready %s, ContainersReady %s; want Running and %s", got.Status.Phase,
					podCondition(got, corev1.PodReady), podCondition(got, corev1.ContainersReady), tt.want)
			}
		})
	}
}

func TestExecutionLivenessFailureFailsPod(t *testing.T) {
	t.Parallel()
	pod := executionPod("app")
	pod.Spec.TerminationGracePeriodSeconds = ptr.To(int64(7))
	pod.Spec.Containers[0].LivenessProbe = execProbe(0)
	e, service := newExecutor(t, pod)
	calls := &hookCalls{}
	e.hooks = calls.hooks()
	execer := &probeExecer{}
	e.probes = newProbeManager(execer, e.config.MaxPods, e.hooks)
	t.Cleanup(e.probes.Close)
	reconcilePasses(t, e, 6)
	if _, found := e.probes.Status(string(pod.UID)); !found {
		t.Fatal("probes not started for the running container")
	}
	execer.set(1)
	waitProbeStatus(t, e, string(pod.UID), func(_, failed bool) bool { return failed })
	reconcilePasses(t, e, 4)
	service.mu.Lock()
	starts := slices.DeleteFunc(slices.Clone(service.calls), func(call string) bool { return call != "StartContainer" })
	grace := slices.Clone(service.stopGrace)
	service.mu.Unlock()
	// The container is stopped with the Pod's grace period and never restarted.
	if len(starts) != 1 || len(grace) == 0 || grace[0] != 7 {
		t.Fatalf("starts %v, stop grace %v", starts, grace)
	}
	got, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The fake container exits 0 after the stop; the Pod still fails.
	if got.Status.Phase != corev1.PodFailed || got.Status.Reason != reasonLivenessProbeFailed ||
		got.Status.ContainerStatuses[0].State.Terminated == nil || strings.Contains(got.Status.Message, "tenant-secret-arg") {
		t.Fatalf("status after liveness failure = %+v", got.Status)
	}
	if liveness := slices.DeleteFunc(calls.list(), func(call string) bool { return call != "liveness app" }); len(liveness) != 1 {
		t.Fatalf("liveness hooks = %v", calls.list())
	}
	// The terminal Pod is cleaned up and its probes are stopped.
	if _, found := e.probes.Status(string(pod.UID)); found {
		t.Fatal("probes still run after cleanup")
	}
	if record := e.journal.Records[string(pod.UID)]; record == nil || record.Stage != "finished" {
		t.Fatalf("record after liveness failure = %+v", record)
	}
}

func TestExecutionLivenessFailureAfterExit(t *testing.T) {
	t.Parallel()
	pod := executionPod("app")
	pod.Spec.Containers[0].LivenessProbe = execProbe(0)
	e, service := newExecutor(t, pod)
	e.probes = newProbeManager(&probeExecer{exit: 1}, e.config.MaxPods, e.hooks)
	t.Cleanup(e.probes.Close)
	reconcilePasses(t, e, 6)
	waitProbeStatus(t, e, string(pod.UID), func(_, failed bool) bool { return failed })
	service.mu.Lock()
	for _, container := range service.containers {
		container.State = runtimeapi.ContainerState_CONTAINER_EXITED
	}
	service.mu.Unlock()
	reconcilePasses(t, e, 1)
	got, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != corev1.PodFailed || got.Status.Reason != reasonLivenessProbeFailed {
		t.Fatalf("exited container after a liveness failure = %s/%s", got.Status.Phase, got.Status.Reason)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if slices.Contains(service.calls, "StopContainer") {
		t.Fatal("exited container stopped again")
	}
}
