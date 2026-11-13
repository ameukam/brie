// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// restartJournal closes and reopens the journal as an agent restart would.
func restartJournal(t *testing.T, e *executor) {
	t.Helper()
	if err := e.journal.close(); err != nil {
		t.Fatal(err)
	}
	journal, err := openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.close() })
	e.journal = journal
	e.workers = newPodWorkers()
	e.synced.Store(false)
}

func countCalls(service *executionRuntime, method string) int {
	service.mu.Lock()
	defer service.mu.Unlock()
	count := 0
	for _, call := range service.calls {
		if call == method {
			count++
		}
	}
	return count
}

func blockedCondition(pod *corev1.Pod) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == executionBlocked {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}

func nodeReady(t *testing.T, e *executor) bool {
	t.Helper()
	_ = e.updateHealth(t.Context())
	node, err := e.api.CoreV1().Nodes().Get(t.Context(), "worker-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			pods := node.Status.Allocatable[corev1.ResourcePods]
			return condition.Status == corev1.ConditionTrue && !pods.IsZero()
		}
	}
	return false
}

// TestExecutionCreationEvidence covers the creation-evidence rule of section
// 6.4 of DESIGN.md: only a definite runtime rejection followed by a complete
// inventory without the object resolves a pending stage as "nothing created".
func TestExecutionCreationEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		method         string
		code           codes.Code
		createOnReject bool
		restart        bool
		foreignClaim   bool
		want           string
	}{
		{name: "sandbox-rejected", method: "RunPodSandbox", code: codes.InvalidArgument, want: "SandboxCreateRejected"},
		{name: "sandbox-rejected-restart", method: "RunPodSandbox", code: codes.FailedPrecondition, restart: true, want: "SandboxCreateRejected"},
		{name: "sandbox-rejected-but-created", method: "RunPodSandbox", code: codes.InvalidArgument, createOnReject: true, want: "running"},
		{name: "sandbox-rejected-foreign-claim", method: "RunPodSandbox", code: codes.NotFound, foreignClaim: true, want: "unresolved"},
		{name: "sandbox-server-deadline", method: "RunPodSandbox", code: codes.DeadlineExceeded, want: "unresolved"},
		{name: "sandbox-unknown", method: "RunPodSandbox", code: codes.Unknown, want: "unresolved"},
		{name: "container-rejected", method: "CreateContainer", code: codes.FailedPrecondition, want: "ContainerCreateRejected"},
		{name: "container-rejected-restart", method: "CreateContainer", code: codes.InvalidArgument, restart: true, want: "ContainerCreateRejected"},
		{name: "container-rejected-but-created", method: "CreateContainer", code: codes.NotFound, createOnReject: true, want: "running"},
		{name: "container-lost-reply", method: "CreateContainer", code: codes.Unavailable, want: "unresolved"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			service.reject = map[string]codes.Code{test.method: test.code}
			service.createOnReject = test.createOnReject
			rejected := false
			for range 8 {
				err := e.reconcile(t.Context())
				if err == nil || rejected {
					continue
				}
				if status.Code(err) != test.code {
					t.Fatalf("unexpected reconciliation error: %v", err)
				}
				rejected = true
				if test.restart {
					restartJournal(t, e)
				}
				if test.foreignClaim {
					service.mu.Lock()
					service.sandboxes["foreign"] = &runtimeapi.PodSandbox{Id: "foreign", CreatedAt: 1,
						Metadata: &runtimeapi.PodSandboxMetadata{Name: "app", Namespace: "default", Uid: "app-uid"},
						Labels:   map[string]string{podUIDLabel: "app-uid"}}
					service.mu.Unlock()
				}
			}
			if !rejected {
				t.Fatal("create was not rejected")
			}
			if calls := countCalls(service, test.method); calls != 1 {
				t.Fatalf("%s issued %d times after its outcome", test.method, calls)
			}
			record := e.record("app-uid")
			pod, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
			switch test.want {
			case "unresolved":
				if err != nil || terminalPod(pod) || !e.recovery.Load() || e.synced.Load() ||
					record.Stage == "finished" || record.Stage == "reserved" || record.Stage == "sandbox" {
					t.Fatalf("uncertain create resolved without evidence: %+v %+v", record, pod.Status)
				}
				if nodeReady(t, e) {
					t.Fatal("node ready with an unresolved create")
				}
			case "running":
				if err != nil || pod.Status.Phase != corev1.PodRunning || record.CreateRejected != "" {
					t.Fatalf("created object not adopted: %+v %+v", record, pod.Status)
				}
			default:
				if err != nil || pod.Status.Phase != corev1.PodFailed ||
					pod.Status.ContainerStatuses[0].State.Waiting.Reason != test.want {
					t.Fatalf("rejected create not published: %v %+v", err, pod.Status)
				}
				if record.Stage != "finished" || record.CreateRejected != test.code.String() ||
					len(service.sandboxes) != 0 || len(service.containers) != 0 {
					t.Fatalf("rejected execution not released: %+v", record)
				}
				if e.recovery.Load() || !nodeReady(t, e) {
					t.Fatal("a rejected create held the node barrier")
				}
				// Admission resumes for other Pods.
				service.mu.Lock()
				service.reject = nil
				service.mu.Unlock()
				if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), executionPod("next"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				reconcilePasses(t, e, 5)
				next, err := e.api.CoreV1().Pods("default").Get(t.Context(), "next", metav1.GetOptions{})
				if err != nil || next.Status.Phase != corev1.PodRunning {
					t.Fatalf("admission did not resume: %v %+v", err, next.Status)
				}
			}
		})
	}
}

func TestExecutionJournalRejectsInvalidCreationEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ name, code string }{
		{"lost-reply-code", "Unavailable"},
		{"unknown-code", "Bogus"},
		{"started-stage", "InvalidArgument"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			e, _ := newExecutor(t, executionPod("app"))
			reconcilePasses(t, e, 5)
			record := e.journal.Records["app-uid"]
			record.CreateRejected = test.code
			if test.name != "started-stage" {
				record.Stage = "sandbox-pending"
			}
			if err := e.journal.save(); err != nil {
				t.Fatal(err)
			}
			if err := e.journal.close(); err != nil {
				t.Fatal(err)
			}
			if _, err := openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config); err == nil {
				t.Fatal("invalid creation evidence accepted")
			}
		})
	}
}

// TestExecutionPodFaultIsolation covers section 6.4 item 2 of DESIGN.md: a
// fault confined to one Pod's inputs is published on that Pod and leaves node
// readiness and other admissions unchanged.
func TestExecutionPodFaultIsolation(t *testing.T) {
	t.Parallel()
	t.Run("spec-change", func(t *testing.T) {
		t.Parallel()
		first := executionPod("first")
		first.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
		e, service := newExecutor(t, first)
		reconcilePasses(t, e, 5)
		pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "first", metav1.GetOptions{})
		pod.Spec.Containers[0].Image = "registry.example/other@sha256:" + "1111111111111111111111111111111111111111111111111111111111111111"
		if _, err := e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), executionPod("second"), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		reconcilePasses(t, e, 5)
		pod, _ = e.api.CoreV1().Pods("default").Get(t.Context(), "first", metav1.GetOptions{})
		condition := blockedCondition(pod)
		if condition == nil || condition.Reason != "ExecutionConfigChanged" || pod.Status.Phase != corev1.PodRunning {
			t.Fatalf("spec change not published on its Pod: %+v", pod.Status)
		}
		second, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "second", metav1.GetOptions{})
		if second.Status.Phase != corev1.PodRunning {
			t.Fatalf("spec change blocked another admission: %+v", second.Status)
		}
		if countCalls(service, "StopContainer") != 0 || e.journal.Records["first-uid"].Stage != "running" {
			t.Fatal("spec change mutated the running execution")
		}
		if e.recovery.Load() || !nodeReady(t, e) {
			t.Fatal("spec change raised the node barrier")
		}
		// Deletion still cleans up the blocked execution.
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		if _, err := e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		reconcilePasses(t, e, 2)
		// The finished record is compacted once the Pod is gone.
		if e.journal.Records["first-uid"] != nil || countCalls(service, "RemovePodSandbox") != 1 {
			t.Fatal("blocked execution was not cleaned up on deletion")
		}
	})
	t.Run("pull-failure", func(t *testing.T) {
		t.Parallel()
		first := executionPod("first")
		first.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
		e, service := newExecutor(t, first)
		service.pullError = status.Error(codes.NotFound, "manifest unknown")
		reconcilePasses(t, e, 3)
		pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "first", metav1.GetOptions{})
		condition := blockedCondition(pod)
		if condition == nil || condition.Reason != "ImagePullFailed" || pod.Status.Phase != corev1.PodPending {
			t.Fatalf("pull failure not published on its Pod: %+v", pod.Status)
		}
		if e.recovery.Load() || !nodeReady(t, e) || countCalls(service, "PullImage") != 1 {
			t.Fatal("pull failure raised the node barrier or retried without backoff")
		}
		service.mu.Lock()
		service.pullError = nil
		service.mu.Unlock()
		if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), executionPod("second"), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		reconcilePasses(t, e, 5)
		second, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "second", metav1.GetOptions{})
		if second.Status.Phase != corev1.PodRunning || countCalls(service, "PullImage") != 2 ||
			countCalls(service, "RunPodSandbox") != 1 {
			t.Fatalf("pull failure blocked another admission or retried early: %+v", second.Status)
		}
		e.workers.mu.Lock()
		backoff := e.workers.pulls["first-uid"]
		backoff.next = time.Now().Add(-time.Second)
		e.workers.pulls["first-uid"] = backoff
		e.workers.mu.Unlock()
		reconcilePasses(t, e, 5)
		pod, _ = e.api.CoreV1().Pods("default").Get(t.Context(), "first", metav1.GetOptions{})
		if pod.Status.Phase != corev1.PodRunning || blockedCondition(pod) != nil {
			t.Fatalf("pull retry did not recover the Pod: %+v", pod.Status)
		}
	})
}

func TestPodWorkersPullBackoff(t *testing.T) {
	t.Parallel()
	workers := newPodWorkers()
	now := time.Now()
	previous := time.Duration(0)
	for failure := range 10 {
		delay := workers.pullFailed("uid", now)
		base := min(pullBackoffBase<<min(failure, 5), pullBackoffLimit)
		if delay < base || delay > base+base/2 || delay < previous/2 {
			t.Fatalf("failure %d: delay %s outside [%s, %s]", failure, delay, base, base+base/2)
		}
		if workers.pullAllowed("uid", now) || !workers.pullAllowed("uid", now.Add(delay)) {
			t.Fatal("pull allowed during backoff or refused after it")
		}
		previous = delay
	}
	workers.pullSucceeded("uid")
	if !workers.pullAllowed("uid", now) {
		t.Fatal("successful pull kept its backoff")
	}
}

// TestExecutionWorkersPrioritizeCleanup covers section 6.4 item 4 of
// DESIGN.md: a slow creation call holds only its own Pod, while deletion and
// status work for other Pods proceeds on separate workers and CRI slots.
func TestExecutionWorkersPrioritizeCleanup(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("done"))
	reconcilePasses(t, e, 5)
	gate := make(chan struct{})
	service.mu.Lock()
	service.pullGate = gate
	service.mu.Unlock()
	slow := executionPod("slow")
	if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), slow, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			close(gate)
		}
		e.workers.running.Wait()
	}()
	waitFor := func(what string, done func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !done() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			if err := e.reconcilePass(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("the slow pull", func() bool { return countCalls(service, "PullImage") == 2 })
	pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "done", metav1.GetOptions{})
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	if _, err := e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("cleanup behind the slow pull", func() bool {
		_, err := e.api.CoreV1().Pods("default").Get(t.Context(), "done", metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})
	if countCalls(service, "PullImage") != 2 || !e.workers.busySet()["slow-uid"] {
		t.Fatal("a Pod with an operation in flight was dispatched again")
	}
	if record := e.record("done-uid"); record.Stage != "finished" {
		t.Fatalf("cleanup incomplete: %+v", record)
	}
	close(gate)
	released = true
	e.workers.running.Wait()
	reconcilePasses(t, e, 5)
	running, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "slow", metav1.GetOptions{})
	if running.Status.Phase != corev1.PodRunning || countCalls(service, "PullImage") != 2 {
		t.Fatalf("slow Pod did not complete after its pull: %+v", running.Status)
	}
}

// TestPodWorkersBounds checks one operation per UID and the pool bound.
func TestPodWorkersBounds(t *testing.T) {
	t.Parallel()
	workers := newPodWorkers()
	release := make(chan struct{})
	var pass sync.WaitGroup
	block := func() error { <-release; return nil }
	for i := range creationWorkers {
		if !workers.start(string(rune('a'+i)), workers.creation, &pass, block) {
			t.Fatal("free creation slot refused")
		}
	}
	if workers.start("z", workers.creation, &pass, block) {
		t.Fatal("full creation pool accepted work")
	}
	if workers.start("a", workers.priority, &pass, block) {
		t.Fatal("second operation for a busy Pod accepted")
	}
	if !workers.start("z", workers.priority, &pass, func() error { return context.Canceled }) {
		t.Fatal("full creation pool blocked priority work")
	}
	close(release)
	pass.Wait()
	if !workers.outstanding(func(string) bool { return true }) {
		t.Fatal("operation error not recorded")
	}
	if workers.outstanding(func(string) bool { return false }) {
		t.Fatal("fault kept for an absent Pod")
	}
}

// TestExecutionBarrierClearsAfterCleanPass is a regression test: ambiguity and
// admission errors hold the barrier only until a later pass completes cleanly.
func TestExecutionBarrierClearsAfterCleanPass(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"ambiguous", "admission"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			var failures atomic.Int32
			if kind == "ambiguous" {
				service.sandboxes["claim"] = &runtimeapi.PodSandbox{Id: "claim", CreatedAt: 1,
					Metadata: &runtimeapi.PodSandboxMetadata{Name: "app", Namespace: "default", Uid: "app-uid"}}
			} else {
				e.config.CPU = "1"
				failures.Store(1)
				e.api.(*fake.Clientset).PrependReactor("update", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
					if action.GetSubresource() == "status" && failures.CompareAndSwap(1, 0) {
						return true, nil, apierrors.NewServiceUnavailable("status unavailable")
					}
					return false, nil, nil
				})
			}
			for range 3 {
				if err := e.reconcile(t.Context()); err == nil && kind == "ambiguous" {
					t.Fatal("ambiguous ownership not reported")
				}
			}
			if !e.recovery.Load() && kind == "ambiguous" || failures.Load() != 0 {
				t.Fatal("the injected fault did not occur or did not hold the barrier")
			}
			service.mu.Lock()
			delete(service.sandboxes, "claim")
			service.mu.Unlock()
			e.config.CPU = "4"
			if kind == "admission" {
				if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), executionPod("next"), metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			reconcilePasses(t, e, 6)
			name := map[string]string{"ambiguous": "app", "admission": "next"}[kind]
			pod, err := e.api.CoreV1().Pods("default").Get(t.Context(), name, metav1.GetOptions{})
			if err != nil || pod.Status.Phase != corev1.PodRunning || e.recovery.Load() || !e.synced.Load() {
				t.Fatalf("barrier not cleared after a clean pass: %v %+v", err, pod.Status)
			}
		})
	}
}
