// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type results struct {
	mu     sync.Mutex
	counts map[string]int
}

func (r *results) observe(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[result]++
}

func (r *results) get(result string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[result]
}

func testPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "team", UID: "uid-1"}}
}

func waitEvents(t *testing.T, client *fake.Clientset, namespace string, want int) []corev1.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		list, err := client.CoreV1().Events(namespace).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Items) >= want {
			return list.Items
		}
		if time.Now().After(deadline) {
			t.Fatalf("events in %s = %d, want %d", namespace, len(list.Items), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRecorderEmitsBoundedEvents(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	counts := &results{counts: map[string]int{}}
	recorder := New(context.Background(), client, "worker-1", Options{Observe: counts.observe})
	defer recorder.Shutdown()
	pod := testPod()
	recorder.PodRefused(pod, "InsufficientResources", "CPU budget exhausted")
	recorder.ImagePullFailed(pod, "pull failed: NotFound")
	recorder.CleanupBlocked(pod, "waiting for positive deletion evidence")
	recorder.ProbeFailed(pod, true, "liveness probe failed 3 times")
	recorder.ProbeFailed(pod, false, "readiness probe failed")
	recorder.BarrierRaised("UnresolvedExecution")
	recorder.BarrierLowered()
	podEvents := waitEvents(t, client, "team", 5)
	reasons := map[string]corev1.Event{}
	for _, event := range podEvents {
		reasons[event.Reason] = event
	}
	for _, reason := range []string{"InsufficientResources", ReasonImagePullFailed, ReasonCleanupBlocked, ReasonLivenessProbeFailed, ReasonReadinessProbeFailed} {
		event, found := reasons[reason]
		if !found {
			t.Fatalf("missing Pod Event %s in %v", reason, podEvents)
		}
		if event.InvolvedObject.UID != "uid-1" || event.Type != corev1.EventTypeWarning ||
			event.Source.Component != "brie-node" || event.Source.Host != "worker-1" {
			t.Fatalf("event %s = %+v", reason, event)
		}
	}
	nodeEvents := waitEvents(t, client, metav1.NamespaceDefault, 2)
	for _, event := range nodeEvents {
		if event.InvolvedObject.Kind != "Node" || event.InvolvedObject.Name != "worker-1" {
			t.Fatalf("node event = %+v", event.InvolvedObject)
		}
	}
	if counts.get("recorded") != 7 {
		t.Fatalf("recorded = %d", counts.get("recorded"))
	}
}

func TestRecorderDeduplicates(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	client := fake.NewClientset()
	counts := &results{counts: map[string]int{}}
	recorder := New(context.Background(), client, "worker-1", Options{Observe: counts.observe, Now: clock, DedupeWindow: time.Minute})
	defer recorder.Shutdown()
	pod := testPod()
	for range 10 {
		recorder.ImagePullFailed(pod, "pull failed")
	}
	if counts.get("recorded") != 1 || counts.get("deduplicated") != 9 {
		t.Fatalf("counts = %v", counts.counts)
	}
	clockMu.Lock()
	now = now.Add(time.Minute)
	clockMu.Unlock()
	recorder.ImagePullFailed(pod, "pull failed")
	if counts.get("recorded") != 2 {
		t.Fatalf("after window recorded = %d", counts.get("recorded"))
	}
	waitEvents(t, client, "team", 1)
}

func TestRecorderRateLimit(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	counts := &results{counts: map[string]int{}}
	recorder := New(context.Background(), client, "worker-1", Options{Observe: counts.observe, QPS: 0.001, Burst: 3})
	defer recorder.Shutdown()
	pod := testPod()
	for i := range 10 {
		recorder.PodRefused(pod, "UnsupportedPod", strings.Repeat("x", i+1))
	}
	if counts.get("recorded") != 3 || counts.get("dropped") != 7 {
		t.Fatalf("counts = %v", counts.counts)
	}
}

func TestDedupeCacheBound(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	recorder := New(context.Background(), fake.NewClientset(), "worker-1", Options{MaxKeys: 4, Now: func() time.Time { return now }, QPS: 1000, Burst: 1000})
	defer recorder.Shutdown()
	for i := range 10 {
		if !recorder.firstInWindow(strings.Repeat("k", i+1)) {
			t.Fatal("new key deduplicated")
		}
		if len(recorder.seen) > 4 {
			t.Fatalf("cache size %d exceeds bound", len(recorder.seen))
		}
	}
	for i := 0; len(recorder.seen) < 4; i++ {
		recorder.firstInWindow(strings.Repeat("f", i+1))
	}
	now = now.Add(time.Hour)
	recorder.firstInWindow("fresh")
	if len(recorder.seen) != 1 {
		t.Fatalf("expired keys not evicted: %d", len(recorder.seen))
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		message string
		limit   int
		want    string
	}{
		{"short", 10, "short"},
		{"0123456789abc", 10, "0123456..."},
		{"ééééé", 8, "éé..."},
	} {
		got := truncate(tt.message, tt.limit)
		if got != tt.want || len(got) > tt.limit || !utf8.ValidString(got) {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.message, tt.limit, got, tt.want)
		}
	}
}
