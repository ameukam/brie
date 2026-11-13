// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// fakePodWatches serves the agent's Pod watches from test-controlled fake
// watchers and records each watch's options and the number of Pod lists.
type fakePodWatches struct {
	mu       sync.Mutex
	options  []metav1.ListOptions
	lists    int
	listErrs []error
	errs     []error
	watchers chan *watch.RaceFreeFakeWatcher
}

func watchPods(e *executor) *fakePodWatches {
	fakes := &fakePodWatches{watchers: make(chan *watch.RaceFreeFakeWatcher, 16)}
	api := e.api.(*fake.Clientset)
	api.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		fakes.mu.Lock()
		defer fakes.mu.Unlock()
		fakes.lists++
		if len(fakes.listErrs) != 0 {
			err := fakes.listErrs[0]
			fakes.listErrs = fakes.listErrs[1:]
			return true, nil, err
		}
		return false, nil, nil
	})
	api.PrependWatchReactor("pods", func(action ktesting.Action) (bool, watch.Interface, error) {
		fakes.mu.Lock()
		defer fakes.mu.Unlock()
		fakes.options = append(fakes.options, action.(ktesting.WatchActionImpl).GetListOptions())
		if len(fakes.errs) != 0 {
			err := fakes.errs[0]
			fakes.errs = fakes.errs[1:]
			return true, nil, err
		}
		watcher := watch.NewRaceFreeFake()
		fakes.watchers <- watcher
		return true, watcher, nil
	})
	return fakes
}

func (f *fakePodWatches) next(t *testing.T) (*watch.RaceFreeFakeWatcher, metav1.ListOptions) {
	t.Helper()
	select {
	case watcher := <-f.watchers:
		f.mu.Lock()
		defer f.mu.Unlock()
		return watcher, f.options[len(f.options)-1]
	case <-time.After(5 * time.Second):
		t.Fatal("no Pod watch was opened")
		return nil, metav1.ListOptions{}
	}
}

func (f *fakePodWatches) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists
}

// startWatch runs the assigned-Pod watch of e with short test intervals.
func startWatch(t *testing.T, e *executor, resync time.Duration) *assignedPodWatch {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	view := newAssignedPodWatch(make(chan struct{}, 1))
	view.base, view.limit, view.resync = time.Millisecond, 5*time.Millisecond, resync
	e.watch = view
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.watchAssigned(ctx, view)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return view
}

func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func viewed(view *assignedPodWatch, uid string) bool {
	pods, _, _, err := view.snapshot()
	for _, pod := range pods {
		if string(pod.UID) == uid {
			return err == nil
		}
	}
	return false
}

func versioned(pod *corev1.Pod, version string) *corev1.Pod {
	pod = pod.DeepCopy()
	pod.ResourceVersion = version
	return pod
}

// TestExecutionWatchAssignment covers watch-based assignment (sections 6.2,
// 6.3 rule 1, and 15.1 of DESIGN.md): the node-scoped watch with bookmarks
// feeds the passes, and a DELETED event is journaled as deletion evidence
// before cleanup.
func TestExecutionWatchAssignment(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t)
	fakes := watchPods(e)
	view := startWatch(t, e, time.Hour)
	watcher, options := fakes.next(t)
	if options.FieldSelector != "spec.nodeName=worker-1" || !options.AllowWatchBookmarks ||
		options.ResourceVersion == "" || options.ResourceVersion != view.currentVersion() {
		t.Fatalf("watch not node-scoped with bookmarks from the list version: %+v", options)
	}
	pod := executionPod("app")
	if _, err := e.api.CoreV1().Pods("default").Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	watcher.Add(versioned(pod, "100"))
	eventually(t, "the added Pod", func() bool { return viewed(view, "app-uid") })
	reconcilePasses(t, e, 5)
	record := e.journal.Records["app-uid"]
	if record == nil || record.Stage != "running" {
		t.Fatalf("watched Pod not executed: %+v", record)
	}
	// The API object stays readable, so only the DELETED event can prove
	// deletion; Pod reads are denied as the Node authorizer would.
	gets := denyPodReads(e)
	watcher.Delete(versioned(pod, "200"))
	eventually(t, "the deleted Pod", func() bool { return view.currentVersion() == "200" })
	reconcilePasses(t, e, 1)
	if record.DeletionObserved != "200" || record.Stage != "finished" || *gets != 0 ||
		countCalls(service, "RemovePodSandbox") != 1 {
		t.Fatalf("DELETED event not journaled as deletion evidence before cleanup: reads=%d %+v", *gets, record)
	}
	reconcilePasses(t, e, 1)
	if e.journal.Records["app-uid"] != nil || fakes.listCount() != 1 {
		t.Fatalf("record not compacted or passes listed Pods: lists=%d", fakes.listCount())
	}
}

func TestExecutionWatchRecovery(t *testing.T) {
	t.Parallel()
	expired := apierrors.NewResourceExpired("too old resource version")
	for _, tc := range []struct {
		name string
		// break disrupts the first watch stream.
		disrupt func(*testing.T, *executor, *fakePodWatches, *watch.RaceFreeFakeWatcher)
		resync  time.Duration
		// relist reports whether recovery must list the Pods again; version
		// is the resource version the next watch must resume from otherwise.
		relist  bool
		version string
	}{
		{
			name: "bookmark then server close resumes from the bookmark",
			disrupt: func(t *testing.T, e *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Action(watch.Bookmark, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "150"}})
				eventually(t, "the bookmark", func() bool { return e.watch.currentVersion() == "150" })
				watcher.Stop()
			},
			version: "150",
		},
		{
			name: "transient watch failure resumes without a relist",
			disrupt: func(t *testing.T, e *executor, fakes *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Add(versioned(executionPod("app"), "120"))
				eventually(t, "the added Pod", func() bool { return viewed(e.watch, "app-uid") })
				fakes.mu.Lock()
				fakes.errs = append(fakes.errs, apierrors.NewServiceUnavailable("apiserver restarting"))
				fakes.mu.Unlock()
				watcher.Stop()
			},
			version: "120",
		},
		{
			name: "expired resource version event relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Error(&metav1.Status{Status: metav1.StatusFailure, Code: http.StatusGone, Reason: metav1.StatusReasonExpired})
			},
			relist: true,
		},
		{
			name: "gone status without a reason relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Error(&metav1.Status{Status: metav1.StatusFailure, Code: http.StatusGone})
			},
			relist: true,
		},
		{
			name: "expired resource version on watch relists",
			disrupt: func(_ *testing.T, _ *executor, fakes *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				fakes.mu.Lock()
				fakes.errs = append(fakes.errs, expired)
				fakes.mu.Unlock()
				watcher.Stop()
			},
			relist: true,
		},
		{
			name: "other watch error event relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Error(&metav1.Status{Status: metav1.StatusFailure, Code: http.StatusInternalServerError})
			},
			relist: true,
		},
		{
			name: "invalid assignment relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				pod := versioned(executionPod("other"), "130")
				pod.Spec.NodeName = "worker-2"
				watcher.Add(pod)
			},
			relist: true,
		},
		{
			name: "deleted UID reappearing relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Delete(versioned(executionPod("app"), "130"))
				watcher.Add(versioned(executionPod("app"), "131"))
			},
			relist: true,
		},
		{
			name: "event without a resource version relists",
			disrupt: func(_ *testing.T, _ *executor, _ *fakePodWatches, watcher *watch.RaceFreeFakeWatcher) {
				watcher.Add(executionPod("app"))
			},
			relist: true,
		},
		{
			name:    "periodic full resync relists",
			disrupt: func(*testing.T, *executor, *fakePodWatches, *watch.RaceFreeFakeWatcher) {},
			resync:  50 * time.Millisecond,
			relist:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, _ := newExecutor(t)
			fakes := watchPods(e)
			resync := tc.resync
			if resync == 0 {
				resync = time.Hour
			}
			startWatch(t, e, resync)
			watcher, _ := fakes.next(t)
			tc.disrupt(t, e, fakes, watcher)
			_, options := fakes.next(t)
			if tc.relist {
				if fakes.listCount() < 2 || options.ResourceVersion != e.watch.currentVersion() {
					t.Fatalf("watch did not relist: lists=%d options=%+v", fakes.listCount(), options)
				}
			} else if fakes.listCount() != 1 || options.ResourceVersion != tc.version {
				t.Fatalf("watch did not resume from %s: lists=%d options=%+v", tc.version, fakes.listCount(), options)
			}
			eventually(t, "a usable view", func() bool {
				_, _, _, err := e.watch.snapshot()
				return err == nil
			})
		})
	}
}

// TestExecutionWatchUnavailableBlocksPasses covers the startup barrier with a
// watch: without a complete list, a pass mutates nothing and stays unsynced.
func TestExecutionWatchUnavailableBlocksPasses(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	fakes := watchPods(e)
	fakes.listErrs = []error{apierrors.NewServiceUnavailable("etcd unavailable"), apierrors.NewServiceUnavailable("etcd unavailable")}
	startWatch(t, e, time.Hour)
	if err := e.reconcile(t.Context()); err == nil && fakes.listCount() < 3 {
		t.Fatal("pass ran without a synchronized assigned-Pod view")
	}
	fakes.next(t)
	if fakes.listCount() != 3 {
		t.Fatalf("failed lists were not retried with backoff: %d", fakes.listCount())
	}
	reconcilePasses(t, e, 5)
	if record := e.journal.Records["app-uid"]; record == nil || record.Stage != "running" ||
		countCalls(service, "RunPodSandbox") != 1 {
		t.Fatalf("Pod not executed after the view synchronized: %+v", record)
	}
}

func TestAssignedPodWatchRejectsUnusableView(t *testing.T) {
	t.Parallel()
	view := newAssignedPodWatch(make(chan struct{}, 1))
	if _, _, _, err := view.snapshot(); err == nil {
		t.Fatal("view usable before the first list")
	}
	view.replace(nil, "10")
	for _, tc := range []struct {
		name  string
		event watch.Event
	}{
		{"unexpected object", watch.Event{Type: watch.Added, Object: &corev1.Node{}}},
		{"unknown event type", watch.Event{Type: "UNKNOWN", Object: versioned(executionPod("app"), "11")}},
		{"oversized resource version", watch.Event{Type: watch.Added,
			Object: versioned(executionPod("app"), strings.Repeat("9", maxResourceVersion+1))}},
		{"empty UID", watch.Event{Type: watch.Modified, Object: func() *corev1.Pod {
			pod := versioned(executionPod("app"), "11")
			pod.UID = ""
			return pod
		}()}},
	} {
		if err := view.apply("worker-1", tc.event); !errors.Is(err, errRelist) {
			t.Fatalf("%s: expected a relist, got %v", tc.name, err)
		}
	}
	if err := view.apply("worker-1", watch.Event{Type: watch.Error, Object: &metav1.Status{Code: http.StatusInternalServerError}}); err == nil ||
		errors.Is(err, errRelist) {
		t.Fatalf("non-expiry watch error misclassified: %v", err)
	}
}

func TestWatchBackoff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		failures int
		low      time.Duration
	}{
		{1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second}, {5, 16 * time.Second},
		{6, 30 * time.Second}, {50, 30 * time.Second},
	} {
		for range 20 {
			delay := watchBackoff(tc.failures, time.Second, 30*time.Second)
			if delay < tc.low || delay > 30*time.Second || delay > tc.low*3/2 {
				t.Fatalf("failures=%d: delay %s outside [%s, min(30s, 1.5x)]", tc.failures, delay, tc.low)
			}
		}
	}
}
