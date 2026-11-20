// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
)

// Assigned-Pod watch intervals (sections 6.2 and 15.1 of DESIGN.md). The
// watch replaces the periodic API list: a complete quorum-consistent list
// seeds the view, a node-scoped watch with bookmarks keeps it current, and a
// full relist runs every assignedResync as a safety net and whenever the watch
// cannot resume. inventoryInterval still bounds how long a runtime change,
// such as a container exit, waits for a pass; it reads only the local CRI
// inventory and the watched view, not the API. These are implementation
// constants, not tuned values or a detection SLO.
const (
	inventoryInterval  = 2 * time.Second
	assignedResync     = 5 * time.Minute
	watchBackoffBase   = time.Second
	watchBackoffLimit  = 30 * time.Second
	maxAssignedPodView = 10000
)

// assignedPodWatch is the node's view of its assigned Pods. The view is
// usable only while it derives from a complete, quorum-consistent list
// followed by every watch event since that list without a gap. Absence of a
// Pod UID from a usable view is then positive deletion evidence (section 6.3
// of DESIGN.md, rule 1).
type assignedPodWatch struct {
	mu sync.Mutex
	// usable is false until the first list and whenever the watch stream is
	// broken and has not yet resumed or been replaced by a relist.
	usable  bool
	version string
	pods    map[string]*corev1.Pod
	// deleted maps a Pod UID to the resource version of the watch DELETED
	// event that removed it since the last list.
	deleted map[string]string
	// wake is signaled after every change to the view.
	wake chan<- struct{}
	// base, limit, and resync are the intervals above; tests shorten them.
	base, limit, resync time.Duration
	// failures counts consecutive list or watch attempts that failed or
	// ended without progress; it sets the next backoff delay.
	failures int
}

func newAssignedPodWatch(wake chan<- struct{}) *assignedPodWatch {
	return &assignedPodWatch{wake: wake, base: watchBackoffBase, limit: watchBackoffLimit, resync: assignedResync}
}

// snapshot returns copies of the viewed Pods in reconciliation order, the
// resource version of the view, and the watch deletion evidence.
func (w *assignedPodWatch) snapshot() ([]corev1.Pod, string, map[string]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.usable {
		return nil, "", nil, errors.New("assigned-Pod watch is not synchronized")
	}
	pods := make([]corev1.Pod, 0, len(w.pods))
	for _, pod := range w.pods {
		pods = append(pods, *pod.DeepCopy())
	}
	sortAssignedPods(pods)
	deleted := make(map[string]string, len(w.deleted))
	for uid, version := range w.deleted {
		deleted[uid] = version
	}
	return pods, w.version, deleted, nil
}

func (w *assignedPodWatch) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// replace installs a complete list as the view and drops watch evidence that
// the list supersedes.
func (w *assignedPodWatch) replace(pods []corev1.Pod, version string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pods = make(map[string]*corev1.Pod, len(pods))
	for i := range pods {
		w.pods[string(pods[i].UID)] = pods[i].DeepCopy()
	}
	w.deleted = make(map[string]string)
	w.version, w.usable = version, true
	w.notify()
}

func (w *assignedPodWatch) setUsable(usable bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.usable = usable
}

func (w *assignedPodWatch) currentVersion() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.version
}

// backoff counts one more failed attempt and returns the delay before the
// next one.
func (w *assignedPodWatch) backoff() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures++
	return watchBackoff(w.failures, w.base, w.limit)
}

// progressed resets the failure count after a watch stream applied an event
// or stayed open until its deadline. Opening a stream or completing a list is
// not progress, so streams that break at once keep backing off.
func (w *assignedPodWatch) progressed() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures = 0
}

func (w *assignedPodWatch) failureCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failures
}

// errRelist reports that the watch cannot resume from its resource version
// or delivered an event the view cannot apply; only a full relist recovers.
var errRelist = errors.New("assigned-Pod watch requires a full relist")

// apply applies one watch event to the view.
func (w *assignedPodWatch) apply(node string, event watch.Event) error {
	if event.Type == watch.Error {
		status := apierrors.FromObject(event.Object)
		if expiredVersion(status) {
			return fmt.Errorf("%w: %w", errRelist, status)
		}
		return fmt.Errorf("assigned-Pod watch error: %w", status)
	}
	pod, ok := event.Object.(*corev1.Pod)
	if !ok {
		return fmt.Errorf("%w: unexpected watch object %T", errRelist, event.Object)
	}
	version := pod.ResourceVersion
	if version == "" || len(version) > maxResourceVersion {
		return fmt.Errorf("%w: watch event without a usable resource version", errRelist)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch event.Type {
	case watch.Bookmark:
		w.version = version
		return nil
	case watch.Added, watch.Modified, watch.Deleted:
	default:
		return fmt.Errorf("%w: unknown watch event %q", errRelist, event.Type)
	}
	uid := string(pod.UID)
	if uid == "" || pod.Spec.NodeName != node {
		return fmt.Errorf("%w: watch event with an invalid assignment or UID", errRelist)
	}
	if event.Type == watch.Deleted {
		delete(w.pods, uid)
		w.deleted[uid] = version
	} else {
		if w.deleted[uid] != "" {
			return fmt.Errorf("%w: deleted Pod UID reappeared", errRelist)
		}
		w.pods[uid] = pod.DeepCopy()
	}
	if len(w.pods) > maxAssignedPodView || len(w.deleted) > maxAssignedPodView {
		return fmt.Errorf("%w: assigned-Pod view exceeds the bounded reconstruction limit", errRelist)
	}
	w.version = version
	w.notify()
	return nil
}

// expiredVersion reports whether err says the requested resource version is
// no longer available (HTTP 410 Gone or reason Expired).
func expiredVersion(err error) bool {
	var status apierrors.APIStatus
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err) ||
		errors.As(err, &status) && status.Status().Code == http.StatusGone
}

// watchBackoff returns the delay before retry attempt failures (starting at
// 1): base doubled per failure up to limit, plus up to 50% jitter, never more
// than limit.
func watchBackoff(failures int, base, limit time.Duration) time.Duration {
	delay := base
	for i := 1; i < failures && delay < limit; i++ {
		delay *= 2
	}
	delay = min(delay, limit)
	return min(limit, delay+rand.N(delay/2+1))
}

// watchAssigned keeps the view current until ctx ends. Each cycle lists the
// assigned Pods completely, then watches from the list's resource version
// with bookmarks. A closed stream resumes from the latest resource version; an
// expired version, an inapplicable event, or the resync deadline starts a new
// cycle. Failures back off with bounded exponential delays.
func (e *executor) watchAssigned(ctx context.Context, w *assignedPodWatch) {
	wait := func() bool {
		timer := time.NewTimer(w.backoff())
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	for ctx.Err() == nil {
		pods, version, err := e.assignedPods(ctx)
		if err == nil && (version == "" || len(version) > maxResourceVersion) {
			err = errors.New("assigned-Pod list has no usable resource version")
		}
		if err != nil {
			w.setUsable(false)
			e.logger.Print(err)
			if !wait() {
				return
			}
			continue
		}
		w.replace(pods, version)
		err = e.watchUntilRelist(ctx, w, time.Now().Add(w.resync), wait)
		if err != nil && ctx.Err() == nil {
			e.logger.Print(err)
			if !wait() {
				return
			}
		}
	}
}

// watchUntilRelist streams events into the view until the resync deadline,
// ctx end, or an error that requires a relist.
func (e *executor) watchUntilRelist(ctx context.Context, w *assignedPodWatch, deadline time.Time, wait func() bool) error {
	selector := fields.OneTermEqualSelector("spec.nodeName", e.node.NodeName).String()
	for ctx.Err() == nil {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		timeout := max(int64(1), int64(remaining/time.Second))
		stream, err := e.api.CoreV1().Pods(corev1.NamespaceAll).Watch(ctx, metav1.ListOptions{
			FieldSelector: selector, ResourceVersion: w.currentVersion(), AllowWatchBookmarks: true,
			TimeoutSeconds: &timeout,
		})
		if err != nil {
			w.setUsable(false)
			if expiredVersion(err) {
				return fmt.Errorf("%w: %w", errRelist, err)
			}
			e.logger.Printf("watch assigned Pods: %v", err)
			if !wait() {
				return nil
			}
			continue
		}
		// The stream resumes exactly after the view's resource version, so
		// the view stays usable across reconnects.
		w.setUsable(true)
		events, err := consumeWatch(ctx, w, e.node.NodeName, stream, deadline)
		stream.Stop()
		if events != 0 || !time.Now().Before(deadline) {
			w.progressed()
		}
		if err != nil {
			w.setUsable(false)
			return err
		}
		// A stream that closes without any event before the deadline is
		// retried with backoff rather than in a tight loop.
		if events == 0 && time.Now().Before(deadline) && !wait() {
			return nil
		}
	}
	return nil
}

// consumeWatch applies events until the stream closes, ctx ends, the deadline
// passes, or an event fails. It returns the number of events applied.
func consumeWatch(ctx context.Context, w *assignedPodWatch, node string, stream watch.Interface, deadline time.Time) (int, error) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for events := 0; ; events++ {
		select {
		case <-ctx.Done():
			return events, nil
		case <-timer.C:
			return events, nil
		case event, ok := <-stream.ResultChan():
			if !ok {
				return events, nil
			}
			if err := w.apply(node, event); err != nil {
				return events, err
			}
		}
	}
}

// sortAssignedPods orders deleting Pods first, then the rest by creation time,
// so admission runs in creation order.
func sortAssignedPods(pods []corev1.Pod) {
	sort.SliceStable(pods, func(i, j int) bool {
		if (pods[i].DeletionTimestamp != nil) != (pods[j].DeletionTimestamp != nil) {
			return pods[i].DeletionTimestamp != nil
		}
		if !pods[i].CreationTimestamp.Equal(&pods[j].CreationTimestamp) {
			return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
		}
		// Equal timestamps keep the API server's list order.
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		return pods[i].Name < pods[j].Name
	})
}
