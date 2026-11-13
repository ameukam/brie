// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package events records the node agent's Kubernetes Events (DESIGN 12.3):
// admission refusals, image pull failures and backoff, admission barrier entry
// and exit, Pods whose execution is blocked, liveness failures, and cleanup
// blocked on missing deletion evidence. Events supplement Pod and
// Node status; they never replace it.
//
// Identical Events are deduplicated locally for a window, a token bucket
// bounds the total rate, and client-go's correlator then aggregates and
// spam-filters per object before core/v1 Events are created or patched.
// Under the Node authorizer, a node identity may create, update, and patch
// Events, so no extra RBAC is required.
package events

import (
	"context"
	"sync"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
)

// Stable Event reasons. Admission refusals use the published Pod status
// reason, such as UnsupportedPod or InsufficientResources.
const (
	ReasonImagePullFailed      = "ImagePullFailed"
	ReasonImagePullBackOff     = "ImagePullBackOff"
	ReasonBarrierRaised        = "AdmissionBarrierRaised"
	ReasonBarrierLowered       = "AdmissionBarrierLowered"
	ReasonCleanupBlocked       = "CleanupBlocked"
	ReasonLivenessProbeFailed  = "LivenessProbeFailed"
	ReasonReadinessProbeFailed = "ReadinessProbeFailed"
)

// MaxMessageBytes bounds an Event message.
const MaxMessageBytes = 1024

// Options bounds the recorder. Zero values select the defaults.
type Options struct {
	// DedupeWindow suppresses an identical Event (object, reason, message)
	// seen within the window. Default five minutes.
	DedupeWindow time.Duration
	// QPS and Burst bound all Events from this agent. Default 1 per second, burst 25.
	QPS   float32
	Burst int
	// MaxKeys bounds the deduplication cache. Default 4096.
	MaxKeys int
	// Observe receives "recorded", "deduplicated", or "dropped" for each Event.
	Observe func(result string)
	// Now overrides the clock for tests.
	Now func() time.Time
}

// Recorder emits bounded Events for one node.
type Recorder struct {
	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder
	nodeName    string
	window      time.Duration
	maxKeys     int
	limiter     flowcontrol.RateLimiter
	observe     func(string)
	now         func() time.Time

	mu   sync.Mutex
	seen map[string]time.Time
}

// New starts a recorder writing core/v1 Events through api. Call Shutdown to
// stop it; Events still queued may be lost.
func New(ctx context.Context, api kubernetes.Interface, nodeName string, options Options) *Recorder {
	if options.DedupeWindow <= 0 {
		options.DedupeWindow = 5 * time.Minute
	}
	if options.QPS <= 0 {
		options.QPS = 1
	}
	if options.Burst <= 0 {
		options.Burst = 25
	}
	if options.MaxKeys <= 0 {
		options.MaxKeys = 4096
	}
	if options.Observe == nil {
		options.Observe = func(string) {}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	broadcaster := record.NewBroadcaster(record.WithContext(ctx), record.WithCorrelatorOptions(record.CorrelatorOptions{
		LRUCacheSize: options.MaxKeys, BurstSize: options.Burst, QPS: options.QPS,
	}))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: api.CoreV1().Events("")})
	return &Recorder{
		broadcaster: broadcaster,
		recorder:    broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "brie-node", Host: nodeName}),
		nodeName:    nodeName, window: options.DedupeWindow, maxKeys: options.MaxKeys,
		limiter: flowcontrol.NewTokenBucketRateLimiter(options.QPS, options.Burst),
		observe: options.Observe, now: options.Now, seen: make(map[string]time.Time),
	}
}

// Shutdown stops the broadcaster.
func (r *Recorder) Shutdown() { r.broadcaster.Shutdown() }

// PodRefused records an admission refusal with its published status reason.
func (r *Recorder) PodRefused(pod *corev1.Pod, reason, message string) {
	r.emit(podReference(pod), corev1.EventTypeWarning, reason, message)
}

// ImagePullFailed records a failed image pull for a Pod.
func (r *Recorder) ImagePullFailed(pod *corev1.Pod, message string) {
	r.emit(podReference(pod), corev1.EventTypeWarning, ReasonImagePullFailed, message)
}

// ImagePullBackOff records that a Pod's image pull backs off after a failure.
func (r *Recorder) ImagePullBackOff(pod *corev1.Pod, message string) {
	r.emit(podReference(pod), corev1.EventTypeWarning, ReasonImagePullBackOff, message)
}

// ExecutionBlocked records the stable reason of a Pod's ExecutionBlocked
// condition, such as ExecutionConfigChanged.
func (r *Recorder) ExecutionBlocked(pod *corev1.Pod, reason, message string) {
	r.emit(podReference(pod), corev1.EventTypeWarning, reason, message)
}

// CleanupBlocked records that cleanup waits for positive deletion evidence.
func (r *Recorder) CleanupBlocked(pod *corev1.Pod, message string) {
	r.emit(podReference(pod), corev1.EventTypeWarning, ReasonCleanupBlocked, message)
}

// ProbeFailed records a readiness or liveness failure past its threshold.
func (r *Recorder) ProbeFailed(pod *corev1.Pod, liveness bool, message string) {
	reason := ReasonReadinessProbeFailed
	if liveness {
		reason = ReasonLivenessProbeFailed
	}
	r.emit(podReference(pod), corev1.EventTypeWarning, reason, message)
}

// BarrierRaised records node-level admission barrier entry. The cause must
// be a bounded, non-tenant string; Node Events are readable cluster-wide.
func (r *Recorder) BarrierRaised(cause string) {
	r.emit(r.nodeReference(), corev1.EventTypeWarning, ReasonBarrierRaised, "Admission barrier raised: "+cause)
}

// BarrierLowered records admission barrier exit.
func (r *Recorder) BarrierLowered() {
	r.emit(r.nodeReference(), corev1.EventTypeNormal, ReasonBarrierLowered, "Admission barrier lowered")
}

func podReference(pod *corev1.Pod) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind: "Pod", APIVersion: "v1", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID,
	}
}

// nodeReference follows kubelet, which uses the node name as the reference UID.
func (r *Recorder) nodeReference() *corev1.ObjectReference {
	return &corev1.ObjectReference{Kind: "Node", APIVersion: "v1", Name: r.nodeName, UID: types.UID(r.nodeName)}
}

func (r *Recorder) emit(reference *corev1.ObjectReference, kind, reason, message string) {
	message = truncate(message, MaxMessageBytes)
	key := string(reference.UID) + "\x00" + reference.Kind + "\x00" + reference.Name + "\x00" + reason + "\x00" + message
	if !r.firstInWindow(key) {
		r.observe("deduplicated")
		return
	}
	if !r.limiter.TryAccept() {
		r.observe("dropped")
		return
	}
	r.recorder.Event(reference, kind, reason, message)
	r.observe("recorded")
}

func (r *Recorder) firstInWindow(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if at, found := r.seen[key]; found && now.Sub(at) < r.window {
		return false
	}
	if len(r.seen) >= r.maxKeys {
		for candidate, at := range r.seen {
			if now.Sub(at) >= r.window {
				delete(r.seen, candidate)
			}
		}
		if len(r.seen) >= r.maxKeys {
			clear(r.seen)
		}
	}
	r.seen[key] = now
	return true
}

func truncate(message string, limit int) string {
	if len(message) <= limit {
		return message
	}
	cut := limit - len("...")
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + "..."
}
