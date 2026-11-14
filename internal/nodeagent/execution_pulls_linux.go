// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ameukam/brie/internal/cri"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Image pull workers (section 10.4 of DESIGN.md). Pulls run outside Pod
// reconciliation on their own CRI client with pullWorkers call slots and the
// imagePullDeadline per call. The image size is unknown before the pull, so
// the deadline is one implementation constant, not a value sized to the image.
// A failed pull backs off per image reference: the delay doubles from the base
// up to the limit, with up to 50% added jitter, and never exceeds the limit.
const (
	pullWorkers       = 2
	imagePullDeadline = 10 * time.Minute
	pullBackoffBase   = 10 * time.Second
	pullBackoffLimit  = 5 * time.Minute
	// maxPullMessage bounds the runtime error text published on a Pod.
	maxPullMessage = 512
)

// Container waiting reasons published while a Pod waits for its image. They
// match the reasons kubelet publishes for the same states.
const (
	reasonPulling     = "ContainerCreating"
	reasonErrPull     = "ErrImagePull"
	reasonPullBackOff = "ImagePullBackOff"
)

// imagePull is the state of one image reference. At most one pull per
// reference is in flight; every Pod that needs the reference waits on it.
type imagePull struct {
	running  bool
	failures int
	next     time.Time
	err      string
	// waiters maps a Pod UID to the waiting reason last returned for it. A
	// failed attempt clears the reasons so each waiter reports ErrImagePull
	// once before ImagePullBackOff.
	waiters map[string]string
}

// imagePuller runs bounded, deduplicated image pulls. A pull failure is
// confined to the Pods that wait on that image; it never changes Node
// readiness or the admission barrier.
type imagePuller struct {
	client *cri.Client
	// wake, when set, is signaled after each finished pull.
	wake  chan<- struct{}
	mu    sync.Mutex
	pulls map[string]*imagePull
	ready map[string]string
	// pulled maps a Pod UID to the image ID that a successful pull returned
	// for it. Image collection protects that ID until retain drops the UID.
	pulled  map[string]string
	slots   chan struct{}
	running sync.WaitGroup
}

func newImagePuller(client *cri.Client) *imagePuller {
	return &imagePuller{
		client: client, pulls: make(map[string]*imagePull), ready: make(map[string]string),
		pulled: make(map[string]string), slots: make(chan struct{}, pullWorkers),
	}
}

// pullBackoff returns the jittered delay after the given number of earlier
// failures of the same image reference.
func pullBackoff(failures int) time.Duration {
	delay := min(pullBackoffBase<<min(failures, 5), pullBackoffLimit)
	return min(pullBackoffLimit, delay+rand.N(delay/2+1))
}

// unchanged reports whether uid already reported the current state of its
// pending image, so the caller can skip API and runtime reads until a pull
// finishes or its backoff expires.
func (p *imagePuller) unchanged(uid, image string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	pull := p.pulls[image]
	if p.ready[uid] == image || pull == nil {
		return false
	}
	last := pull.waiters[uid]
	return (last == reasonPulling || last == reasonPullBackOff) && (pull.running || now.Before(pull.next))
}

// request asks for image on behalf of uid. It returns true once a pull that uid
// waited on succeeded; otherwise it returns the container waiting reason and
// message to publish. ctx bounds a pull it starts and must outlive the caller's
// operation, normally the agent's lifetime. The caller holds e.imageGate for
// reading, so no pull starts while image collection holds the gate.
func (p *imagePuller) request(ctx context.Context, uid, image string, sandbox *runtimeapi.PodSandboxConfig, now time.Time) (bool, string, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready[uid] == image {
		delete(p.ready, uid)
		return true, "", ""
	}
	pull := p.pulls[image]
	if pull == nil {
		pull = &imagePull{waiters: make(map[string]string)}
		p.pulls[image] = pull
	}
	last := pull.waiters[uid]
	if !pull.running && !now.Before(pull.next) {
		select {
		case p.slots <- struct{}{}:
			pull.running = true
			p.running.Add(1)
			go p.pull(ctx, image, sandbox)
		default:
		}
	}
	reason, message := reasonPulling, "Pulling image "+image
	switch {
	case !pull.running && pull.failures == 0:
		message = "Waiting for an image pull worker for " + image
	case pull.failures != 0 && !pull.running && last != reasonErrPull && last != reasonPullBackOff:
		reason, message = reasonErrPull, pull.err
	case pull.failures != 0:
		reason, message = reasonPullBackOff, "Back-off pulling image "+image+": "+pull.err
	}
	pull.waiters[uid] = reason
	return false, reason, message
}

func (p *imagePuller) pull(ctx context.Context, image string, sandbox *runtimeapi.PodSandboxConfig) {
	defer p.running.Done()
	id, err := p.client.PullImage(ctx, image, sandbox)
	p.mu.Lock()
	<-p.slots
	pull := p.pulls[image]
	pull.running = false
	switch {
	case err == nil:
		for uid := range pull.waiters {
			p.ready[uid] = image
			p.pulled[uid] = id
		}
		delete(p.pulls, image)
	case ctx.Err() == nil:
		pull.next = time.Now().Add(pullBackoff(pull.failures))
		pull.failures++
		pull.err = pullMessage(err)
		for uid := range pull.waiters {
			pull.waiters[uid] = ""
		}
	}
	p.mu.Unlock()
	if p.wake != nil {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

// retain drops the pull state of Pod UIDs for which keep is false, and the
// state of idle image references without waiters.
func (p *imagePuller) retain(keep func(uid string) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for uid := range p.ready {
		if !keep(uid) {
			delete(p.ready, uid)
		}
	}
	for uid := range p.pulled {
		if !keep(uid) {
			delete(p.pulled, uid)
		}
	}
	for image, pull := range p.pulls {
		for uid := range pull.waiters {
			if !keep(uid) {
				delete(pull.waiters, uid)
			}
		}
		if !pull.running && len(pull.waiters) == 0 {
			delete(p.pulls, image)
		}
	}
}

// collectionView reports whether any pull is running and returns the image
// IDs that finished pulls returned for Pods still retained.
func (p *imagePuller) collectionView() (bool, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	running := false
	for _, pull := range p.pulls {
		running = running || pull.running
	}
	return running, slices.Collect(maps.Values(p.pulled))
}

// wait blocks until every started pull has finished.
func (p *imagePuller) wait() { p.running.Wait() }

// pullMessage bounds runtime error text before it is published on a Pod.
func pullMessage(err error) string {
	message := err.Error()
	return strings.ToValidUTF8(message[:min(len(message), maxPullMessage)], "")
}

// imageReady reports whether the Pod's image is present or was pulled on the
// Pod's behalf. Otherwise it publishes the container waiting reason and
// returns false. The caller holds e.imageGate for reading, so image collection
// cannot remove an image between this observation and the journal record
// that protects it. An ImageStatus error leaves presence unknown and requests
// a pull, which CRI defines as idempotent for a present image.
func (e *executor) imageReady(ctx context.Context, runtime *cri.Client, pod *corev1.Pod, image string, sandbox *runtimeapi.PodSandboxConfig) (bool, error) {
	present, err := runtime.ImageStatus(ctx, image)
	if err == nil && present != nil {
		return true, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	ready, reason, message := e.pulls.request(ctx, string(pod.UID), image, sandbox, time.Now())
	if ready {
		return true, nil
	}
	return false, e.publishImageWaiting(ctx, pod, reason, message)
}

// publishImageWaiting reports a Pending Pod whose container waits for its
// image. It writes only when the waiting state changes.
func (e *executor) publishImageWaiting(ctx context.Context, expected *corev1.Pod, reason, message string) error {
	pods := e.api.CoreV1().Pods(expected.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := pods.Get(ctx, expected.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pod.UID != expected.UID || pod.Spec.NodeName != e.node.NodeName || len(pod.Spec.Containers) != 1 {
			return errors.New("refusing status for a changed Pod identity or assignment")
		}
		if terminalPod(pod) {
			return nil
		}
		before := pod.Status.DeepCopy()
		spec := pod.Spec.Containers[0]
		pod.Status.Phase, pod.Status.HostIP = corev1.PodPending, e.config.NodeIP
		if pod.Status.StartTime == nil {
			now := metav1.Now()
			pod.Status.StartTime = &now
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: spec.Name, Image: spec.Image,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message}},
		}}
		if equality.Semantic.DeepEqual(before, &pod.Status) {
			return nil
		}
		_, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		return err
	})
}
