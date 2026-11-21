// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/probe"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// journalSaved reports the journal after a durable save to the Journal hook.
// The journal calls it from save, so the caller holds e.mu or runs before the
// workers start.
func (e *executor) journalSaved(bytes int) {
	if e.hooks.Journal == nil {
		return
	}
	state := JournalState{Records: len(e.journal.Records), Bytes: bytes, PendingSince: make(map[string]time.Time)}
	for _, record := range e.journal.Records {
		if record.Stage == "finished" {
			continue
		}
		state.ReservedCPUMillicores += record.CPU
		state.ReservedMemoryBytes += record.Memory
		state.ReservedExecutions++
		if !pendingStage(record.Stage) {
			continue
		}
		oldest, found := state.PendingSince[record.Stage]
		// An unknown entry time (zero) is the oldest possible.
		if !found || !oldest.IsZero() && (record.PendingSince.IsZero() || record.PendingSince.Before(oldest)) {
			state.PendingSince[record.Stage] = record.PendingSince
		}
	}
	e.hooks.Journal(state)
}

// recordPod is the reference of a journaled execution whose Pod object may
// no longer exist. It carries no spec.
func recordPod(record *executionRecord) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: record.Namespace, Name: record.Name, UID: types.UID(record.UID),
	}}
}

// cleanupAmbiguous is the CleanupBlocked message for an execution whose
// runtime ownership is ambiguous.
const cleanupAmbiguous = "Cleanup waits: runtime ownership is ambiguous; execution preserved for recovery"

// profileFeatures returns the Pod profile increments this executor enforces.
func (e *executor) profileFeatures() []ProfileFeature {
	if e.probes == nil {
		return nil
	}
	return []ProfileFeature{ProfileProbes}
}

// probeReady reports whether a Pod's readiness probe allows Ready. A Pod
// without a readiness probe is ready on runtime evidence alone. A Pod with one
// is ready only after the probe manager observed success: a probe that has not
// run, or no probe manager, never counts as success.
func (e *executor) probeReady(pod *corev1.Pod) bool {
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].ReadinessProbe == nil {
		return true
	}
	if e.probes == nil {
		return false
	}
	status, found := e.probes.Status(string(pod.UID))
	return found && status.Ready
}

// trackProbes starts the Pod's probes for its running container once. The
// container ID of an execution never changes, because Brie never restarts it.
func (e *executor) trackProbes(pod *corev1.Pod, sandbox *runtimeapi.PodSandboxStatus, container *runtimeapi.ContainerStatus) error {
	if e.probes == nil || len(pod.Spec.Containers) != 1 {
		return nil
	}
	spec := pod.Spec.Containers[0]
	if spec.ReadinessProbe == nil && spec.LivenessProbe == nil {
		return nil
	}
	if _, found := e.probes.Status(string(pod.UID)); found {
		return nil
	}
	target := probe.Target{
		PodUID: string(pod.UID), ContainerID: container.Id, Ports: spec.Ports,
		Readiness: spec.ReadinessProbe, Liveness: spec.LivenessProbe,
	}
	if ip, err := netip.ParseAddr(sandbox.GetNetwork().GetIp()); err == nil && ip.IsGlobalUnicast() {
		target.PodIP = ip.String()
	}
	if container.StartedAt > 0 {
		target.StartedAt = time.Unix(0, container.StartedAt)
	}
	if err := e.probes.Add(target); err != nil {
		return fmt.Errorf("start probes: %w", err)
	}
	return nil
}

// untrackProbes stops the Pod's probes.
func (e *executor) untrackProbes(uid string) {
	if e.probes != nil {
		e.probes.Remove(uid)
	}
}

// reasonLivenessProbeFailed is the stable Pod status reason after a liveness failure.
const reasonLivenessProbeFailed = "LivenessProbeFailed"

// enforceLiveness fails a Pod whose liveness probe failed. Under restartPolicy
// Never the running container is stopped through CRI with the Pod's
// termination grace period and is never restarted, and the Pod is published
// Failed with reason LivenessProbeFailed. It reports whether it handled the Pod.
func (e *executor) enforceLiveness(ctx context.Context, runtime *cri.Client, pod *corev1.Pod, sandbox *runtimeapi.PodSandboxStatus, container *runtimeapi.ContainerStatus) (bool, error) {
	if e.probes == nil {
		return false, nil
	}
	status, found := e.probes.Status(string(pod.UID))
	if !found || !status.LivenessFailed {
		return false, nil
	}
	if container.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return true, err
		}
		grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
		if pod.Spec.TerminationGracePeriodSeconds != nil {
			grace = *pod.Spec.TerminationGracePeriodSeconds
		}
		if err := runtime.StopContainer(ctx, container.Id, grace); err != nil {
			return true, err
		}
		stopped, err := runtime.ContainerStatus(ctx, container.Id)
		if err != nil {
			return true, err
		}
		if stopped.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
			return true, errors.New("container still running after a liveness stop")
		}
		container = stopped
	}
	message := "Liveness probe failed; the container was stopped and is not restarted"
	if status.Message != "" {
		message += ": " + status.Message
	}
	if err := e.publishPodStatus(ctx, pod, sandbox, container, reasonLivenessProbeFailed, message); err != nil {
		return true, err
	}
	e.hooks.livenessFailed(pod, message)
	return true, nil
}

// probeWorkers bounds probes running at once and the call slots of their CRI client.
const probeWorkers = 16

// newProbeManager creates the probe manager for at most maxPods Pods. A probe
// status change signals wake, when set, without blocking, so the next pass
// publishes readiness and enforces a liveness failure without waiting for the
// poll interval.
func newProbeManager(execer probe.Execer, maxPods int, hooks ExecutionHooks, wake chan<- struct{}) *probe.Manager {
	options := probe.Options{MaxPods: maxPods, MaxConcurrent: probeWorkers}
	if wake != nil {
		options.Notify = func(string) {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}
	if hooks.ProbeResult != nil {
		options.Observe = func(kind probe.Kind, handler string, result probe.Result) {
			hooks.ProbeResult(string(kind), handler, string(result))
		}
	}
	return probe.NewManager(execer, options)
}
