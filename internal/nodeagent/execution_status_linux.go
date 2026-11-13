// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"net/netip"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (e *executor) publishPodStatus(ctx context.Context, expected *corev1.Pod, sandbox *runtimeapi.PodSandboxStatus, container *runtimeapi.ContainerStatus, reason, message string) error {
	pods := e.api.CoreV1().Pods(expected.Namespace)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := pods.Get(ctx, expected.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pod.UID != expected.UID || pod.Spec.NodeName != e.node.NodeName {
			return errors.New("refusing status for a changed Pod identity or assignment")
		}
		if terminalPod(pod) {
			return nil
		}
		before := pod.Status.DeepCopy()
		conditions := pod.Status.Conditions[:0:0]
		for _, condition := range pod.Status.Conditions {
			if condition.Type != executionBlocked {
				conditions = append(conditions, condition)
			}
		}
		pod.Status.Conditions = conditions
		pod.Status.Phase, pod.Status.Reason, pod.Status.Message = corev1.PodPending, reason, message
		pod.Status.HostIP = e.config.NodeIP
		pod.Status.PodIP, pod.Status.PodIPs = "", nil
		if pod.Status.StartTime == nil {
			now := metav1.Now()
			pod.Status.StartTime = &now
		}
		ready := false
		sandboxReady := false
		if sandbox != nil {
			ip := sandbox.GetNetwork().GetIp()
			if parsed, err := netip.ParseAddr(ip); err == nil && parsed.IsGlobalUnicast() {
				pod.Status.PodIP, pod.Status.PodIPs = ip, []corev1.PodIP{{IP: ip}}
				sandboxReady = sandbox.State == runtimeapi.PodSandboxState_SANDBOX_READY
				ready = sandboxReady && e.healthy.Load()
			}
		}
		if len(pod.Spec.Containers) == 1 {
			spec := pod.Spec.Containers[0]
			observed := corev1.ContainerStatus{
				Name: spec.Name, Image: spec.Image,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			}
			if container != nil {
				observed.ContainerID, observed.ImageID = "containerd://"+container.Id, container.ImageRef
				switch container.State {
				case runtimeapi.ContainerState_CONTAINER_RUNNING:
					pod.Status.Phase = corev1.PodRunning
					observed.Ready = ready && e.probeReady(pod)
					started := true
					observed.Started = &started
					observed.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: criTime(container.StartedAt)}}
				case runtimeapi.ContainerState_CONTAINER_EXITED:
					pod.Status.Phase = corev1.PodFailed
					if container.ExitCode == 0 {
						pod.Status.Phase = corev1.PodSucceeded
					}
					observed.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						ExitCode: container.ExitCode, Reason: container.Reason, Message: container.Message,
						StartedAt: criTime(container.StartedAt), FinishedAt: criTime(container.FinishedAt),
						ContainerID: observed.ContainerID,
					}}
				case runtimeapi.ContainerState_CONTAINER_UNKNOWN:
					pod.Status.Phase = corev1.PodUnknown
					observed.State.Waiting.Reason = "RuntimeStateUnknown"
				}
			}
			if reason != "" && observed.State.Terminated == nil {
				observed.Ready = false
				observed.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message}}
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{observed}
			ready = observed.Ready
		} else {
			ready = false
		}
		// A reason fails the Pod whatever its container reported, such as an
		// exit code 0 after a liveness stop.
		if reason != "" {
			pod.Status.Phase = corev1.PodFailed
		}
		for _, kind := range []corev1.PodConditionType{corev1.PodInitialized, corev1.PodReadyToStartContainers, corev1.PodReady, corev1.ContainersReady} {
			condition := corev1.PodCondition{
				Type: kind, Status: corev1.ConditionFalse, Reason: "ContainersNotReady",
				LastTransitionTime: metav1.Now(),
			}
			if ready {
				condition.Status, condition.Reason = corev1.ConditionTrue, "ContainersReady"
			}
			if kind == corev1.PodInitialized && reason == "" {
				condition.Status, condition.Reason = corev1.ConditionTrue, "NoInitContainers"
			}
			if kind == corev1.PodReadyToStartContainers {
				condition.Status, condition.Reason = corev1.ConditionFalse, "SandboxNotReady"
				if sandboxReady {
					condition.Status, condition.Reason = corev1.ConditionTrue, "SandboxReady"
				}
			}
			found := false
			for i, old := range pod.Status.Conditions {
				if old.Type == kind {
					if old.Status == condition.Status {
						condition.LastTransitionTime = old.LastTransitionTime
					}
					pod.Status.Conditions[i], found = condition, true
				}
			}
			if !found {
				pod.Status.Conditions = append(pod.Status.Conditions, condition)
			}
		}
		if equality.Semantic.DeepEqual(before, &pod.Status) {
			return nil
		}
		_, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		return err
	})
}

// executionBlocked is the Pod condition that publishes a fault confined to the
// Pod's own inputs. The Pod keeps its phase and reservation; the node stays
// ready. Publishing a runtime status for the Pod removes the condition.
const executionBlocked corev1.PodConditionType = "brie.dev/ExecutionBlocked"

// publishPodBlocked sets the executionBlocked condition with a stable reason
// and reports it once it is newly published.
func (e *executor) publishPodBlocked(ctx context.Context, expected *corev1.Pod, reason, message string) error {
	pods := e.api.CoreV1().Pods(expected.Namespace)
	published := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := pods.Get(ctx, expected.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pod.UID != expected.UID || pod.Spec.NodeName != e.node.NodeName {
			return errors.New("refusing status for a changed Pod identity or assignment")
		}
		if terminalPod(pod) {
			return nil
		}
		condition := corev1.PodCondition{
			Type: executionBlocked, Status: corev1.ConditionTrue, Reason: reason, Message: message,
			LastTransitionTime: metav1.Now(),
		}
		if pod.Status.Phase == "" {
			pod.Status.Phase = corev1.PodPending
		}
		for i, old := range pod.Status.Conditions {
			if old.Type != executionBlocked {
				continue
			}
			if old.Status == condition.Status && old.Reason == reason && old.Message == message {
				return nil
			}
			pod.Status.Conditions[i] = condition
			_, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
			published = err == nil
			return err
		}
		pod.Status.Conditions = append(pod.Status.Conditions, condition)
		_, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		published = err == nil
		return err
	})
	if published {
		e.hooks.executionBlocked(expected, reason, message)
	}
	return err
}

func criTime(nanoseconds int64) metav1.Time {
	if nanoseconds <= 0 {
		return metav1.Time{}
	}
	return metav1.NewTime(time.Unix(0, nanoseconds))
}
