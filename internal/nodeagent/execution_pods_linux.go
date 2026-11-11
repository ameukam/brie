// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type podInventory struct {
	sandbox   *runtimeapi.PodSandbox
	container *runtimeapi.Container
}

func (e *executor) assignedPods(ctx context.Context) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	options := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", e.node.NodeName).String(), Limit: 100,
	}
	for {
		page, err := e.api.CoreV1().Pods(corev1.NamespaceAll).List(ctx, options)
		if err != nil {
			return nil, fmt.Errorf("list assigned Pods: %w", err)
		}
		for _, pod := range page.Items {
			if pod.Spec.NodeName != e.node.NodeName || pod.UID == "" {
				return nil, errors.New("assigned-Pod list contains an invalid assignment or UID")
			}
			pods = append(pods, pod)
		}
		if len(pods) > 10000 {
			return nil, errors.New("assigned-Pod inventory exceeds the bounded reconstruction limit")
		}
		if page.Continue == "" {
			break
		}
		options.Continue = page.Continue
	}
	sort.SliceStable(pods, func(i, j int) bool {
		if (pods[i].DeletionTimestamp != nil) != (pods[j].DeletionTimestamp != nil) {
			return pods[i].DeletionTimestamp != nil
		}
		return pods[i].CreationTimestamp.Before(&pods[j].CreationTimestamp)
	})
	return pods, nil
}

func (e *executor) reconcile(ctx context.Context) (retErr error) {
	defer func() {
		e.synced.Store(retErr == nil)
	}()
	pods, err := e.assignedPods(ctx)
	if err != nil {
		return err
	}
	sandboxes, err := e.runtime.ListPodSandbox(ctx)
	if err != nil {
		return err
	}
	containers, err := e.runtime.ListContainers(ctx)
	if err != nil {
		return err
	}
	byUID := make(map[string]*corev1.Pod)
	for i := range pods {
		uid := string(pods[i].UID)
		if byUID[uid] != nil {
			return errors.New("duplicate assigned Pod UID")
		}
		byUID[uid] = &pods[i]
	}
	inventories := make(map[string]*podInventory)
	bySandbox := make(map[string]string)
	ambiguous := make(map[string]bool)
	blocked := !e.budgetFits(0, 0)
	for _, sandbox := range sandboxes {
		uid := sandbox.Metadata.Uid
		record := e.journal.Records[uid]
		pod := byUID[uid]
		if record == nil || pod == nil || !e.labelsMatch(sandbox.Labels, record) ||
			sandbox.RuntimeHandler != e.config.RuntimeHandler ||
			sandbox.Metadata.Namespace != record.Namespace || sandbox.Metadata.Name != record.Name ||
			sandbox.Metadata.Attempt != 0 || inventories[uid] != nil ||
			record.SandboxID != "" && record.SandboxID != sandbox.Id {
			blocked = true
			if pod != nil {
				ambiguous[uid] = true
			}
			continue
		}
		if record.Stage == "reserved" || record.Stage == "finished" {
			ambiguous[uid], blocked = true, true
			continue
		}
		inventories[uid] = &podInventory{sandbox: sandbox}
		bySandbox[sandbox.Id] = uid
	}
	for _, container := range containers {
		uid := bySandbox[container.PodSandboxId]
		record := e.journal.Records[uid]
		inventory := inventories[uid]
		if record == nil || inventory == nil || len(byUID[uid].Spec.Containers) != 1 || !e.labelsMatch(container.Labels, record) ||
			container.Metadata.Name != byUID[uid].Spec.Containers[0].Name ||
			container.Metadata.Attempt != 0 || inventory.container != nil ||
			record.ContainerID != "" && record.ContainerID != container.Id {
			blocked = true
			if uid != "" {
				ambiguous[uid] = true
			} else if byUID[container.Labels[podUIDLabel]] != nil {
				ambiguous[container.Labels[podUIDLabel]] = true
			}
			continue
		}
		inventory.container = container
		if record.ContainerID == "" && record.Stage != "container-pending" {
			ambiguous[uid], blocked = true, true
		}
	}
	for uid, record := range e.journal.Records {
		if record.Stage == "finished" {
			continue
		}
		inventory := inventories[uid]
		if byUID[uid] == nil ||
			record.Stage != "reserved" && (inventory == nil || inventory.sandbox == nil) ||
			(record.ContainerID != "" || record.Stage == "container-pending") && (inventory == nil || inventory.container == nil) {
			blocked = true
		}
	}
	e.recovery.Store(blocked)
	// A complete read barrier precedes all mutations. Calls are serialized and
	// reconcile at most one create/start transition per Pod on each pass.
	var podErrors []error
	for i := range pods {
		pod := &pods[i]
		if ambiguous[string(pod.UID)] {
			podErrors = append(podErrors, fmt.Errorf("pod %s/%s has ambiguous runtime ownership; preserving execution for recovery", pod.Namespace, pod.Name))
			continue
		}
		inventory := inventories[string(pod.UID)]
		if inventory == nil {
			inventory = &podInventory{}
		}
		if err := e.reconcilePod(ctx, pod, inventory, !blocked); err != nil {
			blocked = true
			e.recovery.Store(true)
			e.synced.Store(false)
			podErrors = append(podErrors, fmt.Errorf("reconcile Pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	return errors.Join(podErrors...)
}

func (e *executor) currentPod(ctx context.Context, expected *corev1.Pod, destructive bool) (*corev1.Pod, error) {
	node, err := e.api.CoreV1().Nodes().Get(ctx, e.node.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if node.UID != e.nodeUID || node.Annotations[executionMarker] != e.journal.Owner {
		e.healthy.Store(false)
		return nil, errors.New("execution Node identity changed")
	}
	pod, err := e.api.CoreV1().Pods(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if pod.UID != expected.UID || pod.Spec.NodeName != e.node.NodeName {
		return nil, errors.New("pod identity or assignment changed")
	}
	if destructive {
		if pod.DeletionTimestamp == nil && !terminalPod(pod) {
			return nil, errors.New("cleanup requires current deletion or terminal intent")
		}
	} else {
		if pod.DeletionTimestamp != nil || terminalPod(pod) || !e.healthy.Load() {
			return nil, errors.New("execution is no longer authorized or runtime health is unavailable")
		}
		hash, err := podExecutionHash(pod)
		if err != nil {
			return nil, err
		}
		record := e.journal.Records[string(pod.UID)]
		if record != nil && hash != record.Hash {
			return nil, errors.New("pod execution configuration changed")
		}
	}
	return pod, nil
}

func (e *executor) reconcilePod(ctx context.Context, pod *corev1.Pod, inventory *podInventory, admit bool) error {
	record := e.journal.Records[string(pod.UID)]
	if pod.DeletionTimestamp != nil || terminalPod(pod) {
		return e.cleanupPod(ctx, pod, record, inventory)
	}
	if record == nil {
		if !admit || !e.synced.Load() || !e.healthy.Load() {
			return nil
		}
		if err := ValidatePodProfile(pod, e.node.NodeName, e.config.RuntimeClass); err != nil {
			return e.publishPodStatus(ctx, pod, nil, nil, "UnsupportedPod", err.Error())
		}
		if !equality.Semantic.DeepEqual(pod.Spec.Overhead, e.overhead) {
			return e.publishPodStatus(ctx, pod, nil, nil, "RuntimeClassOverheadMismatch", "Pod overhead must match the pinned RuntimeClass")
		}
		cpu, memory, err := executionResources(pod.Spec.Containers[0].Resources.Limits)
		if err != nil {
			return e.publishPodStatus(ctx, pod, nil, nil, "UnsupportedResources", err.Error())
		}
		overheadCPU, overheadMemory, _ := executionResources(e.overhead)
		cpu, memory, fits := addBudget(cpu, memory, overheadCPU, overheadMemory)
		if !fits || !e.budgetFits(cpu, memory) {
			return e.publishPodStatus(ctx, pod, nil, nil, "InsufficientResources", "Configured CPU, memory, or Pod budget exhausted")
		}
		hash, err := podExecutionHash(pod)
		if err != nil {
			return err
		}
		record = &executionRecord{UID: string(pod.UID), Namespace: pod.Namespace, Name: pod.Name,
			Hash: hash, Stage: "reserved", CPU: cpu, Memory: memory}
		e.journal.Records[record.UID] = record
		if err := e.journal.save(); err != nil {
			return err
		}
	}
	if record.Stage == "finished" {
		return errors.New("completed execution cannot be recreated from a nonterminal API Pod")
	}
	if hash, err := podExecutionHash(pod); err != nil || hash != record.Hash {
		return errors.New("assigned Pod no longer matches its durable execution configuration")
	}
	if record.Stage != "reserved" && inventory.sandbox == nil {
		return errors.New("sandbox execution remains unresolved; refusing replacement or new admission")
	}
	if (record.ContainerID != "" || record.Stage == "container-pending") && inventory.container == nil {
		return errors.New("container execution remains unresolved; refusing replacement or new admission")
	}
	if inventory.sandbox != nil {
		sandbox, err := e.runtime.PodSandboxStatus(ctx, inventory.sandbox.Id)
		if err != nil {
			return err
		}
		if !e.labelsMatch(sandbox.Labels, record) || sandbox.Metadata.Uid != record.UID ||
			sandbox.RuntimeHandler != e.config.RuntimeHandler {
			return errors.New("sandbox status changed execution identity")
		}
		if record.SandboxID == "" && record.Stage == "sandbox-pending" {
			record.SandboxID, record.Stage = sandbox.Id, "sandbox"
			if err := e.journal.save(); err != nil {
				return err
			}
		}
		if inventory.container != nil {
			container, err := e.runtime.ContainerStatus(ctx, inventory.container.Id)
			if err != nil {
				return err
			}
			if !e.labelsMatch(container.Labels, record) || container.Metadata.Name != pod.Spec.Containers[0].Name ||
				container.Metadata.Attempt != 0 {
				return errors.New("container status changed execution identity")
			}
			if record.ContainerID == "" && record.Stage == "container-pending" {
				record.ContainerID, record.Stage = container.Id, "created"
				if err := e.journal.save(); err != nil {
					return err
				}
			}
			if container.State == runtimeapi.ContainerState_CONTAINER_RUNNING ||
				container.State == runtimeapi.ContainerState_CONTAINER_EXITED {
				if record.Stage == "start-pending" {
					record.Stage = "running"
					if err := e.journal.save(); err != nil {
						return err
					}
				}
				return e.publishPodStatus(ctx, pod, sandbox, container, "", "")
			}
			if container.State == runtimeapi.ContainerState_CONTAINER_UNKNOWN {
				return e.publishPodStatus(ctx, pod, sandbox, container, "", "")
			}
			if record.Stage != "created" {
				return errors.New("container start outcome remains unknown; refusing another start")
			}
		}
		if err := e.publishPodStatus(ctx, pod, sandbox, nil, "", ""); err != nil {
			return err
		}
		if sandbox.State != runtimeapi.PodSandboxState_SANDBOX_READY {
			return nil
		}
	}
	if !admit || !e.healthy.Load() || !e.synced.Load() {
		return nil
	}
	sandbox, container := podRuntimeConfigs(pod, e.config, e.journal.Owner, record.Hash)
	switch record.Stage {
	case "reserved":
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		image, err := e.runtime.ImageStatus(ctx, container.Image.Image)
		if err != nil {
			return err
		}
		if image == nil {
			if _, err := e.runtime.PullImage(ctx, container.Image.Image, sandbox); err != nil {
				return err
			}
		}
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		if err := os.MkdirAll(sandbox.LogDirectory, 0750); err != nil {
			return err
		}
		record.Stage = "sandbox-pending"
		if err := e.journal.save(); err != nil {
			return err
		}
		id, err := e.runtime.RunPodSandbox(ctx, sandbox, e.config.RuntimeHandler)
		if err != nil {
			return err
		}
		record.SandboxID, record.Stage = id, "sandbox"
		return e.journal.save()
	case "sandbox":
		if inventory.sandbox == nil {
			return errors.New("known sandbox is absent; refusing replacement execution")
		}
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		record.Stage = "container-pending"
		if err := e.journal.save(); err != nil {
			return err
		}
		id, err := e.runtime.CreateContainer(ctx, record.SandboxID, container, sandbox)
		if err != nil {
			return err
		}
		record.ContainerID, record.Stage = id, "created"
		return e.journal.save()
	case "created":
		if inventory.container == nil {
			return errors.New("known container is absent; refusing replacement execution")
		}
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		record.Stage = "start-pending"
		if err := e.journal.save(); err != nil {
			return err
		}
		if err := e.runtime.StartContainer(ctx, record.ContainerID); err != nil {
			return err
		}
		record.Stage = "running"
		return e.journal.save()
	default:
		return errors.New("execution outcome remains unknown; waiting for runtime evidence without retrying mutations")
	}
}

func terminalPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func (e *executor) cleanupPod(ctx context.Context, pod *corev1.Pod, record *executionRecord, inventory *podInventory) error {
	if record != nil {
		if record.Stage == "sandbox-pending" && inventory.sandbox == nil {
			return errors.New("sandbox creation is unresolved; deletion must wait for runtime evidence")
		}
		if inventory.sandbox != nil {
			if _, err := e.currentPod(ctx, pod, true); err != nil {
				return err
			}
			if inventory.container != nil {
				seconds := int64(0)
				if pod.DeletionTimestamp != nil {
					seconds = max(0, int64(math.Ceil(time.Until(pod.DeletionTimestamp.Time).Seconds())))
				}
				if err := e.runtime.StopContainer(ctx, inventory.container.Id, seconds); err != nil {
					return err
				}
			}
			if _, err := e.currentPod(ctx, pod, true); err != nil {
				return err
			}
			if err := e.runtime.StopPodSandbox(ctx, inventory.sandbox.Id); err != nil {
				return err
			}
			if inventory.container != nil {
				if _, err := e.currentPod(ctx, pod, true); err != nil {
					return err
				}
				if err := e.runtime.RemoveContainer(ctx, inventory.container.Id); err != nil {
					return err
				}
			}
			if _, err := e.currentPod(ctx, pod, true); err != nil {
				return err
			}
			if err := e.runtime.RemovePodSandbox(ctx, inventory.sandbox.Id); err != nil {
				return err
			}
		}
		if record.Stage != "finished" {
			record.Stage = "finished"
			if err := e.journal.save(); err != nil {
				return err
			}
		}
	}
	if pod.DeletionTimestamp == nil {
		return nil
	}
	current, err := e.currentPod(ctx, pod, true)
	if err != nil {
		return err
	}
	zero := int64(0)
	err = e.api.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion},
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
