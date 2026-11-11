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

// assignedPods returns every Pod bound to this node and the resource version of
// the list snapshot. The options leave ResourceVersion unset, so the API server
// serves a quorum-consistent read of the most recent state, and continuation
// pages come from the same snapshot. Only a complete, successful list with a
// resource version is returned; any error discards partial results.
func (e *executor) assignedPods(ctx context.Context) ([]corev1.Pod, string, error) {
	var pods []corev1.Pod
	var version string
	options := metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", e.node.NodeName).String(), Limit: 100,
	}
	for first := true; ; first = false {
		page, err := e.api.CoreV1().Pods(corev1.NamespaceAll).List(ctx, options)
		if err != nil {
			return nil, "", fmt.Errorf("list assigned Pods: %w", err)
		}
		if first {
			version = page.ResourceVersion
		} else if page.ResourceVersion != version {
			return nil, "", errors.New("assigned-Pod list pages come from different snapshots")
		}
		for _, pod := range page.Items {
			if pod.Spec.NodeName != e.node.NodeName || pod.UID == "" {
				return nil, "", errors.New("assigned-Pod list contains an invalid assignment or UID")
			}
			pods = append(pods, pod)
		}
		if len(pods) > 10000 {
			return nil, "", errors.New("assigned-Pod inventory exceeds the bounded reconstruction limit")
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
	return pods, version, nil
}

func (e *executor) reconcile(ctx context.Context) (retErr error) {
	defer func() {
		e.synced.Store(retErr == nil)
	}()
	pods, listVersion, err := e.assignedPods(ctx)
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
		// A sandbox is inventoried only when the journal record, the sandbox
		// metadata, and the owner labels agree, whether or not its Pod is listed.
		if record == nil || !e.labelsMatch(sandbox.Labels, record) ||
			sandbox.RuntimeHandler != e.config.RuntimeHandler ||
			sandbox.Metadata.Namespace != record.Namespace || sandbox.Metadata.Name != record.Name ||
			sandbox.Metadata.Attempt != 0 || inventories[uid] != nil ||
			record.SandboxID != "" && record.SandboxID != sandbox.Id {
			blocked = true
			if pod != nil || record != nil {
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
		pod := byUID[uid]
		if record == nil || inventory == nil || !e.labelsMatch(container.Labels, record) ||
			pod != nil && (len(pod.Spec.Containers) != 1 || container.Metadata.Name != pod.Spec.Containers[0].Name) ||
			container.Metadata.Attempt != 0 || inventory.container != nil ||
			record.ContainerID != "" && record.ContainerID != container.Id {
			blocked = true
			if uid != "" {
				ambiguous[uid] = true
			} else if claimed := container.Labels[podUIDLabel]; byUID[claimed] != nil || e.journal.Records[claimed] != nil {
				ambiguous[claimed] = true
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
		// A recorded deletion contradicted by a listed Pod is never resolved
		// automatically, for example after a control-plane restore.
		if record.DeletionObserved != "" && byUID[uid] != nil {
			ambiguous[uid] = true
		}
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
	// Executions whose Pods are absent from the complete list are deleted;
	// clean them up before any other work (section 6.3 of DESIGN.md).
	orphans := make([]string, 0)
	for uid, record := range e.journal.Records {
		if record.Stage != "finished" && byUID[uid] == nil {
			orphans = append(orphans, uid)
		}
	}
	sort.Strings(orphans)
	for _, uid := range orphans {
		record := e.journal.Records[uid]
		if ambiguous[uid] {
			podErrors = append(podErrors, fmt.Errorf("deleted Pod %s/%s has ambiguous runtime ownership; preserving execution for recovery", record.Namespace, record.Name))
			continue
		}
		inventory := inventories[uid]
		if inventory == nil {
			inventory = &podInventory{}
		}
		if err := e.cleanupDeleted(ctx, record, inventory, listVersion); err != nil {
			podErrors = append(podErrors, fmt.Errorf("clean up deleted Pod %s/%s: %w", record.Namespace, record.Name, err))
		}
	}
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

// currentNode confirms that the registered Node still carries this agent's
// execution identity.
func (e *executor) currentNode(ctx context.Context) error {
	node, err := e.api.CoreV1().Nodes().Get(ctx, e.node.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID != e.nodeUID || node.Annotations[executionMarker] != e.journal.Owner {
		e.healthy.Store(false)
		return errors.New("execution Node identity changed")
	}
	return nil
}

func (e *executor) currentPod(ctx context.Context, expected *corev1.Pod, destructive bool) (*corev1.Pod, error) {
	if err := e.currentNode(ctx); err != nil {
		return nil, err
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
		grace := int64(0)
		if pod.DeletionTimestamp != nil {
			grace = max(0, int64(math.Ceil(time.Until(pod.DeletionTimestamp.Time).Seconds())))
		}
		if err := e.removeExecution(ctx, record, inventory, grace, func(ctx context.Context) error {
			_, err := e.currentPod(ctx, pod, true)
			return err
		}); err != nil {
			return err
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

// cleanupDeleted removes an execution whose Pod is absent from a complete,
// quorum-consistent node-scoped list. Because spec.nodeName is immutable once
// bound, that absence is positive evidence of deletion. The evidence is
// journaled before the first destructive step, and then authorizes the
// remaining steps across restarts without a live Pod read, which the Node
// authorizer rejects once the Pod is gone. A deleted Pod has no remaining
// grace period, so the container is stopped immediately, as for force deletion.
func (e *executor) cleanupDeleted(ctx context.Context, record *executionRecord, inventory *podInventory, listVersion string) error {
	if record.DeletionObserved == "" {
		if listVersion == "" || len(listVersion) > maxResourceVersion {
			return errors.New("assigned-Pod list has no usable resource version; deletion is unproven")
		}
		record.DeletionObserved = listVersion
		if err := e.journal.save(); err != nil {
			return err
		}
	}
	return e.removeExecution(ctx, record, inventory, 0, e.currentNode)
}

// removeExecution stops and removes the runtime objects of one execution this
// agent created and releases its reservation. authorize runs before every
// destructive CRI call. Every step is idempotent.
func (e *executor) removeExecution(ctx context.Context, record *executionRecord, inventory *podInventory, grace int64, authorize func(context.Context) error) error {
	if record.Stage == "sandbox-pending" && inventory.sandbox == nil {
		return errors.New("sandbox creation is unresolved; deletion must wait for runtime evidence")
	}
	if inventory.sandbox != nil {
		if err := authorize(ctx); err != nil {
			return err
		}
		if inventory.container != nil {
			if err := e.runtime.StopContainer(ctx, inventory.container.Id, grace); err != nil {
				return err
			}
		}
		if err := authorize(ctx); err != nil {
			return err
		}
		if err := e.runtime.StopPodSandbox(ctx, inventory.sandbox.Id); err != nil {
			return err
		}
		if inventory.container != nil {
			if err := authorize(ctx); err != nil {
				return err
			}
			if err := e.runtime.RemoveContainer(ctx, inventory.container.Id); err != nil {
				return err
			}
		}
		if err := authorize(ctx); err != nil {
			return err
		}
		if err := e.runtime.RemovePodSandbox(ctx, inventory.sandbox.Id); err != nil {
			return err
		}
	}
	if record.Stage != "finished" {
		record.Stage = "finished"
		return e.journal.save()
	}
	return nil
}
