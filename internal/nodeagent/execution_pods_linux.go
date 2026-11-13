// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ameukam/brie/internal/cri"
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
	// createRejected is set when the creation-evidence rule proves that the
	// pending create call created nothing (createRejectionProven).
	createRejected bool
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
	sortAssignedPods(pods)
	return pods, version, nil
}

// assignedView returns the assigned Pods for one pass, the resource version
// at which the view is complete, and the resource versions of watch DELETED
// events since the last list. With a watch, the view comes from it and no API
// request is made; without one, the pass lists the Pods itself.
func (e *executor) assignedView(ctx context.Context) ([]corev1.Pod, string, map[string]string, error) {
	if e.watch != nil {
		return e.watch.snapshot()
	}
	pods, version, err := e.assignedPods(ctx)
	return pods, version, nil, err
}

// reconcile runs one pass and waits for the operations and image pulls it
// starts.
func (e *executor) reconcile(ctx context.Context) error {
	return e.reconcilePass(ctx, true)
}

// reconcilePass reads the assigned Pods, the journal, and the complete CRI
// inventory before it mutates anything, then starts at most one operation per
// Pod on the bounded workers. Deletion, cleanup, and status work runs on the
// priority pool; creation runs on the creation pool. Pods with an operation in
// flight are left out of the pass, including its consistency checks, because
// their journal record and inventory may change while the lists are read.
// Without wait, the pass returns once its operations are started.
func (e *executor) reconcilePass(ctx context.Context, wait bool) error {
	err := e.runPass(ctx, wait)
	observed := err
	if observed == nil && !e.synced.Load() {
		observed = errIncompletePass
	}
	e.hooks.passFinished(observed)
	return err
}

// errIncompletePass reports a pass that returned without error but left the
// node unsynced, for example with a worker fault outstanding.
var errIncompletePass = errors.New("reconciliation pass incomplete")

func (e *executor) runPass(ctx context.Context, wait bool) error {
	busy := e.workers.busySet()
	e.mu.Lock()
	records := make(map[string]*executionRecord, len(e.journal.Records))
	for uid, record := range e.journal.Records {
		snapshot := *record
		records[uid] = &snapshot
	}
	blocked := !e.budgetFits(0, 0)
	e.mu.Unlock()
	// The journal snapshot precedes the lists, so a create rejection journaled
	// by a finished operation is always older than the inventory read below.
	pods, listVersion, deletions, err := e.assignedView(ctx)
	if err != nil {
		e.synced.Store(false)
		return err
	}
	sandboxes, err := e.runtime.ListPodSandbox(ctx)
	if err != nil {
		e.synced.Store(false)
		return err
	}
	containers, err := e.runtime.ListContainers(ctx)
	if err != nil {
		e.synced.Store(false)
		return err
	}
	byUID := make(map[string]*corev1.Pod)
	for i := range pods {
		uid := string(pods[i].UID)
		if byUID[uid] != nil {
			e.synced.Store(false)
			return errors.New("duplicate assigned Pod UID")
		}
		byUID[uid] = &pods[i]
	}
	inventories := make(map[string]*podInventory)
	bySandbox := make(map[string]string)
	busySandbox := make(map[string]bool)
	ambiguous := make(map[string]bool)
	for _, sandbox := range sandboxes {
		uid := sandbox.Metadata.Uid
		if busy[uid] || busy[sandbox.Labels[podUIDLabel]] {
			busySandbox[sandbox.Id] = true
			continue
		}
		record := records[uid]
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
		if busySandbox[container.PodSandboxId] || busy[container.Labels[podUIDLabel]] {
			continue
		}
		uid := bySandbox[container.PodSandboxId]
		record := records[uid]
		inventory := inventories[uid]
		pod := byUID[uid]
		if record == nil || inventory == nil || !e.labelsMatch(container.Labels, record) ||
			pod != nil && (len(pod.Spec.Containers) != 1 || container.Metadata.Name != pod.Spec.Containers[0].Name) ||
			container.Metadata.Attempt != 0 || inventory.container != nil ||
			record.ContainerID != "" && record.ContainerID != container.Id {
			blocked = true
			if uid != "" {
				ambiguous[uid] = true
			} else if claimed := container.Labels[podUIDLabel]; byUID[claimed] != nil || records[claimed] != nil {
				ambiguous[claimed] = true
			}
			continue
		}
		inventory.container = container
		if record.ContainerID == "" && record.Stage != "container-pending" {
			ambiguous[uid], blocked = true, true
		}
	}
	for uid, record := range records {
		if record.Stage == "finished" || busy[uid] {
			continue
		}
		inventory := inventories[uid]
		if inventory == nil {
			inventory = &podInventory{}
			inventories[uid] = inventory
		}
		// A recorded deletion contradicted by a listed Pod is never resolved
		// automatically, for example after a control-plane restore.
		if record.DeletionObserved != "" && byUID[uid] != nil {
			ambiguous[uid] = true
		}
		inventory.createRejected = createRejectionProven(record, inventory, sandboxes, containers)
		if byUID[uid] == nil ||
			!inventory.createRejected && (record.Stage != "reserved" && inventory.sandbox == nil ||
				(record.ContainerID != "" || record.Stage == "container-pending") && inventory.container == nil) {
			blocked = true
		}
	}
	var compact []string
	for uid, record := range records {
		if !busy[uid] && compactable(record, byUID[uid] != nil, sandboxes, containers) {
			compact = append(compact, uid)
		}
	}
	if err := e.compactJournal(compact); err != nil {
		e.synced.Store(false)
		return err
	}
	faulted := e.workers.outstanding(func(uid string) bool {
		return byUID[uid] != nil || records[uid] != nil && records[uid].Stage != "finished"
	})
	e.pulls.retain(func(uid string) bool {
		return byUID[uid] != nil && records[uid] != nil && records[uid].Stage == "reserved" || busy[uid]
	})
	e.recovery.Store(blocked || faulted)
	admit := !blocked && !faulted

	var pass sync.WaitGroup
	var passMu sync.Mutex
	var passErrors []error
	// passFailed records an ambiguity or admission error of this pass. Like a
	// worker fault, it keeps the barrier until a later pass completes cleanly.
	passFailed := false
	report := func(err error) {
		passMu.Lock()
		defer passMu.Unlock()
		passErrors = append(passErrors, err)
	}
	start := func(uid string, creation bool, describe string, task func(context.Context, *cri.Client) error) {
		pool, client := e.workers.priority, e.runtime
		if creation {
			pool, client = e.workers.creation, e.create
		}
		e.workers.start(uid, pool, &pass, func() error {
			err := task(ctx, client)
			if fault := (*podFault)(nil); errors.As(err, &fault) {
				e.logger.Printf("%s: %v", describe, err)
				return nil
			}
			if err != nil {
				err = fmt.Errorf("%s: %w", describe, err)
				e.recovery.Store(true)
				e.synced.Store(false)
				if wait {
					report(err)
				} else {
					e.logger.Print(err)
				}
			}
			return err
		})
	}
	// Executions whose Pods are absent from the complete list are deleted;
	// clean them up before any other work (section 6.3 of DESIGN.md).
	orphans := make([]string, 0)
	for uid, record := range records {
		if record.Stage != "finished" && byUID[uid] == nil && !busy[uid] {
			orphans = append(orphans, uid)
		}
	}
	sort.Strings(orphans)
	for _, uid := range orphans {
		record := records[uid]
		describe := fmt.Sprintf("clean up deleted Pod %s/%s", record.Namespace, record.Name)
		if ambiguous[uid] {
			passFailed = true
			report(fmt.Errorf("%s: ambiguous runtime ownership; preserving execution for recovery", describe))
			continue
		}
		inventory := inventories[uid]
		// A watch DELETED event is the most specific evidence; otherwise
		// the view's own resource version proves the absence.
		evidence := listVersion
		if version := deletions[uid]; version != "" {
			evidence = version
		}
		start(uid, false, describe, func(ctx context.Context, client *cri.Client) error {
			return e.cleanupDeleted(ctx, client, e.record(uid), inventory, evidence)
		})
	}
	for i := range pods {
		pod := &pods[i]
		uid := string(pod.UID)
		describe := fmt.Sprintf("reconcile Pod %s/%s", pod.Namespace, pod.Name)
		if busy[uid] {
			continue
		}
		if ambiguous[uid] {
			passFailed = true
			report(fmt.Errorf("%s: ambiguous runtime ownership; preserving execution for recovery", describe))
			continue
		}
		record := records[uid]
		deleting := pod.DeletionTimestamp != nil || terminalPod(pod)
		if record == nil && !deleting {
			// Admission runs serially in creation order, so an older Pod is
			// never refused for a budget a younger Pod took in the same pass.
			if !admit || !e.synced.Load() || !e.healthy.Load() {
				continue
			}
			admitted, err := e.admitPod(ctx, pod)
			if err != nil {
				passFailed, admit = true, false
				e.recovery.Store(true)
				report(fmt.Errorf("%s: %w", describe, err))
				continue
			}
			if admitted == nil {
				continue
			}
			record = admitted
		}
		inventory := inventories[uid]
		if inventory == nil {
			inventory = &podInventory{}
		}
		creation := !deleting && record.CreateRejected == "" &&
			(record.Stage == "reserved" || record.Stage == "sandbox" || record.Stage == "created")
		// A Pod waiting on an image pull that has nothing new to report takes
		// no creation slot, so it cannot starve younger Pods of pull requests.
		if creation && record.Stage == "reserved" && len(pod.Spec.Containers) == 1 &&
			e.pulls.unchanged(uid, pod.Spec.Containers[0].Image, time.Now()) {
			continue
		}
		allowCreate := creation && admit
		start(uid, creation, describe, func(ctx context.Context, client *cri.Client) error {
			return e.reconcilePod(ctx, client, pod, inventory, allowCreate)
		})
	}
	if wait {
		pass.Wait()
		e.pulls.wait()
	}
	faulted = e.workers.outstanding(func(uid string) bool {
		return byUID[uid] != nil || e.unfinished(uid)
	})
	if faulted || passFailed {
		e.recovery.Store(true)
	}
	e.synced.Store(!faulted && !passFailed)
	passMu.Lock()
	defer passMu.Unlock()
	return errors.Join(passErrors...)
}

// createRejectionProven applies the creation-evidence rule (section 6.4 of
// DESIGN.md) to one execution this agent journaled. A pending stage is proven
// to have created nothing only when both hold:
//
//  1. The runtime answered the create call with a definite rejection
//     (cri.DefiniteRejection), journaled in the record before this pass read
//     its inventory. A lost, cancelled, or timed-out reply is never evidence.
//  2. The complete, successful sandbox and container lists of this pass hold no
//     object that claims the Pod: for a sandbox, none whose metadata UID or Pod
//     UID label names it, whatever its owner; for a container, none in the
//     execution's sandbox and none whose Pod UID label names it.
func createRejectionProven(record *executionRecord, inventory *podInventory, sandboxes []*runtimeapi.PodSandbox, containers []*runtimeapi.Container) bool {
	if record.CreateRejected == "" {
		return false
	}
	switch record.Stage {
	case "sandbox-pending":
		for _, sandbox := range sandboxes {
			if sandbox.Metadata.Uid == record.UID || sandbox.Labels[podUIDLabel] == record.UID {
				return false
			}
		}
	case "container-pending":
		if inventory.sandbox == nil {
			return false
		}
		for _, container := range containers {
			if container.PodSandboxId == record.SandboxID || container.Labels[podUIDLabel] == record.UID {
				return false
			}
		}
	default:
		return false
	}
	return true
}

// record returns the live journal record for uid. Only the operation in flight
// for uid writes it, through commit.
func (e *executor) record(uid string) *executionRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.journal.Records[uid]
}

// unfinished reports whether uid has a journal record that is not finished.
// It reads the stage under e.mu because an operation still in flight for uid
// may commit a new stage concurrently.
func (e *executor) unfinished(uid string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	record := e.journal.Records[uid]
	return record != nil && record.Stage != "finished"
}

// commit applies change, which may modify only record, and durably saves the
// journal. A failed save restores record, so no later step can act on a change
// that was never journaled. The operation in flight for a Pod is the only
// writer of that Pod's record.
func (e *executor) commit(record *executionRecord, change func()) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	previous := *record
	change()
	if err := e.journal.save(); err != nil {
		*record = previous
		return err
	}
	return nil
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
		record := e.record(string(pod.UID))
		if record != nil && hash != record.Hash {
			return nil, errors.New("pod execution configuration changed")
		}
	}
	return pod, nil
}

// admitPod runs the admission pipeline of section 7.2 of DESIGN.md for a Pod
// without a journal record. It returns the reserved record, or nil after
// publishing a refusal.
func (e *executor) admitPod(ctx context.Context, pod *corev1.Pod) (*executionRecord, error) {
	if err := ValidatePodProfile(pod, e.node.NodeName, e.config.RuntimeClass); err != nil {
		return nil, e.publishPodStatus(ctx, pod, nil, nil, "UnsupportedPod", err.Error())
	}
	if !equality.Semantic.DeepEqual(pod.Spec.Overhead, e.overhead) {
		return nil, e.publishPodStatus(ctx, pod, nil, nil, "RuntimeClassOverheadMismatch", "Pod overhead must match the pinned RuntimeClass")
	}
	cpu, memory, err := executionResources(pod.Spec.Containers[0].Resources.Limits)
	if err != nil {
		return nil, e.publishPodStatus(ctx, pod, nil, nil, "UnsupportedResources", err.Error())
	}
	hash, err := podExecutionHash(pod)
	if err != nil {
		return nil, err
	}
	overheadCPU, overheadMemory, _ := executionResources(e.overhead)
	cpu, memory, fits := addBudget(cpu, memory, overheadCPU, overheadMemory)
	record := &executionRecord{UID: string(pod.UID), Namespace: pod.Namespace, Name: pod.Name,
		Hash: hash, Stage: "reserved", CPU: cpu, Memory: memory, Image: pod.Spec.Containers[0].Image}
	e.mu.Lock()
	fits = fits && e.budgetFits(cpu, memory)
	if fits {
		e.journal.Records[record.UID] = record
		if err = e.journal.save(); err != nil {
			delete(e.journal.Records, record.UID)
		}
	}
	e.mu.Unlock()
	if !fits {
		return nil, e.publishPodStatus(ctx, pod, nil, nil, "InsufficientResources", "Configured CPU, memory, or Pod budget exhausted")
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

// resolveCreateRejection returns a pending stage whose create the
// creation-evidence rule proved ineffective to the stage before the call. The
// rejection stays journaled, so the create is never retried.
func (e *executor) resolveCreateRejection(record *executionRecord, inventory *podInventory) error {
	if !inventory.createRejected {
		return nil
	}
	inventory.createRejected = false
	return e.commit(record, func() {
		switch record.Stage {
		case "sandbox-pending":
			record.Stage = "reserved"
		case "container-pending":
			record.Stage = "sandbox"
		}
	})
}

// recordCreateError journals a definite runtime rejection of the create call
// of the current pending stage and returns err. Other errors leave the stage
// pending and unresolved.
func (e *executor) recordCreateError(record *executionRecord, err error) error {
	code, rejected := cri.DefiniteRejection(err)
	if !rejected {
		return err
	}
	return errors.Join(err, e.commit(record, func() { record.CreateRejected = code.String() }))
}

func (e *executor) reconcilePod(ctx context.Context, runtime *cri.Client, pod *corev1.Pod, inventory *podInventory, admit bool) error {
	record := e.record(string(pod.UID))
	if pod.DeletionTimestamp != nil || terminalPod(pod) {
		return e.cleanupPod(ctx, runtime, pod, record, inventory)
	}
	if record.Stage == "finished" {
		return errors.New("completed execution cannot be recreated from a nonterminal API Pod")
	}
	if hash, err := podExecutionHash(pod); err != nil || hash != record.Hash {
		// A permitted update to the Pod spec is confined to this Pod. The
		// execution is preserved without further mutation until deletion.
		fault := &podFault{reason: "ExecutionConfigChanged",
			message: "Pod spec no longer matches its admitted execution; delete the Pod to replace it"}
		// Only a published fault is confined to the Pod; a failed publication
		// is an ordinary error.
		if err := e.publishPodBlocked(ctx, pod, fault.reason, fault.message); err != nil {
			return err
		}
		return fault
	}
	if err := e.resolveCreateRejection(record, inventory); err != nil {
		return err
	}
	if record.CreateRejected != "" && (record.Stage == "reserved" || record.Stage == "sandbox") {
		reason := "SandboxCreateRejected"
		if record.Stage == "sandbox" {
			reason = "ContainerCreateRejected"
		}
		message := "Runtime rejected creation with " + record.CreateRejected + "; nothing was created"
		return e.publishPodStatus(ctx, pod, nil, nil, reason, message)
	}
	if record.Stage != "reserved" && inventory.sandbox == nil {
		return errors.New("sandbox execution remains unresolved; refusing replacement or new admission")
	}
	if (record.ContainerID != "" || record.Stage == "container-pending") && inventory.container == nil {
		return errors.New("container execution remains unresolved; refusing replacement or new admission")
	}
	if inventory.sandbox != nil {
		sandbox, err := runtime.PodSandboxStatus(ctx, inventory.sandbox.Id)
		if err != nil {
			return err
		}
		if !e.labelsMatch(sandbox.Labels, record) || sandbox.Metadata.Uid != record.UID ||
			sandbox.RuntimeHandler != e.config.RuntimeHandler {
			return errors.New("sandbox status changed execution identity")
		}
		if record.SandboxID == "" && record.Stage == "sandbox-pending" {
			if err := e.commit(record, func() { record.SandboxID, record.Stage, record.CreateRejected = sandbox.Id, "sandbox", "" }); err != nil {
				return err
			}
		}
		if inventory.container != nil {
			container, err := runtime.ContainerStatus(ctx, inventory.container.Id)
			if err != nil {
				return err
			}
			if !e.labelsMatch(container.Labels, record) || container.Metadata.Name != pod.Spec.Containers[0].Name ||
				container.Metadata.Attempt != 0 {
				return errors.New("container status changed execution identity")
			}
			if record.ContainerID == "" && record.Stage == "container-pending" {
				if err := e.commit(record, func() { record.ContainerID, record.Stage, record.CreateRejected = container.Id, "created", "" }); err != nil {
					return err
				}
			}
			if container.State == runtimeapi.ContainerState_CONTAINER_RUNNING ||
				container.State == runtimeapi.ContainerState_CONTAINER_EXITED {
				if record.Stage == "start-pending" {
					if err := e.commit(record, func() { record.Stage = "running" }); err != nil {
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
		image := container.Image.Image
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		e.imageGate.RLock()
		ready, err := e.imageReady(ctx, runtime, pod, image, sandbox)
		e.imageGate.RUnlock()
		if err != nil || !ready {
			return err
		}
		// Re-read Pod intent after the image wait and before sandbox creation.
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		if err := os.MkdirAll(sandbox.LogDirectory, 0750); err != nil {
			return err
		}
		if err := e.commit(record, func() { record.Stage = "sandbox-pending" }); err != nil {
			return err
		}
		id, err := runtime.RunPodSandbox(ctx, sandbox, e.config.RuntimeHandler)
		if err != nil {
			return e.recordCreateError(record, err)
		}
		return e.commit(record, func() { record.SandboxID, record.Stage = id, "sandbox" })
	case "sandbox":
		if inventory.sandbox == nil {
			return errors.New("known sandbox is absent; refusing replacement execution")
		}
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		if err := e.commit(record, func() { record.Stage = "container-pending" }); err != nil {
			return err
		}
		id, err := runtime.CreateContainer(ctx, record.SandboxID, container, sandbox)
		if err != nil {
			return e.recordCreateError(record, err)
		}
		return e.commit(record, func() { record.ContainerID, record.Stage = id, "created" })
	case "created":
		if inventory.container == nil {
			return errors.New("known container is absent; refusing replacement execution")
		}
		if _, err := e.currentPod(ctx, pod, false); err != nil {
			return err
		}
		if err := e.commit(record, func() { record.Stage = "start-pending" }); err != nil {
			return err
		}
		if err := runtime.StartContainer(ctx, record.ContainerID); err != nil {
			return err
		}
		return e.commit(record, func() { record.Stage = "running" })
	default:
		return errors.New("execution outcome remains unknown; waiting for runtime evidence without retrying mutations")
	}
}

func terminalPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func (e *executor) cleanupPod(ctx context.Context, runtime *cri.Client, pod *corev1.Pod, record *executionRecord, inventory *podInventory) error {
	if record != nil {
		if err := e.resolveCreateRejection(record, inventory); err != nil {
			return err
		}
		grace := int64(0)
		if pod.DeletionTimestamp != nil {
			grace = max(0, int64(math.Ceil(time.Until(pod.DeletionTimestamp.Time).Seconds())))
		}
		if err := e.removeExecution(ctx, runtime, record, inventory, grace, func(ctx context.Context) error {
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
// quorum-consistent node-scoped list or from the watched view that a watch
// DELETED event updated. Because spec.nodeName is immutable once bound, that
// absence is positive evidence of deletion. The evidence is
// journaled before the first destructive step, and then authorizes the
// remaining steps across restarts without a live Pod read, which the Node
// authorizer rejects once the Pod is gone. A deleted Pod has no remaining
// grace period, so the container is stopped immediately, as for force deletion.
func (e *executor) cleanupDeleted(ctx context.Context, runtime *cri.Client, record *executionRecord, inventory *podInventory, evidence string) error {
	if err := e.resolveCreateRejection(record, inventory); err != nil {
		return err
	}
	if record.DeletionObserved == "" {
		if evidence == "" || len(evidence) > maxResourceVersion {
			return errors.New("assigned-Pod list has no usable resource version; deletion is unproven")
		}
		if err := e.commit(record, func() { record.DeletionObserved = evidence }); err != nil {
			return err
		}
	}
	return e.removeExecution(ctx, runtime, record, inventory, 0, e.currentNode)
}

// removeExecution stops and removes the runtime objects of one execution this
// agent created and releases its reservation. authorize runs before every
// destructive CRI call. Every step is idempotent.
func (e *executor) removeExecution(ctx context.Context, runtime *cri.Client, record *executionRecord, inventory *podInventory, grace int64, authorize func(context.Context) error) error {
	if record.Stage == "sandbox-pending" && inventory.sandbox == nil {
		return errors.New("sandbox creation is unresolved; deletion must wait for runtime evidence")
	}
	if inventory.sandbox != nil {
		if err := authorize(ctx); err != nil {
			return err
		}
		if inventory.container != nil {
			if err := runtime.StopContainer(ctx, inventory.container.Id, grace); err != nil {
				return err
			}
		}
		if err := authorize(ctx); err != nil {
			return err
		}
		if err := runtime.StopPodSandbox(ctx, inventory.sandbox.Id); err != nil {
			return err
		}
		if inventory.container != nil {
			if err := authorize(ctx); err != nil {
				return err
			}
			if err := runtime.RemoveContainer(ctx, inventory.container.Id); err != nil {
				return err
			}
		}
		if err := authorize(ctx); err != nil {
			return err
		}
		if err := runtime.RemovePodSandbox(ctx, inventory.sandbox.Id); err != nil {
			return err
		}
		return e.commit(record, func() { record.Stage, record.SandboxRemoved = "finished", true })
	}
	if record.Stage != "finished" {
		return e.commit(record, func() { record.Stage = "finished" })
	}
	return nil
}

// compactable reports whether journal compaction (section 6.4 of DESIGN.md)
// may drop record. Every condition must hold in the same pass:
//
//  1. The record is finished, so its reservation is released and no operation
//     for it remains.
//  2. RemovePodSandbox succeeded for the sandbox the record names, or the
//     record never named one. A sandbox that disappeared without a successful
//     RemovePodSandbox, including one an operator removed, keeps its record.
//  3. The Pod UID is absent from the complete assigned-Pod view, so the Pod is
//     deleted and its UID cannot be admitted again.
//  4. The complete, successful sandbox and container lists of the pass hold no
//     object that the record names or that claims the Pod UID, whatever its
//     owner.
func compactable(record *executionRecord, listed bool, sandboxes []*runtimeapi.PodSandbox, containers []*runtimeapi.Container) bool {
	if record.Stage != "finished" || listed || record.SandboxID != "" && !record.SandboxRemoved {
		return false
	}
	for _, sandbox := range sandboxes {
		if sandbox.Id == record.SandboxID || sandbox.Metadata.Uid == record.UID || sandbox.Labels[podUIDLabel] == record.UID {
			return false
		}
	}
	for _, container := range containers {
		if record.ContainerID != "" && container.Id == record.ContainerID ||
			record.SandboxID != "" && container.PodSandboxId == record.SandboxID ||
			container.Labels[podUIDLabel] == record.UID {
			return false
		}
	}
	return true
}

// compactJournal durably drops the records in uids that are still finished.
// The caller runs it before it starts any operation of the pass, so no worker
// holds those records. On a failed save the records are restored.
func (e *executor) compactJournal(uids []string) error {
	if len(uids) == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	dropped := make(map[string]*executionRecord, len(uids))
	for _, uid := range uids {
		if record := e.journal.Records[uid]; record != nil && record.Stage == "finished" {
			dropped[uid] = record
			delete(e.journal.Records, uid)
		}
	}
	if len(dropped) == 0 {
		return nil
	}
	if err := e.journal.save(); err != nil {
		maps.Copy(e.journal.Records, dropped)
		return fmt.Errorf("compact execution journal: %w", err)
	}
	return nil
}
