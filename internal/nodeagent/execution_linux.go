// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/probe"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

type executor struct {
	node   Config
	config ExecutionConfig
	api    kubernetes.Interface
	// runtime serves inventory, stop, delete, and status calls; create serves
	// image, sandbox, and container creation; health serves the health loop.
	// Each client bounds its own calls in flight.
	runtime *cri.Client
	create  *cri.Client
	health  *cri.Client
	// mu guards journal records and saves once reconciliation workers run.
	mu      sync.Mutex
	journal *executionJournal
	workers *podWorkers
	// pulls runs image pulls outside Pod reconciliation (section 10.4 of
	// DESIGN.md).
	pulls *imagePuller
	// images, when set, collects unreferenced images; imageGate orders
	// image presence checks and pull starts (read) against collection
	// (write).
	images    *imageCollector
	imageGate sync.RWMutex
	// watch, when set, serves the assigned-Pod view of each pass; run sets
	// it. Without it, each pass lists the assigned Pods.
	watch    *assignedPodWatch
	nodeUID  types.UID
	overhead corev1.ResourceList
	healthy  atomic.Bool
	synced   atomic.Bool
	recovery atomic.Bool
	network  string
	logger   *log.Logger
	hooks    ExecutionHooks
	// probes, when set, runs readiness and liveness probes (ExecutionConfig.Probes).
	probes *probe.Manager
}

// Execute explicitly enables scheduling and CRI execution for the restricted
// profile. It never directly accesses a VMM and does not kill workloads on exit.
func Execute(ctx context.Context, node Config, config ExecutionConfig, kubeconfig string, stderr io.Writer) error {
	return ExecuteObserved(ctx, node, config, kubeconfig, stderr, ExecutionHooks{})
}

// ExecuteObserved is Execute with observation hooks.
func ExecuteObserved(ctx context.Context, node Config, config ExecutionConfig, kubeconfig string, stderr io.Writer, hooks ExecutionHooks) (retErr error) {
	if err := node.Validate(); err != nil {
		return fmt.Errorf("validate agent configuration: %w", err)
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate execution configuration: %w", err)
	}
	if node.RuntimeSocket == "" || kubeconfig == "" {
		return errors.New("execution requires --runtime-socket and --kubeconfig")
	}
	if err := ctx.Err(); err != nil {
		return shutdownError(err)
	}
	rest, err := registrationConfig(kubeconfig)
	if err != nil {
		return err
	}
	rest.UserAgent = "brie-node/execution"
	api, err := kubernetes.NewForConfig(rest)
	if err != nil {
		return fmt.Errorf("create execution API client: %w", err)
	}
	journal, err := openExecutionJournal(config.StateDir, node.NodeName, config)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, journal.close()) }()
	// One slot beyond the priority workers keeps inventory lists from
	// queueing behind slow stop or delete calls.
	workloads, err := cri.NewConcurrent(node.RuntimeSocket, 2*time.Minute, priorityWorkers+1)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, workloads.Close()) }()
	creation, err := cri.NewConcurrent(node.RuntimeSocket, 2*time.Minute, creationWorkers)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, creation.Close()) }()
	health, err := cri.New(node.RuntimeSocket, 10*time.Second)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, health.Close()) }()
	pulls, err := cri.NewConcurrent(node.RuntimeSocket, imagePullDeadline, pullWorkers)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, pulls.Close()) }()
	hooks.observeCRI(workloads, creation, health, pulls)
	agent := &executor{
		node: node, config: config, api: api, journal: journal, runtime: workloads, create: creation, health: health,
		workers: newPodWorkers(), pulls: newImagePuller(pulls), logger: log.New(stderr, "brie-node: ", 0), hooks: hooks,
	}
	journal.saved = agent.journalSaved
	if config.Probes {
		// Exec probes take up to 60 seconds; their client allows more.
		probes, err := cri.NewConcurrent(node.RuntimeSocket, 2*time.Minute, probeWorkers)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, probes.Close()) }()
		hooks.observeCRI(probes)
		agent.probes = newProbeManager(probes, config.MaxPods, hooks)
		defer agent.probes.Close()
	}
	if threshold, _ := imageGCThresholdBytes(config.ImageGCThreshold); threshold > 0 {
		collection, err := cri.New(node.RuntimeSocket, 2*time.Minute)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, collection.Close()) }()
		hooks.observeCRI(collection)
		agent.images = &imageCollector{client: collection, threshold: threshold, unusedSince: make(map[string]time.Time)}
	}
	if err := agent.initialize(ctx); err != nil {
		if ctx.Err() != nil {
			return shutdownError(ctx.Err())
		}
		return err
	}
	return agent.run(ctx)
}

func (e *executor) initialize(ctx context.Context) error {
	version, err := e.health.Version(ctx)
	if err != nil {
		return err
	}
	if version.RuntimeName != "containerd" {
		return errors.New("execution requires the configured containerd CRI runtime")
	}
	class, err := e.api.NodeV1().RuntimeClasses().Get(ctx, e.config.RuntimeClass, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read execution RuntimeClass: %w", err)
	}
	if class.Handler != e.config.RuntimeHandler || class.UID == "" || class.Overhead == nil ||
		len(class.Overhead.PodFixed) != 2 {
		return errors.New("RuntimeClass must match the configured handler and declare measured CPU/memory overhead")
	}
	if _, _, err := executionResources(class.Overhead.PodFixed); err != nil {
		return fmt.Errorf("RuntimeClass overhead: %w", err)
	}
	if e.journal.ClassUID != "" && e.journal.ClassUID != string(class.UID) {
		return errors.New("RuntimeClass identity changed; preserve execution state for recovery")
	}
	e.journal.ClassUID = string(class.UID)
	e.overhead = class.Overhead.PodFixed.DeepCopy()
	if err := e.journal.save(); err != nil {
		return err
	}
	nodes := e.api.CoreV1().Nodes()
	node, err := nodes.Get(ctx, e.node.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if e.journal.NodeUID != "" {
			return errors.New("registered execution Node disappeared; refusing a new incarnation")
		}
		node, err = nodes.Create(ctx, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: e.node.NodeName, Annotations: map[string]string{executionMarker: e.journal.Owner},
				Labels: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": runtime.GOARCH,
					"kubernetes.io/hostname": e.node.NodeName, runtimeLabel: e.config.RuntimeHandler},
			},
			Spec: corev1.NodeSpec{Taints: []corev1.Taint{
				{Key: runtimeLabel, Value: e.config.RuntimeHandler, Effect: corev1.TaintEffectNoSchedule},
			}},
		}, metav1.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("register execution Node: %w", err)
	}
	if node.UID == "" {
		return errors.New("execution Node has no UID")
	}
	if node.Annotations[executionMarker] != e.journal.Owner {
		return errors.New("execution requires a new node name or its original execution journal; existing Nodes are not taken over")
	}
	e.nodeUID = node.UID
	if e.journal.NodeUID != "" && e.journal.NodeUID != string(node.UID) {
		return errors.New("registered execution Node UID changed")
	}
	e.journal.NodeUID = string(node.UID)
	return e.journal.save()
}

func (e *executor) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := 4
	if e.images != nil {
		loops++
	}
	done := make(chan struct{}, loops)
	loop := func(interval time.Duration, wake <-chan struct{}, step func(context.Context) error) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := step(ctx); err != nil {
				e.logger.Print(err)
			}
			select {
			case <-ctx.Done():
				done <- struct{}{}
				return
			case <-ticker.C:
			case <-wake:
			}
		}
	}
	// A change to the assigned-Pod view wakes the next pass. Finished Pod
	// operations do not, because status operations finish on every pass.
	wake := make(chan struct{}, 1)
	e.watch = newAssignedPodWatch(wake)
	e.pulls.wake = wake
	go func() {
		e.watchAssigned(ctx, e.watch)
		done <- struct{}{}
	}()
	go loop(heartbeatInterval, nil, func(ctx context.Context) error {
		err := renewLease(ctx, e.api.CoordinationV1().Leases(leaseNamespace), e.node.NodeName, e.nodeUID)
		e.hooks.leaseRenewed(err)
		return err
	})
	go loop(5*time.Second, nil, e.updateHealth)
	go loop(inventoryInterval, wake, func(ctx context.Context) error { return e.reconcilePass(ctx, false) })
	if e.images != nil {
		go loop(imageGCInterval, nil, e.collectImages)
	}
	<-ctx.Done()
	cancel()
	for range loops {
		<-done
	}
	e.workers.running.Wait()
	e.pulls.wait()
	return shutdownError(ctx.Err())
}

func (e *executor) updateHealth(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			e.healthy.Store(false)
		}
	}()
	nodes := e.api.CoreV1().Nodes()
	node, err := nodes.Get(ctx, e.node.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if node.UID != e.nodeUID || node.Annotations[executionMarker] != e.journal.Owner {
		return errors.New("execution Node identity changed")
	}
	class, err := e.api.NodeV1().RuntimeClasses().Get(ctx, e.config.RuntimeClass, metav1.GetOptions{})
	if err == nil && (string(class.UID) != e.journal.ClassUID || class.Handler != e.config.RuntimeHandler ||
		class.Overhead == nil || !equality.Semantic.DeepEqual(class.Overhead.PodFixed, e.overhead)) {
		err = errors.New("execution RuntimeClass changed")
	}
	if err == nil {
		tainted := false
		for _, taint := range node.Spec.Taints {
			tainted = tainted || taint.Key == runtimeLabel && taint.Value == e.config.RuntimeHandler &&
				taint.Effect == corev1.TaintEffectNoSchedule
		}
		if !tainted || node.Labels[runtimeLabel] != e.config.RuntimeHandler {
			err = errors.New("execution Node runtime label or scheduling taint changed")
		}
	}
	if err == nil {
		prefix, parseErr := netip.ParsePrefix(node.Spec.PodCIDR)
		if parseErr != nil || len(node.Spec.PodCIDRs) > 1 {
			err = errors.New("waiting for one valid controller-assigned Pod CIDR")
		} else if prefix.String() != e.network {
			err = e.health.UpdatePodCIDR(ctx, prefix.String())
			if err == nil {
				e.network = prefix.String()
			}
		}
	}
	if err == nil {
		observed, statusErr := e.health.Status(ctx)
		err = statusErr
		if err == nil {
			ready := true
			for _, condition := range observed.Conditions {
				if condition.Type == "RuntimeReady" || condition.Type == "NetworkReady" {
					ready = ready && condition.Status
				}
			}
			e.healthy.Store(ready)
		}
	}
	if err != nil {
		e.healthy.Store(false)
	}
	ready := e.healthy.Load() && e.synced.Load() && !e.recovery.Load()
	e.hooks.healthUpdated(HealthState{RuntimeHealthy: e.healthy.Load(), Synced: e.synced.Load(), RecoveryRequired: e.recovery.Load()})
	statusErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, getErr := nodes.Get(ctx, e.node.NodeName, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if node.UID != e.nodeUID || node.Annotations[executionMarker] != e.journal.Owner {
			e.healthy.Store(false)
			return errors.New("refusing changed execution Node identity")
		}
		capacity := corev1.ResourceList{
			corev1.ResourceCPU: parseQuantity(e.config.CPU), corev1.ResourceMemory: parseQuantity(e.config.Memory),
			corev1.ResourcePods: *resource.NewQuantity(int64(e.config.MaxPods), resource.DecimalSI),
		}
		node.Status.Capacity, node.Status.Allocatable = capacity.DeepCopy(), capacity.DeepCopy()
		// NodeRestriction forbids node identities from updating taints.
		// Gate scheduling with allocatable; the node controller owns readiness taints.
		if !ready {
			for name := range node.Status.Allocatable {
				node.Status.Allocatable[name] = *resource.NewQuantity(0, resource.DecimalSI)
			}
		}
		node.Status.NodeInfo.OperatingSystem, node.Status.NodeInfo.Architecture = "linux", runtime.GOARCH
		addresses := []corev1.NodeAddress{
			{Type: corev1.NodeInternalIP, Address: e.config.NodeIP}, {Type: corev1.NodeHostName, Address: e.node.NodeName},
		}
		for _, address := range node.Status.Addresses {
			if address.Type != corev1.NodeInternalIP && address.Type != corev1.NodeHostName {
				addresses = append(addresses, address)
			}
		}
		node.Status.Addresses = addresses
		now := metav1.Now()
		condition := corev1.NodeCondition{
			Type: corev1.NodeReady, Status: corev1.ConditionFalse, Reason: "ExecutionNotReady",
			Message:           "Waiting for runtime/network health and complete API/runtime reconstruction",
			LastHeartbeatTime: now, LastTransitionTime: now,
		}
		if ready {
			condition.Status, condition.Reason, condition.Message = corev1.ConditionTrue, "ExecutionReady", "Restricted CRI execution enabled"
		} else if e.recovery.Load() {
			condition.Reason, condition.Message = "RecoveryRequired", "Unmatched, over-budget, or uncertain execution blocks new admission"
		}
		found := false
		for i, old := range node.Status.Conditions {
			if old.Type == corev1.NodeReady {
				if old.Status == condition.Status {
					condition.LastTransitionTime = old.LastTransitionTime
				}
				node.Status.Conditions[i], found = condition, true
			}
		}
		if !found {
			node.Status.Conditions = append(node.Status.Conditions, condition)
		}
		_, getErr = nodes.UpdateStatus(ctx, node, metav1.UpdateOptions{})
		return getErr
	})
	return errors.Join(err, statusErr)
}

func (e *executor) labelsMatch(labels map[string]string, record *executionRecord) bool {
	return labels[executionMarker] == e.journal.Owner && labels[podUIDLabel] == record.UID &&
		labels[configHashLabel] == record.Hash
}

// budgetFits reports whether a reservation fits the configured envelope next
// to every unfinished record. The caller holds e.mu.
func (e *executor) budgetFits(cpu, memory int64) bool {
	remainingCPU, remainingMemory, _ := executionResources(corev1.ResourceList{
		corev1.ResourceCPU: parseQuantity(e.config.CPU), corev1.ResourceMemory: parseQuantity(e.config.Memory),
	})
	count := 0
	for _, record := range e.journal.Records {
		if record.Stage == "finished" {
			continue
		}
		if record.CPU > remainingCPU || record.Memory > remainingMemory {
			return false
		}
		remainingCPU, remainingMemory = remainingCPU-record.CPU, remainingMemory-record.Memory
		count++
	}
	if cpu == 0 && memory == 0 {
		return count <= e.config.MaxPods
	}
	return count < e.config.MaxPods && cpu <= remainingCPU && memory <= remainingMemory
}
