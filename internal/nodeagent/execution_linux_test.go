// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type executionRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	runtimeapi.UnimplementedImageServiceServer
	mu         sync.Mutex
	sandboxes  map[string]*runtimeapi.PodSandbox
	containers map[string]*runtimeapi.Container
	configs    map[string]*runtimeapi.ContainerConfig
	calls      []string
	loseReply  string
	unhealthy  bool
	listError  bool
	onPull     func()
	failStop   bool
	stopGrace  []int64
	// reject answers the named create method with a status code. With
	// createOnReject, the object is created before the rejection is returned.
	reject         map[string]codes.Code
	createOnReject bool
	pullError      error
	// pullGate, when set, holds PullImage until it is closed.
	pullGate chan struct{}
	// images, imageFsUsed, and removedImages back image collection;
	// failImages names an image or list method that fails.
	images        []*runtimeapi.Image
	imageFsUsed   uint64
	removedImages []string
	failImages    string
}

func (s *executionRuntime) imageFailure(method string) error {
	if s.failImages == method {
		return status.Error(codes.Unavailable, method+" unavailable")
	}
	return nil
}

func (s *executionRuntime) ListImages(context.Context, *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	response := &runtimeapi.ListImagesResponse{}
	for _, image := range s.images {
		response.Images = append(response.Images, proto.Clone(image).(*runtimeapi.Image))
	}
	return response, s.imageFailure("ListImages")
}

func (s *executionRuntime) RemoveImage(_ context.Context, request *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RemoveImage")
	if err := s.imageFailure("RemoveImage"); err != nil {
		return nil, err
	}
	s.removedImages = append(s.removedImages, request.GetImage().GetImage())
	s.images = slices.DeleteFunc(s.images, func(image *runtimeapi.Image) bool { return image.Id == request.GetImage().GetImage() })
	return &runtimeapi.RemoveImageResponse{}, nil
}

func (s *executionRuntime) ImageFsInfo(context.Context, *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &runtimeapi.ImageFsInfoResponse{ImageFilesystems: []*runtimeapi.FilesystemUsage{{
		Timestamp: 1, FsId: &runtimeapi.FilesystemIdentifier{Mountpoint: "/var/lib/containerd"},
		UsedBytes: &runtimeapi.UInt64Value{Value: s.imageFsUsed},
	}}}, s.imageFailure("ImageFsInfo")
}

func (s *executionRuntime) Version(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	return &runtimeapi.VersionResponse{Version: "0.1.0", RuntimeName: "containerd", RuntimeVersion: "test", RuntimeApiVersion: "v1"}, nil
}

func (s *executionRuntime) Status(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &runtimeapi.StatusResponse{Status: &runtimeapi.RuntimeStatus{Conditions: []*runtimeapi.RuntimeCondition{
		{Type: runtimeapi.RuntimeReady, Status: !s.unhealthy}, {Type: runtimeapi.NetworkReady, Status: !s.unhealthy},
	}}}, nil
}

func (s *executionRuntime) UpdateRuntimeConfig(context.Context, *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
}

func (s *executionRuntime) ImageStatus(context.Context, *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	return &runtimeapi.ImageStatusResponse{}, nil
}

func (s *executionRuntime) PullImage(context.Context, *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	s.mu.Lock()
	gate := s.pullGate
	s.calls = append(s.calls, "PullImage")
	s.mu.Unlock()
	if gate != nil {
		<-gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onPull != nil {
		s.onPull()
	}
	if s.pullError != nil {
		return nil, s.pullError
	}
	return &runtimeapi.PullImageResponse{ImageRef: "sha256:image"}, nil
}

func (s *executionRuntime) RunPodSandbox(_ context.Context, request *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RunPodSandbox")
	if code, ok := s.reject["RunPodSandbox"]; ok && !s.createOnReject {
		return nil, status.Error(code, "sandbox rejected")
	}
	id := "sandbox-" + request.Config.Metadata.Uid
	s.sandboxes[id] = &runtimeapi.PodSandbox{
		Id: id, Metadata: proto.Clone(request.Config.Metadata).(*runtimeapi.PodSandboxMetadata),
		State: runtimeapi.PodSandboxState_SANDBOX_READY, CreatedAt: time.Now().UnixNano(),
		Labels: request.Config.Labels, RuntimeHandler: request.RuntimeHandler,
	}
	if code, ok := s.reject["RunPodSandbox"]; ok {
		return nil, status.Error(code, "sandbox rejected after a partial create")
	}
	if s.loseReply == "RunPodSandbox" {
		return nil, status.Error(codes.Unavailable, "lost committed sandbox reply")
	}
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: id}, nil
}

func (s *executionRuntime) CreateContainer(_ context.Context, request *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "CreateContainer")
	if code, ok := s.reject["CreateContainer"]; ok && !s.createOnReject {
		return nil, status.Error(code, "container rejected")
	}
	id := "container-" + request.PodSandboxId
	s.containers[id] = &runtimeapi.Container{
		Id: id, PodSandboxId: request.PodSandboxId,
		Metadata: proto.Clone(request.Config.Metadata).(*runtimeapi.ContainerMetadata),
		State:    runtimeapi.ContainerState_CONTAINER_CREATED, CreatedAt: time.Now().UnixNano(),
		Labels: request.Config.Labels, Image: request.Config.Image, ImageRef: "sha256:image",
	}
	s.configs[id] = request.Config
	if code, ok := s.reject["CreateContainer"]; ok {
		return nil, status.Error(code, "container rejected after a partial create")
	}
	if s.loseReply == "CreateContainer" {
		return nil, status.Error(codes.Unavailable, "lost committed container reply")
	}
	return &runtimeapi.CreateContainerResponse{ContainerId: id}, nil
}

func (s *executionRuntime) StartContainer(_ context.Context, request *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "StartContainer")
	s.containers[request.ContainerId].State = runtimeapi.ContainerState_CONTAINER_RUNNING
	if s.loseReply == "StartContainer" {
		return nil, status.Error(codes.Unavailable, "lost committed start reply")
	}
	return &runtimeapi.StartContainerResponse{}, nil
}

func (s *executionRuntime) ListPodSandbox(context.Context, *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listError {
		return nil, status.Error(codes.Unavailable, "inventory unavailable")
	}
	if err := s.imageFailure("ListPodSandbox"); err != nil {
		return nil, err
	}
	response := &runtimeapi.ListPodSandboxResponse{}
	for _, sandbox := range s.sandboxes {
		response.Items = append(response.Items, proto.Clone(sandbox).(*runtimeapi.PodSandbox))
	}
	return response, nil
}

func (s *executionRuntime) ListContainers(context.Context, *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.imageFailure("ListContainers"); err != nil {
		return nil, err
	}
	response := &runtimeapi.ListContainersResponse{}
	for _, container := range s.containers {
		response.Containers = append(response.Containers, proto.Clone(container).(*runtimeapi.Container))
	}
	return response, nil
}

func (s *executionRuntime) PodSandboxStatus(_ context.Context, request *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sandbox := s.sandboxes[request.PodSandboxId]
	if sandbox == nil {
		return nil, status.Error(codes.NotFound, "sandbox disappeared")
	}
	return &runtimeapi.PodSandboxStatusResponse{Status: &runtimeapi.PodSandboxStatus{
		Id: sandbox.Id, Metadata: sandbox.Metadata, State: sandbox.State, CreatedAt: sandbox.CreatedAt,
		Labels: sandbox.Labels, RuntimeHandler: sandbox.RuntimeHandler,
		Network: &runtimeapi.PodSandboxNetworkStatus{Ip: "10.244.0.2"},
	}}, nil
}

func (s *executionRuntime) ContainerStatus(_ context.Context, request *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	container := s.containers[request.ContainerId]
	if container == nil {
		return nil, status.Error(codes.NotFound, "container disappeared")
	}
	return &runtimeapi.ContainerStatusResponse{Status: &runtimeapi.ContainerStatus{
		Id: container.Id, Metadata: container.Metadata, State: container.State, CreatedAt: container.CreatedAt,
		Labels: container.Labels, Image: container.Image, ImageRef: container.ImageRef, StartedAt: container.CreatedAt,
		FinishedAt: container.CreatedAt + 1,
	}}, nil
}

func (s *executionRuntime) StopContainer(_ context.Context, request *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "StopContainer")
	s.stopGrace = append(s.stopGrace, request.Timeout)
	if container := s.containers[request.ContainerId]; container != nil {
		container.State = runtimeapi.ContainerState_CONTAINER_EXITED
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

func (s *executionRuntime) StopPodSandbox(context.Context, *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "StopPodSandbox")
	if s.failStop {
		s.failStop = false
		return nil, status.Error(codes.Unavailable, "sandbox stop interrupted")
	}
	return &runtimeapi.StopPodSandboxResponse{}, nil
}

func (s *executionRuntime) RemoveContainer(_ context.Context, request *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RemoveContainer")
	delete(s.containers, request.ContainerId)
	return &runtimeapi.RemoveContainerResponse{}, nil
}

func (s *executionRuntime) RemovePodSandbox(_ context.Context, request *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "RemovePodSandbox")
	delete(s.sandboxes, request.PodSandboxId)
	return &runtimeapi.RemovePodSandboxResponse{}, nil
}

func executionConfig(t *testing.T) ExecutionConfig {
	t.Helper()
	return ExecutionConfig{
		RuntimeClass: "kata-fc", RuntimeHandler: "kata-fc", CPU: "4", Memory: "2Gi", MaxPods: 4,
		NodeIP: "192.0.2.10", ClusterDNS: "10.96.0.10", ClusterDomain: "cluster.local",
		StateDir: filepath.Join(t.TempDir(), "state"), LogDir: t.TempDir(),
	}
}

func executionPod(name string) *corev1.Pod {
	pod := profilePod()
	pod.Name, pod.UID = name, types.UID(name+"-uid")
	pod.Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}
	pod.Spec.Tolerations = []corev1.Toleration{{Key: runtimeLabel, Value: "kata-fc", Effect: corev1.TaintEffectNoSchedule}}
	pod.Spec.NodeSelector = map[string]string{runtimeLabel: "kata-fc"}
	return pod
}

func newExecutor(t *testing.T, pods ...*corev1.Pod) (*executor, *executionRuntime) {
	t.Helper()
	config := executionConfig(t)
	journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.close() })
	api := fake.NewClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", UID: "node-uid",
			Annotations: map[string]string{executionMarker: journal.Owner}, Labels: map[string]string{runtimeLabel: "kata-fc"}},
		Spec: corev1.NodeSpec{PodCIDR: "10.244.0.0/24", Taints: []corev1.Taint{
			{Key: runtimeLabel, Value: "kata-fc", Effect: corev1.TaintEffectNoSchedule},
		}},
	}, &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "kata-fc", UID: "class-uid"}, Handler: "kata-fc",
		Overhead: &nodev1.Overhead{PodFixed: executionPod("overhead").Spec.Overhead},
	})
	for _, pod := range pods {
		if _, err := api.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	service := &executionRuntime{
		sandboxes: make(map[string]*runtimeapi.PodSandbox), containers: make(map[string]*runtimeapi.Container),
		configs: make(map[string]*runtimeapi.ContainerConfig),
	}
	socket := filepath.Join(t.TempDir(), "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, service)
	runtimeapi.RegisterImageServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client, err := cri.New(socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	health, err := cri.New(socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = health.Close() })
	creation, err := cri.NewConcurrent(socket, time.Second, creationWorkers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = creation.Close() })
	pulls, err := cri.NewConcurrent(socket, 5*time.Second, pullWorkers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pulls.Close() })
	e := &executor{node: Config{NodeName: "worker-1", RuntimeSocket: socket}, config: config,
		api: api, journal: journal, runtime: client, create: creation, health: health,
		workers: newPodWorkers(), pulls: newImagePuller(pulls), logger: log.New(io.Discard, "", 0)}
	if err := e.initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := e.updateHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	return e, service
}

func reconcilePasses(t *testing.T, e *executor, count int) {
	t.Helper()
	for range count {
		if err := e.reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExecutionMultiplePodsAndRecovery(t *testing.T) {
	t.Parallel()
	for _, lost := range []string{"", "RunPodSandbox", "CreateContainer", "StartContainer"} {
		t.Run("lost-"+lost, func(t *testing.T) {
			e, service := newExecutor(t, executionPod("first"), executionPod("second"))
			e.config.MaxPods = 2
			service.loseReply = lost
			restarted := false
			for range 10 {
				err := e.reconcile(t.Context())
				if err != nil && status.Code(err) != codes.Unavailable {
					t.Fatal(err)
				}
				if err != nil && !restarted {
					if err := e.journal.close(); err != nil {
						t.Fatal(err)
					}
					e.journal, err = openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config)
					if err != nil {
						t.Fatal(err)
					}
					journal := e.journal
					t.Cleanup(func() { _ = journal.close() })
					e.synced.Store(false)
					restarted = true
				}
			}
			for _, name := range []string{"first", "second"} {
				pod, err := e.api.CoreV1().Pods("default").Get(t.Context(), name, metav1.GetOptions{})
				if err != nil || pod.Status.Phase != corev1.PodRunning || !pod.Status.ContainerStatuses[0].Ready {
					t.Fatalf("Pod not running: %v, %v", pod.Status, err)
				}
			}
			service.mu.Lock()
			before := len(service.calls)
			for _, method := range []string{"RunPodSandbox", "CreateContainer", "StartContainer"} {
				count := 0
				for _, call := range service.calls {
					if call == method {
						count++
					}
				}
				if count != 2 {
					t.Fatalf("%s called %d times for two Pods", method, count)
				}
			}
			for _, config := range service.configs {
				if !config.Linux.SecurityContext.NoNewPrivs || config.Linux.Resources.CpuQuota != 100000 ||
					config.Linux.Resources.MemoryLimitInBytes != 128*1024*1024 ||
					len(config.Linux.SecurityContext.Capabilities.DropCapabilities) != 1 ||
					len(config.Mounts) != 0 || len(config.Annotations) != 0 {
					t.Fatalf("unsafe or incomplete CRI translation: %v", config)
				}
			}
			service.mu.Unlock()
			if err := e.journal.close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config)
			if err != nil {
				t.Fatal(err)
			}
			e.journal = reopened
			t.Cleanup(func() { _ = reopened.close() })
			e.synced.Store(false)
			reconcilePasses(t, e, 2)
			if len(service.calls) != before {
				t.Fatal("restart mutated surviving workloads")
			}
		})
	}
}

func TestExecutionDeletionAndExit(t *testing.T) {
	t.Parallel()
	for _, deleting := range []bool{false, true} {
		t.Run("deleting-"+map[bool]string{false: "false", true: "true"}[deleting], func(t *testing.T) {
			e, service := newExecutor(t, executionPod("app"))
			reconcilePasses(t, e, 5)
			pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
			if deleting {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				if _, err := e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				service.mu.Lock()
				for _, container := range service.containers {
					container.State = runtimeapi.ContainerState_CONTAINER_EXITED
				}
				service.mu.Unlock()
			}
			reconcilePasses(t, e, 3)
			pod, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
			if deleting {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("deleted Pod retained: %v", err)
				}
			} else if err != nil || pod.Status.Phase != corev1.PodSucceeded {
				t.Fatalf("exit not reported truthfully: %v, %v", pod.Status, err)
			}
			if len(service.sandboxes) != 0 || len(service.containers) != 0 {
				t.Fatal("runtime cleanup incomplete")
			}
			// A terminal Pod that is still listed keeps its finished record;
			// a deleted Pod's record is compacted once nothing remains.
			record := e.journal.Records["app-uid"]
			if deleting && record != nil || !deleting && (record == nil || record.Stage != "finished" || !record.SandboxRemoved) {
				t.Fatalf("reservation release or compaction incorrect: %+v", record)
			}
		})
	}
}

func TestExecutionAdmissionAndHealth(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"budget", "unsupported", "foreign", "unavailable", "unhealthy"} {
		t.Run(kind, func(t *testing.T) {
			pod := executionPod("app")
			e, service := newExecutor(t, pod)
			switch kind {
			case "budget":
				e.config.CPU = "1"
			case "unsupported":
				pod.Spec.HostNetwork = true
				_, _ = e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{})
			case "foreign":
				service.sandboxes["foreign"] = &runtimeapi.PodSandbox{Id: "foreign", CreatedAt: 1,
					Metadata: &runtimeapi.PodSandboxMetadata{Name: "foreign", Namespace: "default", Uid: "foreign-uid"}}
			case "unavailable":
				service.listError = true
			case "unhealthy":
				service.unhealthy = true
				if err := e.updateHealth(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				err := e.reconcile(t.Context())
				if kind == "unavailable" && err == nil {
					t.Fatal("runtime outage interpreted as empty inventory")
				}
			}
			if len(service.calls) != 0 {
				t.Fatalf("admission guard bypassed: %v", service.calls)
			}
			if kind == "budget" || kind == "unsupported" {
				got, _ := e.api.CoreV1().Pods("default").Get(t.Context(), pod.Name, metav1.GetOptions{})
				if got.Status.Phase != corev1.PodFailed {
					t.Fatal("rejected Pod has no explicit failed status")
				}
			}
			if err := e.updateHealth(t.Context()); err != nil {
				t.Fatal(err)
			}
			node, _ := e.api.CoreV1().Nodes().Get(t.Context(), "worker-1", metav1.GetOptions{})
			if kind != "budget" && kind != "unsupported" && node.Status.Conditions[0].Status != corev1.ConditionFalse {
				t.Fatal("unreconstructed or unhealthy node reported Ready")
			}
		})
	}
}

func TestExecutionReadinessWithNodeRestriction(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"initializing", "ready", "unhealthy", "recovering"} {
		t.Run(phase, func(t *testing.T) {
			e, service := newExecutor(t, executionPod("app"))
			e.api.(*fake.Clientset).PrependReactor("update", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() == "" {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"},
						e.node.NodeName, errors.New("node identities cannot update taints"))
				}
				return false, nil, nil
			})
			if phase != "initializing" {
				reconcilePasses(t, e, 5)
			}
			service.mu.Lock()
			service.unhealthy = phase == "unhealthy"
			service.mu.Unlock()
			e.recovery.Store(phase == "recovering")
			if err := e.updateHealth(t.Context()); err != nil {
				t.Fatal(err)
			}
			node, err := e.api.CoreV1().Nodes().Get(t.Context(), e.node.NodeName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourcePods} {
				allocatable, ok := node.Status.Allocatable[name]
				if !ok || (phase != "ready" && !allocatable.IsZero()) ||
					(phase == "ready" && allocatable.Cmp(node.Status.Capacity[name]) != 0) {
					t.Fatalf("%s allocatable %s = %v", phase, name, allocatable.String())
				}
			}
		})
	}
}

func TestExecutionConfigurationAndJournal(t *testing.T) {
	t.Parallel()
	for _, change := range []func(*ExecutionConfig){
		func(c *ExecutionConfig) { c.RuntimeClass = "" }, func(c *ExecutionConfig) { c.RuntimeHandler = "bad/handler" },
		func(c *ExecutionConfig) { c.CPU = "invalid" }, func(c *ExecutionConfig) { c.CPU = "0.0001" },
		func(c *ExecutionConfig) { c.Memory = "-1" }, func(c *ExecutionConfig) { c.MaxPods = 0 },
		func(c *ExecutionConfig) { c.MaxPods = 10001 }, func(c *ExecutionConfig) { c.NodeIP = "127.0.0.1" },
		func(c *ExecutionConfig) { c.ClusterDNS = "" }, func(c *ExecutionConfig) { c.ClusterDomain = ".." },
		func(c *ExecutionConfig) { c.StateDir = "relative" }, func(c *ExecutionConfig) { c.LogDir = c.StateDir },
		func(c *ExecutionConfig) { c.ImageGCThreshold = "0" }, func(c *ExecutionConfig) { c.ImageGCThreshold = "-1Gi" },
		func(c *ExecutionConfig) { c.ImageGCThreshold = "0.5" }, func(c *ExecutionConfig) { c.ImageGCThreshold = "invalid" },
	} {
		config := executionConfig(t)
		change(&config)
		if err := config.Validate(); err == nil {
			t.Fatalf("accepted invalid config: %+v", config)
		}
	}
	config := executionConfig(t)
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openExecutionJournal(config.StateDir, "worker-1", config); err == nil {
		t.Fatal("concurrent execution owners accepted")
	}
	if err := journal.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openExecutionJournal(config.StateDir, "other-node", config); err == nil {
		t.Fatal("journal reused for another node")
	}
	if err := os.WriteFile(filepath.Join(config.StateDir, "state.json"), []byte(`{"Version":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := openExecutionJournal(config.StateDir, "worker-1", config); err == nil {
		t.Fatal("unknown journal schema discarded")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Execute(ctx, Config{NodeName: "worker-1", RuntimeSocket: "/unused"}, config, "/unused", io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := Execute(ctx, Config{NodeName: "worker-1"}, config, "", io.Discard); err == nil {
		t.Fatal("execution without runtime or API accepted")
	}
	if err := Execute(ctx, Config{}, config, "", io.Discard); err == nil {
		t.Fatal("execution without a node accepted")
	}
	if err := Execute(ctx, Config{NodeName: "worker-1"}, ExecutionConfig{}, "", io.Discard); err == nil {
		t.Fatal("execution without explicit configuration accepted")
	}
}

func TestExecutionJournalRejectsUnknownPayload(t *testing.T) {
	t.Parallel()
	for name, modify := range map[string]func([]byte) []byte{
		"unknown field": func(data []byte) []byte {
			return append([]byte(`{"Future":true,`), data[1:]...)
		},
		"trailing JSON": func(data []byte) []byte {
			return append(data, []byte(`{}`)...)
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := executionConfig(t)
			journal, err := openExecutionJournal(config.StateDir, "worker-1", config)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(journal)
			if err != nil {
				t.Fatal(err)
			}
			if err := journal.close(); err != nil {
				t.Fatal(err)
			}
			data = modify(data)
			sum := sha256.Sum256(data)
			envelope, err := json.Marshal(struct {
				Data   []byte
				SHA256 string
			}{Data: data, SHA256: hex.EncodeToString(sum[:])})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config.StateDir, "state.json"), envelope, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := openExecutionJournal(config.StateDir, "worker-1", config); err == nil {
				t.Fatal("unsupported journal payload was accepted despite a valid checksum")
			}
		})
	}
}

func TestExecutionUncertainAbsence(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	service.loseReply = "RunPodSandbox"
	// The first pass synchronizes; the second admits the Pod and pulls its image.
	reconcilePasses(t, e, 2)
	if err := e.reconcile(t.Context()); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected uncertain mutation: %v", err)
	}
	service.mu.Lock()
	clear(service.sandboxes)
	before := len(service.calls)
	service.mu.Unlock()
	for range 3 {
		if err := e.reconcile(t.Context()); err == nil {
			t.Fatal("unknown outcome treated as absence")
		}
	}
	if len(service.calls) != before {
		t.Fatal("uncertain mutation retried")
	}
	pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	_, _ = e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{})
	if err := e.reconcile(t.Context()); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("uncertain deletion was finalized: %v", err)
	}
	_, err := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if errors.Is(err, os.ErrNotExist) || apierrors.IsNotFound(err) {
		t.Fatal("deletion lost its uncertain execution identity")
	}
}

func TestExecutionRecoveryDoesNotStarveOtherPods(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unresolved", "ambiguous"} {
		t.Run(kind, func(t *testing.T) {
			first, second := executionPod("first"), executionPod("second")
			first.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
			second.CreationTimestamp = metav1.NewTime(time.Now())
			e, service := newExecutor(t, first, second)
			reconcilePasses(t, e, 5)
			record := e.journal.Records[string(first.UID)]
			service.mu.Lock()
			if kind == "unresolved" {
				delete(service.sandboxes, record.SandboxID)
				delete(service.containers, record.ContainerID)
				record.Stage, record.SandboxID, record.ContainerID = "sandbox-pending", "", ""
			} else {
				service.sandboxes[record.SandboxID].Labels[executionMarker] = "another-owner"
			}
			service.containers[e.journal.Records[string(second.UID)].ContainerID].State = runtimeapi.ContainerState_CONTAINER_EXITED
			before := len(service.calls)
			service.mu.Unlock()
			if err := e.journal.save(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := e.reconcile(t.Context()); err == nil {
					t.Fatal("uncertain execution was not reported")
				}
			}
			pod, err := e.api.CoreV1().Pods(second.Namespace).Get(t.Context(), second.Name, metav1.GetOptions{})
			if err != nil || pod.Status.Phase != corev1.PodSucceeded {
				t.Fatalf("another Pod's exit was not reported: %v, %v", pod, err)
			}
			if len(service.calls) != before+4 || e.journal.Records[string(second.UID)].Stage != "finished" {
				t.Fatal("another Pod's cleanup was starved or uncertain execution was mutated")
			}
			if !e.recovery.Load() || e.synced.Load() || record.Stage == "finished" {
				t.Fatal("uncertain execution lost its admission barrier or reservation")
			}
		})
	}
}

func TestExecutionOwnershipAndAdministratorFields(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	node, _ := e.api.CoreV1().Nodes().Get(t.Context(), "worker-1", metav1.GetOptions{})
	node.Spec.Unschedulable = true
	node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{Key: "administrator", Effect: corev1.TaintEffectNoSchedule})
	node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{Type: "AdministratorCondition", Status: corev1.ConditionTrue})
	node.Status.Addresses = append(node.Status.Addresses, corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "192.0.2.11"})
	_, _ = e.api.CoreV1().Nodes().Update(t.Context(), node, metav1.UpdateOptions{})
	reconcilePasses(t, e, 5)
	if err := e.updateHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	node, _ = e.api.CoreV1().Nodes().Get(t.Context(), "worker-1", metav1.GetOptions{})
	if !node.Spec.Unschedulable || len(node.Spec.Taints) != 2 || len(node.Status.Addresses) != 3 ||
		node.Status.Conditions[len(node.Status.Conditions)-1].Type != "AdministratorCondition" {
		t.Fatalf("administrator-owned Node fields overwritten: %+v", node)
	}
	service.mu.Lock()
	for _, container := range service.containers {
		container.State = runtimeapi.ContainerState_CONTAINER_UNKNOWN
	}
	before := len(service.calls)
	service.mu.Unlock()
	reconcilePasses(t, e, 1)
	pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if pod.Status.Phase != corev1.PodUnknown || pod.Status.ContainerStatuses[0].Ready || len(service.calls) != before {
		t.Fatal("unknown runtime state became an exit, readiness, or restart")
	}
	node.Annotations[executionMarker] = "another-owner"
	_, _ = e.api.CoreV1().Nodes().Update(t.Context(), node, metav1.UpdateOptions{})
	if err := e.updateHealth(t.Context()); err == nil || e.healthy.Load() {
		t.Fatal("changed node ownership retained execution authorization")
	}
	if _, err := e.currentPod(t.Context(), pod, false); err == nil {
		t.Fatal("changed node ownership allowed a mutation")
	}
}

// TestExecutionDeletionDuringPull is a regression test for section 10.4 of
// DESIGN.md: Pod intent is re-read after the image wait and before sandbox
// creation, so a Pod deleted during its pull never reaches the runtime even
// when the pass still sees it undeleted.
func TestExecutionDeletionDuringPull(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	stale, err := e.api.CoreV1().Pods("default").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.onPull = func() {
		pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		_, _ = e.api.CoreV1().Pods("default").Update(t.Context(), pod, metav1.UpdateOptions{})
	}
	reconcilePasses(t, e, 2)
	// The next pass reads a view that predates the deletion.
	e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, stale.DeepCopy(), nil
	})
	if err := e.reconcile(t.Context()); err == nil {
		t.Fatal("deletion during image pull was ignored")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.calls) != 1 || service.calls[0] != "PullImage" || e.journal.Records["app-uid"].Stage != "reserved" {
		t.Fatalf("deleted Pod reached sandbox execution: %v", service.calls)
	}
}

func TestExecutionReplacementUIDDoesNotCleanUp(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 5)
	old := e.journal.Records["app-uid"]
	_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
	replacement := executionPod("app")
	replacement.UID = "replacement-uid"
	_, _ = e.api.CoreV1().Pods("default").Create(t.Context(), replacement, metav1.CreateOptions{})
	gets := denyPodReads(e)
	service.mu.Lock()
	before := len(service.calls)
	service.mu.Unlock()
	reconcilePasses(t, e, 1)
	// The old execution is removed on list evidence, never on a name-based
	// read that would resolve to the replacement, and no replacement starts
	// in the same pass.
	if *gets != 0 || old.Stage != "finished" || old.DeletionObserved == "" ||
		e.journal.Records["replacement-uid"] != nil || !e.recovery.Load() {
		t.Fatalf("old execution not cleaned up by list evidence: reads=%d record=%+v", *gets, old)
	}
	service.mu.Lock()
	removed := service.calls[before:]
	service.mu.Unlock()
	if strings.Join(removed, ",") != "StopContainer,StopPodSandbox,RemoveContainer,RemovePodSandbox" {
		t.Fatalf("unexpected cleanup calls: %v", removed)
	}
}

// denyPodReads makes exact Pod reads fail as the Node authorizer does for a Pod
// no longer bound to the node, and counts them.
func denyPodReads(e *executor) *int {
	var count int
	e.api.(*fake.Clientset).PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		count++
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"},
			action.(ktesting.GetAction).GetName(), errors.New("no relationship found between node and this object"))
	})
	return &count
}

func TestExecutionDeletionEvidence(t *testing.T) {
	t.Parallel()
	podsResource := corev1.SchemeGroupVersion.WithResource("pods")
	podsKind := corev1.SchemeGroupVersion.WithKind("Pod")
	for _, tc := range []struct {
		name string
		// prepare runs after the Pod is running and returns whether its
		// execution must be cleaned up.
		prepare func(*testing.T, *executor, *executionRuntime) bool
		calls   string
	}{
		{
			name: "terminal Pod deleted between polls",
			prepare: func(t *testing.T, e *executor, service *executionRuntime) bool {
				service.mu.Lock()
				for _, container := range service.containers {
					container.State = runtimeapi.ContainerState_CONTAINER_EXITED
				}
				service.mu.Unlock()
				// Publish the exit, then delete the terminal Pod before cleanup.
				pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
				sandbox, _ := e.runtime.PodSandboxStatus(t.Context(), e.journal.Records["app-uid"].SandboxID)
				container, _ := e.runtime.ContainerStatus(t.Context(), e.journal.Records["app-uid"].ContainerID)
				if err := e.publishPodStatus(t.Context(), pod, sandbox, container, "", ""); err != nil {
					t.Fatal(err)
				}
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				return true
			},
			calls: "StopContainer,StopPodSandbox,RemoveContainer,RemovePodSandbox",
		},
		{
			name: "running Pod force-deleted",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				return true
			},
			calls: "StopContainer,StopPodSandbox,RemoveContainer,RemovePodSandbox",
		},
		{
			name: "sandbox already removed by operator",
			prepare: func(t *testing.T, e *executor, service *executionRuntime) bool {
				service.mu.Lock()
				clear(service.sandboxes)
				clear(service.containers)
				service.mu.Unlock()
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				return true
			},
		},
		{
			name: "list unavailable",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewServiceUnavailable("etcd unavailable")
				})
				return false
			},
		},
		{
			name: "list forbidden",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(podsResource.GroupResource(), "", errors.New("denied"))
				})
				return false
			},
		},
		{
			name: "list without resource version",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				tracker := e.api.(*fake.Clientset).Tracker()
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					list, err := tracker.List(podsResource, podsKind, "")
					if err == nil {
						list.(*corev1.PodList).ResourceVersion = ""
					}
					return true, list, err
				})
				return false
			},
		},
		{
			name: "truncated list",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				tracker := e.api.(*fake.Clientset).Tracker()
				pages := 0
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					if pages++; pages%2 == 0 {
						return true, nil, apierrors.NewResourceExpired("continue token expired")
					}
					list, err := tracker.List(podsResource, podsKind, "")
					if err == nil {
						list.(*corev1.PodList).Continue = "next"
					}
					return true, list, err
				})
				return false
			},
		},
		{
			name: "pages from different snapshots",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				tracker := e.api.(*fake.Clientset).Tracker()
				pages := 0
				e.api.(*fake.Clientset).PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					list, err := tracker.List(podsResource, podsKind, "")
					if pages++; err == nil && pages%2 == 1 {
						list.(*corev1.PodList).Continue, list.(*corev1.PodList).ResourceVersion = "next", "1"
					}
					return true, list, err
				})
				return false
			},
		},
		{
			name: "foreign owner label",
			prepare: func(t *testing.T, e *executor, service *executionRuntime) bool {
				service.mu.Lock()
				service.sandboxes[e.journal.Records["app-uid"].SandboxID].Labels[executionMarker] = "another-owner"
				service.mu.Unlock()
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				return false
			},
		},
		{
			name: "container in another sandbox claims the Pod",
			prepare: func(t *testing.T, e *executor, service *executionRuntime) bool {
				service.mu.Lock()
				service.containers["stray"] = &runtimeapi.Container{Id: "stray", PodSandboxId: "foreign", CreatedAt: 1,
					Metadata: &runtimeapi.ContainerMetadata{Name: "app"}, Labels: map[string]string{podUIDLabel: "app-uid"},
					Image: &runtimeapi.ImageSpec{Image: "image"}, ImageRef: "image"}
				service.mu.Unlock()
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
				return false
			},
		},
		{
			name: "recorded deletion contradicted by a listed Pod",
			prepare: func(t *testing.T, e *executor, _ *executionRuntime) bool {
				e.journal.Records["app-uid"].DeletionObserved = "1"
				if err := e.journal.save(); err != nil {
					t.Fatal(err)
				}
				return false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			reconcilePasses(t, e, 5)
			record := e.journal.Records["app-uid"]
			if record.Stage != "running" {
				t.Fatalf("execution not running: %+v", record)
			}
			removes := tc.prepare(t, e, service)
			observed := record.DeletionObserved
			gets := denyPodReads(e)
			service.mu.Lock()
			before := len(service.calls)
			service.mu.Unlock()
			for range 2 {
				err := e.reconcile(t.Context())
				if !removes && err == nil && tc.name != "foreign owner label" {
					t.Fatal("unproven deletion was not reported")
				}
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			calls := strings.Join(service.calls[before:], ",")
			if !removes {
				if calls != "" || record.Stage != "running" || record.DeletionObserved != observed ||
					!e.recovery.Load() && e.synced.Load() || len(service.sandboxes) != 1 {
					t.Fatalf("unproven deletion mutated execution: calls=%q record=%+v", calls, record)
				}
				return
			}
			if calls != tc.calls || *gets != 0 || record.Stage != "finished" || record.DeletionObserved == "" ||
				len(service.sandboxes) != 0 || len(service.containers) != 0 {
				t.Fatalf("deleted execution not removed on list evidence: calls=%q reads=%d record=%+v", calls, *gets, record)
			}
			for _, grace := range service.stopGrace[len(service.stopGrace)-min(len(service.stopGrace), 1):] {
				if tc.calls != "" && grace != 0 {
					t.Fatalf("deleted Pod stopped with grace %d", grace)
				}
			}
			if !e.budgetFits(0, 0) || e.recovery.Load() {
				t.Fatal("reservation or admission barrier not released after cleanup")
			}
		})
	}
}

func TestExecutionDeletionEvidenceSurvivesRestart(t *testing.T) {
	t.Parallel()
	e, service := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 5)
	_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
	gets := denyPodReads(e)
	service.mu.Lock()
	service.failStop = true
	service.mu.Unlock()
	if err := e.reconcile(t.Context()); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected interrupted cleanup: %v", err)
	}
	if err := e.journal.close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.close() })
	e.journal = reopened
	e.synced.Store(false)
	record := reopened.Records["app-uid"]
	if record.DeletionObserved == "" || record.Stage != "running" {
		t.Fatalf("deletion evidence not durable before cleanup: %+v", record)
	}
	reconcilePasses(t, e, 2)
	if *gets != 0 || record.Stage != "finished" || len(service.sandboxes) != 0 {
		t.Fatalf("restart did not resume evidenced cleanup: reads=%d record=%+v", *gets, record)
	}
}

func TestExecutionDeletionEvidenceReleasesUnstartedReservation(t *testing.T) {
	t.Parallel()
	for _, lost := range []string{"", "RunPodSandbox"} {
		t.Run("lost-"+lost, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			service.loseReply = lost
			if lost == "" {
				// Delete the Pod during its image pull, before any sandbox exists.
				service.onPull = func() {
					_ = e.api.CoreV1().Pods("default").Delete(context.Background(), "app", metav1.DeleteOptions{})
				}
			}
			reconcilePasses(t, e, 1)
			_ = e.reconcile(t.Context())
			if lost != "" {
				// The pull finished in the previous pass; this one creates the sandbox.
				_ = e.reconcile(t.Context())
				// The sandbox creation outcome is unknown and its object is gone.
				service.mu.Lock()
				clear(service.sandboxes)
				service.mu.Unlock()
				_ = e.api.CoreV1().Pods("default").Delete(t.Context(), "app", metav1.DeleteOptions{})
			}
			service.mu.Lock()
			before := len(service.calls)
			service.mu.Unlock()
			err := e.reconcile(t.Context())
			record := e.journal.Records["app-uid"]
			if len(service.calls) != before || record.DeletionObserved == "" {
				t.Fatalf("deleted Pod not evidenced or runtime mutated: %v %+v", service.calls, record)
			}
			if lost == "" && (err != nil || record.Stage != "finished") {
				t.Fatalf("unstarted reservation not released: %v %+v", err, record)
			}
			if lost != "" && (err == nil || record.Stage != "sandbox-pending" || !e.recovery.Load()) {
				t.Fatalf("unresolved creation released on deletion evidence: %v %+v", err, record)
			}
		})
	}
}

func TestExecutionJournalRejectsOversizedDeletionEvidence(t *testing.T) {
	t.Parallel()
	e, _ := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 5)
	e.journal.Records["app-uid"].DeletionObserved = strings.Repeat("9", maxResourceVersion+1)
	if err := e.journal.save(); err != nil {
		t.Fatal(err)
	}
	if err := e.journal.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openExecutionJournal(e.config.StateDir, e.node.NodeName, e.config); err == nil {
		t.Fatal("oversized deletion evidence accepted")
	}
}
