// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func goodSandbox() *runtimeapi.PodSandbox {
	return &runtimeapi.PodSandbox{
		Id: "sandbox", Metadata: &runtimeapi.PodSandboxMetadata{Name: "pod", Namespace: "default", Uid: "pod-uid", Attempt: 3},
		State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY, CreatedAt: 123,
		RuntimeHandler: "kata-fc", Labels: map[string]string{"io.kubernetes.pod.uid": "pod-uid"},
		Annotations: map[string]string{"example.test/generation": "7"},
	}
}

func goodContainer() *runtimeapi.Container {
	return &runtimeapi.Container{
		Id: "container", PodSandboxId: "sandbox", Metadata: &runtimeapi.ContainerMetadata{Name: "app", Attempt: 4},
		State: runtimeapi.ContainerState_CONTAINER_UNKNOWN, CreatedAt: 124,
		Image:    &runtimeapi.ImageSpec{Image: "example.test/app@sha256:" + strings.Repeat("a", 64)},
		ImageRef: "digest", ImageId: "image-id", Labels: map[string]string{"io.kubernetes.pod.uid": "pod-uid"},
		Annotations: map[string]string{"example.test/generation": "7"},
	}
}

func goodSandboxStatus() *runtimeapi.PodSandboxStatus {
	sandbox := goodSandbox()
	return &runtimeapi.PodSandboxStatus{
		Id: sandbox.Id, Metadata: sandbox.Metadata, State: sandbox.State, CreatedAt: sandbox.CreatedAt,
		RuntimeHandler: sandbox.RuntimeHandler, Labels: sandbox.Labels, Annotations: sandbox.Annotations,
		Network: &runtimeapi.PodSandboxNetworkStatus{Ip: "192.0.2.1"},
	}
}

func goodContainerStatus() *runtimeapi.ContainerStatus {
	container := goodContainer()
	return &runtimeapi.ContainerStatus{
		Id: container.Id, Metadata: container.Metadata, State: container.State, CreatedAt: container.CreatedAt,
		Image: container.Image, ImageRef: container.ImageRef, ImageId: container.ImageId,
		Labels: container.Labels, Annotations: container.Annotations,
		StartedAt: 125, FinishedAt: 126, ExitCode: 137, Reason: "OOMKilled", Message: "observed",
	}
}

func TestSandboxInventory(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(*runtimeapi.PodSandbox)
		valid  bool
	}{
		{name: "unready", valid: true},
		{name: "ready", valid: true, change: func(s *runtimeapi.PodSandbox) { s.State = runtimeapi.PodSandboxState_SANDBOX_READY }},
		{name: "foreign", valid: true, change: func(s *runtimeapi.PodSandbox) {
			s.Id = "opaque/runtime:id"
			s.RuntimeHandler, s.Labels, s.Annotations = "", nil, nil
		}},
		{name: "missing id", change: func(s *runtimeapi.PodSandbox) { s.Id = "" }},
		{name: "invalid id", change: func(s *runtimeapi.PodSandbox) { s.Id = "\xff" }},
		{name: "metadata", change: func(s *runtimeapi.PodSandbox) { s.Metadata = nil }},
		{name: "name", change: func(s *runtimeapi.PodSandbox) { s.Metadata.Name = "" }},
		{name: "namespace", change: func(s *runtimeapi.PodSandbox) { s.Metadata.Namespace = "" }},
		{name: "uid", change: func(s *runtimeapi.PodSandbox) { s.Metadata.Uid = "" }},
		{name: "future state", change: func(s *runtimeapi.PodSandbox) { s.State = 42 }},
		{name: "creation time", change: func(s *runtimeapi.PodSandbox) { s.CreatedAt = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sandbox := goodSandbox()
			if tt.change != nil {
				tt.change(sandbox)
			}
			observed := goodSandboxStatus()
			observed.Id, observed.Metadata, observed.State, observed.CreatedAt = sandbox.Id, sandbox.Metadata, sandbox.State, sandbox.CreatedAt
			observed.RuntimeHandler, observed.Labels, observed.Annotations = sandbox.RuntimeHandler, sandbox.Labels, sandbox.Annotations
			client := newClient(t, startRuntime(t, &fakeRuntime{
				sandboxes: func(_ context.Context, request *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
					if request.Filter != nil {
						t.Error("inventory must not hide unready or foreign sandboxes")
					}
					return &runtimeapi.ListPodSandboxResponse{Items: []*runtimeapi.PodSandbox{sandbox}}, nil
				},
				sandboxStatus: func(_ context.Context, request *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
					if request.Verbose || request.PodSandboxId != "sandbox" {
						t.Errorf("unexpected status request: %v", request)
					}
					return &runtimeapi.PodSandboxStatusResponse{Status: observed}, nil
				},
			}), time.Second)
			list, err := client.ListPodSandbox(t.Context())
			if tt.valid {
				if err != nil || len(list) != 1 || !proto.Equal(list[0], sandbox) {
					t.Fatalf("inventory lost sandbox identity: %v, %v", list, err)
				}
			} else if err == nil || list != nil {
				t.Fatalf("invalid inventory = %v, %v", list, err)
			}
			got, err := client.PodSandboxStatus(t.Context(), "sandbox")
			validStatus := tt.valid && sandbox.Id == "sandbox"
			if validStatus {
				if err != nil || !proto.Equal(got, observed) {
					t.Fatalf("status lost sandbox identity: %v, %v", got, err)
				}
			} else if err == nil || got != nil {
				t.Fatalf("invalid status = %v, %v", got, err)
			}
		})
	}
}

func TestContainerInventory(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		change func(*runtimeapi.Container)
		valid  bool
	}{
		{name: "unknown", valid: true},
		{name: "created", valid: true, change: func(c *runtimeapi.Container) { c.State = runtimeapi.ContainerState_CONTAINER_CREATED }},
		{name: "running", valid: true, change: func(c *runtimeapi.Container) { c.State = runtimeapi.ContainerState_CONTAINER_RUNNING }},
		{name: "exited", valid: true, change: func(c *runtimeapi.Container) { c.State = runtimeapi.ContainerState_CONTAINER_EXITED }},
		{name: "foreign", valid: true, change: func(c *runtimeapi.Container) { c.Labels, c.Annotations = nil, nil }},
		{name: "missing id", change: func(c *runtimeapi.Container) { c.Id = "" }},
		{name: "nul id", change: func(c *runtimeapi.Container) { c.Id = "container\x00" }},
		{name: "metadata", change: func(c *runtimeapi.Container) { c.Metadata = nil }},
		{name: "name", change: func(c *runtimeapi.Container) { c.Metadata.Name = "" }},
		{name: "future state", change: func(c *runtimeapi.Container) { c.State = -1 }},
		{name: "creation time", change: func(c *runtimeapi.Container) { c.CreatedAt = -1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			container := goodContainer()
			if tt.change != nil {
				tt.change(container)
			}
			observed := goodContainerStatus()
			observed.Id, observed.Metadata, observed.State, observed.CreatedAt = container.Id, container.Metadata, container.State, container.CreatedAt
			observed.Labels, observed.Annotations = container.Labels, container.Annotations
			client := newClient(t, startRuntime(t, &fakeRuntime{
				containers: func(_ context.Context, request *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
					if request.Filter != nil {
						t.Error("inventory must not hide exited, unknown, or foreign containers")
					}
					return &runtimeapi.ListContainersResponse{Containers: []*runtimeapi.Container{container}}, nil
				},
				containerStatus: func(_ context.Context, request *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
					if request.Verbose || request.ContainerId != "container" {
						t.Errorf("unexpected status request: %v", request)
					}
					return &runtimeapi.ContainerStatusResponse{Status: observed}, nil
				},
			}), time.Second)
			list, err := client.ListContainers(t.Context())
			if tt.valid {
				if err != nil || len(list) != 1 || !proto.Equal(list[0], container) {
					t.Fatalf("inventory lost container identity: %v, %v", list, err)
				}
			} else if err == nil || list != nil {
				t.Fatalf("invalid inventory = %v, %v", list, err)
			}
			got, err := client.ContainerStatus(t.Context(), "container")
			if tt.valid {
				if err != nil || !proto.Equal(got, observed) {
					t.Fatalf("status lost container observations: %v, %v", got, err)
				}
			} else if err == nil || got != nil {
				t.Fatalf("invalid status = %v, %v", got, err)
			}
		})
	}
}

func TestIncompleteInventories(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"empty", "nil entry", "duplicate", "missing sandbox", "mismatched id", "missing status"} {
		t.Run(kind, func(t *testing.T) {
			sandboxes := []*runtimeapi.PodSandbox{goodSandbox()}
			containers := []*runtimeapi.Container{goodContainer()}
			sandboxStatus, containerStatus := goodSandboxStatus(), goodContainerStatus()
			switch kind {
			case "empty":
				sandboxes, containers = nil, nil
			case "nil entry":
				sandboxes, containers = append(sandboxes, nil), append(containers, nil)
			case "duplicate":
				sandboxes, containers = append(sandboxes, goodSandbox()), append(containers, goodContainer())
			case "missing sandbox":
				containers[0].PodSandboxId = ""
			case "mismatched id":
				sandboxStatus.Id, containerStatus.Id = "another-sandbox", "another-container"
			case "missing status":
				sandboxStatus, containerStatus = nil, nil
			}
			client := newClient(t, startRuntime(t, &fakeRuntime{
				sandboxes: func(context.Context, *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
					return &runtimeapi.ListPodSandboxResponse{Items: sandboxes}, nil
				},
				containers: func(context.Context, *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
					return &runtimeapi.ListContainersResponse{Containers: containers}, nil
				},
				sandboxStatus: func(context.Context, *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
					return &runtimeapi.PodSandboxStatusResponse{Status: sandboxStatus}, nil
				},
				containerStatus: func(context.Context, *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
					return &runtimeapi.ContainerStatusResponse{Status: containerStatus}, nil
				},
			}), time.Second)
			s, serr := client.ListPodSandbox(t.Context())
			c, cerr := client.ListContainers(t.Context())
			if kind == "empty" {
				if serr != nil || cerr != nil || len(s) != 0 || len(c) != 0 {
					t.Fatalf("empty inventory = %v, %v, %v, %v", s, serr, c, cerr)
				}
			}
			if kind == "nil entry" || kind == "duplicate" {
				if serr == nil || cerr == nil || s != nil || c != nil {
					t.Fatalf("partial inventory escaped validation: %v, %v, %v, %v", s, serr, c, cerr)
				}
			}
			if kind == "missing sandbox" && (cerr == nil || c != nil) {
				t.Fatalf("container without sandbox ID accepted: %v, %v", c, cerr)
			}
			if kind == "mismatched id" || kind == "missing status" {
				s, serr := client.PodSandboxStatus(t.Context(), "sandbox")
				c, cerr := client.ContainerStatus(t.Context(), "container")
				if serr == nil || cerr == nil || s != nil || c != nil {
					t.Fatalf("invalid status accepted: %v, %v, %v, %v", s, serr, c, cerr)
				}
			}
		})
	}
}

func TestInvalidStatusIDs(t *testing.T) {
	t.Parallel()
	client := newClient(t, "/no-runtime/cri.sock", time.Second)
	for _, id := range []string{"", "\xff", "id\x00"} {
		if got, err := client.PodSandboxStatus(t.Context(), id); err == nil || got != nil {
			t.Fatalf("PodSandboxStatus(%q) = %v, %v", id, got, err)
		}
		if got, err := client.ContainerStatus(t.Context(), id); err == nil || got != nil {
			t.Fatalf("ContainerStatus(%q) = %v, %v", id, got, err)
		}
	}
}

func TestInventoryIdentitiesAcrossAttempts(t *testing.T) {
	t.Parallel()
	first, replacement := goodSandbox(), goodSandbox()
	replacement.Id, replacement.Metadata.Uid, replacement.Metadata.Attempt = "replacement-sandbox", "replacement-pod-uid", 0
	replacement.Labels["io.kubernetes.pod.uid"] = replacement.Metadata.Uid
	old, current := goodContainer(), goodContainer()
	current.Id, current.PodSandboxId, current.Metadata.Attempt = "replacement-container", replacement.Id, 0
	current.Labels["io.kubernetes.pod.uid"] = replacement.Metadata.Uid
	old.State = runtimeapi.ContainerState_CONTAINER_EXITED
	current.State = runtimeapi.ContainerState_CONTAINER_RUNNING
	wantSandboxes := &runtimeapi.ListPodSandboxResponse{Items: []*runtimeapi.PodSandbox{first, replacement}}
	wantContainers := &runtimeapi.ListContainersResponse{Containers: []*runtimeapi.Container{old, current}}
	client := newClient(t, startRuntime(t, &fakeRuntime{
		sandboxes: func(context.Context, *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
			return wantSandboxes, nil
		},
		containers: func(context.Context, *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
			return wantContainers, nil
		},
	}), time.Second)
	sandboxes, err := client.ListPodSandbox(t.Context())
	if err != nil || !proto.Equal(&runtimeapi.ListPodSandboxResponse{Items: sandboxes}, wantSandboxes) {
		t.Fatalf("same-name Pod incarnations were lost: %v, %v", sandboxes, err)
	}
	containers, err := client.ListContainers(t.Context())
	if err != nil || !proto.Equal(&runtimeapi.ListContainersResponse{Containers: containers}, wantContainers) {
		t.Fatalf("same-name container attempts were lost: %v, %v", containers, err)
	}
}

func TestStatusOpaqueIDsAndRequestBounds(t *testing.T) {
	t.Parallel()
	id := "opaque/runtime:id"
	client := newClient(t, startRuntime(t, &fakeRuntime{
		sandboxStatus: func(_ context.Context, request *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
			if request.PodSandboxId != id {
				t.Errorf("sandbox ID rewritten: %q", request.PodSandboxId)
			}
			observed := goodSandboxStatus()
			observed.Id = id
			return &runtimeapi.PodSandboxStatusResponse{Status: observed}, nil
		},
		containerStatus: func(_ context.Context, request *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
			if request.ContainerId != id {
				t.Errorf("container ID rewritten: %q", request.ContainerId)
			}
			observed := goodContainerStatus()
			observed.Id = id
			return &runtimeapi.ContainerStatusResponse{Status: observed}, nil
		},
	}), time.Second)
	if _, err := client.PodSandboxStatus(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ContainerStatus(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	oversized := strings.Repeat("x", maxMessageBytes)
	if got, err := client.PodSandboxStatus(t.Context(), oversized); status.Code(err) != codes.ResourceExhausted || got != nil {
		t.Fatalf("oversized sandbox request = %v, %v", got, err)
	}
	if got, err := client.ContainerStatus(t.Context(), oversized); status.Code(err) != codes.ResourceExhausted || got != nil {
		t.Fatalf("oversized container request = %v, %v", got, err)
	}
}
