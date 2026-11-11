// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type lifecycleRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	runtimeapi.UnimplementedImageServiceServer
	failure error
	invalid bool
	absent  bool
}

func (s *lifecycleRuntime) ImageStatus(context.Context, *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	image := &runtimeapi.Image{Id: "image"}
	if s.invalid {
		image.Id = ""
	}
	if s.absent {
		image = nil
	}
	return &runtimeapi.ImageStatusResponse{Image: image}, s.failure
}

func (s *lifecycleRuntime) PullImage(context.Context, *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	id := "image"
	if s.invalid {
		id = ""
	}
	return &runtimeapi.PullImageResponse{ImageRef: id}, s.failure
}

func (s *lifecycleRuntime) RunPodSandbox(context.Context, *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	id := "sandbox"
	if s.invalid {
		id = ""
	}
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: id}, s.failure
}

func (s *lifecycleRuntime) CreateContainer(context.Context, *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	id := "container"
	if s.invalid {
		id = ""
	}
	return &runtimeapi.CreateContainerResponse{ContainerId: id}, s.failure
}

func (s *lifecycleRuntime) StartContainer(context.Context, *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	return &runtimeapi.StartContainerResponse{}, s.failure
}

func (s *lifecycleRuntime) StopContainer(context.Context, *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	return &runtimeapi.StopContainerResponse{}, s.failure
}

func (s *lifecycleRuntime) RemoveContainer(context.Context, *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	return &runtimeapi.RemoveContainerResponse{}, s.failure
}

func (s *lifecycleRuntime) StopPodSandbox(context.Context, *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	return &runtimeapi.StopPodSandboxResponse{}, s.failure
}

func (s *lifecycleRuntime) RemovePodSandbox(context.Context, *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	return &runtimeapi.RemovePodSandboxResponse{}, s.failure
}

func (s *lifecycleRuntime) UpdateRuntimeConfig(context.Context, *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	return &runtimeapi.UpdateRuntimeConfigResponse{}, s.failure
}

func lifecycleClient(t *testing.T, service *lifecycleRuntime) *Client {
	t.Helper()
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
	return newClient(t, socket, time.Second)
}

func lifecycleCalls(c *Client) []func(context.Context) error {
	sandbox := &runtimeapi.PodSandboxConfig{Metadata: &runtimeapi.PodSandboxMetadata{Name: "pod", Namespace: "default", Uid: "uid"}}
	container := &runtimeapi.ContainerConfig{Metadata: &runtimeapi.ContainerMetadata{Name: "app"}, Image: &runtimeapi.ImageSpec{Image: "image"}}
	return []func(context.Context) error{
		func(ctx context.Context) error { _, err := c.ImageStatus(ctx, "image"); return err },
		func(ctx context.Context) error { _, err := c.PullImage(ctx, "image", sandbox); return err },
		func(ctx context.Context) error { _, err := c.RunPodSandbox(ctx, sandbox, "kata-fc"); return err },
		func(ctx context.Context) error {
			_, err := c.CreateContainer(ctx, "sandbox", container, sandbox)
			return err
		},
		func(ctx context.Context) error { return c.StartContainer(ctx, "container") },
		func(ctx context.Context) error { return c.StopContainer(ctx, "container", 30) },
		func(ctx context.Context) error { return c.RemoveContainer(ctx, "container") },
		func(ctx context.Context) error { return c.StopPodSandbox(ctx, "sandbox") },
		func(ctx context.Context) error { return c.RemovePodSandbox(ctx, "sandbox") },
		func(ctx context.Context) error { return c.UpdatePodCIDR(ctx, "10.244.0.0/24") },
	}
}

func TestLifecycleRPCs(t *testing.T) {
	t.Parallel()
	for _, code := range []codes.Code{codes.OK, codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			service := &lifecycleRuntime{}
			if code != codes.OK {
				service.failure = status.Error(code, "runtime response")
			}
			for _, call := range lifecycleCalls(lifecycleClient(t, service)) {
				err := call(t.Context())
				if status.Code(err) != code {
					t.Fatalf("error = %v, want %s", err, code)
				}
				if code == codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) ||
					code == codes.Canceled && !errors.Is(err, context.Canceled) {
					t.Fatal("RPC context error was lost")
				}
			}
		})
	}
	client := lifecycleClient(t, &lifecycleRuntime{})
	for _, call := range lifecycleCalls(client) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := call(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled mutation reached runtime: %v", err)
		}
	}
	invalid := lifecycleClient(t, &lifecycleRuntime{invalid: true})
	for _, call := range lifecycleCalls(invalid)[:4] {
		if err := call(t.Context()); err == nil {
			t.Fatal("incomplete response accepted")
		}
	}
	absent, err := lifecycleClient(t, &lifecycleRuntime{absent: true}).ImageStatus(t.Context(), "image")
	if err != nil || absent != nil {
		t.Fatalf("explicit image absence = %v, %v", absent, err)
	}
}

func TestLifecycleInputValidation(t *testing.T) {
	t.Parallel()
	client := newClient(t, "/no-runtime/cri.sock", time.Second)
	calls := []func() error{
		func() error { _, err := client.RunPodSandbox(t.Context(), nil, "kata-fc"); return err },
		func() error { _, err := client.CreateContainer(t.Context(), "", nil, nil); return err },
		func() error { _, err := client.CreateContainer(t.Context(), "sandbox", nil, nil); return err },
		func() error { return client.StartContainer(t.Context(), "") },
		func() error { return client.StopContainer(t.Context(), "", 0) },
		func() error { return client.StopContainer(t.Context(), "container", -1) },
		func() error { return client.RemoveContainer(t.Context(), "") },
		func() error { return client.StopPodSandbox(t.Context(), "") },
		func() error { return client.RemovePodSandbox(t.Context(), "") },
		func() error { return client.UpdatePodCIDR(t.Context(), "invalid") },
	}
	for _, call := range calls {
		if err := call(); err == nil || status.Code(err) == codes.Unavailable {
			t.Fatalf("invalid input reached transport: %v", err)
		}
	}
}
