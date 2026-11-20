// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// rejectionCodes are the gRPC status codes that only a completed runtime
// handler can return and that mean the runtime refused the request. grpc-go
// never generates them for transport, deadline, cancellation, or decoding
// failures, which instead surface as Unavailable, DeadlineExceeded, Canceled,
// Unknown, Internal, ResourceExhausted, Unimplemented, or Unauthenticated.
// AlreadyExists, Aborted, and DataLoss are excluded because they can describe
// an object that exists or a partially applied change.
var rejectionCodes = map[codes.Code]bool{
	codes.InvalidArgument: true, codes.NotFound: true, codes.FailedPrecondition: true,
	codes.PermissionDenied: true, codes.OutOfRange: true,
}

// DefiniteRejection reports the status code of an error that a runtime
// returned to refuse a mutation, as opposed to a lost or interrupted reply.
// It is necessary but not sufficient evidence that nothing was created: a
// runtime can fail after a partial mutation, so callers must also observe a
// complete inventory without the object.
func DefiniteRejection(err error) (codes.Code, bool) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return codes.OK, false
	}
	code := status.Code(err)
	return code, rejectionCodes[code]
}

// IsRejectionCode reports whether name is the string form of a code that
// DefiniteRejection accepts.
func IsRejectionCode(name string) bool {
	for code := range rejectionCodes {
		if code.String() == name {
			return true
		}
	}
	return false
}

// ImageStatus reads a non-verbose image observation. A nil image with no error
// explicitly means the image is absent, as defined by CRI.
func (c *Client) ImageStatus(ctx context.Context, image string) (*runtimeapi.Image, error) {
	response, err := invoke(ctx, c, "ImageStatus", func(ctx context.Context) (*runtimeapi.ImageStatusResponse, error) {
		return c.images.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: &runtimeapi.ImageSpec{Image: image}})
	})
	if err != nil {
		return nil, err
	}
	if response.GetImage() != nil && response.GetImage().GetId() == "" {
		return nil, errors.New("CRI ImageStatus: image has no ID")
	}
	return response.GetImage(), nil
}

// PullImage requests an unauthenticated pull. There are no automatic retries.
func (c *Client) PullImage(ctx context.Context, image string, sandbox *runtimeapi.PodSandboxConfig) (string, error) {
	response, err := invoke(ctx, c, "PullImage", func(ctx context.Context) (*runtimeapi.PullImageResponse, error) {
		return c.images.PullImage(ctx, &runtimeapi.PullImageRequest{
			Image: &runtimeapi.ImageSpec{Image: image}, SandboxConfig: sandbox,
		})
	})
	if err != nil {
		return "", err
	}
	if response.GetImageRef() == "" {
		return "", errors.New("CRI PullImage: missing image reference")
	}
	return response.GetImageRef(), nil
}

// UpdatePodCIDR sets the desired single-stack CNI network configuration.
func (c *Client) UpdatePodCIDR(ctx context.Context, cidr string) error {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || prefix != prefix.Masked() {
		return errors.New("CRI UpdateRuntimeConfig: invalid Pod CIDR")
	}
	_, err = invoke(ctx, c, "UpdateRuntimeConfig", func(ctx context.Context) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
		return c.runtime.UpdateRuntimeConfig(ctx, &runtimeapi.UpdateRuntimeConfigRequest{
			RuntimeConfig: &runtimeapi.RuntimeConfig{NetworkConfig: &runtimeapi.NetworkConfig{PodCidr: cidr}},
		})
	})
	return err
}

// RunPodSandbox requests a sandbox through its configured runtime owner.
// Any error may follow a committed mutation; reconcile before another create.
func (c *Client) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, handler string) (string, error) {
	if config.GetMetadata().GetUid() == "" || config.GetMetadata().GetName() == "" ||
		config.GetMetadata().GetNamespace() == "" || handler == "" {
		return "", errors.New("CRI RunPodSandbox: metadata and explicit runtime handler required")
	}
	response, err := invoke(ctx, c, "RunPodSandbox", func(ctx context.Context) (*runtimeapi.RunPodSandboxResponse, error) {
		return c.runtime.RunPodSandbox(ctx, &runtimeapi.RunPodSandboxRequest{Config: config, RuntimeHandler: handler})
	})
	if err != nil {
		return "", err
	}
	if err := validateID(response.GetPodSandboxId()); err != nil {
		return "", fmt.Errorf("CRI RunPodSandbox: %w", err)
	}
	return response.GetPodSandboxId(), nil
}

// CreateContainer prepares a container without starting it. Errors have
// uncertain outcomes and must not trigger blind creation retries.
func (c *Client) CreateContainer(ctx context.Context, sandboxID string, config *runtimeapi.ContainerConfig, sandbox *runtimeapi.PodSandboxConfig) (string, error) {
	if err := validateID(sandboxID); err != nil {
		return "", fmt.Errorf("CRI CreateContainer: %w", err)
	}
	if config.GetMetadata().GetName() == "" || config.GetImage().GetImage() == "" {
		return "", errors.New("CRI CreateContainer: container name and image required")
	}
	response, err := invoke(ctx, c, "CreateContainer", func(ctx context.Context) (*runtimeapi.CreateContainerResponse, error) {
		return c.runtime.CreateContainer(ctx, &runtimeapi.CreateContainerRequest{
			PodSandboxId: sandboxID, Config: config, SandboxConfig: sandbox,
		})
	})
	if err != nil {
		return "", err
	}
	if err := validateID(response.GetContainerId()); err != nil {
		return "", fmt.Errorf("CRI CreateContainer: %w", err)
	}
	return response.GetContainerId(), nil
}

// StartContainer starts the exact observed container. Errors require observation.
func (c *Client) StartContainer(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	_, err := invoke(ctx, c, "StartContainer", func(ctx context.Context) (*runtimeapi.StartContainerResponse, error) {
		return c.runtime.StartContainer(ctx, &runtimeapi.StartContainerRequest{ContainerId: id})
	})
	return err
}

// StopContainer requests graceful shutdown; CRI defines this operation as idempotent.
func (c *Client) StopContainer(ctx context.Context, id string, seconds int64) error {
	if err := validateID(id); err != nil {
		return err
	}
	if seconds < 0 {
		return errors.New("CRI StopContainer: negative timeout")
	}
	_, err := invoke(ctx, c, "StopContainer", func(ctx context.Context) (*runtimeapi.StopContainerResponse, error) {
		return c.runtime.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: id, Timeout: seconds})
	})
	return err
}

// RemoveContainer removes a stopped container through CRI, without local VM calls.
func (c *Client) RemoveContainer(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	_, err := invoke(ctx, c, "RemoveContainer", func(ctx context.Context) (*runtimeapi.RemoveContainerResponse, error) {
		return c.runtime.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: id})
	})
	return err
}

// StopPodSandbox delegates sandbox shutdown and CNI teardown to the runtime.
func (c *Client) StopPodSandbox(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	_, err := invoke(ctx, c, "StopPodSandbox", func(ctx context.Context) (*runtimeapi.StopPodSandboxResponse, error) {
		return c.runtime.StopPodSandbox(ctx, &runtimeapi.StopPodSandboxRequest{PodSandboxId: id})
	})
	return err
}

// RemovePodSandbox removes the stopped sandbox through its runtime owner.
func (c *Client) RemovePodSandbox(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	_, err := invoke(ctx, c, "RemovePodSandbox", func(ctx context.Context) (*runtimeapi.RemovePodSandboxResponse, error) {
		return c.runtime.RemovePodSandbox(ctx, &runtimeapi.RemovePodSandboxRequest{PodSandboxId: id})
	})
	return err
}

func invoke[T any](ctx context.Context, c *Client, method string, call func(context.Context) (T, error)) (T, error) {
	var zero T
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("CRI %s: %w", method, err)
	}
	defer release()
	response, err := call(ctx)
	if err != nil {
		return zero, fmt.Errorf("CRI %s: %w", method, rpcError(ctx, err))
	}
	return response, nil
}
