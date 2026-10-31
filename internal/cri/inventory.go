// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// ListPodSandbox reads all sandboxes, including unready and foreign sandboxes.
// An error means inventory is unavailable, not empty. Results are not a snapshot
// shared with ListContainers and never authorize cleanup.
func (c *Client) ListPodSandbox(ctx context.Context) ([]*runtimeapi.PodSandbox, error) {
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI ListPodSandbox: %w", err)
	}
	defer release()
	response, err := c.runtime.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{})
	if err != nil {
		return nil, fmt.Errorf("CRI ListPodSandbox: %w", rpcError(ctx, err))
	}
	seen := make(map[string]bool)
	for _, sandbox := range response.GetItems() {
		if err := validateSandbox(sandbox.GetId(), sandbox.GetMetadata(), sandbox.GetState(), sandbox.GetCreatedAt()); err != nil {
			return nil, fmt.Errorf("CRI ListPodSandbox: %w", err)
		}
		if seen[sandbox.GetId()] {
			return nil, errors.New("CRI ListPodSandbox: duplicate sandbox ID")
		}
		seen[sandbox.GetId()] = true
	}
	return response.GetItems(), nil
}

// ListContainers reads all containers, including exited, unknown, and foreign
// containers. Sandbox IDs, metadata, labels, and annotations are retained as
// observations, not proof of ownership. Errors never return partial results.
func (c *Client) ListContainers(ctx context.Context) ([]*runtimeapi.Container, error) {
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI ListContainers: %w", err)
	}
	defer release()
	response, err := c.runtime.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	if err != nil {
		return nil, fmt.Errorf("CRI ListContainers: %w", rpcError(ctx, err))
	}
	seen := make(map[string]bool)
	for _, container := range response.GetContainers() {
		if err := validateContainer(container.GetId(), container.GetMetadata(), container.GetState(), container.GetCreatedAt()); err != nil {
			return nil, fmt.Errorf("CRI ListContainers: %w", err)
		}
		if err := validateID(container.GetPodSandboxId()); err != nil {
			return nil, fmt.Errorf("CRI ListContainers: sandbox ID: %w", err)
		}
		if seen[container.GetId()] {
			return nil, errors.New("CRI ListContainers: duplicate container ID")
		}
		seen[container.GetId()] = true
	}
	return response.GetContainers(), nil
}

// PodSandboxStatus reads non-verbose status for an exact, opaque runtime ID.
// NotFound is preserved as an error; it is not an empty successful observation.
func (c *Client) PodSandboxStatus(ctx context.Context, id string) (*runtimeapi.PodSandboxStatus, error) {
	if err := validateID(id); err != nil {
		return nil, fmt.Errorf("CRI PodSandboxStatus: %w", err)
	}
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI PodSandboxStatus: %w", err)
	}
	defer release()
	response, err := c.runtime.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: id, Verbose: false})
	if err != nil {
		return nil, fmt.Errorf("CRI PodSandboxStatus: %w", rpcError(ctx, err))
	}
	observed := response.GetStatus()
	if err := validateSandbox(observed.GetId(), observed.GetMetadata(), observed.GetState(), observed.GetCreatedAt()); err != nil {
		return nil, fmt.Errorf("CRI PodSandboxStatus: %w", err)
	}
	if observed.GetId() != id {
		return nil, errors.New("CRI PodSandboxStatus: response sandbox ID does not match request")
	}
	return observed, nil
}

// ContainerStatus reads non-verbose status without interpreting UNKNOWN as an
// exit. ContainerStatus has no sandbox ID; retain the ListContainers association
// and recheck identity during reconciliation rather than joining by name.
func (c *Client) ContainerStatus(ctx context.Context, id string) (*runtimeapi.ContainerStatus, error) {
	if err := validateID(id); err != nil {
		return nil, fmt.Errorf("CRI ContainerStatus: %w", err)
	}
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI ContainerStatus: %w", err)
	}
	defer release()
	response, err := c.runtime.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: id, Verbose: false})
	if err != nil {
		return nil, fmt.Errorf("CRI ContainerStatus: %w", rpcError(ctx, err))
	}
	observed := response.GetStatus()
	if err := validateContainer(observed.GetId(), observed.GetMetadata(), observed.GetState(), observed.GetCreatedAt()); err != nil {
		return nil, fmt.Errorf("CRI ContainerStatus: %w", err)
	}
	if observed.GetId() != id {
		return nil, errors.New("CRI ContainerStatus: response container ID does not match request")
	}
	return observed, nil
}

func validateID(id string) error {
	if id == "" || !utf8.ValidString(id) || strings.ContainsRune(id, '\x00') {
		return errors.New("runtime ID must be nonempty UTF-8 without NUL")
	}
	return nil
}

func validateSandbox(id string, metadata *runtimeapi.PodSandboxMetadata, state runtimeapi.PodSandboxState, createdAt int64) error {
	if err := validateID(id); err != nil {
		return err
	}
	if metadata.GetName() == "" || metadata.GetNamespace() == "" || metadata.GetUid() == "" {
		return errors.New("incomplete sandbox metadata")
	}
	if state != runtimeapi.PodSandboxState_SANDBOX_READY && state != runtimeapi.PodSandboxState_SANDBOX_NOTREADY {
		return errors.New("unknown sandbox state")
	}
	if createdAt <= 0 {
		return errors.New("invalid sandbox creation timestamp")
	}
	return nil
}

func validateContainer(id string, metadata *runtimeapi.ContainerMetadata, state runtimeapi.ContainerState, createdAt int64) error {
	if err := validateID(id); err != nil {
		return err
	}
	if metadata.GetName() == "" {
		return errors.New("incomplete container metadata")
	}
	if _, ok := runtimeapi.ContainerState_name[int32(state)]; !ok {
		return errors.New("unknown container state")
	}
	if createdAt <= 0 {
		return errors.New("invalid container creation timestamp")
	}
	return nil
}
