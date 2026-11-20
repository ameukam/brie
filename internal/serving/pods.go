// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/execinventory"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Runtime is the CRI subset the server uses. *cri.Client implements it.
type Runtime interface {
	ListPodSandbox(context.Context) ([]*runtimeapi.PodSandbox, error)
	ListContainers(context.Context) ([]*runtimeapi.Container, error)
	ContainerStatus(context.Context, string) (*runtimeapi.ContainerStatus, error)
	Exec(context.Context, string, []string, cri.ExecOptions) (*url.URL, error)
	Attach(context.Context, string, cri.ExecOptions) (*url.URL, error)
	PortForward(context.Context, string, []int32) (*url.URL, error)
}

// ExtensionState is a lifecycle-extension state recorded for a sandbox.
type ExtensionState string

// ExtensionRunning is the only state in which the server touches a sandbox.
const ExtensionRunning ExtensionState = "Running"

// ExtensionStates reports the recorded lifecycle-extension state of a Pod's
// sandbox. Brie has no lifecycle extension yet; a nil source means every
// sandbox is in the base CRI lifecycle.
type ExtensionStates interface {
	ExtensionState(ctx context.Context, podUID string) (ExtensionState, error)
}

// Executions reports the executor-verified execution of a Pod.
// *execinventory.Store implements it.
type Executions interface {
	Lookup(namespace, name string) (execinventory.Execution, error)
}

// requestError carries an explicit HTTP status for a refused request.
type requestError struct {
	status  int
	message string
}

func (e *requestError) Error() string { return e.message }

func refuse(status int, format string, args ...any) error {
	return &requestError{status: status, message: fmt.Sprintf(format, args...)}
}

func errorStatus(err error) (int, string) {
	var refused *requestError
	if errors.As(err, &refused) {
		return refused.status, refused.message
	}
	return http.StatusInternalServerError, "runtime observation failed"
}

type target struct {
	podUID    string
	sandbox   *runtimeapi.PodSandbox
	container *runtimeapi.Container
}

// locate finds the executor-verified execution of namespace/pod and its
// sandbox and, when container is not empty, its container, matching runtime
// objects by the verified IDs. A Pod name with several verified executions is
// refused, never resolved by age. It then refuses any sandbox whose recorded
// extension state is not Running; it never resumes one.
func (s *Server) locate(ctx context.Context, namespace, pod, container string) (*target, error) {
	execution, err := s.executions.Lookup(namespace, pod)
	switch {
	case errors.Is(err, execinventory.ErrNotFound):
		return nil, refuse(http.StatusNotFound, "pod %s/%s not found on this node", namespace, pod)
	case errors.Is(err, execinventory.ErrAmbiguous):
		return nil, refuse(http.StatusConflict, "pod %s/%s has several executions on this node; refusing to choose one", namespace, pod)
	case err != nil:
		return nil, refuse(http.StatusServiceUnavailable, "pod %s/%s execution inventory is unavailable", namespace, pod)
	}
	sandboxes, err := s.runtime.ListPodSandbox(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sandboxes: %w", err)
	}
	var found *runtimeapi.PodSandbox
	for _, sandbox := range sandboxes {
		metadata := sandbox.GetMetadata()
		if sandbox.GetId() == execution.SandboxID && metadata.GetNamespace() == namespace && metadata.GetName() == pod &&
			metadata.GetUid() == execution.PodUID && sandbox.GetRuntimeHandler() == s.config.RuntimeHandler {
			found = sandbox
		}
	}
	if found == nil {
		return nil, refuse(http.StatusNotFound, "pod %s/%s not found on this node", namespace, pod)
	}
	result := &target{podUID: execution.PodUID, sandbox: found}
	if s.states != nil {
		state, err := s.states.ExtensionState(ctx, result.podUID)
		if err != nil {
			return nil, refuse(http.StatusServiceUnavailable, "pod %s/%s extension state is unavailable", namespace, pod)
		}
		if state != ExtensionRunning {
			return nil, refuse(http.StatusConflict,
				"pod %s/%s sandbox is in extension state %q; it is not resumed to serve this request", namespace, pod, state)
		}
	}
	if container == "" {
		return result, nil
	}
	if execution.ContainerID == "" || execution.ContainerName != container {
		return nil, refuse(http.StatusNotFound, "container %s not found in pod %s/%s", container, namespace, pod)
	}
	containers, err := s.runtime.ListContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	for _, candidate := range containers {
		if candidate.GetId() == execution.ContainerID && candidate.GetPodSandboxId() == found.GetId() &&
			candidate.GetMetadata().GetName() == container {
			result.container = candidate
		}
	}
	if result.container == nil {
		return nil, refuse(http.StatusNotFound, "container %s not found in pod %s/%s", container, namespace, pod)
	}
	return result, nil
}

func requireReadySandbox(t *target) error {
	if t.sandbox.GetState() != runtimeapi.PodSandboxState_SANDBOX_READY {
		return refuse(http.StatusConflict, "pod sandbox is not ready")
	}
	return nil
}

func requireRunningContainer(t *target) error {
	if err := requireReadySandbox(t); err != nil {
		return err
	}
	if t.container.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
		return refuse(http.StatusConflict, "container is not running")
	}
	return nil
}
