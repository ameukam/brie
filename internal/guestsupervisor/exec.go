// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// DefaultCtlPath and DefaultSocketPath are the in-container locations used by
// the example counter image.
const (
	DefaultCtlPath    = "/brie-guestctl"
	DefaultSocketPath = "/brie/supervisor.sock"
)

// ExecSyncer is the CRI RuntimeService call the host uses to reach the
// supervisor. A runtimeapi.RuntimeServiceClient satisfies it.
type ExecSyncer interface {
	ExecSync(ctx context.Context, request *runtimeapi.ExecSyncRequest, options ...grpc.CallOption) (*runtimeapi.ExecSyncResponse, error)
}

// ExecClient sends protocol requests by running brie-guestctl in the workload
// container through CRI ExecSync, so Kata's agent stays the only guest-control
// path. It does not retry: a lost reply is an unknown outcome, and the caller
// re-sends the same operation ID to observe the recorded result.
type ExecClient struct {
	Runtime     ExecSyncer
	ContainerID string
	CtlPath     string
	SocketPath  string
}

// Command returns the exec argument vector that carries request.
func (c ExecClient) Command(request Request) ([]string, error) {
	if err := validateGuestPath("guestctl", c.CtlPath); err != nil {
		return nil, err
	}
	if err := ValidateSocketPath(c.SocketPath); err != nil {
		return nil, err
	}
	encoded, err := EncodeRequest(request)
	if err != nil {
		return nil, err
	}
	return []string{c.CtlPath, "--socket=" + c.SocketPath, string(encoded)}, nil
}

// Call runs one request. ctx must carry a deadline, which also bounds the
// in-guest command through the CRI timeout.
func (c ExecClient) Call(ctx context.Context, request Request) (Response, error) {
	if c.Runtime == nil || c.ContainerID == "" {
		return Response{}, errors.New("exec client requires a runtime and a container ID")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return Response{}, errors.New("guest supervisor call requires a context deadline")
	}
	command, err := c.Command(request)
	if err != nil {
		return Response{}, err
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return Response{}, fmt.Errorf("guest supervisor call: %w", context.DeadlineExceeded)
	}
	seconds := int64((timeout + time.Second - 1) / time.Second)
	reply, err := c.Runtime.ExecSync(ctx, &runtimeapi.ExecSyncRequest{
		ContainerId: c.ContainerID, Cmd: command, Timeout: seconds,
	})
	if err != nil {
		return Response{}, fmt.Errorf("CRI ExecSync guest supervisor: %w", err)
	}
	if reply.GetExitCode() != 0 {
		return Response{}, fmt.Errorf("guestctl exited with code %d", reply.GetExitCode())
	}
	stdout := reply.GetStdout()
	if len(stdout) > MaxMessageBytes+1 {
		return Response{}, fmt.Errorf("guestctl output exceeds %d bytes", MaxMessageBytes+1)
	}
	response, err := DecodeResponse(bytes.TrimSuffix(stdout, []byte("\n")))
	if err != nil {
		return Response{}, err
	}
	if err := response.ValidateFor(request); err != nil {
		return Response{}, err
	}
	return response, nil
}
