// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// maxExecSyncBytes bounds synchronous exec output. Larger replies fail the call.
const maxExecSyncBytes = 1024 * 1024

// ExecSyncResult is the bounded output of a synchronous exec.
type ExecSyncResult struct {
	Stdout, Stderr []byte
	ExitCode       int32
}

// ExecSync runs a command to completion in a container. The runtime stops the
// command after timeoutSeconds; the client's own deadline also bounds the call.
// It never resumes a paused sandbox; callers check extension state first.
func (c *Client) ExecSync(ctx context.Context, containerID string, command []string, timeoutSeconds int64) (*ExecSyncResult, error) {
	if err := validateID(containerID); err != nil {
		return nil, fmt.Errorf("CRI ExecSync: %w", err)
	}
	if len(command) == 0 || timeoutSeconds <= 0 {
		return nil, errors.New("CRI ExecSync: command and positive timeout required")
	}
	response, err := invoke(ctx, c, "ExecSync", func(ctx context.Context) (*runtimeapi.ExecSyncResponse, error) {
		return c.runtime.ExecSync(ctx, &runtimeapi.ExecSyncRequest{
			ContainerId: containerID, Cmd: command, Timeout: timeoutSeconds,
		}, grpc.MaxCallRecvMsgSize(maxExecSyncBytes))
	})
	if err != nil {
		return nil, err
	}
	return &ExecSyncResult{Stdout: response.GetStdout(), Stderr: response.GetStderr(), ExitCode: response.GetExitCode()}, nil
}

// ExecOptions selects the streams of an exec or attach session.
// At least one stream is required, and a TTY cannot have a separate stderr.
type ExecOptions struct {
	Stdin, Stdout, Stderr, TTY bool
}

func (o ExecOptions) validate() error {
	if !o.Stdin && !o.Stdout && !o.Stderr {
		return errors.New("at least one of stdin, stdout, and stderr is required")
	}
	if o.TTY && o.Stderr {
		return errors.New("stderr cannot be separate from a TTY")
	}
	return nil
}

// Exec prepares a streaming exec session and returns the runtime's streaming URL.
func (c *Client) Exec(ctx context.Context, containerID string, command []string, options ExecOptions) (*url.URL, error) {
	if err := validateID(containerID); err != nil {
		return nil, fmt.Errorf("CRI Exec: %w", err)
	}
	if len(command) == 0 {
		return nil, errors.New("CRI Exec: command required")
	}
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("CRI Exec: %w", err)
	}
	response, err := invoke(ctx, c, "Exec", func(ctx context.Context) (*runtimeapi.ExecResponse, error) {
		return c.runtime.Exec(ctx, &runtimeapi.ExecRequest{
			ContainerId: containerID, Cmd: command, Tty: options.TTY,
			Stdin: options.Stdin, Stdout: options.Stdout, Stderr: options.Stderr,
		})
	})
	if err != nil {
		return nil, err
	}
	return streamingURL("Exec", response.GetUrl())
}

// Attach prepares a streaming attach session and returns the streaming URL.
func (c *Client) Attach(ctx context.Context, containerID string, options ExecOptions) (*url.URL, error) {
	if err := validateID(containerID); err != nil {
		return nil, fmt.Errorf("CRI Attach: %w", err)
	}
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("CRI Attach: %w", err)
	}
	response, err := invoke(ctx, c, "Attach", func(ctx context.Context) (*runtimeapi.AttachResponse, error) {
		return c.runtime.Attach(ctx, &runtimeapi.AttachRequest{
			ContainerId: containerID, Tty: options.TTY,
			Stdin: options.Stdin, Stdout: options.Stdout, Stderr: options.Stderr,
		})
	})
	if err != nil {
		return nil, err
	}
	return streamingURL("Attach", response.GetUrl())
}

// PortForward prepares a port-forward session to a sandbox and returns the
// streaming URL. Ports may be empty when the client names them in the stream.
func (c *Client) PortForward(ctx context.Context, sandboxID string, ports []int32) (*url.URL, error) {
	if err := validateID(sandboxID); err != nil {
		return nil, fmt.Errorf("CRI PortForward: %w", err)
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return nil, errors.New("CRI PortForward: ports must be between 1 and 65535")
		}
	}
	response, err := invoke(ctx, c, "PortForward", func(ctx context.Context) (*runtimeapi.PortForwardResponse, error) {
		return c.runtime.PortForward(ctx, &runtimeapi.PortForwardRequest{PodSandboxId: sandboxID, Port: ports})
	})
	if err != nil {
		return nil, err
	}
	return streamingURL("PortForward", response.GetUrl())
}

// ReopenContainerLog asks the runtime to reopen the container's log file after
// the caller renamed it. An error leaves the runtime writing to the old file.
func (c *Client) ReopenContainerLog(ctx context.Context, containerID string) error {
	if err := validateID(containerID); err != nil {
		return fmt.Errorf("CRI ReopenContainerLog: %w", err)
	}
	_, err := invoke(ctx, c, "ReopenContainerLog", func(ctx context.Context) (*runtimeapi.ReopenContainerLogResponse, error) {
		return c.runtime.ReopenContainerLog(ctx, &runtimeapi.ReopenContainerLogRequest{ContainerId: containerID})
	})
	return err
}

func streamingURL(method, raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("CRI %s: runtime returned an invalid streaming URL", method)
	}
	return parsed, nil
}
