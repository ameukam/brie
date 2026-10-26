// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package cri provides read-only observations of a local CRI v1 runtime.
// It does not admit Pods, mutate runtime state, or establish Node readiness.
package cri

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const maxMessageBytes = 64 * 1024

// Client observes one local runtime, with at most one RPC in flight.
// The connection is private so callers cannot use it for runtime mutations.
type Client struct {
	conn    *grpc.ClientConn
	runtime runtimeapi.RuntimeServiceClient
	timeout time.Duration
	slot    chan struct{}
}

// New creates a client without contacting the runtime. The positive timeout
// bounds each call, including queueing and connection establishment.
// Socket permissions must restrict access to the trusted runtime owner;
// plaintext Unix-socket transport does not authenticate it.
func New(socketPath string, timeout time.Duration) (*Client, error) {
	if !filepath.IsAbs(socketPath) || !utf8.ValidString(socketPath) ||
		strings.ContainsRune(socketPath, '\x00') || len(socketPath) > 107 {
		return nil, errors.New("invalid CRI socket path: must be absolute UTF-8, at most 107 bytes, without NUL")
	}
	if timeout <= 0 {
		return nil, errors.New("invalid CRI request timeout: must be positive")
	}
	conn, err := grpc.NewClient(
		"passthrough:///cri",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}),
		grpc.WithDisableRetry(),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create CRI connection: %w", err)
	}
	return &Client{
		conn: conn, runtime: runtimeapi.NewRuntimeServiceClient(conn),
		timeout: timeout, slot: make(chan struct{}, 1),
	}, nil
}

// Close releases the connection and interrupts in-flight RPCs.
func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close CRI connection: %w", err)
	}
	return nil
}

// Version reads runtime metadata and requires the CRI v1 API. This does not
// verify the configured Kata handler, VMM, or guest compatibility.
func (c *Client) Version(ctx context.Context) (*runtimeapi.VersionResponse, error) {
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI Version: %w", err)
	}
	defer release()
	response, err := c.runtime.Version(ctx, &runtimeapi.VersionRequest{Version: "0.1.0"})
	if err != nil {
		return nil, fmt.Errorf("CRI Version: %w", rpcError(ctx, err))
	}
	if response.GetVersion() == "" || response.GetRuntimeName() == "" ||
		response.GetRuntimeVersion() == "" || response.GetRuntimeApiVersion() != "v1" {
		return nil, errors.New("CRI Version: incomplete metadata or unsupported runtime API version")
	}
	return response, nil
}

// Status reads non-verbose health conditions. False conditions are observations,
// not transport errors. Missing or duplicate conditions are invalid responses;
// even a healthy runtime does not establish Brie Node readiness.
func (c *Client) Status(ctx context.Context) (*runtimeapi.RuntimeStatus, error) {
	ctx, release, err := c.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CRI Status: %w", err)
	}
	defer release()
	response, err := c.runtime.Status(ctx, &runtimeapi.StatusRequest{Verbose: false})
	if err != nil {
		return nil, fmt.Errorf("CRI Status: %w", rpcError(ctx, err))
	}
	observed := response.GetStatus()
	seen := make(map[string]bool)
	for _, condition := range observed.GetConditions() {
		kind := condition.GetType()
		if kind == "" || seen[kind] {
			return nil, errors.New("CRI Status: empty or duplicate condition type")
		}
		seen[kind] = true
	}
	if !seen[runtimeapi.RuntimeReady] || !seen[runtimeapi.NetworkReady] {
		return nil, errors.New("CRI Status: missing RuntimeReady or NetworkReady condition")
	}
	return observed, nil
}

func rpcError(ctx context.Context, err error) error {
	// The server can observe its deadline before the local context timer fires.
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		err = errors.Join(err, context.DeadlineExceeded)
	case codes.Canceled:
		err = errors.Join(err, context.Canceled)
	}
	return errors.Join(err, ctx.Err())
}

func (c *Client) begin(ctx context.Context) (context.Context, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	select {
	case c.slot <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-c.slot
			cancel()
			return nil, nil, err
		}
		return ctx, func() { <-c.slot; cancel() }, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	}
}
