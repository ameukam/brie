// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
)

// Observer receives the short CRI method name, such as "RunPodSandbox", the
// RPC duration excluding queueing for the client's call slot, and the RPC
// error. It must not block.
type Observer func(method string, duration time.Duration, err error)

// SetObserver installs or, with nil, removes the call observer.
func (c *Client) SetObserver(observer Observer) {
	if observer == nil {
		c.observer.Store(nil)
		return
	}
	c.observer.Store(&observer)
}

func (c *Client) observeCall(ctx context.Context, method string, request, reply any, conn *grpc.ClientConn,
	invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, request, reply, conn, options...)
	if observer := c.observer.Load(); observer != nil {
		(*observer)(method[strings.LastIndexByte(method, '/')+1:], time.Since(start), err)
	}
	return err
}
