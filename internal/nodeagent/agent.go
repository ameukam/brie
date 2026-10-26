// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package nodeagent provides explicit host registration and a shutdown boundary.
// It can observe CRI health but does not reconcile Pods or mutate the runtime.
package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Config identifies the host explicitly; there is no inferred hostname.
type Config struct {
	NodeName      string
	RuntimeSocket string
}

// Validate requires a lowercase DNS name with at most 253 bytes and labels of
// at most 63 bytes. Each label must start and end with a letter or digit.
func (c Config) Validate() error {
	if c.NodeName == "" || len(c.NodeName) > 253 {
		return errors.New("node name must contain 1-253 bytes")
	}
	for _, label := range strings.Split(c.NodeName, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("node name must use DNS labels of 1-63 bytes starting and ending with a letter or digit")
		}
		for _, ch := range label {
			if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
				return errors.New("node name must contain only lowercase ASCII letters, digits, hyphens, and dots")
			}
		}
	}
	return nil
}

// Run optionally observes CRI at startup, then waits without contacting Kubernetes.
// Cancellation is a clean shutdown; deadline expiry is an error.
func Run(ctx context.Context, config Config, stderr io.Writer) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate agent configuration: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return shutdownError(err)
	}
	if err := observeRuntime(ctx, config.RuntimeSocket, stderr); err != nil {
		if ctx.Err() != nil {
			return shutdownError(ctx.Err())
		}
		return err
	}
	if _, err := fmt.Fprintf(stderr,
		"brie-node: agent scaffold for %s; node registration and workload execution are not implemented\n",
		config.NodeName); err != nil {
		return fmt.Errorf("write agent startup message: %w", err)
	}
	<-ctx.Done()
	return shutdownError(ctx.Err())
}

func shutdownError(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("agent stopped: %w", err)
}
