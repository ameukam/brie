// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ameukam/brie/internal/cri"
)

func observeRuntime(ctx context.Context, socket string, stderr io.Writer) (retErr error) {
	if socket == "" {
		return nil
	}
	client, err := cri.New(socket, 10*time.Second)
	if err != nil {
		return fmt.Errorf("configure CRI observation: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, client.Close()) }()
	version, err := client.Version(ctx)
	if err != nil {
		return fmt.Errorf("observe runtime: %w", err)
	}
	status, err := client.Status(ctx)
	if err != nil {
		return fmt.Errorf("observe runtime: %w", err)
	}
	if _, err := fmt.Fprintf(stderr,
		"brie-node: CRI runtime=%q version=%q api=%q; startup observation only\n",
		version.GetRuntimeName(), version.GetRuntimeVersion(), version.GetRuntimeApiVersion()); err != nil {
		return fmt.Errorf("write runtime observation: %w", err)
	}
	for _, condition := range status.GetConditions() {
		if _, err := fmt.Fprintf(stderr, "brie-node: CRI condition=%q status=%t reason=%q message=%q\n",
			condition.GetType(), condition.GetStatus(), condition.GetReason(), condition.GetMessage()); err != nil {
			return fmt.Errorf("write runtime condition: %w", err)
		}
	}
	return nil
}
