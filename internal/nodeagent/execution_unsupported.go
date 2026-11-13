// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package nodeagent

import (
	"context"
	"errors"
	"io"
)

// Execute is unavailable outside Linux.
func Execute(context.Context, Config, ExecutionConfig, string, io.Writer) error {
	return errors.New("workload execution requires Linux")
}

// ExecuteObserved is unavailable outside Linux.
func ExecuteObserved(context.Context, Config, ExecutionConfig, string, io.Writer, ExecutionHooks) error {
	return errors.New("workload execution requires Linux")
}
