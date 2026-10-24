//go:build e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestConformanceResults(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode string
		data string
		pass bool
	}{
		{name: "conformance pass", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"passed"}`, pass: true},
		{name: "unselected skipped", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"passed"}
{"name":"other","status":"skipped"}`, pass: true},
		{name: "empty", mode: "certified-conformance"},
		{name: "no selected tests", mode: "certified-conformance", data: `{"name":"other","status":"passed"}`},
		{name: "failed", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"failed"}`},
		{name: "skipped selected test", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"passed"}
{"name":"second [Conformance]","status":"skipped"}`},
		{name: "unknown", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"unknown"}`},
		{name: "timeout", mode: "certified-conformance", data: `{"name":"test [Conformance]","status":"timeout"}`},
		{name: "missing status", mode: "certified-conformance", data: `{"name":"test [Conformance]"}`},
		{name: "invalid JSON", mode: "certified-conformance", data: `{`},
		{name: "quick pass", mode: "quick", data: `{"name":"Pods should be submitted and removed [Conformance]","status":"passed"}`, pass: true},
		{name: "quick missing", mode: "quick", data: `{"name":"other [Conformance]","status":"passed"}`},
		{name: "unknown mode", mode: "other", data: `{"name":"test [Conformance]","status":"passed"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "jq", "-s", "-e", "--arg", "mode", tt.mode, "-f", "check-results.jq")
			cmd.Stdin = strings.NewReader(tt.data)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("validate results: %v", ctx.Err())
			}
			var execErr *exec.Error
			if errors.As(err, &execErr) {
				t.Fatalf("execute jq: %v", err)
			}
			if (err == nil) != tt.pass {
				t.Fatalf("validate results: %v, want pass=%t\n%s", err, tt.pass, output)
			}
		})
	}
}
