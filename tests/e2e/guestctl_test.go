//go:build e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildCommand builds one ./cmd package into a temporary directory.
func buildCommand(t *testing.T, name string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binary, "./cmd/"+name)
	cmd.Dir = filepath.Join("..", "..")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return binary
}

// runCommand runs a binary and returns its exit code and output.
func runCommand(t *testing.T, binary string, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err == nil {
		return 0, stdout.String(), stderr.String()
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("execute %s: %v", binary, err)
	}
	return exitErr.ExitCode(), stdout.String(), stderr.String()
}

func TestGuestctlCLI(t *testing.T) {
	binary := buildCommand(t, "brie-guestctl")
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	abort := `{"version":"brie.guestsupervisor/v1","operation":"abort","abort":{"operation_id":"op-1"}}`
	for _, tt := range []struct {
		name   string
		args   []string
		code   int
		stderr string
	}{
		{name: "help", args: []string{"--help"}, stderr: "Usage: brie-guestctl"},
		{name: "no request", code: 2, stderr: "exactly one request argument"},
		{name: "non-canonical request", args: []string{abort + "\n"}, code: 2, stderr: "not canonically encoded"},
		{name: "unknown field", args: []string{strings.Replace(abort, `"version"`, `"x":1,"version"`, 1)}, code: 2, stderr: "unknown field"},
		{name: "missing supervisor", args: []string{"--socket=" + filepath.Join(dir, "missing.sock"), abort}, code: 1, stderr: "connect to supervisor"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(t, binary, tt.args...)
			if code != tt.code || stdout != "" || !strings.Contains(stderr, tt.stderr) {
				t.Fatalf("exit=%d stdout=%q stderr=%q; want exit=%d stderr containing %q", code, stdout, stderr, tt.code, tt.stderr)
			}
		})
	}
}
