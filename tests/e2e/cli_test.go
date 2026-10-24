//go:build e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCLI(t *testing.T) {
	for _, build := range []struct {
		name    string
		version string
		flags   []string
	}{
		{name: "development", version: "dev"},
		{name: "release", version: "v0.0.0-e2e", flags: []string{"-ldflags=-X main.version=v0.0.0-e2e"}},
	} {
		t.Run(build.name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "brie-node")
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			args := append([]string{"build", "-trimpath", "-o", binary}, build.flags...)
			args = append(args, "./cmd/brie-node")
			cmd := exec.CommandContext(ctx, "go", args...)
			cmd.Dir = filepath.Join("..", "..")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("build CLI: %v\n%s", err, output)
			}

			for _, tt := range []struct {
				name   string
				args   []string
				stdout string
				stderr string
				code   int
			}{
				{name: "version", args: []string{"--version"}, stdout: "brie-node " + build.version + "\n"},
				{name: "single dash", args: []string{"-version"}, stdout: "brie-node " + build.version + "\n"},
				{name: "help", args: []string{"--help"}, stderr: "Usage of brie-node:"},
				{name: "short help", args: []string{"-h"}, stderr: "Usage of brie-node:"},
				{name: "no command", stderr: "brie-node: node execution is not implemented yet", code: 1},
				{name: "disabled version", args: []string{"--version=false"}, stderr: "brie-node: node execution is not implemented yet", code: 1},
				{name: "unknown flag", args: []string{"--unknown"}, stderr: "brie-node: parse flags:", code: 1},
				{name: "invalid boolean", args: []string{"--version=invalid"}, stderr: "brie-node: parse flags:", code: 1},
				{name: "positional argument", args: []string{"start"}, stderr: "brie-node: unexpected arguments:", code: 1},
				{name: "version with argument", args: []string{"--version", "start"}, stderr: "brie-node: unexpected arguments:", code: 1},
				{name: "flag terminator", args: []string{"--", "--version"}, stderr: "brie-node: unexpected arguments:", code: 1},
				{name: "run missing name", args: []string{"--run"}, stderr: "validate agent configuration:", code: 1},
				{name: "invalid node name", args: []string{"--run", "--node-name=Bad"}, stderr: "validate agent configuration:", code: 1},
				{name: "conflicting modes", args: []string{"--run", "--version"}, stderr: "cannot be combined", code: 1},
				{name: "name without run", args: []string{"--node-name=worker-1"}, stderr: "requires --run", code: 1},
				{name: "empty name without run", args: []string{"--node-name="}, stderr: "requires --run", code: 1},
				{name: "empty kubeconfig", args: []string{"--run", "--node-name=worker-1", "--kubeconfig="}, stderr: "must not be empty", code: 1},
				{name: "missing kubeconfig file", args: []string{"--run", "--node-name=worker-1", "--kubeconfig=" + filepath.Join(t.TempDir(), "missing")}, stderr: "load registration kubeconfig", code: 1},
				{name: "kubeconfig without run", args: []string{"--kubeconfig=/unused"}, stderr: "requires --run", code: 1},
			} {
				t.Run(tt.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, binary, tt.args...)
					var stdout, stderr bytes.Buffer
					cmd.Stdout, cmd.Stderr = &stdout, &stderr
					err := cmd.Run()
					code := 0
					if err != nil {
						var exitErr *exec.ExitError
						if !errors.As(err, &exitErr) {
							t.Fatalf("execute CLI: %v", err)
						}
						code = exitErr.ExitCode()
					}
					if code != tt.code || stdout.String() != tt.stdout {
						t.Errorf("exit=%d stdout=%q; want exit=%d stdout=%q; stderr=%q",
							code, stdout.String(), tt.code, tt.stdout, stderr.String())
					}
					if (tt.stderr == "" && stderr.Len() != 0) ||
						(tt.stderr != "" && !strings.Contains(stderr.String(), tt.stderr)) {
						t.Errorf("stderr=%q; want containing %q", stderr.String(), tt.stderr)
					}
				})
			}
			for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
				t.Run("shutdown/"+sig.String(), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, binary, "--run", "--node-name=worker-1")
					var stdout bytes.Buffer
					cmd.Stdout = &stdout
					stderr, err := cmd.StderrPipe()
					if err != nil {
						t.Fatal(err)
					}
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					scanner := bufio.NewScanner(stderr)
					want := "brie-node: agent scaffold for worker-1; node registration and workload execution are not implemented"
					if !scanner.Scan() || scanner.Text() != want {
						cancel()
						waitErr := cmd.Wait()
						t.Fatalf("startup=%q scan error=%v wait error=%v", scanner.Text(), scanner.Err(), waitErr)
					}
					signalErr := cmd.Process.Signal(sig)
					if signalErr != nil {
						cancel()
					}
					var extra []string
					for scanner.Scan() {
						extra = append(extra, scanner.Text())
					}
					scanErr := scanner.Err()
					waitErr := cmd.Wait()
					if signalErr != nil || scanErr != nil || waitErr != nil || ctx.Err() != nil {
						t.Fatalf("shutdown: signal=%v scan=%v wait=%v context=%v", signalErr, scanErr, waitErr, ctx.Err())
					}
					if stdout.Len() != 0 || len(extra) != 0 {
						t.Fatalf("unexpected output: stdout=%q stderr=%q", stdout.String(), extra)
					}
				})
			}
		})
	}
}
