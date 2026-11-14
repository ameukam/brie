// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		wantOut  string
		wantErr  string
		wantHelp bool
	}{
		{
			name:    "version",
			args:    []string{"--version"},
			wantOut: "brie-node v0.0.0-test\n",
		},
		{
			name:    "single dash version",
			args:    []string{"-version"},
			wantOut: "brie-node v0.0.0-test\n",
		},
		{
			name:     "help",
			args:     []string{"--help"},
			wantHelp: true,
		},
		{
			name:     "short help",
			args:     []string{"-h"},
			wantHelp: true,
		},
		{
			name:    "no command",
			wantErr: "no mode selected",
		},
		{
			name:    "disabled version",
			args:    []string{"--version=false"},
			wantErr: "no mode selected",
		},
		{
			name:    "unknown flag",
			args:    []string{"--unknown"},
			wantErr: "parse flags",
		},
		{
			name:    "invalid boolean",
			args:    []string{"--version=invalid"},
			wantErr: "parse flags",
		},
		{
			name:    "positional argument",
			args:    []string{"start"},
			wantErr: "unexpected arguments",
		},
		{
			name:    "version with positional argument",
			args:    []string{"--version", "start"},
			wantErr: "unexpected arguments",
		},
		{name: "run without name", args: []string{"--run"}, wantErr: "validate agent configuration"},
		{name: "invalid name", args: []string{"--run", "--node-name=Bad"}, wantErr: "validate agent configuration"},
		{name: "conflicting modes", args: []string{"--run", "--version"}, wantErr: "cannot be combined"},
		{name: "name without run", args: []string{"--node-name=node-1"}, wantErr: "requires --run"},
		{name: "empty name without run", args: []string{"--node-name="}, wantErr: "requires --run"},
		{name: "name with version", args: []string{"--version", "--node-name=node-1"}, wantErr: "requires --run"},
		{name: "disabled run", args: []string{"--run=false"}, wantErr: "no mode selected"},
		{name: "cancelled agent", args: []string{"--run", "--node-name=node-1"}},
		{name: "cancelled registration", args: []string{"--run", "--node-name=node-1", "--kubeconfig=/unused"}},
		{name: "empty kubeconfig", args: []string{"--run", "--node-name=node-1", "--kubeconfig="}, wantErr: "must not be empty"},
		{name: "kubeconfig without run", args: []string{"--kubeconfig=/unused"}, wantErr: "requires --run"},
		{name: "relative kubeconfig", args: []string{"--run", "--node-name=node-1", "--kubeconfig=relative"}, wantErr: "absolute kubeconfig"},
		{name: "runtime without run", args: []string{"--runtime-socket=/unused"}, wantErr: "require --run"},
		{name: "runtime with version", args: []string{"--version", "--runtime-socket=/unused"}, wantErr: "require --run"},
		{name: "empty runtime", args: []string{"--run", "--node-name=node-1", "--runtime-socket="}, wantErr: "must not be empty"},
		{name: "cancelled runtime", args: []string{"--run", "--node-name=node-1", "--runtime-socket=/unused"}},
		{name: "workloads without run", args: []string{"--enable-workloads"}, wantErr: "require --run"},
		{name: "execution configuration without opt-in", args: []string{"--run", "--node-name=node-1", "--max-pods=5"}, wantErr: "requires --enable-workloads"},
		{name: "missing execution configuration", args: []string{"--run", "--node-name=node-1", "--enable-workloads"}, wantErr: "validate execution configuration"},
		{name: "invalid log format", args: []string{"--run", "--node-name=node-1", "--log-format=xml"}, wantErr: "--log-format"},
		{name: "log format without run", args: []string{"--log-format=json"}, wantErr: "requires --run"},
		{name: "cancelled json registration", args: []string{"--run", "--node-name=node-1", "--kubeconfig=/unused", "--log-format=json"}},
		{name: "serving without workloads", args: []string{"--run", "--node-name=node-1", "--enable-serving"}, wantErr: "requires --enable-workloads"},
		{name: "events without workloads", args: []string{"--run", "--node-name=node-1", "--enable-events"}, wantErr: "requires --enable-workloads"},
		{name: "rotation without workloads", args: []string{"--run", "--node-name=node-1", "--container-log-max-size=10Mi"}, wantErr: "requires --enable-workloads"},
		{name: "serving flag without serving", args: []string{"--run", "--node-name=node-1", "--enable-workloads", "--tls-cert-file=/c"}, wantErr: "require --enable-serving"},
		{name: "serving without TLS", args: []string{"--run", "--node-name=node-1", "--enable-workloads", "--enable-serving"}, wantErr: "--tls-cert-file"},
		{name: "max files without size", args: []string{"--run", "--node-name=node-1", "--enable-workloads", "--container-log-max-files=3"}, wantErr: "requires --container-log-max-size"},
		{name: "invalid max size", args: []string{"--run", "--node-name=node-1", "--enable-workloads", "--container-log-max-size=1Ki"}, wantErr: "max size"},
		{name: "invalid max files", args: []string{"--run", "--node-name=node-1", "--enable-workloads", "--container-log-max-size=10Mi", "--container-log-max-files=1"}, wantErr: "max-files"},
		{name: "observability with missing execution configuration", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--enable-events",
		}, wantErr: "validate execution configuration"},
		{name: "cancelled observed execution", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--kubeconfig=/unused", "--runtime-socket=/unused",
			"--runtime-class=kata-fc", "--runtime-handler=kata-fc", "--allocatable-cpu=4", "--allocatable-memory=8Gi",
			"--max-pods=10", "--node-ip=192.0.2.10", "--cluster-dns=10.96.0.10", "--cluster-domain=cluster.local",
			"--state-dir=/unused/state", "--pod-log-dir=/unused/logs", "--log-format=json",
			"--enable-serving", "--tls-cert-file=/c", "--tls-private-key-file=/k", "--client-ca-file=/ca",
			"--enable-events", "--container-log-max-size=10Mi", "--container-log-max-files=3",
		}},
		{name: "cancelled execution", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--kubeconfig=/unused", "--runtime-socket=/unused",
			"--runtime-class=kata-fc", "--runtime-handler=kata-fc", "--allocatable-cpu=4", "--allocatable-memory=8Gi",
			"--max-pods=10", "--node-ip=192.0.2.10", "--cluster-dns=10.96.0.10", "--cluster-domain=cluster.local",
			"--state-dir=/unused/state", "--pod-log-dir=/unused/logs",
		}},
		{name: "image collection without opt-in", args: []string{"--run", "--node-name=node-1", "--image-gc-threshold=50Gi"}, wantErr: "requires --enable-workloads"},
		{name: "invalid image collection threshold", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--kubeconfig=/unused", "--runtime-socket=/unused",
			"--runtime-class=kata-fc", "--runtime-handler=kata-fc", "--allocatable-cpu=4", "--allocatable-memory=8Gi",
			"--max-pods=10", "--node-ip=192.0.2.10", "--cluster-dns=10.96.0.10", "--cluster-domain=cluster.local",
			"--state-dir=/unused/state", "--pod-log-dir=/unused/logs", "--image-gc-threshold=0.5",
		}, wantErr: "image-gc-threshold"},
		{name: "cancelled execution with image collection", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--kubeconfig=/unused", "--runtime-socket=/unused",
			"--runtime-class=kata-fc", "--runtime-handler=kata-fc", "--allocatable-cpu=4", "--allocatable-memory=8Gi",
			"--max-pods=10", "--node-ip=192.0.2.10", "--cluster-dns=10.96.0.10", "--cluster-domain=cluster.local",
			"--state-dir=/unused/state", "--pod-log-dir=/unused/logs", "--image-gc-threshold=50Gi",
		}},
		{name: "probes without workloads", args: []string{"--run", "--node-name=node-1", "--enable-probes"}, wantErr: "requires --enable-workloads"},
		{name: "probes without run", args: []string{"--enable-probes"}, wantErr: "require --run"},
		{name: "cancelled execution with probes", args: []string{
			"--run", "--node-name=node-1", "--enable-workloads", "--kubeconfig=/unused", "--runtime-socket=/unused",
			"--runtime-class=kata-fc", "--runtime-handler=kata-fc", "--allocatable-cpu=4", "--allocatable-memory=8Gi",
			"--max-pods=10", "--node-ip=192.0.2.10", "--cluster-dns=10.96.0.10", "--cluster-domain=cluster.local",
			"--state-dir=/unused/state", "--pod-log-dir=/unused/logs", "--enable-probes",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := run(ctx, tt.args, "v0.0.0-test", &stdout, &stderr)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("run() error = %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("run() error = %v, want containing %q", err, tt.wantErr)
			}
			if got := stdout.String(); got != tt.wantOut {
				t.Errorf("stdout = %q, want %q", got, tt.wantOut)
			}
			if tt.wantHelp && !strings.Contains(stderr.String(), "Usage of brie-node") {
				t.Errorf("stderr = %q, want usage information", stderr.String())
			}
		})
	}
}

func TestRunWriteError(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("output unavailable")
	var stderr bytes.Buffer
	err := run(t.Context(), []string{"--version"}, "test", failingWriter{err: writeErr}, &stderr)
	if !errors.Is(err, writeErr) {
		t.Fatalf("run() error = %v, want wrapped %v", err, writeErr)
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func FuzzRun(f *testing.F) {
	for _, seed := range []string{
		"", "--version", "--help", "-h", "--version=false",
		"--unknown", "--version extra", "--version=invalid", "-- --version",
		"--run", "--run --node-name=node-1", "--run --version", "--node-name=",
		"--run --node-name=node-1 --kubeconfig=/unused", "--run --kubeconfig=",
		"--runtime-socket=", "--run --node-name=node-1 --runtime-socket=/unused",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		var stdout, stderr bytes.Buffer
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := run(ctx, strings.Fields(input), "fuzz", &stdout, &stderr)
		if err != nil && stdout.Len() != 0 {
			t.Fatalf("failed command wrote to stdout: %q", stdout.String())
		}
		if got := stdout.String(); got != "" && got != "brie-node fuzz\n" {
			t.Fatalf("unexpected successful output: %q", got)
		}
	})
}
