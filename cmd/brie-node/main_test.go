// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
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
			wantErr: "node execution is not implemented",
		},
		{
			name:    "disabled version",
			args:    []string{"--version=false"},
			wantErr: "node execution is not implemented",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout, stderr bytes.Buffer
			err := run(tt.args, "v0.0.0-test", &stdout, &stderr)
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
	err := run([]string{"--version"}, "test", failingWriter{err: writeErr}, &stderr)
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
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		var stdout, stderr bytes.Buffer
		err := run(strings.Fields(input), "fuzz", &stdout, &stderr)
		if err != nil && stdout.Len() != 0 {
			t.Fatalf("failed command wrote to stdout: %q", stdout.String())
		}
		if got := stdout.String(); got != "" && got != "brie-node fuzz\n" {
			t.Fatalf("unexpected successful output: %q", got)
		}
	})
}
