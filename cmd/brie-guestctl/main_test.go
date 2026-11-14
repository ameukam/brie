// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/guestsupervisor"
)

type readyWorkload struct{}

func (readyWorkload) Prepare(context.Context, guestsupervisor.PrepareRequest) (guestsupervisor.SafePoint, error) {
	return guestsupervisor.SafePoint{ID: "counter-1", ExternalEffectWatermark: 1}, nil
}

func (readyWorkload) Resume(context.Context, guestsupervisor.ResumeRequest) error { return nil }

func (readyWorkload) Abort(context.Context, string) error { return nil }

func TestRun(t *testing.T) {
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s.sock")
	listener, err := guestsupervisor.Listen(t.Context(), socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- guestsupervisor.NewSupervisor(readyWorkload{}).Serve(ctx, listener) }()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Error(err)
		}
	}()

	prepare, err := guestsupervisor.EncodeRequest(guestsupervisor.Request{
		Version: guestsupervisor.ProtocolVersion, Operation: guestsupervisor.OperationPrepare,
		Prepare: &guestsupervisor.PrepareRequest{OperationID: "op-1", Epoch: 1, Deadline: time.Now().Add(time.Minute)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		args   []string
		code   int
		stdout string
		stderr string
	}{
		{name: "prepare", args: []string{"--socket=" + socket, string(prepare)}, stdout: `"outcome":"safe_point"`},
		{name: "help", args: []string{"--help"}, stderr: "Usage: brie-guestctl"},
		{name: "unknown flag", args: []string{"--bogus"}, code: 2, stderr: "flag provided but not defined"},
		{name: "no request", args: []string{"--socket=" + socket}, code: 2, stderr: "exactly one request"},
		{name: "invalid request", args: []string{"--socket=" + socket, "{}"}, code: 2, stderr: "decode request"},
		{name: "missing socket", args: []string{"--socket=" + filepath.Join(dir, "missing.sock"), string(prepare)}, code: 1, stderr: "connect to supervisor"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(t.Context(), tt.args, &stdout, &stderr)
			if code != tt.code || !strings.Contains(stdout.String(), tt.stdout) || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("run = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
			if tt.code == 0 && tt.stdout != "" {
				if _, err := guestsupervisor.DecodeResponse(bytes.TrimSuffix(stdout.Bytes(), []byte("\n"))); err != nil {
					t.Fatalf("stdout is not a canonical response: %v", err)
				}
			}
		})
	}
}
