// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// shortSocketPath returns a socket path that fits sun_path regardless of the
// test name length.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func startSupervisor(t *testing.T, workload Workload) string {
	t.Helper()
	path := shortSocketPath(t)
	listener, err := Listen(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	served := make(chan error, 1)
	go func() { served <- NewSupervisor(workload).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})
	return path
}

func TestCallOverSocket(t *testing.T) {
	workload := &fakeWorkload{point: SafePoint{ID: "counter-2", ExternalEffectWatermark: 2}}
	path := startSupervisor(t, workload)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	response, err := Call(ctx, path, prepareRequest("op-1", 1, time.Now().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if response.Outcome != OutcomeSafePoint || *response.SafePoint != workload.point {
		t.Fatalf("prepare response = %+v", response)
	}
	response, err = Call(ctx, path, resumeRequest("op-1", 1, time.Now()))
	if err != nil || response.Outcome != OutcomeReady {
		t.Fatalf("resume response = %+v, %v", response, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, %v; want 0600", info.Mode(), err)
	}
}

func TestServeRejectsMalformedRequest(t *testing.T) {
	path := startSupervisor(t, &fakeWorkload{})
	for _, payload := range []string{"{}", strings.Repeat("x", MaxMessageBytes+10)} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, MaxMessageBytes)
		n, _ := conn.Read(buffer)
		_ = conn.Close()
		response, err := DecodeResponse(buffer[:n])
		if err != nil || response.Outcome != OutcomeRefusal || response.Refusal.Reason != "invalid request" || response.OperationID != "unknown" {
			t.Fatalf("response = %+v, %v", response, err)
		}
	}
}

func TestCallErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	valid := abortRequest("op-1")
	if _, err := Call(ctx, "relative.sock", valid); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("relative path error = %v", err)
	}
	if _, err := Call(ctx, "/x.sock", Request{}); err == nil || !strings.Contains(err.Error(), "invalid request") {
		t.Fatalf("invalid request error = %v", err)
	}
	if _, err := Call(ctx, shortSocketPath(t), valid); err == nil || !strings.Contains(err.Error(), "connect") {
		t.Fatalf("missing socket error = %v", err)
	}

	for _, tt := range []struct {
		name  string
		reply string
		want  string
	}{
		{name: "garbage", reply: "garbage", want: "decode response"},
		{name: "mismatched operation", reply: `{"version":"brie.guestsupervisor/v1","outcome":"aborted","operation_id":"op-2"}`, want: "does not match"},
		{name: "silent", reply: "", want: "decode response"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := shortSocketPath(t)
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_, _ = io.ReadAll(conn)
				_, _ = conn.Write([]byte(tt.reply))
				_ = conn.Close()
			}()
			if _, err := Call(ctx, path, valid); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Call error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCallHonorsContext(t *testing.T) {
	path := shortSocketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			defer func() { _ = conn.Close() }()
			time.Sleep(time.Second)
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := Call(ctx, path, abortRequest("op-1")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call error = %v, want deadline exceeded", err)
	}
}

func TestListen(t *testing.T) {
	ctx := t.Context()
	if _, err := Listen(ctx, "relative"); err == nil {
		t.Fatal("Listen accepted a relative path")
	}
	if _, err := Listen(ctx, "/"+strings.Repeat("a", 107)); err == nil {
		t.Fatal("Listen accepted an overlong path")
	}

	regular := shortSocketPath(t)
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(ctx, regular); err == nil || !strings.Contains(err.Error(), "not a socket") {
		t.Fatalf("regular file error = %v", err)
	}

	live := shortSocketPath(t)
	first, err := Listen(ctx, live)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(ctx, live); err == nil || !strings.Contains(err.Error(), "another supervisor") {
		t.Fatalf("live socket error = %v", err)
	}

	// A stale socket file left by a dead process is replaced.
	first.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(live); err != nil {
		t.Fatalf("stale socket missing: %v", err)
	}
	second, err := Listen(ctx, live)
	if err != nil {
		t.Fatalf("Listen on stale socket: %v", err)
	}
	_ = second.Close()

	missingDir := filepath.Join(filepath.Dir(live), "missing", "s.sock")
	if _, err := Listen(ctx, missingDir); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("missing directory error = %v", err)
	}
}

func TestServeReturnsAcceptError(t *testing.T) {
	listener, err := net.Listen("unix", shortSocketPath(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = listener.Close()
	if err := NewSupervisor(&fakeWorkload{}).Serve(t.Context(), listener); err == nil || !strings.Contains(err.Error(), "accept") {
		t.Fatalf("Serve error = %v", err)
	}
}
