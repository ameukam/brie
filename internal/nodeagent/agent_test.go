// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		node string
		ok   bool
	}{
		{name: "short", node: "a", ok: true},
		{name: "numeric", node: "1", ok: true},
		{name: "hostname", node: "worker-1.example.test", ok: true},
		{name: "maximum label", node: strings.Repeat("a", 63), ok: true},
		{name: "maximum name", node: strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 61), ok: true},
		{name: "empty"},
		{name: "long label", node: strings.Repeat("a", 64)},
		{name: "long name", node: strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 62)},
		{name: "uppercase", node: "Worker"},
		{name: "leading hyphen", node: "-worker"},
		{name: "trailing hyphen", node: "worker-"},
		{name: "leading dot", node: ".worker"},
		{name: "trailing dot", node: "worker."},
		{name: "empty label", node: "worker..example"},
		{name: "underscore", node: "worker_1"},
		{name: "whitespace", node: "worker 1"},
		{name: "newline", node: "worker\n"},
		{name: "NUL", node: "worker\x00"},
		{name: "non ASCII", node: "w\xc3\xb6rker"},
		{name: "invalid UTF8", node: "worker\xff"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := (Config{NodeName: tt.node}).Validate(); (err == nil) != tt.ok {
				t.Errorf("Validate() = %v, want valid=%t", err, tt.ok)
			}
		})
	}
}

func TestRun(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("stderr unavailable")
	for _, tt := range []struct {
		name    string
		config  Config
		expired bool
		writer  io.Writer
		wantErr error
	}{
		{name: "cancelled", config: Config{NodeName: "worker-1"}, writer: io.Discard},
		{name: "deadline", config: Config{NodeName: "worker-1"}, expired: true, writer: io.Discard, wantErr: context.DeadlineExceeded},
		{name: "write failure", config: Config{NodeName: "worker-1"}, writer: failingWriter{err: writeErr}, wantErr: writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- Run(ctx, tt.config, tt.writer) }()
				synctest.Wait()
				if !errors.Is(tt.wantErr, writeErr) {
					select {
					case err := <-done:
						t.Fatalf("Run returned before shutdown: %v", err)
					default:
					}
				}
				if tt.expired {
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				if err := <-done; !errors.Is(err, tt.wantErr) {
					t.Errorf("Run() = %v, want %v", err, tt.wantErr)
				}
			})
		})
	}
}

func TestRunStartup(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var stderr bytes.Buffer
		done := make(chan error, 1)
		go func() { done <- Run(ctx, Config{NodeName: "worker-1"}, &stderr) }()
		synctest.Wait()
		want := "brie-node: agent scaffold for worker-1; node registration and workload execution are not implemented\n"
		if got := stderr.String(); got != want {
			t.Errorf("stderr = %q, want %q", got, want)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunBeforeStartup(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		config  Config
		expired bool
		wantErr error
		invalid bool
	}{
		{name: "invalid config", invalid: true},
		{name: "cancelled", config: Config{NodeName: "worker-1"}},
		{name: "expired", config: Config{NodeName: "worker-1"}, expired: true, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			deadline := time.Now().Add(time.Minute)
			if tt.expired {
				deadline = time.Now().Add(-time.Second)
			}
			ctx, cancel := context.WithDeadline(t.Context(), deadline)
			cancel()
			var stderr bytes.Buffer
			err := Run(ctx, tt.config, &stderr)
			if tt.invalid {
				if err == nil || !strings.Contains(err.Error(), "validate agent configuration") {
					t.Fatalf("Run() = %v, want configuration error", err)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Errorf("Run() = %v, want %v", err, tt.wantErr)
			}
			if stderr.Len() != 0 {
				t.Errorf("unexpected startup output: %q", stderr.String())
			}
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func FuzzConfig(f *testing.F) {
	for _, node := range []string{"", "worker-1", "worker.example", "-bad", "Bad", "a..b", "a\x00", "a\xff"} {
		f.Add(node)
	}
	f.Fuzz(func(t *testing.T, node string) {
		config := Config{NodeName: node}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var stderr bytes.Buffer
		err := Run(ctx, config, &stderr)
		if (err == nil) != (config.Validate() == nil) {
			t.Fatalf("Run() = %v disagrees with configuration validation", err)
		}
		if stderr.Len() != 0 {
			t.Fatalf("cancelled agent wrote to stderr: %q", stderr.String())
		}
	})
}
