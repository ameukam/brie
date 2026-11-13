// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/guestsupervisor"
)

const (
	testSandboxUID = "0f8fad5b-d9cb-469f-a165-70867728950e"
	testPodUID     = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	testNonce      = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
)

func newTestCounter(t *testing.T) *counter {
	t.Helper()
	c, err := openCounter(filepath.Join(t.TempDir(), "counter.json"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func resumeRequest(epoch int64) guestsupervisor.ResumeRequest {
	return guestsupervisor.ResumeRequest{
		OperationID: "op-1", SandboxUID: testSandboxUID, PodUID: testPodUID,
		Epoch: epoch, WallClock: time.Now(), EntropyNonce: testNonce,
	}
}

func post(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/increment", nil))
	return recorder
}

func status(t *testing.T, handler http.Handler) statusReply {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/status", nil))
	var reply statusReply
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func TestDecodeState(t *testing.T) {
	for _, tt := range []struct {
		name  string
		data  string
		count uint64
		want  string
	}{
		{name: "valid", data: "{\"version\":\"brie.counter/v1\",\"count\":7}\n", count: 7},
		{name: "missing newline", data: `{"version":"brie.counter/v1","count":7}`, want: "canonical"},
		{name: "unknown field", data: `{"version":"brie.counter/v1","count":7,"x":1}`, want: "unknown field"},
		{name: "wrong version", data: "{\"version\":\"v0\",\"count\":7}\n", want: "unsupported"},
		{name: "negative", data: "{\"version\":\"brie.counter/v1\",\"count\":-1}\n", want: "decode"},
		{name: "empty", data: "", want: "decode"},
		{name: "oversized", data: strings.Repeat(" ", maxStateBytes+1), want: "exceeds"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state, err := decodeState([]byte(tt.data))
			if tt.want == "" {
				if err != nil || state.Count != tt.count {
					t.Fatalf("decodeState = %+v, %v", state, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("decodeState error = %v, want %q", err, tt.want)
			}
		})
	}
}

func FuzzDecodeState(f *testing.F) {
	f.Add(encodeState(counterState{Version: stateVersion, Count: 3}))
	f.Add([]byte(`{"version":"brie.counter/v1","count":1,"count":2}` + "\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		state, err := decodeState(data)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeState(state), data) {
			t.Fatalf("accepted non-canonical state %q", data)
		}
	})
}

func TestOpenCounter(t *testing.T) {
	if _, err := openCounter("relative.json"); err == nil {
		t.Fatal("openCounter accepted a relative path")
	}
	dir := t.TempDir()
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openCounter(corrupt); err == nil {
		t.Fatal("openCounter reset a corrupt state file")
	}
	if _, err := openCounter(dir); err == nil {
		t.Fatal("openCounter accepted a directory")
	}
	existing := filepath.Join(dir, "existing.json")
	if err := os.WriteFile(existing, encodeState(counterState{Version: stateVersion, Count: 41}), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := openCounter(existing)
	if err != nil || c.count != 41 || !c.admitting || len(c.instanceID) != 32 {
		t.Fatalf("openCounter = %+v, %v", c, err)
	}
}

func TestIncrementPersists(t *testing.T) {
	c := newTestCounter(t)
	handler := c.handler()
	for want := uint64(1); want <= 3; want++ {
		recorder := post(t, handler)
		var reply incrementReply
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &reply) != nil || reply.Count != want {
			t.Fatalf("increment %d: %d %q", want, recorder.Code, recorder.Body.String())
		}
	}
	reopened, err := openCounter(c.statePath)
	if err != nil || reopened.count != 3 {
		t.Fatalf("reopened count = %+v, %v", reopened, err)
	}
	if _, err := os.Stat(c.statePath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary state file left behind: %v", err)
	}

	// A write failure is not acknowledged and does not advance the count.
	c.statePath = filepath.Join(t.TempDir(), "missing", "counter.json")
	if recorder := post(t, handler); recorder.Code != http.StatusInternalServerError {
		t.Fatalf("failed write status = %d", recorder.Code)
	}
	if got := status(t, handler).Count; got != 3 {
		t.Fatalf("count after failed write = %d", got)
	}
}

func TestPrepareResumeCycle(t *testing.T) {
	c := newTestCounter(t)
	handler := c.handler()
	post(t, handler)
	post(t, handler)
	before := status(t, handler)

	point, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{})
	if err != nil || point.ID != "counter-2" || point.ExternalEffectWatermark != 2 {
		t.Fatalf("Prepare = %+v, %v", point, err)
	}
	if recorder := post(t, handler); recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") == "" {
		t.Fatalf("increment while prepared = %d", recorder.Code)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while prepared = %d", recorder.Code)
	}

	if err := c.Resume(t.Context(), resumeRequest(4)); err != nil {
		t.Fatal(err)
	}
	after := status(t, handler)
	if !after.Admitting || after.Count != 2 || after.Instance == before.Instance ||
		after.Identity != (identity{SandboxUID: testSandboxUID, PodUID: testPodUID, Epoch: 4}) {
		t.Fatalf("status after resume = %+v (before %+v)", after, before)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("readyz after resume = %d", recorder.Code)
	}
	if recorder := post(t, handler); recorder.Code != http.StatusOK {
		t.Fatalf("increment after resume = %d", recorder.Code)
	}
}

func TestPrepareWithoutStateFile(t *testing.T) {
	c := newTestCounter(t)
	point, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{})
	if err != nil || point.ID != "counter-0" || point.ExternalEffectWatermark != 0 {
		t.Fatalf("Prepare = %+v, %v", point, err)
	}
	if _, err := os.Stat(c.statePath); err != nil {
		t.Fatalf("state file not written at prepare: %v", err)
	}
}

func TestPrepareDrainsInFlightWork(t *testing.T) {
	c := newTestCounter(t)
	if !c.admit() {
		t.Fatal("admit refused")
	}
	prepared := make(chan error, 1)
	go func() {
		_, err := c.Prepare(context.Background(), guestsupervisor.PrepareRequest{})
		prepared <- err
	}()
	for {
		c.mu.Lock()
		closed := !c.admitting
		c.mu.Unlock()
		if closed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if c.admit() {
		t.Fatal("admit accepted work during preparation")
	}
	select {
	case err := <-prepared:
		t.Fatalf("Prepare returned before drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	c.finish()
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
}

func TestPrepareFailuresReopen(t *testing.T) {
	t.Run("drain deadline", func(t *testing.T) {
		c := newTestCounter(t)
		c.admit()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, err := c.Prepare(ctx, guestsupervisor.PrepareRequest{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Prepare error = %v", err)
		}
		c.finish()
		if !c.admit() {
			t.Fatal("admission stayed closed after a failed prepare")
		}
	})
	t.Run("state file changed", func(t *testing.T) {
		c := newTestCounter(t)
		post(t, c.handler())
		if err := os.WriteFile(c.statePath, encodeState(counterState{Version: stateVersion, Count: 5}), 0o600); err != nil {
			t.Fatal(err)
		}
		var refusal *guestsupervisor.RefusalError
		if _, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{}); !errors.As(err, &refusal) || !c.admit() {
			t.Fatalf("Prepare error = %v", err)
		}
	})
	t.Run("state file lost", func(t *testing.T) {
		c := newTestCounter(t)
		post(t, c.handler())
		if err := os.Remove(c.statePath); err != nil {
			t.Fatal(err)
		}
		_, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{})
		var refusal *guestsupervisor.RefusalError
		if !errors.As(err, &refusal) || !c.admit() {
			t.Fatalf("Prepare error = %v, admitting %v", err, c.admitting)
		}
		if _, err := os.Stat(c.statePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("prepare recreated a lost state file: %v", err)
		}
	})
}

func TestResumeRefusals(t *testing.T) {
	for _, tt := range []struct {
		name    string
		mutate  func(*testing.T, *counter, *guestsupervisor.ResumeRequest)
		reason  string
		applied bool
	}{
		{name: "state file changed", reason: "does not match", mutate: func(t *testing.T, c *counter, _ *guestsupervisor.ResumeRequest) {
			if err := os.WriteFile(c.statePath, encodeState(counterState{Version: stateVersion, Count: 99}), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "state file missing", reason: "unreadable", mutate: func(t *testing.T, c *counter, _ *guestsupervisor.ResumeRequest) {
			if err := os.Remove(c.statePath); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "invalid nonce", reason: "nonce", mutate: func(_ *testing.T, _ *counter, r *guestsupervisor.ResumeRequest) {
			r.EntropyNonce = "zz"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCounter(t)
			post(t, c.handler())
			if _, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{}); err != nil {
				t.Fatal(err)
			}
			request := resumeRequest(2)
			tt.mutate(t, c, &request)
			err := c.Resume(t.Context(), request)
			var refusal *guestsupervisor.RefusalError
			if !errors.As(err, &refusal) || !strings.Contains(refusal.Reason, tt.reason) {
				t.Fatalf("Resume error = %v, want reason %q", err, tt.reason)
			}
			if c.admit() || c.identity != (identity{}) {
				t.Fatalf("refused resume opened the barrier: %+v", c.identity)
			}
		})
	}
}

func TestAbortReopens(t *testing.T) {
	c := newTestCounter(t)
	instance := c.instanceID
	if _, err := c.Prepare(t.Context(), guestsupervisor.PrepareRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Abort(t.Context(), "op-1"); err != nil {
		t.Fatal(err)
	}
	if !c.admit() || c.instanceID != instance || c.identity != (identity{}) {
		t.Fatal("abort changed identity or left admission closed")
	}
}

func TestRun(t *testing.T) {
	for _, tt := range []struct {
		name   string
		args   []string
		code   int
		stderr string
	}{
		{name: "help", args: []string{"--help"}, stderr: "Usage of brie-counter"},
		{name: "unknown flag", args: []string{"--bogus"}, code: 2, stderr: "not defined"},
		{name: "positional", args: []string{"start"}, code: 2, stderr: "unexpected arguments"},
		{name: "relative state", args: []string{"--state=counter.json"}, code: 1, stderr: "state path must be absolute"},
		{name: "relative socket", args: []string{"--state=/nonexistent/counter.json", "--supervisor-socket=s.sock"}, code: 1, stderr: "supervisor socket path"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			if code := run(t.Context(), tt.args, &stderr); code != tt.code || !strings.Contains(stderr.String(), tt.stderr) {
				t.Fatalf("run = %d, stderr %q", code, stderr.String())
			}
		})
	}
}

func TestServeShutsDown(t *testing.T) {
	dir, err := os.MkdirTemp("", "ctr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s.sock")
	state := filepath.Join(dir, "counter.json")

	var stderr bytes.Buffer
	if err := serve(t.Context(), "127.0.0.1:-1", state, socket, &stderr); err == nil || !strings.Contains(err.Error(), "listen for HTTP") {
		t.Fatalf("serve with a bad address = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, "127.0.0.1:0", state, socket, &bytes.Buffer{}) }()
	callCtx, cancelCall := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelCall()
	var response guestsupervisor.Response
	for {
		response, err = guestsupervisor.Call(callCtx, socket, guestsupervisor.Request{
			Version: guestsupervisor.ProtocolVersion, Operation: guestsupervisor.OperationAbort,
			Abort: &guestsupervisor.AbortRequest{OperationID: "op-1"},
		})
		if err == nil || callCtx.Err() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || response.Outcome != guestsupervisor.OutcomeAborted {
		t.Fatalf("abort over socket = %+v, %v", response, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve = %v", err)
	}
}
