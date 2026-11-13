// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/ameukam/brie/internal/guestsupervisor"
)

// stateVersion identifies the state file schema.
const stateVersion = "brie.counter/v1"

// maxStateBytes bounds the state file read on startup and resume.
const maxStateBytes = 1024

// counterState is the durable state file content.
type counterState struct {
	Version string `json:"version"`
	Count   uint64 `json:"count"`
}

// decodeState accepts only the canonical encoding written by encodeState.
func decodeState(data []byte) (counterState, error) {
	if len(data) > maxStateBytes {
		return counterState{}, fmt.Errorf("state file exceeds %d bytes", maxStateBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var state counterState
	if err := decoder.Decode(&state); err != nil {
		return counterState{}, fmt.Errorf("decode state file: %w", err)
	}
	if state.Version != stateVersion {
		return counterState{}, fmt.Errorf("unsupported state file version %q", state.Version)
	}
	if !bytes.Equal(encodeState(state), data) {
		return counterState{}, errors.New("state file is not canonically encoded")
	}
	return state, nil
}

func encodeState(state counterState) []byte {
	data, _ := json.Marshal(state) // A struct of a string and an integer always encodes.
	return append(data, '\n')
}

// identity is the execution identity supplied by the latest resume. It is
// empty until the first resume because the restricted Pod profile provides
// no downward API.
type identity struct {
	SandboxUID string `json:"sandbox_uid,omitempty"`
	PodUID     string `json:"pod_uid,omitempty"`
	Epoch      int64  `json:"epoch,omitempty"`
}

// counter is a cooperative HTTP workload. Every acknowledged increment is
// durable in the state file before its reply, so the acknowledged-request
// watermark equals the persisted count.
type counter struct {
	statePath string

	mu         sync.Mutex
	admitting  bool
	inflight   int
	drained    chan struct{}
	count      uint64
	identity   identity
	instanceID string

	// stateMu serializes state file writes.
	stateMu sync.Mutex
}

// openCounter loads the state file, or starts from zero when it does not
// exist. A present but unreadable or invalid file is an error, never a reset.
func openCounter(statePath string) (*counter, error) {
	if !filepath.IsAbs(statePath) {
		return nil, errors.New("state path must be absolute")
	}
	c := &counter{statePath: statePath, admitting: true}
	state, err := c.readState()
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		c.count = state.Count
	}
	instanceID, err := newInstanceID(nil)
	if err != nil {
		return nil, err
	}
	c.instanceID = instanceID
	return c, nil
}

func (c *counter) readState() (counterState, error) {
	file, err := os.Open(c.statePath)
	if err != nil {
		return counterState{}, fmt.Errorf("open state file: %w", err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxStateBytes+1))
	if err != nil {
		return counterState{}, fmt.Errorf("read state file: %w", err)
	}
	return decodeState(data)
}

// writeState replaces the state file atomically and durably: the new content
// and the directory entry are both fsynced before it returns.
func (c *counter) writeState(count uint64) error {
	dir := filepath.Dir(c.statePath)
	temporary := c.statePath + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create state file: %w", err)
	}
	if _, err := file.Write(encodeState(counterState{Version: stateVersion, Count: count})); err != nil {
		_ = file.Close()
		return fmt.Errorf("write state file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync state file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close state file: %w", err)
	}
	if err := os.Rename(temporary, c.statePath); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return syncPath(dir)
}

// syncPath flushes a file or directory to stable storage.
func syncPath(path string) error {
	handle, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s for sync: %w", filepath.Base(path), err)
	}
	defer func() { _ = handle.Close() }()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	return nil
}

// admit registers one unit of work unless preparation closed admission.
func (c *counter) admit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.admitting {
		return false
	}
	c.inflight++
	return true
}

func (c *counter) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight--
	if c.inflight == 0 && c.drained != nil {
		close(c.drained)
		c.drained = nil
	}
}

func (c *counter) reopen() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.admitting = true
}

// increment persists the next count and returns it with the instance ID.
func (c *counter) increment() (uint64, string, error) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.mu.Lock()
	next := c.count + 1
	c.mu.Unlock()
	if err := c.writeState(next); err != nil {
		return 0, "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = next
	return next, c.instanceID, nil
}

// Prepare closes admission, waits for in-flight requests, flushes the state
// file, and reports the persisted count as both the safe point and the
// acknowledged-request watermark. On failure it reopens admission.
func (c *counter) Prepare(ctx context.Context, _ guestsupervisor.PrepareRequest) (guestsupervisor.SafePoint, error) {
	c.mu.Lock()
	c.admitting = false
	var drained chan struct{}
	if c.inflight > 0 {
		if c.drained == nil {
			c.drained = make(chan struct{})
		}
		drained = c.drained
	}
	c.mu.Unlock()
	if drained != nil {
		select {
		case <-drained:
		case <-ctx.Done():
			c.reopen()
			return guestsupervisor.SafePoint{}, ctx.Err()
		}
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	c.mu.Lock()
	count := c.count
	c.mu.Unlock()
	state, err := c.readState()
	switch {
	case errors.Is(err, os.ErrNotExist) && count == 0:
		err = c.writeState(0)
	case err == nil && state.Count == count:
		if err = syncPath(c.statePath); err == nil {
			err = syncPath(filepath.Dir(c.statePath))
		}
	case err == nil:
		err = errors.New("state file does not match the acknowledged count")
	}
	if err != nil {
		c.reopen()
		return guestsupervisor.SafePoint{}, guestsupervisor.Refuse("state file could not be flushed")
	}
	return guestsupervisor.SafePoint{ID: fmt.Sprintf("counter-%d", count), ExternalEffectWatermark: count}, nil
}

// Resume checks that the state file still matches the safe point, binds the
// new identity, replaces the instance ID using fresh entropy, and only then
// reopens admission.
func (c *counter) Resume(_ context.Context, request guestsupervisor.ResumeRequest) error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	state, err := c.readState()
	if err != nil {
		return guestsupervisor.Refuse("state file is unreadable")
	}
	nonce, err := hex.DecodeString(request.EntropyNonce)
	if err != nil {
		return guestsupervisor.Refuse("entropy nonce is invalid")
	}
	instanceID, err := newInstanceID(nonce)
	if err != nil {
		return guestsupervisor.Refuse("guest entropy is unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if state.Count != c.count {
		return guestsupervisor.Refuse("state file does not match the safe point")
	}
	c.identity = identity{SandboxUID: request.SandboxUID, PodUID: request.PodUID, Epoch: request.Epoch}
	c.instanceID = instanceID
	c.admitting = true
	return nil
}

// Abort reopens admission with the existing identity.
func (c *counter) Abort(context.Context, string) error {
	c.reopen()
	return nil
}

// newInstanceID derives a per-execution identifier from guest randomness and
// the host nonce, so restored copies of one snapshot never share it even if
// the guest generator was not reseeded.
func newInstanceID(nonce []byte) (string, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("read guest entropy: %w", err)
	}
	sum := sha256.Sum256(append(random, nonce...))
	return hex.EncodeToString(sum[:16]), nil
}

type incrementReply struct {
	Count    uint64 `json:"count"`
	Instance string `json:"instance"`
}

type statusReply struct {
	Count     uint64   `json:"count"`
	Instance  string   `json:"instance"`
	Admitting bool     `json:"admitting"`
	Identity  identity `json:"identity"`
}

func (c *counter) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /increment", func(w http.ResponseWriter, _ *http.Request) {
		if !c.admit() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "not accepting work", http.StatusServiceUnavailable)
			return
		}
		defer c.finish()
		count, instance, err := c.increment()
		if err != nil {
			http.Error(w, "state not persisted", http.StatusInternalServerError)
			return
		}
		writeJSON(w, incrementReply{Count: count, Instance: instance})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		reply := statusReply{Count: c.count, Instance: c.instanceID, Admitting: c.admitting, Identity: c.identity}
		c.mu.Unlock()
		writeJSON(w, reply)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		c.mu.Lock()
		admitting := c.admitting
		c.mu.Unlock()
		if !admitting {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
