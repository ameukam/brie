// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package firecracker provides the runtime owner's Unix-socket API transport.
// It does not supervise VMs, journal operations, or implement lifecycle ownership.
package firecracker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const maxResponseBytes = 64 * 1024

// Client sends at most one request at a time to one Firecracker socket.
// Requests are never redirected or automatically retried.
// All mutations require the caller to reconcile uncertain outcomes after errors.
type Client struct {
	http    *http.Client
	timeout time.Duration
	slot    chan struct{}
}

// New creates a client with a positive timeout covering queueing, dialing, and
// the entire response. It does not connect to or authenticate the socket owner.
func New(socketPath string, timeout time.Duration) (*Client, error) {
	if !filepath.IsAbs(socketPath) || strings.ContainsRune(socketPath, '\x00') {
		return nil, errors.New("invalid Firecracker socket path: must be absolute and contain no NUL bytes")
	}
	if timeout <= 0 {
		return nil, errors.New("invalid Firecracker request timeout: must be positive")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
		// Fresh connections prevent net/http from replaying requests on stale ones.
		DisableKeepAlives:      true,
		MaxConnsPerHost:        1,
		MaxResponseHeaderBytes: maxResponseBytes,
	}
	return &Client{
		http: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		timeout: timeout,
		slot:    make(chan struct{}, 1),
	}, nil
}

// InstanceInfo is the observed metadata returned by GET /.
type InstanceInfo struct {
	AppName    string `json:"app_name"`
	ID         string `json:"id"`
	State      string `json:"state"`
	VMMVersion string `json:"vmm_version"`
}

// Info reads instance metadata, not proof of sandbox ownership or guest health.
func (c *Client) Info(ctx context.Context) (InstanceInfo, error) {
	body, err := c.request(ctx, http.MethodGet, "/", nil, http.StatusOK)
	if err != nil {
		return InstanceInfo{}, err
	}
	return decodeInfo(body)
}

func decodeInfo(body []byte) (InstanceInfo, error) {
	var info InstanceInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return InstanceInfo{}, fmt.Errorf("decode Firecracker instance info: %w", err)
	}
	if info.AppName == "" || info.ID == "" || info.VMMVersion == "" {
		return InstanceInfo{}, errors.New("incomplete Firecracker instance info: missing required metadata")
	}
	switch info.State {
	case "Not started", "Running", "Paused":
		return info, nil
	default:
		return InstanceInfo{}, fmt.Errorf("unknown Firecracker instance state %q", info.State)
	}
}

// MachineConfig is the minimal pre-boot CPU/memory profile. PUT resets optional
// Firecracker settings to their defaults (including SMT and dirty-page tracking).
type MachineConfig struct {
	VCPUCount int   `json:"vcpu_count"`
	MemoryMiB int64 `json:"mem_size_mib"`
}

// Validate checks the supported minimal machine profile.
func (config MachineConfig) Validate() error {
	if config.VCPUCount < 1 || config.VCPUCount > 32 || config.MemoryMiB <= 0 {
		return errors.New("invalid Firecracker machine configuration: requires 1-32 vCPUs and positive memory MiB")
	}
	return nil
}

// ConfigureMachine sets the complete minimal machine profile before boot.
// An error after sending a request does not prove the mutation was not applied.
// The runtime owner must reconcile uncertain outcomes before further mutations.
func (c *Client) ConfigureMachine(ctx context.Context, config MachineConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	return c.putJSON(ctx, "/machine-config", config)
}

func (c *Client) putJSON(ctx context.Context, path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode Firecracker PUT %s: %w", path, err)
	}
	_, err = c.request(ctx, http.MethodPut, path, body, http.StatusNoContent)
	return err
}

// APIError describes an unexpected HTTP status, including a decoded fault when
// available. Callers can inspect it with errors.As.
type APIError struct {
	StatusCode   int
	FaultMessage string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("firecracker HTTP %d: %q", e.StatusCode, e.FaultMessage)
}

func (c *Client) request(ctx context.Context, method, path string, body []byte, wantStatus int) (result []byte, retErr error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.slot <- struct{}{}:
		defer func() { <-c.slot }()
	case <-ctx.Done():
		return nil, fmt.Errorf("firecracker %s %s waiting for request slot: %w", method, path, ctx.Err())
	}

	req, err := http.NewRequestWithContext(ctx, method, "http://firecracker"+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Firecracker request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("firecracker %s %s: %w", method, path, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			result = nil
			retErr = errors.Join(retErr, fmt.Errorf("close Firecracker %s %s response: %w", method, path, err))
		}
	}()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Firecracker %s %s response: %w", method, path, err)
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("firecracker %s %s HTTP %d response exceeds %d bytes",
			method, path, resp.StatusCode, maxResponseBytes)
	}
	if resp.StatusCode != wantStatus {
		apiErr := &APIError{StatusCode: resp.StatusCode}
		var fault struct {
			Message string `json:"fault_message"`
		}
		if err := json.Unmarshal(data, &fault); err != nil {
			return nil, fmt.Errorf("firecracker %s %s: %w; decode fault: %w", method, path, apiErr, err)
		}
		if fault.Message == "" {
			return nil, fmt.Errorf("firecracker %s %s: %w; missing fault_message", method, path, apiErr)
		}
		apiErr.FaultMessage = fault.Message
		return nil, fmt.Errorf("firecracker %s %s: %w", method, path, apiErr)
	}
	return data, nil
}
