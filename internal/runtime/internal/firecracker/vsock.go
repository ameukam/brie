// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package firecracker

import (
	"context"
	"errors"
)

// VsockConfig defines the guest control device, not Pod networking.
// The owner must exclusively assign its host socket path.
type VsockConfig struct {
	GuestCID uint32 `json:"guest_cid"`
	UDSPath  string `json:"uds_path"`
}

// Validate excludes reserved CIDs and leaves room for Firecracker's
// "_<uint32 port>" suffix on guest-initiated Unix connections.
func (config VsockConfig) Validate() error {
	if config.GuestCID < 3 || config.GuestCID == ^uint32(0) {
		return errors.New("vsock guest CID must be between 3 and 4294967294")
	}
	if !validArtifactPath(config.UDSPath) || len(config.UDSPath) > 96 {
		return errors.New("vsock socket path must be absolute UTF-8 without NUL bytes and at most 96 bytes")
	}
	return nil
}

// ConfigureVsock configures one pre-boot device without retries. Success does
// not prove a guest listener exists or authenticate a guest control protocol.
func (c *Client) ConfigureVsock(ctx context.Context, config VsockConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	return c.putJSON(ctx, "/vsock", config)
}
