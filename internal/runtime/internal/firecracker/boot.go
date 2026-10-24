// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package firecracker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// BootSource describes a kernel and its command line. Paths are interpreted
// inside the Firecracker process's filesystem namespace, including its jail.
type BootSource struct {
	KernelImagePath string `json:"kernel_image_path"`
	BootArgs        string `json:"boot_args"`
}

// Validate checks the kernel path and command line before any machine mutation.
func (source BootSource) Validate() error {
	if !validArtifactPath(source.KernelImagePath) {
		return errors.New("invalid Firecracker kernel path: must be absolute UTF-8 and contain no NUL bytes")
	}
	if !utf8.ValidString(source.BootArgs) || strings.ContainsRune(source.BootArgs, '\x00') {
		return errors.New("invalid Firecracker boot arguments: must be UTF-8 and contain no NUL bytes")
	}
	return nil
}

// ConfigureBootSource replaces the pre-boot kernel configuration. Initrd boot
// is not supported by this minimal profile. It does not inspect or open files.
func (c *Client) ConfigureBootSource(ctx context.Context, source BootSource) error {
	if err := source.Validate(); err != nil {
		return err
	}
	return c.putJSON(ctx, "/boot-source", source)
}

// RootDrive describes a virtio-blk root filesystem. The runtime owner must
// prepare and exclusively assign the backing file before configuring it.
type RootDrive struct {
	PathOnHost string
	ReadOnly   bool
}

// Validate checks the root filesystem path before any machine mutation.
func (drive RootDrive) Validate() error {
	if !validArtifactPath(drive.PathOnHost) {
		return errors.New("invalid Firecracker root drive path: must be absolute UTF-8 and contain no NUL bytes")
	}
	return nil
}

// ConfigureRootDrive sets the single rootfs drive before boot, using the Sync
// IO engine and Writeback cache so guest flush requests are honored. This does
// not establish checkpoint durability or ownership of the backing file.
func (c *Client) ConfigureRootDrive(ctx context.Context, drive RootDrive) error {
	if err := drive.Validate(); err != nil {
		return err
	}
	return c.putJSON(ctx, "/drives/rootfs", struct {
		ID        string `json:"drive_id"`
		Path      string `json:"path_on_host"`
		Root      bool   `json:"is_root_device"`
		ReadOnly  bool   `json:"is_read_only"`
		CacheType string `json:"cache_type"`
		IOEngine  string `json:"io_engine"`
	}{
		ID: "rootfs", Path: drive.PathOnHost, Root: true, ReadOnly: drive.ReadOnly,
		CacheType: "Writeback", IOEngine: "Sync",
	})
}

// Start requests InstanceStart once. Success is an API acknowledgment, not guest
// readiness. A lost reply leaves an uncertain outcome: the owner must observe
// instance state before deciding what to do next, not blindly repeat Start.
func (c *Client) Start(ctx context.Context) error {
	return c.putJSON(ctx, "/actions", struct {
		ActionType string `json:"action_type"`
	}{ActionType: "InstanceStart"})
}

func validArtifactPath(path string) bool {
	return filepath.IsAbs(path) && utf8.ValidString(path) && !strings.ContainsRune(path, '\x00')
}
