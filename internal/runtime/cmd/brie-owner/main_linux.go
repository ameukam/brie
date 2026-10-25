// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brie-owner supervises one disposable Firecracker execution attempt.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
	"github.com/ameukam/brie/internal/runtime/internal/owner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("brie-owner", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var config owner.ServiceConfig
	guestCID := flags.Uint64("guest-cid", 0, "Optional guest vsock CID (3-4294967294; 0 disables vsock)")
	flags.StringVar(&config.Owner.SandboxUID, "sandbox-uid", "", "Sandbox execution identity")
	flags.StringVar(&config.Owner.PodUID, "pod-uid", "", "Pod execution identity")
	flags.DurationVar(&config.Owner.OperationTimeout, "timeout", 10*time.Second, "Positive lifecycle operation timeout")
	flags.StringVar(&config.Directory, "directory", "", "Fresh absolute private directory under a trusted parent")
	flags.StringVar(&config.Binary, "firecracker", "", "Absolute Firecracker executable")
	flags.StringVar(&config.Boot.BootSource.KernelImagePath, "kernel", "", "Absolute guest kernel path")
	flags.StringVar(&config.Boot.BootSource.BootArgs, "boot-args", "", "Guest kernel command line")
	flags.StringVar(&config.Boot.RootDrive.PathOnHost, "rootfs", "", "Absolute guest rootfs path")
	flags.BoolVar(&config.Boot.RootDrive.ReadOnly, "read-only", true, "Use a read-only guest rootfs")
	flags.IntVar(&config.Boot.Machine.VCPUCount, "vcpus", 1, "Guest vCPU count")
	flags.Int64Var(&config.Boot.Machine.MemoryMiB, "memory-mib", 128, "Guest memory in MiB")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse owner flags: %w", err)
	}
	if flags.NArg() != 0 {
		return errors.New("owner accepts no positional arguments")
	}
	if *guestCID != 0 {
		if *guestCID < 3 || *guestCID >= 1<<32-1 {
			return errors.New("guest CID must be between 3 and 4294967294")
		}
		config.Boot.Vsock = &firecracker.VsockConfig{
			GuestCID: uint32(*guestCID),
			UDSPath:  filepath.Join(config.Directory, "vmm", "vsock.sock"),
		}
	}
	return owner.Serve(ctx, config, log.New(stderr, "brie-owner: ", 0))
}
