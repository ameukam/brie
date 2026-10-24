//go:build integration && linux

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

func TestRealOwnerBoot(t *testing.T) {
	if os.Getenv("BRIE_REAL_VM") != "1" {
		t.Skip("requires an approved disposable KVM host and BRIE_REAL_VM=1")
	}
	if runtime.GOARCH != "arm64" {
		t.Fatal("this experiment requires arm64")
	}
	for _, key := range []string{"FIRECRACKER_BINARY", "GUEST_KERNEL", "GUEST_ROOTFS"} {
		path := os.Getenv(key)
		info, err := os.Stat(path)
		if !filepath.IsAbs(path) || err != nil || !info.Mode().IsRegular() {
			t.Fatalf("%s must name an existing absolute regular file", key)
		}
	}
	dir, err := os.MkdirTemp("", "brie-owner-real-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("retained experiment evidence: %s", dir)
	launcher, err := NewFirecrackerLauncher(os.Getenv("FIRECRACKER_BINARY"), filepath.Join(dir, "vmm"))
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(Config{
		SandboxUID: "synthetic-sandbox", PodUID: "synthetic-pod",
		OperationTimeout: 10 * time.Second, JournalDir: filepath.Join(dir, "journal"),
	}, launcher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if o.Status().State != Idle {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := o.Stop(ctx); err != nil {
				t.Error(err)
			}
		}
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := o.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	client, err := firecracker.New(launcher.SocketPath(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.Info(t.Context())
	if err != nil || info.VMMVersion != "1.17.0" || info.State != "Not started" {
		t.Fatalf("unexpected pre-boot metadata: %+v, %v", info, err)
	}
	if err := o.Boot(t.Context(), client, BootConfig{
		Machine: firecracker.MachineConfig{VCPUCount: 1, MemoryMiB: 128},
		BootSource: firecracker.BootSource{
			KernelImagePath: os.Getenv("GUEST_KERNEL"),
			BootArgs:        "console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda ro init=/sbin/init",
		},
		RootDrive: firecracker.RootDrive{PathOnHost: os.Getenv("GUEST_ROOTFS"), ReadOnly: true},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(launcher.dir, "vmm.log"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "BRIE_SYNTHETIC_GUEST_BOOT_OK") {
			break
		}
		if o.Status().State != Running {
			t.Fatalf("VMM exited before guest marker: %+v", o.Status())
		}
		select {
		case <-ctx.Done():
			t.Fatal("guest marker not observed: ", ctx.Err())
		case <-ticker.C:
		}
	}
	info, err = client.Info(t.Context())
	if err != nil || info.State != "Running" {
		t.Fatalf("unexpected post-boot metadata: %+v, %v", info, err)
	}
	assertJournalState(t, o.journal.path, Running, BootStartAcknowledged)
	if err := o.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalState(t, o.journal.path, Exited, BootStartAcknowledged)
	t.Log("owner launched, configured, booted and reaped Firecracker; synthetic guest marker observed")
}
