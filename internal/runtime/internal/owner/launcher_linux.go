// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

// FirecrackerLauncher starts a foreground VMM for a disposable host experiment.
// It does not provide a jailer, tenant isolation, or owner restart recovery.
type FirecrackerLauncher struct {
	binary string
	dir    string
}

// NewFirecrackerLauncher requires an executable and a fresh runtime directory.
// Start creates the directory; it never reuses or removes existing resources.
func NewFirecrackerLauncher(binary, dir string) (*FirecrackerLauncher, error) {
	if !filepath.IsAbs(binary) || strings.ContainsRune(binary, '\x00') {
		return nil, errors.New("firecracker executable path must be absolute without NUL bytes")
	}
	if !filepath.IsAbs(dir) || strings.ContainsRune(dir, '\x00') {
		return nil, errors.New("firecracker runtime directory must be absolute without NUL bytes")
	}
	dir = filepath.Clean(dir)
	if len(filepath.Join(dir, "api.sock")) > 107 {
		return nil, errors.New("firecracker API socket path exceeds the Linux Unix-socket limit")
	}
	info, err := os.Lstat(binary)
	if err != nil {
		return nil, fmt.Errorf("inspect Firecracker executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("firecracker executable must be an executable regular file, not a symlink")
	}
	return &FirecrackerLauncher{binary: binary, dir: dir}, nil
}

// SocketPath is the API endpoint created by this launch attempt.
func (l *FirecrackerLauncher) SocketPath() string {
	return filepath.Join(l.dir, "api.sock")
}

// Start waits for matching pre-boot API metadata, without configuring the VM.
// Cancellation limits startup only; the owner must explicitly stop the VMM.
func (l *FirecrackerLauncher) Start(ctx context.Context, config Config) (Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("start Firecracker: %w", err)
	}
	if config.OperationTimeout <= 0 {
		return nil, errors.New("firecracker startup requires a positive operation timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	if err := os.Mkdir(l.dir, 0o700); err != nil {
		return nil, fmt.Errorf("create fresh Firecracker runtime directory: %w", err)
	}
	log, err := os.OpenFile(filepath.Join(l.dir, "vmm.log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create Firecracker log: %w", err)
	}
	identity := executionIdentity(config.SandboxUID, config.PodUID)
	cmd := exec.Command(l.binary, "--api-sock", l.SocketPath(), "--id", identity)
	cmd.Dir = l.dir
	cmd.Env = []string{}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		return nil, errors.Join(fmt.Errorf("launch Firecracker: %w", err), log.Close())
	}
	process := &firecrackerProcess{cmd: cmd, done: make(chan struct{})}
	go process.reap()
	if err := log.Close(); err != nil {
		return nil, process.abort(fmt.Errorf("close parent Firecracker log: %w", err))
	}
	if err := l.waitReady(ctx, identity, process); err != nil {
		return nil, process.abort(err)
	}
	return process, nil
}

func (l *FirecrackerLauncher) waitReady(ctx context.Context, identity string, process *firecrackerProcess) error {
	client, err := firecracker.New(l.SocketPath(), time.Second)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		info, err := client.Info(ctx)
		if err == nil {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("wait for Firecracker API: %w", err)
			}
			if info.ID != identity || info.AppName != "Firecracker" || info.State != "Not started" {
				return fmt.Errorf("unexpected Firecracker startup metadata: id=%q app=%q state=%q", info.ID, info.AppName, info.State)
			}
			return nil
		}
		lastErr = err
		select {
		case <-process.done:
			return errors.Join(errors.New("firecracker exited before API readiness"), process.Wait())
		case <-ctx.Done():
			return errors.Join(fmt.Errorf("wait for Firecracker API: %w", ctx.Err()), lastErr)
		case <-ticker.C:
		}
	}
}

type firecrackerProcess struct {
	cmd      *exec.Cmd
	done     chan struct{}
	mu       sync.Mutex
	stopping bool
	err      error
}

func (p *firecrackerProcess) PID() int { return p.cmd.Process.Pid }

func (p *firecrackerProcess) reap() {
	err := p.cmd.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	var exitErr *exec.ExitError
	if p.stopping && errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
			err = nil
		}
	}
	p.err = err
	close(p.done)
}

func (p *firecrackerProcess) Wait() error {
	<-p.done
	return p.err
}

// Stop terminates only the owned foreground process. This is not guest shutdown.
func (p *firecrackerProcess) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return fmt.Errorf("kill owned Firecracker process: %w", err)
	}
	p.stopping = true
	return nil
}

func (p *firecrackerProcess) abort(cause error) error {
	stopErr := p.Stop(context.Background())
	return errors.Join(cause, stopErr, p.Wait())
}
