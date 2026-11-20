// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package logrotation rotates CRI container logs by size and removes Pod log
// directories of journal-finished Pods (DESIGN 12.3). Rotation renames the
// current file and asks the runtime to reopen it through CRI
// ReopenContainerLog; it never touches a VMM or a container's processes.
// File operations stay inside the Pod's own log directory and never follow
// symlinks.
package logrotation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ameukam/brie/internal/podlog"
	"k8s.io/apimachinery/pkg/api/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Bounds of the rotation configuration.
const (
	MinMaxSize  = 1024 * 1024
	MaxMaxSize  = 10 * 1024 * 1024 * 1024
	MinMaxFiles = 2
	MaxMaxFiles = 100
	// timestampFormat suffixes rotated files, as kubelet does.
	timestampFormat = "20060102-150405"
)

// executionOwnerLabel marks sandboxes the node agent created.
const executionOwnerLabel = "brie.dev/execution-owner"

// ParseMaxSize parses a size such as "10Mi" and checks its bounds.
func ParseMaxSize(value string) (int64, error) {
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, errors.New("container log max size must be a quantity such as 10Mi")
	}
	size, exact := quantity.AsInt64()
	if !exact || size < MinMaxSize || size > MaxMaxSize {
		return 0, fmt.Errorf("container log max size must be between %d and %d bytes", int64(MinMaxSize), int64(MaxMaxSize))
	}
	return size, nil
}

// Runtime is the CRI subset rotation uses. *cri.Client implements it.
type Runtime interface {
	ListPodSandbox(context.Context) ([]*runtimeapi.PodSandbox, error)
	ListContainers(context.Context) ([]*runtimeapi.Container, error)
	ContainerStatus(context.Context, string) (*runtimeapi.ContainerStatus, error)
	ReopenContainerLog(context.Context, string) error
}

// FinishedPods reports UIDs whose journal records are finished: their
// sandboxes were removed through CRI. Only these log directories are removed.
type FinishedPods func(context.Context) ([]string, error)

// Config is the rotation policy.
type Config struct {
	LogDir         string
	RuntimeHandler string
	// MaxSize rotates a log larger than this many bytes.
	MaxSize int64
	// MaxFiles keeps at most this many files per container, including the current one.
	MaxFiles int
	// Interval between passes. Default ten seconds.
	Interval time.Duration
}

// Rotator applies the policy to running containers of owned sandboxes.
type Rotator struct {
	config   Config
	runtime  Runtime
	finished FinishedPods
	observe  func(string)
	now      func() time.Time
}

// New validates the policy. finished may be nil, which disables directory
// cleanup. observe receives "rotated", "reopen_failed", or "failed".
func New(config Config, runtime Runtime, finished FinishedPods, observe func(string)) (*Rotator, error) {
	if !filepath.IsAbs(config.LogDir) || filepath.Clean(config.LogDir) == "/" || config.RuntimeHandler == "" {
		return nil, errors.New("log rotation requires an absolute non-root log directory and a runtime handler")
	}
	if config.MaxSize < MinMaxSize || config.MaxSize > MaxMaxSize {
		return nil, errors.New("container log max size is out of bounds")
	}
	if config.MaxFiles < MinMaxFiles || config.MaxFiles > MaxMaxFiles {
		return nil, fmt.Errorf("container log max files must be between %d and %d", MinMaxFiles, MaxMaxFiles)
	}
	if runtime == nil {
		return nil, errors.New("log rotation requires a runtime client")
	}
	if config.Interval <= 0 {
		config.Interval = 10 * time.Second
	}
	if observe == nil {
		observe = func(string) {}
	}
	return &Rotator{config: config, runtime: runtime, finished: finished, observe: observe, now: time.Now}, nil
}

// Run rotates and cleans up every interval until ctx is cancelled. Pass
// errors are reported through report and do not stop the loop.
func (r *Rotator) Run(ctx context.Context, report func(error)) error {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		if err := errors.Join(r.RotateOnce(ctx), r.CleanupOnce(ctx)); err != nil && ctx.Err() == nil && report != nil {
			report(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Rotator) ownedSandboxes(ctx context.Context) (map[string]*runtimeapi.PodSandbox, error) {
	sandboxes, err := r.runtime.ListPodSandbox(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sandboxes: %w", err)
	}
	owned := make(map[string]*runtimeapi.PodSandbox)
	for _, sandbox := range sandboxes {
		if sandbox.GetLabels()[executionOwnerLabel] != "" && sandbox.GetRuntimeHandler() == r.config.RuntimeHandler {
			owned[sandbox.GetId()] = sandbox
		}
	}
	return owned, nil
}

// RotateOnce rotates the log of every running owned container above MaxSize.
func (r *Rotator) RotateOnce(ctx context.Context) error {
	owned, err := r.ownedSandboxes(ctx)
	if err != nil {
		return err
	}
	containers, err := r.runtime.ListContainers(ctx)
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}
	var failures []error
	for _, container := range containers {
		sandbox, found := owned[container.GetPodSandboxId()]
		if !found || container.GetState() != runtimeapi.ContainerState_CONTAINER_RUNNING {
			continue
		}
		if err := r.rotateContainer(ctx, sandbox.GetMetadata().GetUid(), container.GetId()); err != nil {
			failures = append(failures, fmt.Errorf("container %s: %w", container.GetId(), err))
		}
	}
	return errors.Join(failures...)
}

func (r *Rotator) rotateContainer(ctx context.Context, podUID, containerID string) error {
	status, err := r.runtime.ContainerStatus(ctx, containerID)
	if err != nil {
		return err
	}
	dir := podlog.Directory(r.config.LogDir, podUID)
	logPath := status.GetLogPath()
	if logPath == "" || filepath.Clean(logPath) != logPath || filepath.Dir(logPath) != dir {
		r.observe("failed")
		return errors.New("container log path is outside the Pod log directory")
	}
	base := filepath.Base(logPath)
	root, err := openDirectory(dir)
	if err != nil {
		r.observe("failed")
		return err
	}
	defer func() { _ = root.Close() }()
	info, err := root.Lstat(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		r.observe("failed")
		return errors.New("container log is not a regular file")
	}
	if info.Size() <= r.config.MaxSize {
		return nil
	}
	if err := r.pruneRotated(root, base, r.config.MaxFiles-2); err != nil {
		r.observe("failed")
		return err
	}
	rotated := base + "." + r.now().UTC().Format(timestampFormat)
	if _, err := root.Lstat(rotated); err == nil {
		// Rotated within the same second; the next pass retries.
		return nil
	}
	if err := root.Rename(base, rotated); err != nil {
		r.observe("failed")
		return fmt.Errorf("rename container log: %w", err)
	}
	if err := r.runtime.ReopenContainerLog(ctx, containerID); err != nil {
		r.observe("reopen_failed")
		return errors.Join(err, restoreName(root, rotated, base))
	}
	r.observe("rotated")
	return nil
}

// restoreName gives the rotated file its original name again after a failed
// reopen. A reopen error is ambiguous: the runtime may have reopened the log
// and lost the reply. The name is therefore restored with a hard link, which
// never replaces an existing file; when the runtime already created a new
// current file, both files are kept.
func restoreName(root *os.Root, rotated, base string) error {
	if err := root.Link(rotated, base); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("the runtime created a new container log despite the reopen error; both files were kept")
		}
		return fmt.Errorf("restore container log name: %w", err)
	}
	if err := root.Remove(rotated); err != nil {
		return fmt.Errorf("remove rotated container log link: %w", err)
	}
	return nil
}

// pruneRotated removes the oldest rotated files of base until at most keep remain.
func (r *Rotator) pruneRotated(root *os.Root, base string, keep int) error {
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("read Pod log directory: %w", err)
	}
	names, err := directory.Readdirnames(-1)
	_ = directory.Close()
	if err != nil {
		return fmt.Errorf("read Pod log directory: %w", err)
	}
	var rotated []string
	for _, name := range names {
		suffix, found := strings.CutPrefix(name, base+".")
		if !found {
			continue
		}
		if _, err := time.Parse(timestampFormat, suffix); err == nil {
			rotated = append(rotated, name)
		}
	}
	slices.Sort(rotated)
	for len(rotated) > keep {
		if err := root.Remove(rotated[0]); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove rotated log: %w", err)
		}
		rotated = rotated[1:]
	}
	return nil
}

func openDirectory(dir string) (*os.Root, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect Pod log directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("the Pod log directory is not a directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open Pod log directory: %w", err)
	}
	return root, nil
}

// CleanupOnce removes the log directories of journal-finished Pods. A
// directory is kept while any sandbox for that UID remains in the runtime
// inventory, and directories of unknown Pods are never touched.
func (r *Rotator) CleanupOnce(ctx context.Context) error {
	if r.finished == nil {
		return nil
	}
	finished, err := r.finished(ctx)
	if err != nil {
		return fmt.Errorf("read finished executions: %w", err)
	}
	if len(finished) == 0 {
		return nil
	}
	sandboxes, err := r.runtime.ListPodSandbox(ctx)
	if err != nil {
		return fmt.Errorf("list sandboxes: %w", err)
	}
	present := make(map[string]bool)
	for _, sandbox := range sandboxes {
		present[sandbox.GetMetadata().GetUid()] = true
	}
	root, err := openDirectory(r.config.LogDir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	var failures []error
	for _, uid := range finished {
		if uid == "" || present[uid] {
			continue
		}
		name := filepath.Base(podlog.Directory(r.config.LogDir, uid))
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.IsDir() {
			failures = append(failures, errors.New("finished Pod log path is not a directory"))
			continue
		}
		if err := root.RemoveAll(name); err != nil {
			failures = append(failures, fmt.Errorf("remove finished Pod log directory: %w", err))
		}
	}
	return errors.Join(failures...)
}
