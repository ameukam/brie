// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package owner provides an in-memory, single-process supervision boundary.
// It does not implement durable ownership, a journal, or a Firecracker launcher.
package owner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Config identifies one execution attempt. Reuse requires a new owner.
type Config struct {
	SandboxUID       string
	PodUID           string
	OperationTimeout time.Duration
}

// Process is a process already started by a Launcher. Wait must reap it and
// return exactly once it has exited. Stop requests termination; only Wait proves
// exit. Stop must respect its context and must not affect other processes.
type Process interface {
	Wait() error
	Stop(context.Context) error
}

// Launcher starts one process. It must respect the deadline and must not tie
// the process lifetime to the start context (for example via exec.CommandContext).
// On error it must return no Process and reconcile or reap anything it started.
// The owner cannot infer absence from a failed start and will not retry it.
type Launcher interface {
	Start(context.Context, Config) (Process, error)
}

// State describes process supervision, not VM boot or guest readiness.
type State string

const (
	// Idle means no start has been attempted.
	Idle State = "Idle"
	// Starting means the launcher is handling the single start attempt.
	Starting State = "Starting"
	// Running means a process was returned and Wait has not observed exit.
	Running State = "Running"
	// Stopping means termination was requested but exit is not yet observed.
	Stopping State = "Stopping"
	// Exited means Wait has completed.
	Exited State = "Exited"
	// Unknown means a start or stop failed without proof of exit.
	Unknown State = "Unknown"
)

// Status is an immutable observation. Err is the latest lifecycle or exit error.
type Status struct {
	State State
	Err   error
}

// Owner serializes lifecycle requests for one execution attempt. It must live
// outside the node agent; this in-memory boundary is not a cross-process fence.
type Owner struct {
	config   Config
	launcher Launcher
	slot     chan struct{}
	mu       sync.Mutex
	status   Status
	process  Process
	exited   chan struct{}
}

// New creates an idle owner without starting anything.
func New(config Config, launcher Launcher) (*Owner, error) {
	if strings.TrimSpace(config.SandboxUID) == "" || strings.TrimSpace(config.PodUID) == "" {
		return nil, errors.New("owner requires sandbox and Pod UIDs")
	}
	if config.OperationTimeout <= 0 {
		return nil, errors.New("owner operation timeout must be positive")
	}
	if launcher == nil {
		return nil, errors.New("owner requires a process launcher")
	}
	return &Owner{
		config: config, launcher: launcher, slot: make(chan struct{}, 1),
		status: Status{State: Idle}, exited: make(chan struct{}),
	}, nil
}

// Status returns the last observed process state without waiting for mutations.
func (o *Owner) Status() Status {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status
}

func (o *Owner) acquire(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, o.config.OperationTimeout)
	select {
	case o.slot <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-o.slot
			cancel()
			return nil, nil, fmt.Errorf("acquire owner operation: %w", err)
		}
		return ctx, cancel, nil
	case <-ctx.Done():
		cancel()
		return nil, nil, fmt.Errorf("wait for owner operation: %w", ctx.Err())
	}
}

// Start performs at most one launch attempt, never an automatic retry. A failed
// launch leaves Unknown; this owner cannot start again even after process exit.
func (o *Owner) Start(ctx context.Context) error {
	ctx, cancel, err := o.acquire(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer func() { <-o.slot }()
	o.mu.Lock()
	if o.status.State != Idle {
		state := o.status.State
		o.mu.Unlock()
		return fmt.Errorf("cannot start owner in state %s", state)
	}
	o.status = Status{State: Starting}
	o.mu.Unlock()
	process, err := o.launcher.Start(ctx, o.config)
	if err == nil && process == nil {
		err = errors.New("launcher returned no process")
	}
	o.mu.Lock()
	if err != nil {
		err = fmt.Errorf("start owned process: %w", err)
		o.status = Status{State: Unknown, Err: err}
		o.mu.Unlock()
		return err
	}
	o.process = process
	o.status = Status{State: Running}
	o.mu.Unlock()
	go o.observeExit(process)
	return nil
}

func (o *Owner) observeExit(process Process) {
	err := process.Wait()
	if err != nil {
		err = fmt.Errorf("owned process exited: %w", err)
	}
	o.mu.Lock()
	o.status = Status{State: Exited, Err: err}
	close(o.exited)
	o.mu.Unlock()
}

// Stop requests termination once, then waits for observed exit. An uncertain
// stop is never automatically repeated; another call waits for observation.
// It never deletes resources or treats cancellation as proof of process exit.
func (o *Owner) Stop(ctx context.Context) error {
	ctx, cancel, err := o.acquire(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer func() { <-o.slot }()
	o.mu.Lock()
	state := o.status.State
	process := o.process
	switch state {
	case Idle:
		o.mu.Unlock()
		return errors.New("cannot stop owner before start")
	case Exited:
		err := o.status.Err
		o.mu.Unlock()
		return err
	case Unknown:
		if process == nil {
			err := o.status.Err
			o.mu.Unlock()
			return fmt.Errorf("cannot stop an unobserved process: %w", err)
		}
	case Running:
		o.status = Status{State: Stopping}
	}
	o.mu.Unlock()
	if state == Running {
		if err := process.Stop(ctx); err != nil {
			err = fmt.Errorf("stop owned process: %w", err)
			o.mu.Lock()
			if o.status.State != Exited {
				o.status = Status{State: Unknown, Err: err}
			}
			o.mu.Unlock()
			return err
		}
	}
	select {
	case <-o.exited:
		return o.Status().Err
	case <-ctx.Done():
		return fmt.Errorf("wait for owned process exit: %w", ctx.Err())
	}
}
