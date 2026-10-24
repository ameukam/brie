// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package owner provides a single-process supervision boundary backed by a
// durable lifecycle journal and a Linux advisory fence.
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
	JournalDir       string
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

// Owner serializes lifecycle requests for one execution attempt. The journal
// prevents reuse after interruption; the advisory fence excludes concurrent
// owners on the same host.
type Owner struct {
	config       Config
	launcher     Launcher
	slot         chan struct{}
	transitionMu sync.Mutex
	mu           sync.Mutex
	status       Status
	boot         BootStatus
	process      Process
	exited       chan struct{}
	journal      *lifecycleJournal
	closed       bool
}

// New creates an idle owner, records its initial state, and acquires a
// host-local fence for the execution identity.
func New(config Config, launcher Launcher) (*Owner, error) {
	if strings.TrimSpace(config.SandboxUID) == "" || strings.TrimSpace(config.PodUID) == "" {
		return nil, errors.New("owner requires sandbox and Pod UIDs")
	}
	if config.OperationTimeout <= 0 {
		return nil, errors.New("owner operation timeout must be positive")
	}
	if config.JournalDir == "" {
		return nil, errors.New("owner requires a journal directory")
	}
	if launcher == nil {
		return nil, errors.New("owner requires a process launcher")
	}
	journal, err := openLifecycleJournal(config.JournalDir, config.SandboxUID, config.PodUID)
	if err != nil {
		return nil, fmt.Errorf("open owner journal: %w", err)
	}
	return &Owner{
		config: config, launcher: launcher, slot: make(chan struct{}, 1),
		status: Status{State: Idle}, boot: BootStatus{State: BootUnconfigured}, exited: make(chan struct{}),
		journal: journal,
	}, nil
}

// Status returns the last observed process state without waiting for mutations.
func (o *Owner) Status() Status {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.status
}

// Close releases the cross-process fence only when no process may remain.
// The journal is retained so this execution identity cannot be reused.
func (o *Owner) Close() error {
	_, cancel, err := o.acquire(context.Background())
	if err != nil {
		return fmt.Errorf("acquire owner for close: %w", err)
	}
	defer cancel()
	defer func() { <-o.slot }()
	o.transitionMu.Lock()
	defer o.transitionMu.Unlock()
	o.mu.Lock()
	state := o.status.State
	closed := o.closed
	o.mu.Unlock()
	if state != Idle && state != Exited {
		return fmt.Errorf("cannot release owner fence in state %s", state)
	}
	if closed {
		return nil
	}
	err = o.journal.close()
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	return err
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
	o.transitionMu.Lock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		o.transitionMu.Unlock()
		return errors.New("cannot start a closed owner")
	}
	if o.status.State != Idle {
		state := o.status.State
		o.mu.Unlock()
		o.transitionMu.Unlock()
		return fmt.Errorf("cannot start owner in state %s", state)
	}
	bootState := o.boot.State
	o.mu.Unlock()
	if err := o.journal.write(journalRecord{Process: Starting, Boot: bootState}); err != nil {
		err = fmt.Errorf("persist owner starting state: %w", err)
		o.mu.Lock()
		o.status = Status{State: Unknown, Err: err}
		o.mu.Unlock()
		o.transitionMu.Unlock()
		return err
	}
	o.mu.Lock()
	o.status = Status{State: Starting}
	o.mu.Unlock()
	o.transitionMu.Unlock()

	process, err := o.launcher.Start(ctx, o.config)
	if err == nil && process == nil {
		err = errors.New("launcher returned no process")
	}
	o.transitionMu.Lock()
	o.mu.Lock()
	bootState = o.boot.State
	o.mu.Unlock()
	pid := processPID(process)
	nextState := Running
	if err != nil {
		nextState = Unknown
	}
	persistErr := o.journal.write(journalRecord{Process: nextState, Boot: bootState, ProcessPID: pid})
	if err != nil {
		err = fmt.Errorf("start owned process: %w", err)
	}
	if persistErr != nil {
		persistErr = fmt.Errorf("persist owner %s state: %w", nextState, persistErr)
		err = errors.Join(err, persistErr)
	}
	o.mu.Lock()
	if process != nil {
		o.process = process
	}
	if err != nil {
		o.status = Status{State: Unknown, Err: err}
	} else {
		o.status = Status{State: Running}
	}
	o.mu.Unlock()
	o.transitionMu.Unlock()
	if process != nil {
		go o.observeExit(process)
	}
	return err
}

func (o *Owner) observeExit(process Process) {
	exitErr := process.Wait()
	if exitErr != nil {
		exitErr = fmt.Errorf("owned process exited: %w", exitErr)
	}
	o.transitionMu.Lock()
	defer o.transitionMu.Unlock()
	o.mu.Lock()
	bootState := o.boot.State
	o.mu.Unlock()
	if bootState != BootStartAcknowledged {
		bootState = BootUnknown
	}
	persistErr := o.journal.write(journalRecord{
		Process: Exited, Boot: bootState, ProcessPID: processPID(process),
	})
	if persistErr != nil {
		exitErr = errors.Join(exitErr, fmt.Errorf("persist owner exited state: %w", persistErr))
	}
	o.mu.Lock()
	o.status = Status{State: Exited, Err: exitErr}
	if bootState == BootUnknown {
		o.boot = BootStatus{State: BootUnknown, Err: exitErr}
	}
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
	o.transitionMu.Lock()
	o.mu.Lock()
	state := o.status.State
	process := o.process
	switch state {
	case Idle:
		o.mu.Unlock()
		o.transitionMu.Unlock()
		return errors.New("cannot stop owner before start")
	case Exited:
		err := o.status.Err
		o.mu.Unlock()
		o.transitionMu.Unlock()
		return err
	case Unknown:
		if process == nil {
			err := o.status.Err
			o.mu.Unlock()
			o.transitionMu.Unlock()
			return fmt.Errorf("cannot stop an unobserved process: %w", err)
		}
	case Running:
	}
	bootState := o.boot.State
	o.mu.Unlock()
	if state == Running {
		if err := o.journal.write(journalRecord{
			Process: Stopping, Boot: bootState, ProcessPID: processPID(process),
		}); err != nil {
			err = fmt.Errorf("persist owner stopping state: %w", err)
			o.mu.Lock()
			o.status = Status{State: Unknown, Err: err}
			o.mu.Unlock()
			o.transitionMu.Unlock()
			return err
		}
		o.mu.Lock()
		o.status = Status{State: Stopping}
		o.mu.Unlock()
		o.transitionMu.Unlock()
		if err := process.Stop(ctx); err != nil {
			err = fmt.Errorf("stop owned process: %w", err)
			o.transitionMu.Lock()
			o.mu.Lock()
			shouldPersist := o.status.State != Exited
			o.mu.Unlock()
			if shouldPersist {
				if persistErr := o.journal.write(journalRecord{
					Process: Unknown, Boot: bootState, ProcessPID: processPID(process),
				}); persistErr != nil {
					err = errors.Join(err, fmt.Errorf("persist owner unknown state: %w", persistErr))
				}
			}
			o.mu.Lock()
			if shouldPersist {
				o.status = Status{State: Unknown, Err: err}
			}
			o.mu.Unlock()
			o.transitionMu.Unlock()
			return err
		}
	} else {
		o.transitionMu.Unlock()
	}
	select {
	case <-o.exited:
		return o.Status().Err
	case <-ctx.Done():
		return fmt.Errorf("wait for owned process exit: %w", ctx.Err())
	}
}

type processWithPID interface {
	PID() int
}

func processPID(process Process) int {
	if processWithPID, ok := process.(processWithPID); ok {
		return processWithPID.PID()
	}
	return 0
}
