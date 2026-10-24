// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type launchFunc func(context.Context, Config) (Process, error)

func (f launchFunc) Start(ctx context.Context, config Config) (Process, error) {
	return f(ctx, config)
}

type fakeProcess struct {
	result    chan error
	once      sync.Once
	waitCalls atomic.Int32
	stopCalls atomic.Int32
	stop      func(context.Context) error
}

func newProcess(t *testing.T) *fakeProcess {
	t.Helper()
	p := &fakeProcess{result: make(chan error, 1)}
	t.Cleanup(func() { p.exit(nil) })
	return p
}

func (p *fakeProcess) exit(err error) { p.once.Do(func() { p.result <- err }) }

func (p *fakeProcess) Wait() error {
	p.waitCalls.Add(1)
	return <-p.result
}

func (p *fakeProcess) Stop(ctx context.Context) error {
	p.stopCalls.Add(1)
	if p.stop != nil {
		return p.stop(ctx)
	}
	p.exit(nil)
	return nil
}

func testConfig() Config {
	return Config{SandboxUID: "sandbox-1", PodUID: "pod-1", OperationTimeout: time.Second}
}

func newOwner(t *testing.T, launch Launcher) *Owner {
	t.Helper()
	config := testConfig()
	config.JournalDir = filepath.Join(t.TempDir(), "journal")
	o, err := New(config, launch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		state := o.Status().State
		if state == Idle || state == Exited {
			if err := o.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return o
}

func TestConfig(t *testing.T) {
	t.Parallel()
	valid := testConfig()
	valid.JournalDir = filepath.Join(t.TempDir(), "journal")
	missingSandbox := valid
	missingSandbox.SandboxUID = ""
	blankPod := valid
	blankPod.PodUID = " \n"
	zeroTimeout := valid
	zeroTimeout.OperationTimeout = 0
	negativeTimeout := valid
	negativeTimeout.OperationTimeout = -time.Second
	missingJournal := valid
	missingJournal.JournalDir = ""
	for _, tt := range []struct {
		name   string
		config Config
		nilRun bool
	}{
		{name: "missing sandbox", config: missingSandbox},
		{name: "blank Pod", config: blankPod},
		{name: "zero timeout", config: zeroTimeout},
		{name: "negative timeout", config: negativeTimeout},
		{name: "missing journal", config: missingJournal},
		{name: "missing launcher", config: valid, nilRun: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var launch Launcher = launchFunc(func(context.Context, Config) (Process, error) {
				t.Fatal("validation launched a process")
				return nil, nil
			})
			if tt.nilRun {
				launch = nil
			}
			if _, err := New(tt.config, launch); err == nil {
				t.Fatal("expected invalid configuration")
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newProcess(t)
		starts := 0
		o := newOwner(t, launchFunc(func(_ context.Context, config Config) (Process, error) {
			starts++
			expected := testConfig()
			if config.SandboxUID != expected.SandboxUID || config.PodUID != expected.PodUID ||
				config.OperationTimeout != expected.OperationTimeout || config.JournalDir == "" {
				t.Errorf("launcher config = %+v", config)
			}
			return p, nil
		}))
		if o.Status().State != Idle {
			t.Fatal("new owner is not idle")
		}
		if err := o.Stop(t.Context()); err == nil {
			t.Fatal("stop before start succeeded")
		}
		if err := o.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if o.Status().State != Running {
			t.Fatalf("status = %+v", o.Status())
		}
		if err := o.Start(t.Context()); err == nil {
			t.Fatal("second start succeeded")
		}
		if err := o.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := o.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := o.Start(t.Context()); err == nil {
			t.Fatal("restart after exit succeeded")
		}
		if o.Status().State != Exited || starts != 1 || p.waitCalls.Load() != 1 || p.stopCalls.Load() != 1 {
			t.Fatalf("status=%+v starts=%d waits=%d stops=%d", o.Status(), starts, p.waitCalls.Load(), p.stopCalls.Load())
		}
	})
}

func TestStartFailure(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "launch error", err: errors.New("launch outcome uncertain")},
		{name: "missing process"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			starts := 0
			o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
				starts++
				return nil, tt.err
			}))
			err := o.Start(t.Context())
			if err == nil || (tt.err != nil && !errors.Is(err, tt.err)) {
				t.Fatalf("Start() = %v, want launch failure", err)
			}
			if o.Status().State != Unknown || o.Status().Err == nil {
				t.Fatalf("failed launch status = %+v", o.Status())
			}
			assertJournalState(t, o.journal.path, Unknown, BootUnconfigured)
			if err := o.Start(t.Context()); err == nil {
				t.Fatal("retried failed launch")
			}
			if err := o.Stop(t.Context()); err == nil {
				t.Fatal("unknown process was treated as stopped")
			}
			if starts != 1 {
				t.Fatalf("launch attempts = %d", starts)
			}
		})
	}
}

func TestUnexpectedExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newProcess(t)
		o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) { return p, nil }))
		if err := o.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		crash := errors.New("process crashed")
		p.exit(crash)
		synctest.Wait()
		status := o.Status()
		if status.State != Exited || !errors.Is(status.Err, crash) {
			t.Fatalf("status = %+v", status)
		}
		if err := o.Stop(t.Context()); !errors.Is(err, crash) {
			t.Fatalf("Stop() = %v, want crash", err)
		}
		if p.stopCalls.Load() != 0 {
			t.Fatal("sent a termination request after observed exit")
		}
	})
}

func TestCanceledCallsDoNotMutate(t *testing.T) {
	o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
		t.Fatal("cancelled call launched a process")
		return nil, nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, operation := range []func(context.Context) error{o.Start, o.Stop} {
		if err := operation(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call = %v", err)
		}
	}
	if o.Status().State != Idle {
		t.Fatalf("cancelled operation changed status: %+v", o.Status())
	}
}

func TestClosePreventsStartAfterReleasingFence(t *testing.T) {
	starts := 0
	o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
		starts++
		return nil, errors.New("unexpected launch")
	}))
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.Start(t.Context()); err == nil {
		t.Fatal("closed owner started")
	}
	if starts != 0 {
		t.Fatalf("closed owner launch attempts = %d", starts)
	}
}

func TestSerializationAndQueueDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := newOwner(t, launchFunc(func(ctx context.Context, _ Config) (Process, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}))
		done := make(chan error, 1)
		go func() { done <- o.Start(t.Context()) }()
		synctest.Wait()
		if o.Status().State != Starting {
			t.Fatalf("status read blocked or wrong: %+v", o.Status())
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		err := o.Stop(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for owner operation") {
			t.Fatalf("queued Stop() = %v", err)
		}
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Start() = %v", err)
		}
		if o.Status().State != Unknown {
			t.Fatalf("timed-out launch = %+v", o.Status())
		}
	})
}

func TestStopUncertainty(t *testing.T) {
	for _, tt := range []struct {
		name      string
		failStop  bool
		blockStop bool
		wantState State
	}{
		{name: "exit not yet observed", wantState: Stopping},
		{name: "stop reply lost", failStop: true, wantState: Unknown},
		{name: "stop deadline", blockStop: true, wantState: Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newProcess(t)
				stopErr := errors.New("stop reply lost")
				p.stop = func(ctx context.Context) error {
					if tt.failStop {
						return stopErr
					}
					if tt.blockStop {
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				}
				o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) { return p, nil }))
				if err := o.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				err := o.Stop(t.Context())
				wantErr := error(context.DeadlineExceeded)
				if tt.failStop {
					wantErr = stopErr
				}
				if !errors.Is(err, wantErr) || o.Status().State != tt.wantState {
					t.Fatalf("Stop() = %v, status=%+v", err, o.Status())
				}
				assertJournalState(t, o.journal.path, tt.wantState, BootUnconfigured)
				if err := o.Stop(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("second Stop() = %v", err)
				}
				if p.stopCalls.Load() != 1 {
					t.Fatal("uncertain stop was replayed")
				}
				p.exit(nil)
				synctest.Wait()
				if err := o.Stop(t.Context()); err != nil || o.Status().State != Exited {
					t.Fatalf("observed exit: err=%v status=%+v", err, o.Status())
				}
				assertJournalState(t, o.journal.path, Exited, BootUnknown)
			})
		})
	}
}

func TestStartContextDoesNotOwnLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newProcess(t)
		o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) { return p, nil }))
		ctx, cancel := context.WithCancel(t.Context())
		if err := o.Start(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if o.Status().State != Running || p.stopCalls.Load() != 0 {
			t.Fatalf("start deadline or cancellation terminated process: %+v", o.Status())
		}
		if err := o.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConcurrentLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newProcess(t)
		var launches atomic.Int32
		o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) {
			launches.Add(1)
			return p, nil
		}))
		results := make(chan error, 16)
		for range cap(results) {
			go func() { results <- o.Start(t.Context()) }()
		}
		successes := 0
		for range cap(results) {
			if err := <-results; err == nil {
				successes++
			}
		}
		if successes != 1 || launches.Load() != 1 {
			t.Fatalf("successful starts=%d launches=%d", successes, launches.Load())
		}
		for range cap(results) {
			go func() { results <- o.Stop(t.Context()) }()
		}
		for range cap(results) {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if o.Status().State != Exited || p.stopCalls.Load() != 1 || p.waitCalls.Load() != 1 {
			t.Fatalf("status=%+v stops=%d waits=%d", o.Status(), p.stopCalls.Load(), p.waitCalls.Load())
		}
	})
}

func TestExitDuringFailedStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newProcess(t)
		stopErr := errors.New("stop reply lost")
		p.stop = func(context.Context) error {
			p.exit(nil)
			synctest.Wait()
			return stopErr
		}
		o := newOwner(t, launchFunc(func(context.Context, Config) (Process, error) { return p, nil }))
		if err := o.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := o.Stop(t.Context()); !errors.Is(err, stopErr) {
			t.Fatalf("Stop() = %v", err)
		}
		if o.Status().State != Exited || o.Status().Err != nil {
			t.Fatalf("failed stop overwrote observed exit: %+v", o.Status())
		}
	})
}
