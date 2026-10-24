// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"errors"
	"fmt"

	"github.com/ameukam/brie/internal/runtime/internal/firecracker"
)

// BootConfig is the complete minimal Firecracker boot profile owned by one
// execution attempt. The owner sends all pre-boot configuration before Start.
type BootConfig struct {
	Machine    firecracker.MachineConfig
	BootSource firecracker.BootSource
	RootDrive  firecracker.RootDrive
}

// Validate checks the entire profile before the owner sends any mutation.
func (config BootConfig) Validate() error {
	if err := config.Machine.Validate(); err != nil {
		return err
	}
	if err := config.BootSource.Validate(); err != nil {
		return err
	}
	if err := config.RootDrive.Validate(); err != nil {
		return err
	}
	return nil
}

// BootState describes observed API sequencing, not guest readiness.
type BootState string

const (
	// BootUnconfigured means no Firecracker API mutation was attempted.
	BootUnconfigured BootState = "Unconfigured"
	// BootConfiguringMachine means the machine PUT may be in flight.
	BootConfiguringMachine BootState = "ConfiguringMachine"
	// BootConfiguringSource means the boot-source PUT may be in flight.
	BootConfiguringSource BootState = "ConfiguringBootSource"
	// BootConfiguringDrive means the root-drive PUT may be in flight.
	BootConfiguringDrive BootState = "ConfiguringRootDrive"
	// BootStarting means InstanceStart may be in flight.
	BootStarting BootState = "Starting"
	// BootStartAcknowledged means Firecracker acknowledged InstanceStart.
	// It does not mean the guest is running or ready.
	BootStartAcknowledged BootState = "StartAcknowledged"
	// BootUnknown means a mutation outcome is uncertain or the VMM exited.
	BootUnknown BootState = "Unknown"
)

// BootStatus is an immutable observation of the Firecracker API sequence.
type BootStatus struct {
	State BootState
	Err   error
}

// Boot serializes a single ordered sequence through this owner's Firecracker
// client. Any failure after an attempted API mutation leaves BootUnknown and
// this owner will not retry. The VMM process must already be owned and running.
func (o *Owner) Boot(ctx context.Context, client *firecracker.Client, config BootConfig) error {
	if client == nil {
		return errors.New("boot requires a Firecracker API client")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate Firecracker boot profile: %w", err)
	}
	ctx, cancel, err := o.acquire(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	defer func() { <-o.slot }()

	if err := o.beginBoot(BootUnconfigured, BootConfiguringMachine); err != nil {
		return err
	}
	if err := client.ConfigureMachine(ctx, config.Machine); err != nil {
		return o.failBoot("configure machine", err)
	}
	if err := o.advanceBoot(BootConfiguringMachine, BootConfiguringSource); err != nil {
		return err
	}
	if err := client.ConfigureBootSource(ctx, config.BootSource); err != nil {
		return o.failBoot("configure boot source", err)
	}
	if err := o.advanceBoot(BootConfiguringSource, BootConfiguringDrive); err != nil {
		return err
	}
	if err := client.ConfigureRootDrive(ctx, config.RootDrive); err != nil {
		return o.failBoot("configure root drive", err)
	}
	if err := o.advanceBoot(BootConfiguringDrive, BootStarting); err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return o.failBoot("request instance start", err)
	}
	return o.transitionBoot(BootStarting, BootStartAcknowledged)
}

// BootStatus returns the latest Firecracker API observation.
func (o *Owner) BootStatus() BootStatus {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.boot
}

func (o *Owner) beginBoot(expected, next BootState) error {
	return o.transitionBoot(expected, next)
}

func (o *Owner) transitionBoot(expected, next BootState) error {
	o.transitionMu.Lock()
	defer o.transitionMu.Unlock()
	o.mu.Lock()
	if o.status.State != Running {
		state := o.status.State
		o.mu.Unlock()
		return fmt.Errorf("cannot boot while VMM process is %s", state)
	}
	if o.boot.State != expected {
		state := o.boot.State
		o.mu.Unlock()
		return fmt.Errorf("cannot continue Firecracker boot in state %s", state)
	}
	process := o.process
	o.mu.Unlock()
	if err := o.journal.write(journalRecord{
		Process: Running, Boot: next, ProcessPID: processPID(process),
	}); err != nil {
		err = fmt.Errorf("persist Firecracker boot state %s: %w", next, err)
		o.mu.Lock()
		o.boot = BootStatus{State: BootUnknown, Err: err}
		o.mu.Unlock()
		return err
	}
	o.mu.Lock()
	o.boot = BootStatus{State: next}
	o.mu.Unlock()
	return nil
}

func (o *Owner) advanceBoot(expected, next BootState) error {
	o.mu.Lock()
	processState := o.status.State
	bootState := o.boot.State
	o.mu.Unlock()
	if processState != Running {
		return o.failBoot("advance Firecracker boot", fmt.Errorf("VMM process is %s during Firecracker boot", processState))
	}
	if bootState != expected {
		return o.failBoot("advance Firecracker boot", fmt.Errorf("firecracker boot state changed from %s to %s", expected, bootState))
	}
	return o.transitionBoot(expected, next)
}

func (o *Owner) failBoot(operation string, cause error) error {
	err := fmt.Errorf("%s: %w", operation, cause)
	o.transitionMu.Lock()
	o.mu.Lock()
	processState := o.status.State
	process := o.process
	o.mu.Unlock()
	if persistErr := o.journal.write(journalRecord{
		Process: processState, Boot: BootUnknown, ProcessPID: processPID(process),
	}); persistErr != nil {
		err = errors.Join(err, fmt.Errorf("persist unknown Firecracker boot state: %w", persistErr))
	}
	o.mu.Lock()
	o.boot = BootStatus{State: BootUnknown, Err: err}
	o.mu.Unlock()
	o.transitionMu.Unlock()
	return err
}
