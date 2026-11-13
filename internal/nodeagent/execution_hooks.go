// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/execinventory"
	"k8s.io/client-go/rest"
)

// ExecutionHooks observe the executor without changing its decisions. Nil
// fields are skipped. Hooks run on executor loops and must not block.
type ExecutionHooks struct {
	// CRI receives every CRI call of the executor's workload and health clients.
	CRI cri.Observer
	// LeaseRenewal receives each Lease renewal result.
	LeaseRenewal func(err error)
	// Pass receives each reconciliation pass result and its end time.
	Pass func(err error, at time.Time)
	// Health receives the inputs of the Node readiness gate after each health update.
	Health func(HealthState)
	// Inventory receives, after each pass that read the runtime, the
	// executions whose journal record, owner labels, and runtime objects agree.
	// Foreign and ambiguous executions are omitted.
	Inventory func([]execinventory.Execution)
}

// HealthState is the admission barrier input: new Pods are admitted and
// allocatable is published only while RuntimeHealthy and Synced are true and
// RecoveryRequired is false.
type HealthState struct {
	RuntimeHealthy, Synced, RecoveryRequired bool
}

func (h ExecutionHooks) observeCRI(clients ...*cri.Client) {
	if h.CRI == nil {
		return
	}
	for _, client := range clients {
		client.SetObserver(h.CRI)
	}
}

func (h ExecutionHooks) leaseRenewed(err error) {
	if h.LeaseRenewal != nil {
		h.LeaseRenewal(err)
	}
}

func (h ExecutionHooks) passFinished(err error) {
	if h.Pass != nil {
		h.Pass(err, time.Now())
	}
}

func (h ExecutionHooks) inventoryVerified(executions []execinventory.Execution) {
	if h.Inventory != nil {
		h.Inventory(executions)
	}
}

func (h ExecutionHooks) healthUpdated(state HealthState) {
	if h.Health != nil {
		h.Health(state)
	}
}

// APIConfig loads the agent's kubeconfig with the same restrictions as
// registration (HTTPS, verified TLS, static credentials) for the serving
// endpoint's reviews and the Events recorder.
func APIConfig(kubeconfig, userAgent string) (*rest.Config, error) {
	config, err := registrationConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	config.UserAgent = userAgent
	return config, nil
}
