// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/ameukam/brie/internal/execinventory"
	corev1 "k8s.io/api/core/v1"
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
	// Admission receives each published admission decision of DESIGN 7.2:
	// AdmissionAdmitted once the reserved record is committed, otherwise the
	// refusal reason and message published on the Pod.
	Admission func(pod *corev1.Pod, reason, message string)
	// Journal receives the journal size and reservations after each durable save.
	Journal func(JournalState)
	// ImagePullFailed receives a failed image pull for a waiting Pod with the
	// gRPC code name; backoff is false for the first report of a failure.
	ImagePullFailed func(pod *corev1.Pod, backoff bool, code string)
	// ExecutionBlocked receives each newly published ExecutionBlocked condition.
	ExecutionBlocked func(pod *corev1.Pod, reason, message string)
	// CleanupBlocked receives cleanup that waits for positive deletion or
	// runtime evidence. For a deleted Pod, pod carries only the journaled
	// namespace, name, and UID.
	CleanupBlocked func(pod *corev1.Pod, message string)
	// ProbeResult receives each probe outcome: kind, handler, and result.
	ProbeResult func(kind, handler, result string)
	// LivenessFailed receives a Pod published Failed after its liveness probe failed.
	LivenessFailed func(pod *corev1.Pod, message string)
	// Inventory receives, after each pass that read the runtime, the
	// executions whose journal record, owner labels, and runtime objects agree.
	// Foreign, ambiguous, and busy executions are omitted.
	Inventory func([]execinventory.Execution)
}

// AdmissionAdmitted is the admission decision for a reserved Pod.
const AdmissionAdmitted = "Admitted"

// JournalState is the execution journal after a durable save. Reservations
// count unfinished records, including pending and unknown executions.
type JournalState struct {
	Records, Bytes        int
	ReservedCPUMillicores int64
	ReservedMemoryBytes   int64
	ReservedExecutions    int
	// PendingSince maps each pending stage that holds a record to the time its
	// oldest record entered it. A zero time means an entry time is unknown.
	PendingSince map[string]time.Time
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

func (h ExecutionHooks) admission(pod *corev1.Pod, reason, message string) {
	if h.Admission != nil {
		h.Admission(pod, reason, message)
	}
}

func (h ExecutionHooks) imagePullFailed(pod *corev1.Pod, backoff bool, code string) {
	if h.ImagePullFailed != nil {
		h.ImagePullFailed(pod, backoff, code)
	}
}

func (h ExecutionHooks) executionBlocked(pod *corev1.Pod, reason, message string) {
	if h.ExecutionBlocked != nil {
		h.ExecutionBlocked(pod, reason, message)
	}
}

func (h ExecutionHooks) cleanupBlocked(pod *corev1.Pod, message string) {
	if h.CleanupBlocked != nil {
		h.CleanupBlocked(pod, message)
	}
}

func (h ExecutionHooks) livenessFailed(pod *corev1.Pod, message string) {
	if h.LivenessFailed != nil {
		h.LivenessFailed(pod, message)
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
