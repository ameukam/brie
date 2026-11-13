// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package metrics defines the node agent's Prometheus metrics (DESIGN 12.3).
// Every label takes values from a fixed set, so cardinality is bounded and no
// Pod name, namespace, UID, image, or other tenant data becomes a label.
// Ages are computed at scrape time; a signal that was never observed has no
// sample or reports NaN, never zero.
package metrics

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const namespace = "brie_node"

// Admission reasons, matching the published Pod status reasons of DESIGN 7.2.
const (
	ReasonAdmitted                     = "Admitted"
	ReasonUnsupportedPod               = "UnsupportedPod"
	ReasonRuntimeClassOverheadMismatch = "RuntimeClassOverheadMismatch"
	ReasonUnsupportedResources         = "UnsupportedResources"
	ReasonInsufficientResources        = "InsufficientResources"
	// ReasonOther replaces any reason outside this set.
	ReasonOther = "Other"
)

var admissionReasons = []string{
	ReasonAdmitted, ReasonUnsupportedPod, ReasonRuntimeClassOverheadMismatch,
	ReasonUnsupportedResources, ReasonInsufficientResources, ReasonOther,
}

// BarrierCause names why the node-level admission barrier is raised.
type BarrierCause string

// Barrier causes (DESIGN 6.4). BarrierNone lowers the barrier.
const (
	BarrierNone                BarrierCause = ""
	BarrierIncompletePass      BarrierCause = "IncompletePass"
	BarrierUnresolvedExecution BarrierCause = "UnresolvedExecution"
	BarrierRuntimeUnhealthy    BarrierCause = "RuntimeUnhealthy"
	BarrierRecoveryRequired    BarrierCause = "RecoveryRequired"
	BarrierOther               BarrierCause = "Other"
)

var barrierCauses = []BarrierCause{
	BarrierIncompletePass, BarrierUnresolvedExecution, BarrierRuntimeUnhealthy, BarrierRecoveryRequired, BarrierOther,
}

// PendingStages are the journal stages committed before a CRI call.
var PendingStages = []string{"reserved", "sandbox-pending", "container-pending", "start-pending"}

// criMethods bounds the method label; other methods are reported as "other".
var criMethods = map[string]bool{
	"Version": true, "Status": true, "UpdateRuntimeConfig": true,
	"ListPodSandbox": true, "PodSandboxStatus": true, "RunPodSandbox": true, "StopPodSandbox": true, "RemovePodSandbox": true,
	"ListContainers": true, "ContainerStatus": true, "CreateContainer": true, "StartContainer": true,
	"StopContainer": true, "RemoveContainer": true, "ReopenContainerLog": true,
	"ExecSync": true, "Exec": true, "Attach": true, "PortForward": true,
	"ImageStatus": true, "PullImage": true,
}

// Metrics is the agent's registry. Methods are safe for concurrent use.
type Metrics struct {
	registry *prometheus.Registry
	now      func() time.Time

	admissions    *prometheus.CounterVec
	reservations  *observedGauges
	budget        *observedGauges
	barrier       *prometheus.GaugeVec
	criDuration   *prometheus.HistogramVec
	criErrors     *prometheus.CounterVec
	pullDuration  prometheus.Histogram
	pullFailures  prometheus.Counter
	journal       *observedGauges
	leaseFailures prometheus.Counter
	probeResults  *prometheus.CounterVec
	logRotations  *prometheus.CounterVec
	events        *prometheus.CounterVec

	mu            sync.Mutex
	lastPass      time.Time
	pendingOldest map[string]time.Time
}

// New creates a registry with the agent metrics plus Go runtime and process
// collectors. now overrides the clock for tests; nil uses time.Now.
func New(now func() time.Time) *Metrics {
	if now == nil {
		now = time.Now
	}
	m := &Metrics{registry: prometheus.NewRegistry(), now: now, pendingOldest: make(map[string]time.Time)}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help})
	}
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: help})
	}
	m.admissions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "admission_decisions_total", Help: "Pod admission decisions by published reason.",
	}, []string{"reason"})
	m.reservations = &observedGauges{registry: m.registry, gauges: []prometheus.Gauge{
		gauge("reserved_cpu_cores", "CPU reserved by unfinished executions, including overhead. No sample until observed."),
		gauge("reserved_memory_bytes", "Memory reserved by unfinished executions, including overhead. No sample until observed."),
		gauge("reserved_executions", "Unfinished executions holding a reservation. No sample until observed."),
	}}
	m.budget = &observedGauges{registry: m.registry, gauges: []prometheus.Gauge{
		gauge("budget_cpu_cores", "Configured schedulable CPU budget. No sample until set."),
		gauge("budget_memory_bytes", "Configured schedulable memory budget. No sample until set."),
		gauge("budget_executions", "Configured maximum admitted Pods. No sample until set."),
	}}
	m.barrier = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace, Name: "admission_barrier",
		Help: "1 for the cause of a raised admission barrier, 0 otherwise. No samples until observed.",
	}, []string{"cause"})
	m.criDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "cri_request_duration_seconds", Help: "CRI call latency by method.",
		Buckets: []float64{.005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"method"})
	m.criErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "cri_request_errors_total", Help: "CRI call errors by method and gRPC code.",
	}, []string{"method", "code"})
	m.pullDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace, Name: "image_pull_duration_seconds", Help: "CRI PullImage duration, including failures.",
		Buckets: []float64{.1, .5, 1, 2.5, 5, 10, 30, 60, 120},
	})
	m.pullFailures = counter("image_pull_failures_total", "Failed CRI PullImage calls.")
	m.journal = &observedGauges{registry: m.registry, gauges: []prometheus.Gauge{
		gauge("journal_records", "Execution journal records. No sample until observed."),
		gauge("journal_bytes", "Execution journal encoded size. No sample until observed."),
		gauge("journal_records_limit", "Execution journal record bound. No sample until the journal is observed."),
		gauge("journal_bytes_limit", "Execution journal size bound. No sample until the journal is observed."),
	}}
	m.leaseFailures = counter("lease_renewal_failures_total", "Failed node Lease renewals.")
	m.probeResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "probe_results_total", Help: "Probe results by probe kind, handler, and result.",
	}, []string{"probe", "handler", "result"})
	m.logRotations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "container_log_rotations_total", Help: "Container log rotation attempts by result.",
	}, []string{"result"})
	m.events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "events_total", Help: "Kubernetes Events by result after deduplication and rate limits.",
	}, []string{"result"})
	m.registry.MustRegister(
		m.admissions, m.barrier, m.criDuration, m.criErrors, m.pullDuration, m.pullFailures, m.leaseFailures,
		m.probeResults, m.logRotations, m.events, &ageCollector{m: m},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Gatherer returns the registry served at /metrics.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.registry }

// AdmissionDecision counts one decision; unknown reasons count as Other.
func (m *Metrics) AdmissionDecision(reason string) {
	for _, known := range admissionReasons {
		if reason == known {
			m.admissions.WithLabelValues(reason).Inc()
			return
		}
	}
	m.admissions.WithLabelValues(ReasonOther).Inc()
}

// observedGauges are registered on their first observation, so a quantity
// nothing has reported has no sample rather than a false zero (DESIGN 12.1).
type observedGauges struct {
	once     sync.Once
	registry *prometheus.Registry
	gauges   []prometheus.Gauge
}

// set records one value per gauge, in order, then exposes the gauges.
func (o *observedGauges) set(values ...float64) {
	for i, value := range values {
		o.gauges[i].Set(value)
	}
	o.once.Do(func() {
		for _, gauge := range o.gauges {
			o.registry.MustRegister(gauge)
		}
	})
}

// SetReservations records reserved CPU millicores, memory bytes, and
// executions. The reservation gauges have no samples until the first call.
func (m *Metrics) SetReservations(cpuMillicores, memoryBytes int64, executions int) {
	m.reservations.set(float64(cpuMillicores)/1000, float64(memoryBytes), float64(executions))
}

// SetBudget records the configured envelope. The budget gauges have no
// samples until the first call.
func (m *Metrics) SetBudget(cpuMillicores, memoryBytes int64, maxPods int) {
	m.budget.set(float64(cpuMillicores)/1000, float64(memoryBytes), float64(maxPods))
}

// SetBarrier records the current barrier cause; BarrierNone lowers it.
func (m *Metrics) SetBarrier(cause BarrierCause) {
	known := cause == BarrierNone
	for _, candidate := range barrierCauses {
		known = known || cause == candidate
	}
	if !known {
		cause = BarrierOther
	}
	for _, candidate := range barrierCauses {
		value := 0.0
		if candidate == cause {
			value = 1
		}
		m.barrier.WithLabelValues(string(candidate)).Set(value)
	}
}

// PassCompleted records the end of a reconciliation pass without errors.
func (m *Metrics) PassCompleted(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastPass = at
}

// SetPendingStages records, per pending stage, when its oldest record entered
// it. Stages absent from oldest have no pending record; unknown stages are ignored.
func (m *Metrics) SetPendingStages(oldest map[string]time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.pendingOldest)
	for _, stage := range PendingStages {
		if at, found := oldest[stage]; found {
			m.pendingOldest[stage] = at
		}
	}
}

// ObserveCRI records one CRI call; it matches cri.Observer. PullImage calls
// also feed the pull metrics.
func (m *Metrics) ObserveCRI(method string, duration time.Duration, err error) {
	label := method
	if !criMethods[label] {
		label = "other"
	}
	m.criDuration.WithLabelValues(label).Observe(duration.Seconds())
	if err != nil {
		m.criErrors.WithLabelValues(label, errorCode(err)).Inc()
	}
	if method == "PullImage" {
		m.pullDuration.Observe(duration.Seconds())
		if err != nil {
			m.pullFailures.Inc()
		}
	}
}

func errorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded.String()
	}
	code := status.Code(err)
	if !codeNames[code] {
		return codes.Unknown.String()
	}
	return code.String()
}

var codeNames = map[codes.Code]bool{
	codes.Canceled: true, codes.Unknown: true, codes.InvalidArgument: true, codes.DeadlineExceeded: true,
	codes.NotFound: true, codes.AlreadyExists: true, codes.PermissionDenied: true, codes.ResourceExhausted: true,
	codes.FailedPrecondition: true, codes.Aborted: true, codes.OutOfRange: true, codes.Unimplemented: true,
	codes.Internal: true, codes.Unavailable: true, codes.DataLoss: true, codes.Unauthenticated: true,
}

// Journal record and byte bounds of the execution journal (DESIGN 6.4).
const (
	JournalRecordsLimit = 10000
	JournalBytesLimit   = 4 * 1024 * 1024
)

// SetJournal records the journal's size against its bounds. The journal
// gauges have no samples until the first call.
func (m *Metrics) SetJournal(records, bytes int) {
	m.journal.set(float64(records), float64(bytes), JournalRecordsLimit, JournalBytesLimit)
}

// LeaseRenewalFailed counts one failed Lease renewal.
func (m *Metrics) LeaseRenewalFailed() { m.leaseFailures.Inc() }

// Probe result labels.
const (
	ProbeSuccess = "success"
	ProbeFailure = "failure"
	ProbeError   = "error"
)

// ProbeResult counts a probe outcome. probe is readiness or liveness;
// handler is exec, httpGet, or tcpSocket; other values are reported as other.
func (m *Metrics) ProbeResult(probe, handler, result string) {
	m.probeResults.WithLabelValues(
		oneOf(probe, "readiness", "liveness"), oneOf(handler, "exec", "httpGet", "tcpSocket"),
		oneOf(result, ProbeSuccess, ProbeFailure, ProbeError),
	).Inc()
}

// LogRotation counts a rotation attempt: rotated, reopen_failed, or failed.
func (m *Metrics) LogRotation(result string) {
	m.logRotations.WithLabelValues(oneOf(result, "rotated", "reopen_failed", "failed")).Inc()
}

// Event counts an Event: recorded, deduplicated, or dropped.
func (m *Metrics) Event(result string) {
	m.events.WithLabelValues(oneOf(result, "recorded", "deduplicated", "dropped")).Inc()
}

func oneOf(value string, allowed ...string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return "other"
}

var (
	lastPassDesc = prometheus.NewDesc(namespace+"_last_complete_reconcile_age_seconds",
		"Seconds since the last reconciliation pass completed without errors; NaN before the first.", nil, nil)
	pendingDesc = prometheus.NewDesc(namespace+"_oldest_pending_stage_age_seconds",
		"Age of the oldest journal record in each pending stage; no sample when none is pending.", []string{"stage"}, nil)
)

// ageCollector computes ages at scrape time so they grow while nothing changes.
type ageCollector struct{ m *Metrics }

func (c *ageCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- lastPassDesc
	descriptions <- pendingDesc
}

func (c *ageCollector) Collect(samples chan<- prometheus.Metric) {
	m := c.m
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	age := math.NaN()
	if !m.lastPass.IsZero() {
		age = now.Sub(m.lastPass).Seconds()
	}
	samples <- prometheus.MustNewConstMetric(lastPassDesc, prometheus.GaugeValue, age)
	for _, stage := range PendingStages {
		if at, found := m.pendingOldest[stage]; found {
			samples <- prometheus.MustNewConstMetric(pendingDesc, prometheus.GaugeValue, now.Sub(at).Seconds(), stage)
		}
	}
}
