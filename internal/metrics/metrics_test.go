// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/cri"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ cri.Observer = (*Metrics)(nil).ObserveCRI

// samples scrapes the text exposition into "name{labels}" -> value text.
func samples(t *testing.T, m *Metrics) map[string]string {
	t.Helper()
	recorder := httptest.NewRecorder()
	promhttp.HandlerFor(m.Gatherer(), promhttp.HandlerOpts{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("scrape status %d", recorder.Code)
	}
	result := make(map[string]string)
	scanner := bufio.NewScanner(recorder.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		result[line[:cut]] = line[cut+1:]
	}
	return result
}

func expect(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q", key, got[key], value)
		}
	}
}

func count(got map[string]string, prefix string) int {
	n := 0
	for key := range got {
		if strings.HasPrefix(key, prefix) {
			n++
		}
	}
	return n
}

func TestAdmissionAndBudget(t *testing.T) {
	t.Parallel()
	m := New(nil)
	m.AdmissionDecision(ReasonAdmitted)
	m.AdmissionDecision(ReasonInsufficientResources)
	m.AdmissionDecision("tenant-controlled reason")
	m.SetReservations(1500, 1<<30, 2)
	m.SetBudget(4000, 8<<30, 10)
	got := samples(t, m)
	expect(t, got, map[string]string{
		`brie_node_admission_decisions_total{reason="Admitted"}`:              "1",
		`brie_node_admission_decisions_total{reason="InsufficientResources"}`: "1",
		`brie_node_admission_decisions_total{reason="Other"}`:                 "1",
		"brie_node_reserved_cpu_cores":                                        "1.5",
		"brie_node_reserved_memory_bytes":                                     "1.073741824e+09",
		"brie_node_reserved_executions":                                       "2",
		"brie_node_budget_cpu_cores":                                          "4",
		"brie_node_budget_memory_bytes":                                       "8.589934592e+09",
		"brie_node_budget_executions":                                         "10",
	})
	if n := count(got, "brie_node_admission_decisions_total{"); n != 3 {
		t.Fatalf("admission series = %d, want 3", n)
	}
}

// TestUnobservedGaugesHaveNoSamples is a regression test: reservations,
// budget, and journal state nothing has reported are absent, not zero.
func TestUnobservedGaugesHaveNoSamples(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		observe func(*Metrics)
		absent  []string
		present map[string]string
	}{
		{name: "unwired", observe: func(*Metrics) {},
			absent: []string{"brie_node_reserved_", "brie_node_budget_", "brie_node_journal_"}},
		{name: "budget only", observe: func(m *Metrics) { m.SetBudget(4000, 8<<30, 10) },
			absent:  []string{"brie_node_reserved_", "brie_node_journal_"},
			present: map[string]string{"brie_node_budget_executions": "10"}},
		{name: "populated", observe: func(m *Metrics) {
			m.SetReservations(0, 0, 0)
			m.SetReservations(2500, 1<<20, 3)
			m.SetJournal(0, 0)
		}, absent: []string{"brie_node_budget_"}, present: map[string]string{
			"brie_node_reserved_cpu_cores": "2.5", "brie_node_reserved_memory_bytes": "1.048576e+06",
			"brie_node_reserved_executions": "3", "brie_node_journal_records": "0", "brie_node_journal_records_limit": "10000",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := New(nil)
			tt.observe(m)
			got := samples(t, m)
			for _, prefix := range tt.absent {
				if n := count(got, prefix); n != 0 {
					t.Errorf("%d unobserved %s samples exported", n, prefix)
				}
			}
			expect(t, got, tt.present)
		})
	}
}

func TestBarrier(t *testing.T) {
	t.Parallel()
	m := New(nil)
	if n := count(samples(t, m), "brie_node_admission_barrier{"); n != 0 {
		t.Fatal("barrier reported before any observation")
	}
	m.SetBarrier(BarrierUnresolvedExecution)
	expect(t, samples(t, m), map[string]string{
		`brie_node_admission_barrier{cause="UnresolvedExecution"}`: "1",
		`brie_node_admission_barrier{cause="IncompletePass"}`:      "0",
	})
	m.SetBarrier("pod-specific cause")
	expect(t, samples(t, m), map[string]string{
		`brie_node_admission_barrier{cause="Other"}`:               "1",
		`brie_node_admission_barrier{cause="UnresolvedExecution"}`: "0",
	})
	m.SetBarrier(BarrierNone)
	got := samples(t, m)
	if n := count(got, "brie_node_admission_barrier{"); n != len(barrierCauses) {
		t.Fatalf("barrier series = %d", n)
	}
	for _, cause := range barrierCauses {
		if value := got[fmt.Sprintf("brie_node_admission_barrier{cause=%q}", cause)]; value != "0" {
			t.Fatalf("cause %s = %s after lowering", cause, value)
		}
	}
}

func TestAges(t *testing.T) {
	t.Parallel()
	now := time.Unix(10000, 0)
	m := New(func() time.Time { return now })
	got := samples(t, m)
	if value := got["brie_node_last_complete_reconcile_age_seconds"]; value != "NaN" {
		t.Fatalf("age before any pass = %q, want NaN", value)
	}
	if n := count(got, "brie_node_oldest_pending_stage_age_seconds"); n != 0 {
		t.Fatal("pending age reported with nothing pending")
	}
	m.PassCompleted(now.Add(-30 * time.Second))
	m.SetPendingStages(map[string]time.Time{"sandbox-pending": now.Add(-5 * time.Second), "running": now, "reserved": {}})
	got = samples(t, m)
	expect(t, got, map[string]string{
		"brie_node_last_complete_reconcile_age_seconds":                       "30",
		`brie_node_oldest_pending_stage_age_seconds{stage="sandbox-pending"}`: "5",
		// A pending record with an unknown entry time is reported as unknown, not as zero.
		`brie_node_oldest_pending_stage_age_seconds{stage="reserved"}`: "NaN",
	})
	if n := count(got, "brie_node_oldest_pending_stage_age_seconds"); n != 2 {
		t.Fatalf("pending series = %d", n)
	}
}

// TestUnobservedGaugesAreAbsent is a regression test for DESIGN 12.1: a gauge
// that was never set is not exported as a fresh zero.
func TestUnobservedGaugesAreAbsent(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		set     func(*Metrics)
		present []string
		absent  []string
	}{
		{name: "none", set: func(*Metrics) {},
			absent: []string{"brie_node_reserved_", "brie_node_budget_", "brie_node_journal_"}},
		{name: "reservations", set: func(m *Metrics) { m.SetReservations(0, 0, 0) },
			present: []string{"brie_node_reserved_cpu_cores", "brie_node_reserved_memory_bytes", "brie_node_reserved_executions"},
			absent:  []string{"brie_node_budget_", "brie_node_journal_"}},
		{name: "budget", set: func(m *Metrics) { m.SetBudget(1000, 1, 1) },
			present: []string{"brie_node_budget_cpu_cores", "brie_node_budget_memory_bytes", "brie_node_budget_executions"},
			absent:  []string{"brie_node_reserved_", "brie_node_journal_"}},
		{name: "journal", set: func(m *Metrics) { m.SetJournal(0, 0); m.SetJournal(1, 2) },
			present: []string{"brie_node_journal_records", "brie_node_journal_bytes_limit"},
			absent:  []string{"brie_node_reserved_", "brie_node_budget_"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			m := New(nil)
			test.set(m)
			got := samples(t, m)
			for _, name := range test.present {
				if _, found := got[name]; !found {
					t.Errorf("%s missing after it was set", name)
				}
			}
			for _, prefix := range test.absent {
				if n := count(got, prefix); n != 0 {
					t.Errorf("%s* exported %d samples before any observation", prefix, n)
				}
			}
		})
	}
}

func TestObserveCRI(t *testing.T) {
	t.Parallel()
	m := New(nil)
	m.ObserveCRI("RunPodSandbox", time.Second, nil)
	m.ObserveCRI("RunPodSandbox", time.Second, status.Error(codes.Unavailable, "down"))
	m.ObserveCRI("PullImage", 2*time.Second, status.Error(codes.NotFound, "registry.example/tenant-image"))
	m.ObserveCRI("PullImage", time.Second, nil)
	m.ObserveCRI("FutureMethod", time.Second, errors.New("plain"))
	m.ObserveCRI("StopContainer", time.Second, fmt.Errorf("wrapped: %w", context.DeadlineExceeded))
	m.ObserveCRI("StartContainer", time.Second, status.Error(codes.Code(99), "odd"))
	expect(t, samples(t, m), map[string]string{
		`brie_node_cri_request_errors_total{code="Unavailable",method="RunPodSandbox"}`:      "1",
		`brie_node_cri_request_errors_total{code="NotFound",method="PullImage"}`:             "1",
		`brie_node_cri_request_errors_total{code="Unknown",method="other"}`:                  "1",
		`brie_node_cri_request_errors_total{code="DeadlineExceeded",method="StopContainer"}`: "1",
		`brie_node_cri_request_errors_total{code="Unknown",method="StartContainer"}`:         "1",
		`brie_node_cri_request_duration_seconds_count{method="RunPodSandbox"}`:               "2",
		"brie_node_image_pull_failures_total":                                                "1",
		"brie_node_image_pull_duration_seconds_count":                                        "2",
		"brie_node_image_pull_duration_seconds_sum":                                          "3",
	})
}

func TestJournalLeaseAndCounters(t *testing.T) {
	t.Parallel()
	m := New(nil)
	m.SetJournal(12, 3456)
	m.LeaseRenewalFailed()
	m.ProbeResult("readiness", "httpGet", ProbeFailure)
	m.ProbeResult("startup", "grpc", "weird")
	m.LogRotation("rotated")
	m.LogRotation("tenant")
	m.Event("recorded")
	m.Event("x")
	expect(t, samples(t, m), map[string]string{
		"brie_node_journal_records":              "12",
		"brie_node_journal_bytes":                "3456",
		"brie_node_journal_records_limit":        "10000",
		"brie_node_journal_bytes_limit":          "4.194304e+06",
		"brie_node_lease_renewal_failures_total": "1",
		`brie_node_probe_results_total{handler="httpGet",probe="readiness",result="failure"}`: "1",
		`brie_node_probe_results_total{handler="other",probe="other",result="other"}`:         "1",
		`brie_node_container_log_rotations_total{result="rotated"}`:                           "1",
		`brie_node_container_log_rotations_total{result="other"}`:                             "1",
		`brie_node_events_total{result="recorded"}`:                                           "1",
		`brie_node_events_total{result="other"}`:                                              "1",
	})
}

// TestNoTenantLabels is a regression guard: every label value comes from a
// fixed set, so tenant strings passed in never appear in the exposition.
func TestNoTenantLabels(t *testing.T) {
	t.Parallel()
	m := New(nil)
	const tenant = "tenant-namespace-pod-name"
	m.AdmissionDecision(tenant)
	m.SetBarrier(BarrierCause(tenant))
	m.ObserveCRI(tenant, time.Second, status.Error(codes.Internal, tenant))
	m.ProbeResult(tenant, tenant, tenant)
	m.LogRotation(tenant)
	m.Event(tenant)
	m.SetPendingStages(map[string]time.Time{tenant: time.Now()})
	for key := range samples(t, m) {
		if strings.Contains(key, tenant) {
			t.Fatalf("exposition carries tenant data: %s", key)
		}
	}
}
