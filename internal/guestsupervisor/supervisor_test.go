// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeWorkload records calls and returns configured results.
type fakeWorkload struct {
	mu         sync.Mutex
	calls      []string
	point      SafePoint
	prepareErr error
	resumeErr  error
	abortErr   error
	// abortFailures fails that many aborts before abortErr applies.
	abortFailures int
	block         chan struct{}
	resumed       []ResumeRequest
}

func (w *fakeWorkload) Prepare(ctx context.Context, request PrepareRequest) (SafePoint, error) {
	w.record("prepare " + request.OperationID)
	if w.block != nil {
		select {
		case <-w.block:
		case <-ctx.Done():
			return SafePoint{}, ctx.Err()
		}
	}
	return w.point, w.prepareErr
}

func (w *fakeWorkload) Resume(_ context.Context, request ResumeRequest) error {
	w.record("resume " + request.OperationID)
	w.mu.Lock()
	w.resumed = append(w.resumed, request)
	w.mu.Unlock()
	return w.resumeErr
}

func (w *fakeWorkload) Abort(_ context.Context, operationID string) error {
	w.record("abort " + operationID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.abortFailures > 0 {
		w.abortFailures--
		return errors.New("stuck")
	}
	return w.abortErr
}

func (w *fakeWorkload) record(call string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, call)
}

func (w *fakeWorkload) callList() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.calls, ",")
}

func newTestSupervisor(workload Workload) *Supervisor {
	s := NewSupervisor(workload)
	s.now = func() time.Time { return testTime }
	return s
}

type step struct {
	request Request
	outcome Outcome
	reason  string
}

func TestSupervisorTransitions(t *testing.T) {
	deadline := testTime.Add(time.Minute)
	point := SafePoint{ID: "counter-5", ExternalEffectWatermark: 5}
	for _, tt := range []struct {
		name     string
		workload *fakeWorkload
		steps    []step
		calls    string
	}{
		{
			name:     "prepare then resume",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeSafePoint},
				{request: resumeRequest("op-1", 3, testTime), outcome: OutcomeReady},
			},
			calls: "prepare op-1,resume op-1",
		},
		{
			name:     "retries return recorded outcomes",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeSafePoint},
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeSafePoint},
				{request: resumeRequest("op-1", 2, testTime), outcome: OutcomeReady},
				{request: resumeRequest("op-1", 2, testTime.Add(time.Second)), outcome: OutcomeReady},
				{request: abortRequest("op-1"), outcome: OutcomeRefusal, reason: "already resumed"},
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeRefusal, reason: "already completed"},
			},
			calls: "prepare op-1,resume op-1",
		},
		{
			name:     "abort reopens without identity",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeSafePoint},
				{request: abortRequest("op-1"), outcome: OutcomeAborted},
				{request: abortRequest("op-1"), outcome: OutcomeAborted},
				{request: resumeRequest("op-1", 2, testTime), outcome: OutcomeRefusal, reason: "no prepared operation"},
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeRefusal, reason: "already completed"},
				{request: prepareRequest("op-2", 2, deadline), outcome: OutcomeSafePoint},
			},
			calls: "prepare op-1,abort op-1,prepare op-2",
		},
		{
			name:     "abort of unknown operation is a no-op",
			workload: &fakeWorkload{},
			steps:    []step{{request: abortRequest("op-9"), outcome: OutcomeAborted}},
		},
		{
			name:     "stale epochs rejected",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 5, deadline), outcome: OutcomeSafePoint},
				{request: prepareRequest("op-1", 4, deadline), outcome: OutcomeRefusal, reason: "another operation"},
				{request: resumeRequest("op-1", 4, testTime), outcome: OutcomeRefusal, reason: "stale epoch"},
				{request: resumeRequest("op-1", 6, testTime), outcome: OutcomeReady},
				{request: prepareRequest("op-2", 5, deadline), outcome: OutcomeRefusal, reason: "stale epoch"},
			},
			calls: "prepare op-1,resume op-1",
		},
		{
			name:     "conflicting operations",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
				{request: prepareRequest("op-2", 1, deadline), outcome: OutcomeRefusal, reason: "another operation"},
				{request: resumeRequest("op-2", 1, testTime), outcome: OutcomeRefusal, reason: "not prepared"},
				{request: abortRequest("op-2"), outcome: OutcomeRefusal, reason: "another operation"},
			},
			calls: "prepare op-1",
		},
		{
			name:     "deadline bounds",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 1, testTime), outcome: OutcomeRefusal, reason: "already passed"},
				{request: prepareRequest("op-1", 1, testTime.Add(MaxPrepareDuration+time.Second)), outcome: OutcomeRefusal, reason: "too far ahead"},
			},
		},
		{
			name:     "clock skew keeps the barrier closed",
			workload: &fakeWorkload{point: point},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
				{request: resumeRequest("op-1", 1, testTime.Add(-time.Hour)), outcome: OutcomeRefusal, reason: "clock"},
				{request: resumeRequest("op-1", 1, testTime.Add(DefaultMaxClockSkew)), outcome: OutcomeReady},
			},
			calls: "prepare op-1,resume op-1",
		},
		{
			name:     "workload refusal reason is reported",
			workload: &fakeWorkload{prepareErr: Refuse("transaction open")},
			steps:    []step{{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "transaction open"}},
			calls:    "prepare op-1",
		},
		{
			name:     "unprintable refusal reason is replaced",
			workload: &fakeWorkload{prepareErr: Refuse("bad\nreason")},
			steps:    []step{{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "workload refused"}},
			calls:    "prepare op-1",
		},
		{
			name:     "workload error detail is withheld",
			workload: &fakeWorkload{prepareErr: errors.New("tenant secret in error")},
			steps:    []step{{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "workload failed"}},
			calls:    "prepare op-1",
		},
		{
			name:     "invalid safe point is aborted",
			workload: &fakeWorkload{point: SafePoint{ID: "not valid"}},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "invalid safe point"},
				{request: resumeRequest("op-1", 1, testTime), outcome: OutcomeRefusal, reason: "no prepared operation"},
			},
			calls: "prepare op-1,abort op-1",
		},
		{
			name:     "invalid safe point and failed abort",
			workload: &fakeWorkload{point: SafePoint{ID: "not valid"}, abortErr: errors.New("stuck")},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "abort failed"},
				{request: abortRequest("op-1"), outcome: OutcomeRefusal, reason: "workload failed"},
				{request: abortRequest("op-1"), outcome: OutcomeRefusal, reason: "workload failed"},
				{request: abortRequest("op-2"), outcome: OutcomeRefusal, reason: "awaits a successful abort"},
				{request: resumeRequest("op-1", 1, testTime), outcome: OutcomeRefusal, reason: "awaits a successful abort"},
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeRefusal, reason: "awaits a successful abort"},
				{request: prepareRequest("op-2", 1, deadline), outcome: OutcomeRefusal, reason: "awaits a successful abort"},
			},
			calls: "prepare op-1,abort op-1,abort op-1,abort op-1",
		},
		{
			name:     "failed cleanup abort is retried until it succeeds",
			workload: &fakeWorkload{point: SafePoint{ID: "not valid"}, abortFailures: 2},
			steps: []step{
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeRefusal, reason: "abort failed"},
				{request: abortRequest("op-1"), outcome: OutcomeRefusal, reason: "workload failed"},
				{request: abortRequest("op-1"), outcome: OutcomeAborted},
				{request: abortRequest("op-1"), outcome: OutcomeAborted},
				{request: prepareRequest("op-1", 2, deadline), outcome: OutcomeRefusal, reason: "already completed"},
				{request: prepareRequest("op-2", 1, deadline), outcome: OutcomeRefusal, reason: "stale epoch"},
			},
			calls: "prepare op-1,abort op-1,abort op-1,abort op-1",
		},
		{
			name:     "failed resume stays prepared",
			workload: &fakeWorkload{point: point, resumeErr: Refuse("state file mismatch")},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
				{request: resumeRequest("op-1", 1, testTime), outcome: OutcomeRefusal, reason: "state file mismatch"},
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
			},
			calls: "prepare op-1,resume op-1",
		},
		{
			name:     "failed abort stays prepared",
			workload: &fakeWorkload{point: point, abortErr: errors.New("stuck")},
			steps: []step{
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
				{request: abortRequest("op-1"), outcome: OutcomeRefusal, reason: "workload failed"},
				{request: prepareRequest("op-1", 1, deadline), outcome: OutcomeSafePoint},
			},
			calls: "prepare op-1,abort op-1",
		},
		{
			name:     "invalid request",
			workload: &fakeWorkload{},
			steps:    []step{{request: Request{Version: ProtocolVersion, Operation: OperationAbort, Abort: &AbortRequest{OperationID: "a/b"}}, outcome: OutcomeRefusal, reason: "invalid request"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSupervisor(tt.workload)
			for i, step := range tt.steps {
				response := s.Handle(t.Context(), step.request)
				if err := response.Validate(); err != nil {
					t.Fatalf("step %d: invalid response %+v: %v", i, response, err)
				}
				if response.Outcome != step.outcome {
					t.Fatalf("step %d: outcome = %s (%+v), want %s", i, response.Outcome, response.Refusal, step.outcome)
				}
				if step.reason != "" && !strings.Contains(response.Refusal.Reason, step.reason) {
					t.Fatalf("step %d: reason = %q, want %q", i, response.Refusal.Reason, step.reason)
				}
			}
			if got := tt.workload.callList(); got != tt.calls {
				t.Fatalf("workload calls = %q, want %q", got, tt.calls)
			}
		})
	}
}

func TestSupervisorPrepareDeadline(t *testing.T) {
	workload := &fakeWorkload{block: make(chan struct{})}
	s := NewSupervisor(workload)
	response := s.Handle(t.Context(), prepareRequest("op-1", 1, time.Now().Add(50*time.Millisecond)))
	if response.Outcome != OutcomeRefusal || !strings.Contains(response.Refusal.Reason, "deadline") {
		t.Fatalf("response = %+v, want deadline refusal", response)
	}
}

func TestSupervisorSerializesRequests(t *testing.T) {
	workload := &fakeWorkload{block: make(chan struct{}), point: SafePoint{ID: "p", ExternalEffectWatermark: 0}}
	s := NewSupervisor(workload)
	done := make(chan Response)
	go func() { done <- s.Handle(context.Background(), prepareRequest("op-1", 1, time.Now().Add(time.Minute))) }()
	for workload.callList() == "" {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if response := s.Handle(ctx, abortRequest("op-1")); response.Outcome != OutcomeRefusal || response.Refusal.Reason != "supervisor busy" {
		t.Fatalf("concurrent response = %+v, want busy refusal", response)
	}
	close(workload.block)
	if response := <-done; response.Outcome != OutcomeSafePoint {
		t.Fatalf("prepare response = %+v", response)
	}
}
