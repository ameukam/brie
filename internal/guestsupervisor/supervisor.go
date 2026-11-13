// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"context"
	"errors"
	"time"

	"github.com/ameukam/brie/internal/lifecycleid"
)

// MaxPrepareDuration bounds how far ahead a prepare deadline may be.
const MaxPrepareDuration = 5 * time.Minute

// DefaultMaxClockSkew is the largest difference between the guest clock and
// the host wall clock that a resume accepts.
const DefaultMaxClockSkew = 2 * time.Second

// Workload is the cooperative application behind the supervisor.
//
// Prepare must stop accepting new work, drain in-flight work, and flush its
// durable state before returning a safe point. If it returns an error, it must
// have reopened admission itself. Resume must rebind the supplied identity,
// reseed duplicated randomness, and only then reopen admission; on error the
// workload stays closed. Abort reopens admission without a new identity.
//
// Return Refuse to report a reason to the host; other errors are reported
// without detail so that guest error text does not reach host records.
type Workload interface {
	Prepare(ctx context.Context, request PrepareRequest) (SafePoint, error)
	Resume(ctx context.Context, request ResumeRequest) error
	Abort(ctx context.Context, operationID string) error
}

// RefusalError carries a reason that the supervisor reports to the host.
type RefusalError struct {
	Reason string
}

func (e *RefusalError) Error() string { return "refused: " + e.Reason }

// Refuse returns an error whose printable reason the host may record.
func Refuse(reason string) error { return &RefusalError{Reason: reason} }

type phase int

const (
	phaseActive phase = iota
	phasePrepared
	// phaseAbortPending follows an invalid safe point whose cleanup abort
	// failed: admission may still be closed, so only a successful abort of
	// the recorded operation returns to phaseActive.
	phaseAbortPending
)

// Supervisor is the state machine between the host protocol and a Workload.
// It handles one request at a time. Retrying a request after a lost reply
// returns the recorded outcome instead of repeating the workload call.
type Supervisor struct {
	workload     Workload
	now          func() time.Time
	maxClockSkew time.Duration
	slot         chan struct{}

	phase       phase
	operationID string
	epoch       int64
	safePoint   SafePoint
	lastResume  *ResumeRequest
	lastAborted string
}

// NewSupervisor creates a supervisor in the active phase.
func NewSupervisor(workload Workload) *Supervisor {
	return &Supervisor{
		workload: workload, now: time.Now, maxClockSkew: DefaultMaxClockSkew,
		slot: make(chan struct{}, 1),
	}
}

// Handle applies one valid request. It waits for an earlier request to finish
// or for ctx to end.
func (s *Supervisor) Handle(ctx context.Context, request Request) Response {
	operationID := request.OperationID()
	if err := request.Validate(); err != nil {
		return refusal(operationID, "invalid request")
	}
	select {
	case s.slot <- struct{}{}:
		defer func() { <-s.slot }()
	case <-ctx.Done():
		return refusal(operationID, "supervisor busy")
	}
	switch request.Operation {
	case OperationPrepare:
		return s.prepare(ctx, *request.Prepare)
	case OperationResume:
		return s.resume(ctx, *request.Resume)
	default:
		return s.abort(ctx, request.Abort.OperationID)
	}
}

func (s *Supervisor) prepare(ctx context.Context, request PrepareRequest) Response {
	switch s.phase {
	case phasePrepared:
		if request.OperationID == s.operationID && request.Epoch == s.epoch {
			return safePoint(s.operationID, s.safePoint)
		}
		return refusal(request.OperationID, "another operation is prepared")
	case phaseAbortPending:
		return refusal(request.OperationID, abortPendingReason)
	}
	if request.Epoch < s.epoch {
		return refusal(request.OperationID, "stale epoch")
	}
	if s.lastResume != nil && request.OperationID == s.lastResume.OperationID ||
		request.OperationID == s.lastAborted {
		return refusal(request.OperationID, "operation already completed")
	}
	now := s.now()
	if !request.Deadline.After(now) {
		return refusal(request.OperationID, "deadline already passed")
	}
	if request.Deadline.Sub(now) > MaxPrepareDuration {
		return refusal(request.OperationID, "deadline too far ahead")
	}
	ctx, cancel := context.WithDeadline(ctx, request.Deadline)
	defer cancel()
	point, err := s.workload.Prepare(ctx, request)
	if err != nil {
		return workloadRefusal(request.OperationID, err)
	}
	if err := (Response{Version: ProtocolVersion, Outcome: OutcomeSafePoint, OperationID: request.OperationID, SafePoint: &point}).Validate(); err != nil {
		if abortErr := s.workload.Abort(ctx, request.OperationID); abortErr != nil {
			// The workload may still be closed; keep the operation until an
			// abort of it succeeds.
			s.phase, s.operationID, s.epoch = phaseAbortPending, request.OperationID, request.Epoch
			return refusal(request.OperationID, "invalid safe point and abort failed")
		}
		return refusal(request.OperationID, "invalid safe point")
	}
	s.phase, s.operationID, s.epoch, s.safePoint = phasePrepared, request.OperationID, request.Epoch, point
	return safePoint(request.OperationID, point)
}

func (s *Supervisor) resume(ctx context.Context, request ResumeRequest) Response {
	if s.phase == phaseAbortPending {
		return refusal(request.OperationID, abortPendingReason)
	}
	if s.phase != phasePrepared {
		if s.lastResume != nil && sameExecution(*s.lastResume, request) {
			return Response{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: request.OperationID}
		}
		return refusal(request.OperationID, "no prepared operation")
	}
	if request.OperationID != s.operationID {
		return refusal(request.OperationID, "operation is not prepared")
	}
	if request.Epoch < s.epoch {
		return refusal(request.OperationID, "stale epoch")
	}
	if skew := s.now().Sub(request.WallClock).Abs(); skew > s.maxClockSkew {
		return refusal(request.OperationID, "guest clock differs from host wall clock")
	}
	if err := s.workload.Resume(ctx, request); err != nil {
		return workloadRefusal(request.OperationID, err)
	}
	resumed := request
	s.phase, s.operationID, s.epoch, s.lastResume = phaseActive, "", request.Epoch, &resumed
	return Response{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: request.OperationID}
}

func (s *Supervisor) abort(ctx context.Context, operationID string) Response {
	if s.phase == phaseActive || operationID != s.operationID {
		if s.lastResume != nil && operationID == s.lastResume.OperationID {
			return refusal(operationID, "operation already resumed")
		}
		switch s.phase {
		case phasePrepared:
			return refusal(operationID, "another operation is prepared")
		case phaseAbortPending:
			return refusal(operationID, abortPendingReason)
		}
		// Nothing is prepared under this ID, so admission is already open.
		return Response{Version: ProtocolVersion, Outcome: OutcomeAborted, OperationID: operationID}
	}
	if err := s.workload.Abort(ctx, operationID); err != nil {
		return workloadRefusal(operationID, err)
	}
	s.phase, s.operationID, s.lastAborted = phaseActive, "", operationID
	return Response{Version: ProtocolVersion, Outcome: OutcomeAborted, OperationID: operationID}
}

// sameExecution matches a retried resume after a lost reply. The wall clock
// and nonce of a retry are fresh, so they do not take part.
func sameExecution(a, b ResumeRequest) bool {
	return a.OperationID == b.OperationID && a.SandboxUID == b.SandboxUID &&
		a.PodUID == b.PodUID && a.Epoch == b.Epoch
}

// abortPendingReason refuses requests while phaseAbortPending lasts.
const abortPendingReason = "an earlier operation awaits a successful abort"

func safePoint(operationID string, point SafePoint) Response {
	return Response{Version: ProtocolVersion, Outcome: OutcomeSafePoint, OperationID: operationID, SafePoint: &point}
}

func refusal(operationID, reason string) Response {
	if lifecycleid.Token("operation_id", operationID) != nil {
		operationID = "unknown"
	}
	return Response{Version: ProtocolVersion, Outcome: OutcomeRefusal, OperationID: operationID, Refusal: &Refusal{Reason: reason}}
}

func workloadRefusal(operationID string, err error) Response {
	if refusalErr, ok := errors.AsType[*RefusalError](err); ok {
		response := refusal(operationID, refusalErr.Reason)
		if response.Validate() == nil {
			return response
		}
		return refusal(operationID, "workload refused")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return refusal(operationID, "workload did not finish before the deadline")
	}
	return refusal(operationID, "workload failed")
}
