// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package guestsupervisor implements the cooperative prepare/resume protocol
// of DESIGN 3.1 and 3.2 between the host and a trusted supervisor inside a
// workload container.
//
// The supervisor listens only on a Unix socket inside the container. The host
// reaches it by running brie-guestctl through CRI ExecSync, which Kata executes
// through its own agent, so Kata remains the only guest-control channel. The
// protocol is a cooperation contract, not a security boundary: host network
// and broker gates stay closed independently of any guest reply.
package guestsupervisor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ameukam/brie/internal/lifecycleid"
)

// ProtocolVersion identifies this message schema. Checkpoint manifests record
// it as the application resume-protocol version.
const ProtocolVersion = "brie.guestsupervisor/v1"

// MaxMessageBytes bounds every encoded request and response.
const MaxMessageBytes = 4096

// MaxRefusalReasonBytes bounds the operator-visible refusal reason.
const MaxRefusalReasonBytes = 256

// EntropyNonceHexDigits is the length of the hex-encoded 32-byte resume nonce.
const EntropyNonceHexDigits = 64

// Operation names one host request.
type Operation string

// Host requests.
const (
	OperationPrepare Operation = "prepare"
	OperationResume  Operation = "resume"
	OperationAbort   Operation = "abort"
)

// Outcome names one supervisor reply.
type Outcome string

// Supervisor replies. A prepare yields a safe point or a refusal, a resume
// yields ready or a refusal, and an abort yields aborted or a refusal.
const (
	OutcomeSafePoint Outcome = "safe_point"
	OutcomeReady     Outcome = "ready"
	OutcomeAborted   Outcome = "aborted"
	OutcomeRefusal   Outcome = "refusal"
)

// Request is one host-to-supervisor message. Exactly the body that matches
// Operation is set.
type Request struct {
	Version   string          `json:"version"`
	Operation Operation       `json:"operation"`
	Prepare   *PrepareRequest `json:"prepare,omitempty"`
	Resume    *ResumeRequest  `json:"resume,omitempty"`
	Abort     *AbortRequest   `json:"abort,omitempty"`
}

// PrepareRequest asks the workload to stop new work and reach a safe point
// before Deadline.
type PrepareRequest struct {
	OperationID string    `json:"operation_id"`
	Epoch       int64     `json:"epoch"`
	Deadline    time.Time `json:"deadline"`
}

// ResumeRequest carries the identity of the execution attempt that releases
// the restore barrier. WallClock is the host's current time; EntropyNonce is
// fresh host randomness the workload mixes into its own reseeding. The nonce
// is uniqueness input, not a secret: it travels in an exec argument.
type ResumeRequest struct {
	OperationID  string    `json:"operation_id"`
	SandboxUID   string    `json:"sandbox_uid"`
	PodUID       string    `json:"pod_uid"`
	Epoch        int64     `json:"epoch"`
	WallClock    time.Time `json:"wall_clock"`
	EntropyNonce string    `json:"entropy_nonce"`
}

// AbortRequest cancels a prepared operation before its commit boundary and
// reopens admission without a new identity.
type AbortRequest struct {
	OperationID string `json:"operation_id"`
}

// Response is one supervisor-to-host message.
type Response struct {
	Version     string     `json:"version"`
	Outcome     Outcome    `json:"outcome"`
	OperationID string     `json:"operation_id"`
	SafePoint   *SafePoint `json:"safe_point,omitempty"`
	Refusal     *Refusal   `json:"refusal,omitempty"`
}

// SafePoint identifies the application state captured by a checkpoint.
// ExternalEffectWatermark counts the external effects the workload has
// acknowledged up to that state.
type SafePoint struct {
	ID                      string `json:"id"`
	ExternalEffectWatermark uint64 `json:"external_effect_watermark"`
}

// Refusal explains why the workload stays in its previous state.
type Refusal struct {
	Reason string `json:"reason"`
}

// Validate checks the request schema without consulting supervisor state.
func (r Request) Validate() error {
	if r.Version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", r.Version)
	}
	bodies := 0
	for _, set := range []bool{r.Prepare != nil, r.Resume != nil, r.Abort != nil} {
		if set {
			bodies++
		}
	}
	if bodies != 1 {
		return errors.New("request must carry exactly one operation body")
	}
	switch r.Operation {
	case OperationPrepare:
		if r.Prepare == nil {
			return errors.New("prepare request requires a prepare body")
		}
		return r.Prepare.validate()
	case OperationResume:
		if r.Resume == nil {
			return errors.New("resume request requires a resume body")
		}
		return r.Resume.validate()
	case OperationAbort:
		if r.Abort == nil {
			return errors.New("abort request requires an abort body")
		}
		return lifecycleid.Token("operation_id", r.Abort.OperationID)
	default:
		return fmt.Errorf("unsupported operation %q", r.Operation)
	}
}

// OperationID returns the operation identifier of a valid request.
func (r Request) OperationID() string {
	switch {
	case r.Prepare != nil:
		return r.Prepare.OperationID
	case r.Resume != nil:
		return r.Resume.OperationID
	case r.Abort != nil:
		return r.Abort.OperationID
	}
	return ""
}

func (p PrepareRequest) validate() error {
	if err := lifecycleid.Token("operation_id", p.OperationID); err != nil {
		return err
	}
	if err := lifecycleid.Epoch("epoch", p.Epoch); err != nil {
		return err
	}
	if p.Deadline.IsZero() {
		return errors.New("deadline is required")
	}
	return nil
}

func (r ResumeRequest) validate() error {
	if err := lifecycleid.Token("operation_id", r.OperationID); err != nil {
		return err
	}
	if err := lifecycleid.UID("sandbox_uid", r.SandboxUID); err != nil {
		return err
	}
	if err := lifecycleid.UID("pod_uid", r.PodUID); err != nil {
		return err
	}
	if err := lifecycleid.Epoch("epoch", r.Epoch); err != nil {
		return err
	}
	if r.WallClock.IsZero() {
		return errors.New("wall_clock is required")
	}
	return lifecycleid.LowerHex("entropy_nonce", r.EntropyNonce, EntropyNonceHexDigits)
}

// Validate checks the response schema without its request.
func (r Response) Validate() error {
	if r.Version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", r.Version)
	}
	if err := lifecycleid.Token("operation_id", r.OperationID); err != nil {
		return err
	}
	switch r.Outcome {
	case OutcomeSafePoint:
		if r.SafePoint == nil || r.Refusal != nil {
			return errors.New("safe_point response requires only a safe_point body")
		}
		return lifecycleid.Token("safe_point.id", r.SafePoint.ID)
	case OutcomeRefusal:
		if r.Refusal == nil || r.SafePoint != nil {
			return errors.New("refusal response requires only a refusal body")
		}
		return lifecycleid.Text("refusal.reason", r.Refusal.Reason, MaxRefusalReasonBytes)
	case OutcomeReady, OutcomeAborted:
		if r.SafePoint != nil || r.Refusal != nil {
			return fmt.Errorf("%s response must not carry a body", r.Outcome)
		}
		return nil
	default:
		return fmt.Errorf("unsupported outcome %q", r.Outcome)
	}
}

// ValidateFor checks that a valid response answers request.
func (r Response) ValidateFor(request Request) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.OperationID != request.OperationID() {
		return errors.New("response operation_id does not match the request")
	}
	if r.Outcome == OutcomeRefusal {
		return nil
	}
	want := map[Operation]Outcome{
		OperationPrepare: OutcomeSafePoint,
		OperationResume:  OutcomeReady,
		OperationAbort:   OutcomeAborted,
	}[request.Operation]
	if r.Outcome != want {
		return fmt.Errorf("%s request cannot yield a %s response", request.Operation, r.Outcome)
	}
	return nil
}

// EncodeRequest validates and canonically encodes a request.
func EncodeRequest(r Request) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	return encode(r)
}

// EncodeResponse validates and canonically encodes a response.
func EncodeResponse(r Response) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, fmt.Errorf("invalid response: %w", err)
	}
	return encode(r)
}

// DecodeRequest accepts only the canonical encoding of a valid request.
func DecodeRequest(data []byte) (Request, error) {
	var r Request
	if err := decodeCanonical(data, &r); err != nil {
		return Request{}, fmt.Errorf("decode request: %w", err)
	}
	if err := r.Validate(); err != nil {
		return Request{}, fmt.Errorf("invalid request: %w", err)
	}
	return r, nil
}

// DecodeResponse accepts only the canonical encoding of a valid response.
func DecodeResponse(data []byte) (Response, error) {
	var r Response
	if err := decodeCanonical(data, &r); err != nil {
		return Response{}, fmt.Errorf("decode response: %w", err)
	}
	if err := r.Validate(); err != nil {
		return Response{}, fmt.Errorf("invalid response: %w", err)
	}
	return r, nil
}

func encode(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}
	if len(data) > MaxMessageBytes {
		return nil, fmt.Errorf("message exceeds %d bytes", MaxMessageBytes)
	}
	return data, nil
}

// decodeCanonical rejects oversized input, unknown fields, trailing data, and
// any input that is not byte-identical to its re-encoding. The last check
// also rejects duplicate keys, insignificant whitespace, reordered fields,
// and invalid UTF-8.
func decodeCanonical(data []byte, value any) error {
	if len(data) > MaxMessageBytes {
		return fmt.Errorf("message exceeds %d bytes", MaxMessageBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after message")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return errors.New("message is not canonically encoded")
	}
	return nil
}
