// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const (
	testSandboxUID = "0f8fad5b-d9cb-469f-a165-70867728950e"
	testPodUID     = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	testNonce      = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
)

var testTime = time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

func prepareRequest(operationID string, epoch int64, deadline time.Time) Request {
	return Request{Version: ProtocolVersion, Operation: OperationPrepare,
		Prepare: &PrepareRequest{OperationID: operationID, Epoch: epoch, Deadline: deadline}}
}

func resumeRequest(operationID string, epoch int64, wallClock time.Time) Request {
	return Request{Version: ProtocolVersion, Operation: OperationResume, Resume: &ResumeRequest{
		OperationID: operationID, SandboxUID: testSandboxUID, PodUID: testPodUID,
		Epoch: epoch, WallClock: wallClock, EntropyNonce: testNonce,
	}}
}

func abortRequest(operationID string) Request {
	return Request{Version: ProtocolVersion, Operation: OperationAbort, Abort: &AbortRequest{OperationID: operationID}}
}

func TestRequestRoundTrip(t *testing.T) {
	for _, request := range []Request{
		prepareRequest("op-1", 3, testTime),
		resumeRequest("op-1", 4, testTime),
		abortRequest("op-1"),
	} {
		t.Run(string(request.Operation), func(t *testing.T) {
			encoded, err := EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeRequest(encoded)
			if err != nil {
				t.Fatalf("DecodeRequest(%s): %v", encoded, err)
			}
			again, err := EncodeRequest(decoded)
			if err != nil || !bytes.Equal(again, encoded) {
				t.Fatalf("re-encoding differs: %s, %v", again, err)
			}
		})
	}
}

func TestDecodeRequestRejects(t *testing.T) {
	valid, err := EncodeRequest(prepareRequest("op-1", 3, testTime))
	if err != nil {
		t.Fatal(err)
	}
	text := string(valid)
	for _, tt := range []struct {
		name string
		data string
		want string
	}{
		{name: "empty", data: "", want: "decode request"},
		{name: "not JSON", data: "prepare", want: "decode request"},
		{name: "unknown field", data: strings.Replace(text, `"version"`, `"extra":1,"version"`, 1), want: "unknown field"},
		{name: "trailing data", data: text + "{}", want: "trailing data"},
		{name: "trailing newline", data: text + "\n", want: "not canonically encoded"},
		{name: "whitespace", data: strings.Replace(text, `:`, `: `, 1), want: "not canonically encoded"},
		{name: "duplicate key", data: strings.Replace(text, `"version":`, `"version":"x","version":`, 1), want: "not canonically encoded"},
		{name: "fractional zeros", data: strings.Replace(text, `10:00:00Z`, `10:00:00.000Z`, 1), want: "not canonically encoded"},
		{name: "oversized", data: strings.Repeat(" ", MaxMessageBytes+1), want: "exceeds"},
		{name: "wrong version", data: strings.Replace(text, ProtocolVersion, "brie.guestsupervisor/v0", 1), want: "unsupported protocol version"},
		{name: "unknown operation", data: strings.Replace(text, `"operation":"prepare"`, `"operation":"pause"`, 1), want: "unsupported operation"},
		{name: "mismatched body", data: strings.Replace(text, `"operation":"prepare"`, `"operation":"abort"`, 1), want: "requires an abort body"},
		{name: "zero epoch", data: strings.Replace(text, `"epoch":3`, `"epoch":0`, 1), want: "epoch must be positive"},
		{name: "bad operation ID", data: strings.Replace(text, `"op-1"`, `"op/1"`, 1), want: "operation_id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeRequest([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeRequest error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRequestValidate(t *testing.T) {
	resume := func(mutate func(*ResumeRequest)) Request {
		request := resumeRequest("op-1", 1, testTime)
		mutate(request.Resume)
		return request
	}
	for _, tt := range []struct {
		name    string
		request Request
		want    string
	}{
		{name: "no body", request: Request{Version: ProtocolVersion, Operation: OperationPrepare}, want: "exactly one"},
		{name: "two bodies", request: func() Request {
			r := prepareRequest("op-1", 1, testTime)
			r.Abort = &AbortRequest{OperationID: "op-1"}
			return r
		}(), want: "exactly one"},
		{name: "prepare without body", request: Request{Version: ProtocolVersion, Operation: OperationPrepare, Abort: &AbortRequest{OperationID: "x"}}, want: "requires a prepare body"},
		{name: "resume without body", request: Request{Version: ProtocolVersion, Operation: OperationResume, Abort: &AbortRequest{OperationID: "x"}}, want: "requires a resume body"},
		{name: "prepare without deadline", request: prepareRequest("op-1", 1, time.Time{}), want: "deadline"},
		{name: "prepare bad epoch", request: prepareRequest("op-1", -1, testTime), want: "epoch"},
		{name: "prepare bad ID", request: prepareRequest("", 1, testTime), want: "operation_id"},
		{name: "resume bad ID", request: resume(func(r *ResumeRequest) { r.OperationID = "" }), want: "operation_id"},
		{name: "resume bad sandbox", request: resume(func(r *ResumeRequest) { r.SandboxUID = "sandbox" }), want: "sandbox_uid"},
		{name: "resume bad pod", request: resume(func(r *ResumeRequest) { r.PodUID = "pod" }), want: "pod_uid"},
		{name: "resume bad epoch", request: resume(func(r *ResumeRequest) { r.Epoch = 0 }), want: "epoch"},
		{name: "resume without clock", request: resume(func(r *ResumeRequest) { r.WallClock = time.Time{} }), want: "wall_clock"},
		{name: "resume short nonce", request: resume(func(r *ResumeRequest) { r.EntropyNonce = "00" }), want: "entropy_nonce"},
		{name: "abort bad ID", request: abortRequest(""), want: "operation_id"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want %q", err, tt.want)
			}
			if _, err := EncodeRequest(tt.request); err == nil {
				t.Fatal("EncodeRequest accepted an invalid request")
			}
		})
	}
	if (Request{}).OperationID() != "" {
		t.Fatal("empty request has an operation ID")
	}
}

func TestResponseValidateFor(t *testing.T) {
	point := &SafePoint{ID: "counter-1", ExternalEffectWatermark: 1}
	prepare := prepareRequest("op-1", 1, testTime)
	for _, tt := range []struct {
		name     string
		response Response
		request  Request
		want     string
	}{
		{name: "safe point", response: safePoint("op-1", *point), request: prepare},
		{name: "refusal", response: refusal("op-1", "busy"), request: prepare},
		{name: "ready", response: Response{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: "op-1"}, request: resumeRequest("op-1", 1, testTime)},
		{name: "aborted", response: Response{Version: ProtocolVersion, Outcome: OutcomeAborted, OperationID: "op-1"}, request: abortRequest("op-1")},
		{name: "wrong operation", response: safePoint("op-2", *point), request: prepare, want: "does not match"},
		{name: "wrong outcome", response: Response{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: "op-1"}, request: prepare, want: "cannot yield"},
		{name: "wrong version", response: Response{Version: "v0", Outcome: OutcomeReady, OperationID: "op-1"}, request: prepare, want: "protocol version"},
		{name: "bad ID", response: Response{Version: ProtocolVersion, Outcome: OutcomeReady}, request: prepare, want: "operation_id"},
		{name: "safe point without body", response: Response{Version: ProtocolVersion, Outcome: OutcomeSafePoint, OperationID: "op-1"}, request: prepare, want: "safe_point body"},
		{name: "safe point bad ID", response: safePoint("op-1", SafePoint{ID: "a b"}), request: prepare, want: "safe_point.id"},
		{name: "refusal without body", response: Response{Version: ProtocolVersion, Outcome: OutcomeRefusal, OperationID: "op-1"}, request: prepare, want: "refusal body"},
		{name: "refusal control bytes", response: refusal("op-1", "a\x1b"), request: prepare, want: "refusal.reason"},
		{name: "ready with body", response: Response{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: "op-1", SafePoint: point}, request: prepare, want: "must not carry"},
		{name: "unknown outcome", response: Response{Version: ProtocolVersion, Outcome: "paused", OperationID: "op-1"}, request: prepare, want: "unsupported outcome"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.response.ValidateFor(tt.request)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := EncodeResponse(tt.response)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := DecodeResponse(encoded); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateFor error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDecodeResponseRejects(t *testing.T) {
	for _, tt := range []struct {
		name string
		data string
		want string
	}{
		{name: "unknown field", data: `{"version":"brie.guestsupervisor/v1","outcome":"ready","operation_id":"op-1","extra":true}`, want: "unknown field"},
		{name: "invalid", data: `{"version":"brie.guestsupervisor/v1","outcome":"ready","operation_id":""}`, want: "invalid response"},
		{name: "negative watermark", data: `{"version":"brie.guestsupervisor/v1","outcome":"safe_point","operation_id":"op-1","safe_point":{"id":"a","external_effect_watermark":-1}}`, want: "decode response"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeResponse([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeResponse error = %v, want %q", err, tt.want)
			}
		})
	}
	if _, err := EncodeResponse(Response{}); err == nil {
		t.Fatal("EncodeResponse accepted an invalid response")
	}
}

func TestEncodeRejectsOversizedMessage(t *testing.T) {
	if _, err := encode(strings.Repeat("a", MaxMessageBytes)); err == nil {
		t.Fatal("encode accepted an oversized message")
	}
	if _, err := encode(func() {}); err == nil {
		t.Fatal("encode accepted an unsupported value")
	}
}

func FuzzDecodeRequest(f *testing.F) {
	for _, request := range []Request{prepareRequest("op-1", 3, testTime), resumeRequest("op-1", 4, testTime), abortRequest("op-1")} {
		encoded, err := EncodeRequest(request)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Add([]byte(`{"version":"brie.guestsupervisor/v1","operation":"abort","abort":{"operation_id":"a","operation_id":"b"}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		request, err := DecodeRequest(data)
		if err != nil {
			return
		}
		if err := request.Validate(); err != nil {
			t.Fatalf("decoded request is invalid: %v", err)
		}
		encoded, err := EncodeRequest(request)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("accepted non-canonical request %q: %v", data, err)
		}
		if !json.Valid(data) {
			t.Fatalf("accepted invalid JSON %q", data)
		}
	})
}

func FuzzDecodeResponse(f *testing.F) {
	for _, response := range []Response{
		safePoint("op-1", SafePoint{ID: "counter-7", ExternalEffectWatermark: 7}),
		refusal("op-1", "draining timed out"),
		{Version: ProtocolVersion, Outcome: OutcomeReady, OperationID: "op-1"},
	} {
		encoded, err := EncodeResponse(response)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		response, err := DecodeResponse(data)
		if err != nil {
			return
		}
		encoded, err := EncodeResponse(response)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatalf("accepted non-canonical response %q: %v", data, err)
		}
	})
}
