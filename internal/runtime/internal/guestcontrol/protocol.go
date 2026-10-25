// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package guestcontrol defines the bounded control stream shared by Brie runtime
// owners and the guest supervisor. It does not execute workloads.
package guestcontrol

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ProtocolVersion is the current guest-control wire protocol version.
const ProtocolVersion = 1

// MaxMessageBytes bounds the JSON payload in one protocol frame.
const MaxMessageBytes = 4096

// MaxIdentityBytes bounds each execution identity component.
const MaxIdentityBytes = 256

// Identity binds a guest-control message to one execution attempt.
type Identity struct {
	SandboxUID        string `json:"sandbox_uid"`
	PodUID            string `json:"pod_uid"`
	RuntimeGeneration string `json:"runtime_generation"`
}

// Request is one versioned guest-control operation.
type Request struct {
	Version   int      `json:"version"`
	RequestID string   `json:"request_id"`
	Operation string   `json:"operation"`
	Identity  Identity `json:"identity"`
}

// Response reports the result of a single guest-control operation.
type Response struct {
	Version   int      `json:"version"`
	RequestID string   `json:"request_id"`
	Operation string   `json:"operation"`
	Identity  Identity `json:"identity"`
	Status    string   `json:"status"`
	ErrorCode string   `json:"error_code,omitempty"`
}

// NewRequest creates the initial, non-secret protocol probe for an execution.
func NewRequest(identity Identity) (Request, error) {
	if err := identity.validate(); err != nil {
		return Request{}, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Request{}, fmt.Errorf("generate guest-control request ID: %w", err)
	}
	return Request{
		Version: ProtocolVersion, RequestID: hex.EncodeToString(id[:]),
		Operation: "ping", Identity: identity,
	}, nil
}

// Ping sends one bounded health probe on an already-connected guest control stream.
func Ping(ctx context.Context, conn net.Conn, identity Identity) (Response, error) {
	if conn == nil {
		return Response{}, errors.New("guest-control connection is nil")
	}
	request, err := NewRequest(identity)
	if err != nil {
		return Response{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return Response{}, fmt.Errorf("set guest-control deadline: %w", err)
		}
	}
	if err := writeFrame(conn, request); err != nil {
		return Response{}, fmt.Errorf("write guest-control request: %w", errors.Join(err, ctx.Err()))
	}
	var response Response
	if err := readFrame(conn, &response); err != nil {
		return Response{}, fmt.Errorf("read guest-control response: %w", errors.Join(err, ctx.Err()))
	}
	if response.Version != ProtocolVersion || response.RequestID != request.RequestID ||
		response.Operation != request.Operation || response.Identity != identity {
		return Response{}, errors.New("guest-control response does not match request")
	}
	if response.Status != "ok" || response.ErrorCode != "" {
		return Response{}, fmt.Errorf("guest-control ping rejected: %s", response.ErrorCode)
	}
	if err := ctx.Err(); err != nil {
		return Response{}, fmt.Errorf("guest-control ping: %w", err)
	}
	return response, nil
}

// ServeConn handles exactly one bounded protocol request on a guest supervisor
// stream. The caller must authenticate/authorize the stream before invoking it.
func ServeConn(ctx context.Context, conn net.Conn, expected Identity) (result error) {
	if conn == nil {
		return errors.New("guest-control connection is nil")
	}
	defer func() { result = errors.Join(result, conn.Close()) }()
	if err := expected.validate(); err != nil {
		return fmt.Errorf("validate guest-control identity: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set guest-control deadline: %w", err)
		}
	}
	var request Request
	if err := readFrame(conn, &request); err != nil {
		return fmt.Errorf("read guest-control request: %w", err)
	}
	response := Response{
		Version: ProtocolVersion, RequestID: request.RequestID,
		Operation: request.Operation, Identity: expected,
	}
	switch {
	case request.Version != ProtocolVersion:
		response.Status, response.ErrorCode = "error", "unsupported_version"
	case !validRequestID(request.RequestID):
		return errors.New("invalid guest-control request ID")
	case request.Identity != expected:
		response.Status, response.ErrorCode = "error", "execution_identity_mismatch"
	case request.Operation != "ping":
		response.Status, response.ErrorCode = "error", "unsupported_operation"
	default:
		response.Status = "ok"
	}
	if err := writeFrame(conn, response); err != nil {
		return fmt.Errorf("write guest-control response: %w", err)
	}
	return nil
}

// DialFirecrackerVsock connects to Firecracker's host Unix socket and performs
// its CONNECT/OK preamble before returning the guest control stream.
func DialFirecrackerVsock(ctx context.Context, socketPath string, guestPort uint32) (net.Conn, error) {
	if !utf8.ValidString(socketPath) || !strings.HasPrefix(socketPath, "/") ||
		strings.ContainsRune(socketPath, '\x00') || len(socketPath) > 107 {
		return nil, errors.New("invalid Firecracker vsock Unix socket path")
	}
	if guestPort == 0 {
		return nil, errors.New("guest vsock port must be nonzero")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("connect to Firecracker vsock socket: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("set Firecracker vsock deadline: %w", err)
		}
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", guestPort); err != nil {
		return nil, fmt.Errorf("request guest vsock connection: %w", err)
	}
	line, err := readBoundedLine(conn, 32)
	if err != nil {
		return nil, fmt.Errorf("read Firecracker vsock acknowledgement: %w", err)
	}
	fields := strings.Split(strings.TrimSuffix(line, "\n"), " ")
	if len(fields) != 2 || fields[0] != "OK" {
		return nil, fmt.Errorf("invalid Firecracker vsock acknowledgement %q", line)
	}
	hostPort, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil || hostPort == 0 {
		return nil, fmt.Errorf("invalid Firecracker host vsock port %q", fields[1])
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect to guest vsock: %w", err)
	}
	ok = true
	return conn, nil
}

func readBoundedLine(reader io.Reader, limit int) (string, error) {
	var line [32]byte
	if limit > len(line) {
		limit = len(line)
	}
	for i := 0; i < limit; i++ {
		if _, err := io.ReadFull(reader, line[i:i+1]); err != nil {
			return "", err
		}
		if line[i] == '\n' {
			return string(line[:i+1]), nil
		}
	}
	return "", errors.New("vsock acknowledgement exceeds limit")
}

func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode guest-control message: %w", err)
	}
	if len(data) == 0 || len(data) > MaxMessageBytes {
		return fmt.Errorf("guest-control message size %d is outside 1..%d", len(data), MaxMessageBytes)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, data)
}

func readFrame(r io.Reader, target any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return fmt.Errorf("read frame header: %w", err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxMessageBytes {
		return fmt.Errorf("guest-control frame size %d is outside 1..%d", size, MaxMessageBytes)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return fmt.Errorf("read frame body: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode guest-control frame: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("guest-control frame contains multiple JSON values")
		}
		return fmt.Errorf("check guest-control frame trailer: %w", err)
	}
	return nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return errors.New("writer returned an invalid byte count")
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (identity Identity) validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "Sandbox UID", value: identity.SandboxUID},
		{name: "Pod UID", value: identity.PodUID},
		{name: "runtime generation", value: identity.RuntimeGeneration},
	} {
		name, value := field.name, field.value
		if len(value) == 0 || len(value) > MaxIdentityBytes || !utf8.ValidString(value) {
			return fmt.Errorf("guest-control %s must be 1..%d bytes of UTF-8", name, MaxIdentityBytes)
		}
		for _, r := range value {
			if r < 0x20 || r == 0x7f {
				return fmt.Errorf("guest-control %s contains a control character", name)
			}
		}
	}
	return nil
}

func validRequestID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
