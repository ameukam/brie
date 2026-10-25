// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestcontrol

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testIdentity() Identity {
	return Identity{SandboxUID: "sandbox-1", PodUID: "pod-1", RuntimeGeneration: "generation-1"}
}

func TestGuestControlPing(t *testing.T) {
	host, guest := net.Pipe()
	closeTestConn(t, host)
	closeTestConn(t, guest)
	done := make(chan error, 1)
	go func() {
		done <- ServeConn(t.Context(), guest, testIdentity())
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	response, err := Ping(ctx, host, testIdentity())
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != "ok" || response.Identity != testIdentity() ||
		response.Version != ProtocolVersion || response.Operation != "ping" ||
		!validRequestID(response.RequestID) {
		t.Fatalf("unexpected response: %+v", response)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestGuestControlRejectsRequests(t *testing.T) {
	for _, tt := range []struct {
		name      string
		version   int
		requestID string
		op        string
		identity  Identity
		status    string
		code      string
		wantErr   string
	}{
		{name: "unsupported version", version: 99, requestID: strings.Repeat("a", 32), op: "ping", identity: testIdentity(), status: "error", code: "unsupported_version"},
		{name: "wrong identity", version: ProtocolVersion, requestID: strings.Repeat("a", 32), op: "ping", identity: Identity{SandboxUID: "other", PodUID: "pod-1", RuntimeGeneration: "generation-1"}, status: "error", code: "execution_identity_mismatch"},
		{name: "unsupported operation", version: ProtocolVersion, requestID: strings.Repeat("a", 32), op: "exec", identity: testIdentity(), status: "error", code: "unsupported_operation"},
		{name: "invalid request id", version: ProtocolVersion, requestID: "bad", op: "ping", identity: testIdentity(), wantErr: "request ID"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			host, guest := net.Pipe()
			closeTestConn(t, host)
			closeTestConn(t, guest)
			done := make(chan error, 1)
			go func() { done <- ServeConn(t.Context(), guest, testIdentity()) }()
			request := Request{
				Version: tt.version, RequestID: tt.requestID, Operation: tt.op, Identity: tt.identity,
			}
			if err := writeFrame(host, request); err != nil {
				t.Fatal(err)
			}
			var response Response
			err := readFrame(host, &response)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("invalid request was accepted")
				}
				if serveErr := <-done; serveErr == nil || !strings.Contains(serveErr.Error(), tt.wantErr) {
					t.Fatalf("ServeConn() = %v, want %s", serveErr, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if response.Status != tt.status || response.ErrorCode != tt.code ||
				response.RequestID != tt.requestID {
				t.Fatalf("response = %+v", response)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGuestControlFrameValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{name: "empty header", data: nil},
		{name: "truncated header", data: []byte{0}},
		{name: "zero length", data: []byte{0, 0, 0, 0}},
		{name: "oversized", data: []byte{0, 0, 0x10, 1}},
		{name: "truncated body", data: []byte{0, 0, 0, 2, '{'}},
		{name: "invalid JSON", data: frameBytes([]byte("{"))},
		{name: "unknown field", data: frameBytes([]byte(`{"unknown":true}`))},
		{name: "multiple values", data: frameBytes([]byte(`{} {}`))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var request Request
			if err := readFrame(strings.NewReader(string(tt.data)), &request); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
}

func TestDialFirecrackerVsock(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "fc-vsock.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	closeTestListener(t, listener)
	identity := testIdentity()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		closeTestConn(t, conn)
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			serverDone <- err
			return
		}
		if line != "CONNECT 52\n" {
			serverDone <- fmt.Errorf("unexpected connect line %q", line)
			return
		}
		if _, err := io.WriteString(conn, "OK 1073741824\n"); err != nil {
			serverDone <- err
			return
		}
		serverDone <- ServeConn(t.Context(), conn, identity)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := DialFirecrackerVsock(ctx, socket, 52)
	if err != nil {
		t.Fatal(err)
	}
	closeTestConn(t, conn)
	if _, err := Ping(ctx, conn, identity); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestDialFirecrackerVsockRejectsAcknowledgement(t *testing.T) {
	for _, ack := range []string{"ERROR unavailable\n", "OK 0\n", "OK 4294967296\n", "OK nope\n", "OK 3 extra\n"} {
		t.Run(strings.TrimSpace(ack), func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "fc.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			closeTestListener(t, listener)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				closeTestConn(t, conn)
				_, _ = bufio.NewReader(conn).ReadString('\n')
				_, _ = io.WriteString(conn, ack)
			}()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if conn, err := DialFirecrackerVsock(ctx, socket, 52); err == nil {
				_ = conn.Close()
				t.Fatal("invalid acknowledgement accepted")
			}
		})
	}
}

func TestGuestControlValidation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		identity Identity
		ok       bool
	}{
		{name: "valid", identity: testIdentity(), ok: true},
		{name: "missing sandbox", identity: Identity{PodUID: "pod", RuntimeGeneration: "generation"}},
		{name: "missing pod", identity: Identity{SandboxUID: "sandbox", RuntimeGeneration: "generation"}},
		{name: "missing generation", identity: Identity{SandboxUID: "sandbox", PodUID: "pod"}},
		{name: "oversized", identity: Identity{SandboxUID: strings.Repeat("x", MaxIdentityBytes+1), PodUID: "pod", RuntimeGeneration: "generation"}},
		{name: "control character", identity: Identity{SandboxUID: "sandbox\n", PodUID: "pod", RuntimeGeneration: "generation"}},
		{name: "invalid UTF-8", identity: Identity{SandboxUID: "sandbox", PodUID: string([]byte{0xff}), RuntimeGeneration: "generation"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRequest(tt.identity)
			if (err == nil) != tt.ok {
				t.Fatalf("NewRequest() error = %v", err)
			}
		})
	}
	if _, err := NewRequest(testIdentity()); err != nil {
		t.Fatal(err)
	}
}

func TestPingCancellation(t *testing.T) {
	host, peer := net.Pipe()
	closeTestConn(t, host)
	closeTestConn(t, peer)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := Ping(ctx, host, testIdentity())
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("Ping() = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Ping did not stop after cancellation")
	}
}

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, '{', '}'})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{0, 0, 0x10, 1})
	f.Fuzz(func(_ *testing.T, data []byte) {
		if len(data) > MaxMessageBytes+4 {
			return
		}
		var request Request
		_ = readFrame(strings.NewReader(string(data)), &request)
	})
}

func closeTestConn(t *testing.T, conn net.Conn) {
	t.Helper()
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
}

func closeTestListener(t *testing.T, listener net.Listener) {
	t.Helper()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
}

func frameBytes(data []byte) []byte {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	return append(header[:], data...)
}
