// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestcontrol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	guestcontrolpb "github.com/ameukam/brie/internal/runtime/internal/guestcontrol/guestcontrolpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
)

func testIdentity() Identity {
	return Identity{SandboxUID: "sandbox-1", PodUID: "pod-1", RuntimeGeneration: "generation-1"}
}

func TestGuestControlGRPCOverFirecrackerVsock(t *testing.T) {
	identity := testIdentity()
	socketPath := startFirecrackerGRPC(t, identity)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	conn, err := Dial(ctx, socketPath, 52)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})

	got, err := GetInfo(ctx, conn, identity)
	if err != nil {
		t.Fatal(err)
	}
	if got != identity {
		t.Fatalf("GetInfo() = %+v, want %+v", got, identity)
	}

	client := guestcontrolpb.NewGuestSupervisorClient(conn)
	for _, tt := range []struct {
		name     string
		version  uint32
		identity *guestcontrolpb.ExecutionIdentity
		wantCode codes.Code
	}{
		{
			name: "unsupported version", version: ProtocolVersion + 1,
			identity: protoIdentity(identity), wantCode: codes.FailedPrecondition,
		},
		{
			name: "mismatched identity", version: ProtocolVersion,
			identity: protoIdentity(Identity{SandboxUID: "other", PodUID: identity.PodUID, RuntimeGeneration: identity.RuntimeGeneration}),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "invalid identity", version: ProtocolVersion,
			identity: &guestcontrolpb.ExecutionIdentity{PodUid: identity.PodUID, RuntimeGeneration: identity.RuntimeGeneration},
			wantCode: codes.InvalidArgument,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.GetInfo(ctx, &guestcontrolpb.GetInfoRequest{
				ProtocolVersion: tt.version,
				Identity:        tt.identity,
			})
			if status.Code(err) != tt.wantCode {
				t.Fatalf("GetInfo() error = %v, want gRPC code %s", err, tt.wantCode)
			}
		})
	}

	oversized := &guestcontrolpb.GetInfoRequest{
		ProtocolVersion: ProtocolVersion,
		Identity:        protoIdentity(identity),
	}
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, make([]byte, MaxMessageBytes))
	oversized.ProtoReflect().SetUnknown(unknown)
	if _, err := client.GetInfo(ctx, oversized); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized request error = %v, want gRPC code %s", err, codes.ResourceExhausted)
	}

	canceled, cancelRequest := context.WithCancel(t.Context())
	cancelRequest()
	if _, err := client.GetInfo(canceled, &guestcontrolpb.GetInfoRequest{
		ProtocolVersion: ProtocolVersion,
		Identity:        protoIdentity(identity),
	}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled request error = %v, want gRPC code %s", err, codes.Canceled)
	}
}

func TestIdentityValidation(t *testing.T) {
	for _, identity := range []Identity{
		{},
		{SandboxUID: strings.Repeat("x", MaxIdentityBytes+1), PodUID: "pod", RuntimeGeneration: "generation"},
		{SandboxUID: "sandbox\n", PodUID: "pod", RuntimeGeneration: "generation"},
		{SandboxUID: "sandbox", PodUID: string([]byte{0xff}), RuntimeGeneration: "generation"},
	} {
		if err := identity.validate(); err == nil {
			t.Fatalf("identity %+v unexpectedly validated", identity)
		}
	}
	if err := testIdentity().validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGRPCServer(Identity{}); err == nil {
		t.Fatal("NewGRPCServer accepted an invalid identity")
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
			t.Cleanup(func() { _ = listener.Close() })
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				_, _ = bufio.NewReader(conn).ReadString('\n')
				_, _ = fmt.Fprint(conn, ack)
			}()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if conn, err := DialFirecrackerVsock(ctx, socket, 52); err == nil {
				_ = conn.Close()
				t.Fatal("invalid acknowledgement accepted")
			}
			select {
			case <-serverDone:
			case <-time.After(time.Second):
				t.Fatal("mock Firecracker server did not stop")
			}
		})
	}
}

func TestDialHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := Dial(ctx, filepath.Join(t.TempDir(), "missing-vsock.sock"), 52)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial() error = %v, want context deadline", err)
	}
}

func TestReadBoundedLine(t *testing.T) {
	for _, tt := range []struct {
		name    string
		input   string
		limit   int
		want    string
		wantErr bool
	}{
		{name: "line", input: "OK\n", limit: 3, want: "OK\n"},
		{name: "truncated", input: "OK", limit: 3, wantErr: true},
		{name: "too long", input: "OKAY\n", limit: 3, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readBoundedLine(strings.NewReader(tt.input), tt.limit)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Fatalf("readBoundedLine() = %q, %v; want %q, error %t", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestDialFirecrackerVsockValidatesArguments(t *testing.T) {
	ctx := t.Context()
	for _, tt := range []struct {
		name       string
		socketPath string
		guestPort  uint32
	}{
		{name: "relative path", socketPath: "relative", guestPort: 52},
		{name: "empty path", socketPath: "", guestPort: 52},
		{name: "zero port", socketPath: "/tmp/vsock.sock"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := DialFirecrackerVsock(ctx, tt.socketPath, tt.guestPort); err == nil {
				t.Fatal("invalid vsock arguments accepted")
			}
		})
	}
}

func FuzzIdentityValidation(f *testing.F) {
	f.Add("sandbox-1")
	f.Add("sandbox\n")
	f.Add(strings.Repeat("x", MaxIdentityBytes+1))
	f.Fuzz(func(_ *testing.T, sandboxUID string) {
		_ = (Identity{
			SandboxUID: sandboxUID, PodUID: "pod", RuntimeGeneration: "generation",
		}).validate()
	})
}

func startFirecrackerGRPC(t *testing.T, identity Identity) string {
	t.Helper()
	socketPath := filepath.Join(t.TempDir(), "fc-vsock.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewGRPCServer(identity)
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			serverDone <- err
			return
		}
		if line != "CONNECT 52\n" {
			serverDone <- fmt.Errorf("unexpected Firecracker CONNECT line %q", line)
			return
		}
		if _, err := fmt.Fprint(conn, "OK 1073741824\n"); err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Serve(&singleConnListener{
			conn: conn, closed: make(chan struct{}),
		})
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-serverDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("guest-control server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("guest-control server did not stop")
		}
	})
	return socketPath
}

func protoIdentity(identity Identity) *guestcontrolpb.ExecutionIdentity {
	return &guestcontrolpb.ExecutionIdentity{
		SandboxUid:        identity.SandboxUID,
		PodUid:            identity.PodUID,
		RuntimeGeneration: identity.RuntimeGeneration,
	}
}

type singleConnListener struct {
	conn      net.Conn
	closed    chan struct{}
	mu        sync.Mutex
	accepted  bool
	closeOnce sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.accepted {
		l.accepted = true
		conn := l.conn
		l.mu.Unlock()
		return conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}

func (l *singleConnListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *singleConnListener) Addr() net.Addr {
	return &net.UnixAddr{Name: "mock-vsock", Net: "unix"}
}
