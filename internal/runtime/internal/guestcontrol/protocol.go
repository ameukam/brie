// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package guestcontrol defines the gRPC control stream shared by Brie runtime
// owners and the guest supervisor. It does not execute workloads.
package guestcontrol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	guestcontrolpb "github.com/ameukam/brie/internal/runtime/internal/guestcontrol/guestcontrolpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ProtocolVersion is the current guest-control API version.
const ProtocolVersion uint32 = 1

// MaxMessageBytes bounds gRPC messages on the guest-control stream.
const MaxMessageBytes = 4096

// MaxIdentityBytes bounds each execution identity component.
const MaxIdentityBytes = 256

// Identity binds a guest-control request to one execution attempt.
type Identity struct {
	SandboxUID        string
	PodUID            string
	RuntimeGeneration string
}

// NewGRPCServer registers the read-only guest supervisor API with bounded
// messages. Vsock connectivity associates a peer with a VM but does not
// authenticate that peer.
func NewGRPCServer(identity Identity) (*grpc.Server, error) {
	if err := identity.validate(); err != nil {
		return nil, fmt.Errorf("validate guest-control identity: %w", err)
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
	)
	guestcontrolpb.RegisterGuestSupervisorServer(server, &guestSupervisor{identity: identity})
	return server, nil
}

// Dial establishes a bounded, plaintext gRPC connection over Firecracker's
// host vsock socket. Callers must close the returned connection.
func Dial(ctx context.Context, socketPath string, guestPort uint32) (*grpc.ClientConn, error) {
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return DialFirecrackerVsock(ctx, socketPath, guestPort)
	}
	conn, err := grpc.NewClient(
		"passthrough:///guest-control",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageBytes),
			grpc.MaxCallSendMsgSize(MaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dial guest-control gRPC service: %w", err)
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if state == connectivity.Shutdown {
			return nil, errors.Join(errors.New("guest-control gRPC connection shut down"), conn.Close())
		}
		if !conn.WaitForStateChange(ctx, state) {
			return nil, fmt.Errorf(
				"dial guest-control gRPC service: %w",
				errors.Join(ctx.Err(), conn.Close()),
			)
		}
	}
}

// GetInfo asks the guest supervisor to verify the execution identity and
// validates the returned identity and protocol version.
func GetInfo(ctx context.Context, conn grpc.ClientConnInterface, identity Identity) (Identity, error) {
	if conn == nil {
		return Identity{}, errors.New("guest-control gRPC connection is nil")
	}
	if err := identity.validate(); err != nil {
		return Identity{}, err
	}
	response, err := guestcontrolpb.NewGuestSupervisorClient(conn).GetInfo(ctx, &guestcontrolpb.GetInfoRequest{
		ProtocolVersion: ProtocolVersion,
		Identity: &guestcontrolpb.ExecutionIdentity{
			SandboxUid:        identity.SandboxUID,
			PodUid:            identity.PodUID,
			RuntimeGeneration: identity.RuntimeGeneration,
		},
	})
	if err != nil {
		return Identity{}, fmt.Errorf("request guest-control info: %w", err)
	}
	if response.GetProtocolVersion() != ProtocolVersion || response.GetIdentity() == nil {
		return Identity{}, errors.New("guest-control response has an invalid version or missing identity")
	}
	responseIdentity := Identity{
		SandboxUID:        response.GetIdentity().GetSandboxUid(),
		PodUID:            response.GetIdentity().GetPodUid(),
		RuntimeGeneration: response.GetIdentity().GetRuntimeGeneration(),
	}
	if responseIdentity != identity {
		return Identity{}, errors.New("guest-control response identity does not match request")
	}
	return responseIdentity, nil
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
		return nil, fmt.Errorf("read Firecracker vsock acknowledgement: %w", errors.Join(err, ctx.Err()))
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
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("clear Firecracker vsock handshake deadline: %w", err)
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

type guestSupervisor struct {
	guestcontrolpb.UnimplementedGuestSupervisorServer
	identity Identity
}

func (s *guestSupervisor) GetInfo(ctx context.Context, request *guestcontrolpb.GetInfoRequest) (*guestcontrolpb.GetInfoResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || request.GetIdentity() == nil {
		return nil, status.Error(codes.InvalidArgument, "execution identity is required")
	}
	if request.GetProtocolVersion() != ProtocolVersion {
		return nil, status.Errorf(codes.FailedPrecondition, "unsupported guest-control protocol version %d", request.GetProtocolVersion())
	}
	requestIdentity := Identity{
		SandboxUID:        request.GetIdentity().GetSandboxUid(),
		PodUID:            request.GetIdentity().GetPodUid(),
		RuntimeGeneration: request.GetIdentity().GetRuntimeGeneration(),
	}
	if err := requestIdentity.validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid execution identity: %v", err)
	}
	if requestIdentity != s.identity {
		return nil, status.Error(codes.FailedPrecondition, "execution identity does not match guest")
	}
	return &guestcontrolpb.GetInfoResponse{
		ProtocolVersion: ProtocolVersion,
		Identity: &guestcontrolpb.ExecutionIdentity{
			SandboxUid:        s.identity.SandboxUID,
			PodUid:            s.identity.PodUID,
			RuntimeGeneration: s.identity.RuntimeGeneration,
		},
	}, nil
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
