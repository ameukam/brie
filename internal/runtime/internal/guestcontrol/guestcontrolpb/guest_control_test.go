// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestcontrolpb

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestGeneratedMessageMethods(t *testing.T) {
	messages := []interface {
		proto.Message
		String() string
		Reset()
		ProtoMessage()
		Descriptor() ([]byte, []int)
	}{
		&ExecutionIdentity{SandboxUid: "sandbox", PodUid: "pod", RuntimeGeneration: "generation"},
		&GetInfoRequest{ProtocolVersion: 1, Identity: &ExecutionIdentity{SandboxUid: "sandbox"}},
		&GetInfoResponse{ProtocolVersion: 1, Identity: &ExecutionIdentity{SandboxUid: "sandbox"}},
	}
	for _, message := range messages {
		if message.String() == "" {
			t.Fatal("String() returned an empty string")
		}
		message.ProtoMessage()
		message.ProtoReflect().Descriptor()
		_, _ = message.Descriptor()
		message.Reset()
	}

	for _, message := range []proto.Message{
		&ExecutionIdentity{},
		&GetInfoRequest{},
		&GetInfoResponse{},
	} {
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := proto.Unmarshal(data, message); err != nil {
			t.Fatal(err)
		}
	}

	var nilIdentity *ExecutionIdentity
	if nilIdentity.String() == "" || nilIdentity.ProtoReflect().IsValid() {
		t.Fatal("nil ExecutionIdentity methods returned unexpected values")
	}
	if nilIdentity.GetSandboxUid() != "" || nilIdentity.GetPodUid() != "" || nilIdentity.GetRuntimeGeneration() != "" {
		t.Fatal("nil ExecutionIdentity getters returned non-empty values")
	}
	_, _ = nilIdentity.Descriptor()

	var nilRequest *GetInfoRequest
	if nilRequest.String() == "" || nilRequest.ProtoReflect().IsValid() {
		t.Fatal("nil GetInfoRequest methods returned unexpected values")
	}
	if nilRequest.GetProtocolVersion() != 0 || nilRequest.GetIdentity() != nil {
		t.Fatal("nil GetInfoRequest getters returned non-zero values")
	}
	_, _ = nilRequest.Descriptor()

	var nilResponse *GetInfoResponse
	if nilResponse.String() == "" || nilResponse.ProtoReflect().IsValid() {
		t.Fatal("nil GetInfoResponse methods returned unexpected values")
	}
	if nilResponse.GetProtocolVersion() != 0 || nilResponse.GetIdentity() != nil {
		t.Fatal("nil GetInfoResponse getters returned non-zero values")
	}
	_, _ = nilResponse.Descriptor()

	identity := &ExecutionIdentity{SandboxUid: "sandbox", PodUid: "pod", RuntimeGeneration: "generation"}
	if identity.GetSandboxUid() != "sandbox" || identity.GetPodUid() != "pod" || identity.GetRuntimeGeneration() != "generation" {
		t.Fatal("ExecutionIdentity getters returned unexpected values")
	}
	request := &GetInfoRequest{ProtocolVersion: 1, Identity: identity}
	if request.GetProtocolVersion() != 1 || request.GetIdentity() != identity {
		t.Fatal("GetInfoRequest getters returned unexpected values")
	}
	response := &GetInfoResponse{ProtocolVersion: 1, Identity: identity}
	if response.GetProtocolVersion() != 1 || response.GetIdentity() != identity {
		t.Fatal("GetInfoResponse getters returned unexpected values")
	}
	UnimplementedGuestSupervisorServer{}.mustEmbedUnimplementedGuestSupervisorServer()
	if _, err := (UnimplementedGuestSupervisorServer{}).GetInfo(context.Background(), request); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unimplemented GetInfo() error = %v", err)
	}
}

func TestGeneratedGuestSupervisorGRPC(t *testing.T) {
	for _, withInterceptor := range []bool{false, true} {
		t.Run(map[bool]string{false: "without interceptor", true: "with interceptor"}[withInterceptor], func(t *testing.T) {
			listener := bufconn.Listen(1024 * 1024)
			var server *grpc.Server
			if withInterceptor {
				server = grpc.NewServer(grpc.UnaryInterceptor(func(
					ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
				) (any, error) {
					if info.FullMethod != GuestSupervisor_GetInfo_FullMethodName {
						t.Errorf("intercepted method = %q", info.FullMethod)
					}
					return handler(ctx, request)
				}))
			} else {
				server = grpc.NewServer()
			}
			RegisterGuestSupervisorServer(server, testGuestSupervisor{})
			serveDone := make(chan error, 1)
			go func() { serveDone <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
				select {
				case err := <-serveDone:
					if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
						t.Errorf("Serve() error = %v", err)
					}
				case <-time.After(time.Second):
					t.Error("gRPC server did not stop")
				}
			})

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := grpc.NewClient(
				"passthrough:///bufconn",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			response, err := NewGuestSupervisorClient(conn).GetInfo(ctx, &GetInfoRequest{
				ProtocolVersion: 1,
				Identity:        &ExecutionIdentity{SandboxUid: "sandbox"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if response.GetProtocolVersion() != 1 || response.GetIdentity().GetSandboxUid() != "sandbox" {
				t.Fatalf("GetInfo() = %+v", response)
			}
		})
	}
}

type testGuestSupervisor struct {
	UnimplementedGuestSupervisorServer
}

func (testGuestSupervisor) GetInfo(_ context.Context, request *GetInfoRequest) (*GetInfoResponse, error) {
	return &GetInfoResponse{
		ProtocolVersion: request.GetProtocolVersion(),
		Identity:        request.GetIdentity(),
	}, nil
}
