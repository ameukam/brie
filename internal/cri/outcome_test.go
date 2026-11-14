// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestDefiniteRejection(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		err      error
		rejected bool
	}{
		{"nil", nil, false},
		{"invalid-argument", status.Error(codes.InvalidArgument, "bad config"), true},
		{"not-found", status.Error(codes.NotFound, "no image"), true},
		{"failed-precondition", status.Error(codes.FailedPrecondition, "sandbox not ready"), true},
		{"permission-denied", status.Error(codes.PermissionDenied, "denied"), false},
		{"out-of-range", status.Error(codes.OutOfRange, "too large"), true},
		{"wrapped", fmt.Errorf("CRI RunPodSandbox: %w", status.Error(codes.InvalidArgument, "bad")), true},
		{"lost-reply", status.Error(codes.Unavailable, "connection reset"), false},
		{"server-deadline", status.Error(codes.DeadlineExceeded, "deadline"), false},
		{"canceled", status.Error(codes.Canceled, "canceled"), false},
		{"unknown", status.Error(codes.Unknown, "partial create"), false},
		{"internal", status.Error(codes.Internal, "decode"), false},
		{"resource-exhausted", status.Error(codes.ResourceExhausted, "reply too large"), false},
		{"unimplemented", status.Error(codes.Unimplemented, "method"), false},
		{"already-exists", status.Error(codes.AlreadyExists, "name reserved"), false},
		{"aborted", status.Error(codes.Aborted, "conflict"), false},
		{"data-loss", status.Error(codes.DataLoss, "lost"), false},
		{"local-deadline", errors.Join(status.Error(codes.InvalidArgument, "bad"), context.DeadlineExceeded), false},
		{"local-cancel", errors.Join(status.Error(codes.NotFound, "gone"), context.Canceled), false},
		{"plain", errors.New("validation"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			code, rejected := DefiniteRejection(test.err)
			if rejected != test.rejected {
				t.Fatalf("DefiniteRejection(%v) = %v, want %v", test.err, rejected, test.rejected)
			}
			if rejected != IsRejectionCode(code.String()) {
				t.Fatalf("IsRejectionCode(%s) disagrees with DefiniteRejection", code)
			}
		})
	}
	if IsRejectionCode("") || IsRejectionCode("Unavailable") || IsRejectionCode("invalidargument") {
		t.Fatal("IsRejectionCode accepted a code outside the rejection set")
	}
}

// TestDefiniteRejectionThroughClient checks that a server rejection keeps its
// code through the client, while a reply lost to the client deadline does not
// count as a rejection even when the server later answers with one.
func TestDefiniteRejectionThroughClient(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	socket := startRuntime(t, &fakeRuntime{
		version: func(_ context.Context, request *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			if request.Version == "slow" {
				<-release
			}
			return nil, status.Error(codes.InvalidArgument, "rejected")
		},
	})
	defer close(release)
	client := newClient(t, socket, time.Second)
	_, err := invoke(t.Context(), client, "Version", func(ctx context.Context) (*runtimeapi.VersionResponse, error) {
		return client.runtime.Version(ctx, &runtimeapi.VersionRequest{Version: "0.1.0"})
	})
	if code, rejected := DefiniteRejection(err); !rejected || code != codes.InvalidArgument {
		t.Fatalf("server rejection = %v, %v (%v)", code, rejected, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = invoke(ctx, client, "Version", func(ctx context.Context) (*runtimeapi.VersionResponse, error) {
		return client.runtime.Version(ctx, &runtimeapi.VersionRequest{Version: "slow"})
	})
	if _, rejected := DefiniteRejection(err); rejected || err == nil {
		t.Fatalf("lost reply counted as a rejection: %v", err)
	}
}

func TestNewConcurrent(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{-1, 0, 65} {
		if _, err := NewConcurrent("/run/cri.sock", time.Second, concurrency); err == nil {
			t.Fatalf("concurrency %d accepted", concurrency)
		}
	}
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	socket := startRuntime(t, &fakeRuntime{
		version: func(ctx context.Context, _ *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
			entered <- struct{}{}
			select {
			case <-release:
				return goodVersion(), nil
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		},
	})
	client, err := NewConcurrent(socket, 3*time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	done := make(chan error, 2)
	for range 2 {
		go func() { _, err := client.Version(t.Context()); done <- err }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("two calls did not run concurrently")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	if _, err := client.Version(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third call error = %v, want queued deadline", err)
	}
	if len(entered) != 0 {
		t.Fatal("third call exceeded the concurrency bound")
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// serveRawHTTP2 answers every request stream on a Unix socket with respond,
// without running any gRPC handler. It stands in for a transport failure
// between the client and the runtime.
func serveRawHTTP2(t *testing.T, respond func(*http2.Framer, uint32) error) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "raw.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(conn, preface); err != nil {
					return
				}
				framer := http2.NewFramer(conn, conn)
				if framer.WriteSettings() != nil {
					return
				}
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if !frame.IsAck() && framer.WriteSettingsAck() != nil {
							return
						}
					case *http2.HeadersFrame:
						if respond(framer, frame.StreamID) != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return socket
}

// TestDefiniteRejectionExcludesTransportPermissionDenied is a regression test:
// grpc-go reports an HTTP/2 INADEQUATE_SECURITY reset and an HTTP 403 reply as
// PermissionDenied although no runtime handler answered, so neither may count
// as evidence that a create call created nothing.
func TestDefiniteRejectionExcludesTransportPermissionDenied(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		respond func(*http2.Framer, uint32) error
	}{
		{"inadequate-security-reset", func(framer *http2.Framer, stream uint32) error {
			return framer.WriteRSTStream(stream, http2.ErrCodeInadequateSecurity)
		}},
		{"http-403", func(framer *http2.Framer, stream uint32) error {
			var block bytes.Buffer
			if err := hpack.NewEncoder(&block).WriteField(hpack.HeaderField{Name: ":status", Value: "403"}); err != nil {
				return err
			}
			return framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: block.Bytes(),
				EndHeaders: true, EndStream: true})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := newClient(t, serveRawHTTP2(t, test.respond), time.Second)
			_, err := client.RunPodSandbox(t.Context(), &runtimeapi.PodSandboxConfig{
				Metadata: &runtimeapi.PodSandboxMetadata{Name: "app", Namespace: "default", Uid: "app-uid"},
			}, "kata-fc")
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("transport failure = %v, want grpc-go's PermissionDenied", err)
			}
			if _, rejected := DefiniteRejection(err); rejected {
				t.Fatalf("transport-generated %v counted as a runtime rejection", err)
			}
		})
	}
}
