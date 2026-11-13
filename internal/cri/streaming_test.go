// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type streamingRuntime struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	url      string
	failure  error
	output   []byte
	exec     *runtimeapi.ExecRequest
	attach   *runtimeapi.AttachRequest
	forward  *runtimeapi.PortForwardRequest
	execSync *runtimeapi.ExecSyncRequest
	reopened string
}

func (s *streamingRuntime) Exec(_ context.Context, request *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	s.exec = request
	return &runtimeapi.ExecResponse{Url: s.url}, s.failure
}

func (s *streamingRuntime) Attach(_ context.Context, request *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	s.attach = request
	return &runtimeapi.AttachResponse{Url: s.url}, s.failure
}

func (s *streamingRuntime) PortForward(_ context.Context, request *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	s.forward = request
	return &runtimeapi.PortForwardResponse{Url: s.url}, s.failure
}

func (s *streamingRuntime) ExecSync(_ context.Context, request *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	s.execSync = request
	return &runtimeapi.ExecSyncResponse{Stdout: s.output, Stderr: []byte("err"), ExitCode: 3}, s.failure
}

func (s *streamingRuntime) ReopenContainerLog(_ context.Context, request *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	s.reopened = request.GetContainerId()
	return &runtimeapi.ReopenContainerLogResponse{}, s.failure
}

func streamingClient(t *testing.T, fake *streamingRuntime) *Client {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return newClient(t, socket, 5*time.Second)
}

func TestStreamingRequests(t *testing.T) {
	t.Parallel()
	fake := &streamingRuntime{url: "http://127.0.0.1:10010/exec/token"}
	client := streamingClient(t, fake)
	ctx := context.Background()
	got, err := client.Exec(ctx, "c1", []string{"/bin/true"}, ExecOptions{Stdout: true, Stderr: true})
	if err != nil || got.String() != fake.url {
		t.Fatalf("Exec = %v, %v", got, err)
	}
	if fake.exec.GetContainerId() != "c1" || fake.exec.GetCmd()[0] != "/bin/true" || !fake.exec.GetStderr() || fake.exec.GetStdin() {
		t.Fatalf("Exec request = %v", fake.exec)
	}
	if _, err := client.Attach(ctx, "c1", ExecOptions{Stdin: true, TTY: true}); err != nil || !fake.attach.GetTty() || !fake.attach.GetStdin() {
		t.Fatalf("Attach = %v request %v", err, fake.attach)
	}
	if _, err := client.PortForward(ctx, "s1", []int32{8080}); err != nil || fake.forward.GetPodSandboxId() != "s1" || fake.forward.GetPort()[0] != 8080 {
		t.Fatalf("PortForward = %v request %v", err, fake.forward)
	}
	fake.output = []byte("ok")
	result, err := client.ExecSync(ctx, "c1", []string{"probe"}, 2)
	if err != nil || string(result.Stdout) != "ok" || string(result.Stderr) != "err" || result.ExitCode != 3 || fake.execSync.GetTimeout() != 2 {
		t.Fatalf("ExecSync = %+v, %v request %v", result, err, fake.execSync)
	}
	if err := client.ReopenContainerLog(ctx, "c1"); err != nil || fake.reopened != "c1" {
		t.Fatalf("ReopenContainerLog = %v (%q)", err, fake.reopened)
	}
}

func TestExecSyncOutputBound(t *testing.T) {
	t.Parallel()
	fake := &streamingRuntime{output: bytes.Repeat([]byte("x"), 128*1024)}
	client := streamingClient(t, fake)
	if result, err := client.ExecSync(context.Background(), "c1", []string{"probe"}, 1); err != nil || len(result.Stdout) != 128*1024 {
		t.Fatalf("ExecSync above the default message limit = %v", err)
	}
	fake.output = bytes.Repeat([]byte("x"), maxExecSyncBytes+1)
	if _, err := client.ExecSync(context.Background(), "c1", []string{"probe"}, 1); err == nil {
		t.Fatal("ExecSync accepted output beyond its bound")
	}
}

func TestStreamingRejections(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		url  string
		fail error
		call func(*Client) error
		want string
	}{
		{name: "exec empty id", call: func(c *Client) error {
			_, err := c.Exec(ctx, "", []string{"x"}, ExecOptions{Stdout: true})
			return err
		}, want: "runtime ID"},
		{name: "exec no command", call: func(c *Client) error {
			_, err := c.Exec(ctx, "c", nil, ExecOptions{Stdout: true})
			return err
		}, want: "command required"},
		{name: "exec no streams", call: func(c *Client) error {
			_, err := c.Exec(ctx, "c", []string{"x"}, ExecOptions{})
			return err
		}, want: "at least one"},
		{name: "attach tty stderr", call: func(c *Client) error {
			_, err := c.Attach(ctx, "c", ExecOptions{TTY: true, Stderr: true})
			return err
		}, want: "TTY"},
		{name: "attach empty id", call: func(c *Client) error {
			_, err := c.Attach(ctx, "", ExecOptions{Stdout: true})
			return err
		}, want: "runtime ID"},
		{name: "port range", call: func(c *Client) error {
			_, err := c.PortForward(ctx, "s", []int32{0})
			return err
		}, want: "between 1 and 65535"},
		{name: "port empty id", call: func(c *Client) error {
			_, err := c.PortForward(ctx, "", nil)
			return err
		}, want: "runtime ID"},
		{name: "exec sync empty id", call: func(c *Client) error {
			_, err := c.ExecSync(ctx, "", []string{"x"}, 1)
			return err
		}, want: "runtime ID"},
		{name: "exec sync no timeout", call: func(c *Client) error {
			_, err := c.ExecSync(ctx, "c", []string{"x"}, 0)
			return err
		}, want: "positive timeout"},
		{name: "reopen empty id", call: func(c *Client) error { return c.ReopenContainerLog(ctx, "") }, want: "runtime ID"},
		{name: "relative url", url: "/exec/token", call: func(c *Client) error {
			_, err := c.Exec(ctx, "c", []string{"x"}, ExecOptions{Stdout: true})
			return err
		}, want: "invalid streaming URL"},
		{name: "url credentials", url: "http://user:pw@127.0.0.1/x", call: func(c *Client) error {
			_, err := c.Attach(ctx, "c", ExecOptions{Stdout: true})
			return err
		}, want: "invalid streaming URL"},
		{name: "url scheme", url: "ftp://127.0.0.1/x", call: func(c *Client) error {
			_, err := c.PortForward(ctx, "s", nil)
			return err
		}, want: "invalid streaming URL"},
		{name: "runtime error", url: "http://127.0.0.1/x", fail: status.Error(codes.NotFound, "gone"), call: func(c *Client) error {
			_, err := c.Exec(ctx, "c", []string{"x"}, ExecOptions{Stdout: true})
			return err
		}, want: "CRI Exec"},
		{name: "attach runtime error", fail: status.Error(codes.NotFound, "gone"), call: func(c *Client) error {
			_, err := c.Attach(ctx, "c", ExecOptions{Stdout: true})
			return err
		}, want: "CRI Attach"},
		{name: "forward runtime error", fail: status.Error(codes.NotFound, "gone"), call: func(c *Client) error {
			_, err := c.PortForward(ctx, "s", nil)
			return err
		}, want: "CRI PortForward"},
		{name: "exec sync runtime error", fail: status.Error(codes.NotFound, "gone"), call: func(c *Client) error {
			_, err := c.ExecSync(ctx, "c", []string{"x"}, 1)
			return err
		}, want: "CRI ExecSync"},
		{name: "reopen runtime error", fail: status.Error(codes.NotFound, "gone"), call: func(c *Client) error {
			return c.ReopenContainerLog(ctx, "c")
		}, want: "CRI ReopenContainerLog"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := streamingClient(t, &streamingRuntime{url: tt.url, failure: tt.fail})
			if err := tt.call(client); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}
