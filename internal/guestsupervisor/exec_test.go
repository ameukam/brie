// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestsupervisor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// guestExec stands in for containerd and the Kata agent: it checks the exec
// argument vector and relays it to a real supervisor socket, as brie-guestctl
// does inside the guest.
type guestExec struct {
	t        *testing.T
	request  *runtimeapi.ExecSyncRequest
	stdout   []byte
	exitCode int32
	err      error
}

func (g *guestExec) ExecSync(ctx context.Context, request *runtimeapi.ExecSyncRequest, _ ...grpc.CallOption) (*runtimeapi.ExecSyncResponse, error) {
	g.request = request
	if g.err != nil || g.stdout != nil || g.exitCode != 0 {
		return &runtimeapi.ExecSyncResponse{Stdout: g.stdout, ExitCode: g.exitCode}, g.err
	}
	if len(request.Cmd) != 3 || !strings.HasPrefix(request.Cmd[1], "--socket=") {
		g.t.Fatalf("unexpected command %q", request.Cmd)
	}
	decoded, err := DecodeRequest([]byte(request.Cmd[2]))
	if err != nil {
		g.t.Fatalf("guest received an invalid request: %v", err)
	}
	response, err := Call(ctx, strings.TrimPrefix(request.Cmd[1], "--socket="), decoded)
	if err != nil {
		return &runtimeapi.ExecSyncResponse{Stderr: []byte(err.Error()), ExitCode: 1}, nil
	}
	encoded, err := EncodeResponse(response)
	if err != nil {
		g.t.Fatal(err)
	}
	return &runtimeapi.ExecSyncResponse{Stdout: append(encoded, '\n')}, nil
}

func TestExecClientRelaysThroughExecSync(t *testing.T) {
	path := startSupervisor(t, &fakeWorkload{point: SafePoint{ID: "counter-9", ExternalEffectWatermark: 9}})
	runtime := &guestExec{t: t}
	client := ExecClient{Runtime: runtime, ContainerID: "c1", CtlPath: DefaultCtlPath, SocketPath: path}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	response, err := client.Call(ctx, prepareRequest("op-1", 1, time.Now().Add(time.Minute)))
	if err != nil || response.SafePoint == nil || response.SafePoint.ExternalEffectWatermark != 9 {
		t.Fatalf("prepare = %+v, %v", response, err)
	}
	if runtime.request.ContainerId != "c1" || runtime.request.Cmd[0] != DefaultCtlPath || runtime.request.Timeout < 1 || runtime.request.Timeout > 10 {
		t.Fatalf("exec request = %+v", runtime.request)
	}
	response, err = client.Call(ctx, resumeRequest("op-1", 2, time.Now()))
	if err != nil || response.Outcome != OutcomeReady {
		t.Fatalf("resume = %+v, %v", response, err)
	}
}

func TestExecClientErrors(t *testing.T) {
	valid := abortRequest("op-1")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	expired, cancelExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelExpired()
	reply := func(stdout string) *guestExec { return &guestExec{t: t, stdout: []byte(stdout)} }

	for _, tt := range []struct {
		name    string
		ctx     context.Context
		client  ExecClient
		request Request
		want    string
	}{
		{name: "no runtime", ctx: ctx, client: ExecClient{ContainerID: "c1"}, want: "requires a runtime"},
		{name: "no deadline", ctx: context.Background(), client: ExecClient{Runtime: reply(""), ContainerID: "c1"}, want: "deadline"},
		{name: "expired", ctx: expired, client: ExecClient{Runtime: reply(""), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "deadline exceeded"},
		{name: "relative ctl", ctx: ctx, client: ExecClient{Runtime: reply(""), ContainerID: "c1", CtlPath: "ctl", SocketPath: "/s"}, want: "guestctl path"},
		{name: "relative socket", ctx: ctx, client: ExecClient{Runtime: reply(""), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "s"}, want: "supervisor socket path"},
		{name: "invalid request", ctx: ctx, client: ExecClient{Runtime: reply(""), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, request: Request{}, want: "invalid request"},
		{name: "exec failure", ctx: ctx, client: ExecClient{Runtime: &guestExec{t: t, err: errors.New("container not running")}, ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "CRI ExecSync"},
		{name: "nonzero exit", ctx: ctx, client: ExecClient{Runtime: &guestExec{t: t, exitCode: 1}, ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "exited with code 1"},
		{name: "oversized output", ctx: ctx, client: ExecClient{Runtime: reply(strings.Repeat("x", MaxMessageBytes+2)), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "exceeds"},
		{name: "garbage output", ctx: ctx, client: ExecClient{Runtime: reply("ok\n"), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "decode response"},
		{name: "mismatched reply", ctx: ctx, client: ExecClient{Runtime: reply(`{"version":"brie.guestsupervisor/v1","outcome":"ready","operation_id":"op-1"}` + "\n"), ContainerID: "c1", CtlPath: "/ctl", SocketPath: "/s"}, want: "cannot yield"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := valid
			if tt.request.Version != "" || tt.name == "invalid request" {
				request = tt.request
			}
			_, err := tt.client.Call(tt.ctx, request)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Call error = %v, want %q", err, tt.want)
			}
		})
	}
}
