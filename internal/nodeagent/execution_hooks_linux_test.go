// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/execinventory"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestExecutionHooksObserveBarrierInputs(t *testing.T) {
	t.Parallel()
	e, runtime := newExecutor(t)
	var states []HealthState
	var passes []error
	e.hooks = ExecutionHooks{
		Health: func(state HealthState) { states = append(states, state) },
		Pass: func(err error, at time.Time) {
			if at.IsZero() {
				t.Error("pass reported without a time")
			}
			passes = append(passes, err)
		},
	}
	if err := e.reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := e.updateHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(passes) != 1 || passes[0] != nil {
		t.Fatalf("passes = %v", passes)
	}
	if len(states) != 1 || states[0] != (HealthState{RuntimeHealthy: true, Synced: true}) {
		t.Fatalf("states = %+v", states)
	}
	runtime.mu.Lock()
	runtime.listError = true
	runtime.mu.Unlock()
	if err := e.reconcile(t.Context()); err == nil {
		t.Fatal("expected a failed pass")
	}
	if err := e.updateHealth(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(passes) != 2 || passes[1] == nil || states[1].Synced {
		t.Fatalf("after failure passes = %v states = %+v", passes, states)
	}
	// Empty hooks are skipped.
	e.hooks = ExecutionHooks{}
	e.hooks.passFinished(errors.New("ignored"))
	e.hooks.healthUpdated(HealthState{})
	e.hooks.leaseRenewed(nil)
	e.hooks.observeCRI(e.runtime)
}

func TestAPIConfig(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name, server string) string {
		path := filepath.Join(dir, name)
		if err := clientcmd.WriteToFile(clientcmdapi.Config{
			Clusters:  map[string]*clientcmdapi.Cluster{"c": {Server: server}},
			AuthInfos: map[string]*clientcmdapi.AuthInfo{"u": {Token: "t"}},
			Contexts:  map[string]*clientcmdapi.Context{"x": {Cluster: "c", AuthInfo: "u"}}, CurrentContext: "x",
		}, path); err != nil {
			t.Fatal(err)
		}
		return path
	}
	config, err := APIConfig(write("good", "https://api.example.test:6443"), "brie-node/observability")
	if err != nil || config.UserAgent != "brie-node/observability" {
		t.Fatalf("APIConfig = %+v, %v", config, err)
	}
	if _, err := APIConfig(write("plain", "http://api.example.test"), "x"); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("plain HTTP accepted: %v", err)
	}
}

// TestExecutionHooksPublishVerifiedInventory checks that only executions the
// executor verified are published for serving and log rotation.
func TestExecutionHooksPublishVerifiedInventory(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		modify func(service *executionRuntime, owned *runtimeapi.PodSandbox)
		want   int
	}{
		{name: "verified", want: 1},
		{name: "foreign owner with the same Pod name", want: 1, modify: func(service *executionRuntime, owned *runtimeapi.PodSandbox) {
			labels := maps.Clone(owned.Labels)
			labels[executionMarker] = "another-owner"
			labels[podUIDLabel] = "foreign-uid"
			service.sandboxes["foreign"] = &runtimeapi.PodSandbox{Id: "foreign", CreatedAt: owned.CreatedAt + 1, Labels: labels,
				RuntimeHandler: owned.RuntimeHandler, State: owned.State,
				Metadata: &runtimeapi.PodSandboxMetadata{Name: owned.Metadata.Name, Namespace: owned.Metadata.Namespace, Uid: "foreign-uid"}}
		}},
		{name: "duplicate sandbox for the UID", want: 0, modify: func(service *executionRuntime, owned *runtimeapi.PodSandbox) {
			duplicate := proto.Clone(owned).(*runtimeapi.PodSandbox)
			duplicate.Id = "duplicate"
			service.sandboxes["duplicate"] = duplicate
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, service := newExecutor(t, executionPod("app"))
			reconcilePasses(t, e, 5)
			var published [][]execinventory.Execution
			e.hooks = ExecutionHooks{Inventory: func(executions []execinventory.Execution) { published = append(published, executions) }}
			service.mu.Lock()
			var owned *runtimeapi.PodSandbox
			for _, sandbox := range service.sandboxes {
				owned = sandbox
			}
			if tt.modify != nil {
				tt.modify(service, owned)
			}
			service.mu.Unlock()
			_ = e.reconcile(t.Context())
			if len(published) != 1 || len(published[0]) != tt.want {
				t.Fatalf("published = %+v, want one inventory of %d", published, tt.want)
			}
			if tt.want == 0 {
				return
			}
			got := published[0][0]
			if got.Namespace != "default" || got.Name != "app" || got.SandboxID != owned.Id || got.PodUID != owned.Metadata.Uid ||
				got.ContainerID == "" || got.ContainerName == "" {
				t.Fatalf("verified execution = %+v", got)
			}
		})
	}
}
