// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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
