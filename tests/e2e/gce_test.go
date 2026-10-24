//go:build e2e

// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGCEProvisioningGuards(t *testing.T) {
	for _, tt := range []struct {
		name       string
		approve    string
		experiment string
		failWorker string
		pass       bool
		wantCalls  bool
	}{
		{name: "requires cost approval", experiment: "brie-test"},
		{name: "rejects invalid name", experiment: "existing-network", approve: "yes"},
		{name: "stops before control plane on stockout", experiment: "brie-test", approve: "yes", failWorker: "1", wantCalls: true},
		{name: "successful creation policies", experiment: "brie-test", approve: "yes", pass: true, wantCalls: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			logPath := filepath.Join(dir, "calls")
			mock := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$MOCK_LOG"
case "$*" in
  *"instances create "*"worker "*)
    if [ "$MOCK_FAIL_WORKER" = 1 ]; then
      echo "simulated stockout" >&2
      exit 1
    fi
    ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(mock), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "examples/gce/experiment.sh", "create")
			cmd.Dir = filepath.Join("..", "..")
			cmd.Env = append(os.Environ(),
				"PATH="+dir+":"+os.Getenv("PATH"),
				"PROJECT=test-project", "ZONE=us-east5-b", "NAME="+tt.experiment,
				"APPROVE_COSTS="+tt.approve, "MOCK_LOG="+logPath, "MOCK_FAIL_WORKER="+tt.failWorker,
			)
			output, err := cmd.CombinedOutput()
			if (err == nil) != tt.pass {
				t.Fatalf("create: %v; want pass=%t\n%s", err, tt.pass, output)
			}
			data, err := os.ReadFile(logPath)
			if !tt.wantCalls {
				if !os.IsNotExist(err) {
					t.Fatalf("gcloud invoked without valid approval/name: %s, %v", data, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := strings.Split(strings.TrimSpace(string(data)), "\n")
			var worker, control string
			for _, line := range calls {
				if strings.Contains(line, "instances create brie-test-worker ") {
					worker = line
				}
				if strings.Contains(line, "instances create brie-test-control ") {
					control = line
				}
			}
			if worker == "" {
				t.Fatal("worker allocation not attempted")
			}
			if tt.failWorker != "" {
				if control != "" {
					t.Fatal("control plane was created after worker stockout")
				}
				return
			}
			for role, line := range map[string]string{"worker": worker, "control": control} {
				for _, flag := range []string{
					"--no-address", "--no-service-account", "--no-scopes",
					"--termination-time=", "--instance-termination-action=DELETE",
					"--boot-disk-auto-delete", "enable-oslogin=true",
				} {
					if !strings.Contains(line, flag) {
						t.Errorf("%s missing %s: %s", role, flag, line)
					}
				}
			}
			if !strings.Contains(worker, "--maintenance-policy=TERMINATE") ||
				!strings.Contains(control, "--maintenance-policy=MIGRATE") {
				t.Error("machine-specific maintenance policies not preserved")
			}
			for _, key := range []string{"startup-script=", "brie-kubeadm=", "brie-addons=", "brie-features="} {
				if !strings.Contains(control, key) {
					t.Errorf("control plane missing bootstrap metadata %s", key)
				}
			}
			for _, invalidKey := range []string{"brie-kubeadm.yaml=", "brie-addons.sh=", "brie-features.sh="} {
				if strings.Contains(control, invalidKey) {
					t.Errorf("invalid GCE metadata key %s", invalidKey)
				}
			}
		})
	}
}
