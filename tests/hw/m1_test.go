// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ameukam/brie/internal/nodeagent"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const testNamespaceUID = "0b6c9f9e-1111-4222-8333-444455556666"

const testImage = "docker.io/library/busybox@sha256:c542a2b3cb3c982440890284aac884e89dbf468d5ff1c206fd2ba8ea23257420"

func m1Kubectl() []mockResponse {
	return []mockResponse{
		{Match: " version", Output: "Client Version: v1.37.0\n"},
		{Match: `get node brie-worker-1 -o jsonpath={.status.conditions[?(@.type=="Ready")].status}`, Output: "True"},
		{Match: "get runtimeclass kata-fc", Output: "kata-fc"},
		{Match: "create -f - -o jsonpath={.metadata.uid}", Output: testNamespaceUID},
		{Match: "get namespace brie-m1-r20261113 -o jsonpath={.metadata.uid} {.metadata.labels.brie-hw-run}", Output: testNamespaceUID + " " + testRunID},
		{Match: "get pod m1-web -n brie-m1-r20261113 -o jsonpath={.metadata.uid}", Output: "uid-1 brie-worker-1 10.244.1.5 Running"},
		{Match: "get pod m1-web -n brie-m1-r20261113 -o yaml", Output: "kind: Pod\n"},
	}
}

func m1SSH() []mockResponse {
	return []mockResponse{
		{Match: "pgrep -x firecracker", Output: "4242 "},
		{Match: "crictl ps -q --label io.kubernetes.pod.uid=uid-1", Output: "c0ffee "},
		{Match: "curl -fsS --max-time 5 http://10.244.1.5:8080/boot_id", Output: "boot-1234"},
	}
}

func m1Lab(t *testing.T, state mockState) *lab {
	t.Helper()
	state.Resources = append(state.Resources, mockResource{
		Kind: "instances", Name: testPrefix + "-metal-1", Zone: testZone, Labels: runLabels(), Status: "RUNNING",
	})
	l := newLab(t, state)
	l.env["M1_KUBECONFIG"] = writeFile(t, l.dir, "kubeconfig", "token: SECRET-TOKEN-VALUE\n")
	l.env["WORKER"] = testPrefix + "-metal-1"
	l.env["WORKER_ZONE"] = testZone
	l.env["NODE_NAME"] = "brie-worker-1"
	l.env["M1_IMAGE"] = testImage
	return l
}

func m1Results(t *testing.T, l *lab) (map[string]string, string) {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(l.env["HW_LOG_DIR"], "m1-*"))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("evidence directories %v: %v", dirs, err)
	}
	data, err := os.ReadFile(filepath.Join(dirs[0], "results.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	results := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n")[1:] {
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("malformed result line %q", line)
		}
		results[fields[0]] = fields[1]
	}
	return results, dirs[0]
}

func TestM1Harness(t *testing.T) {
	base := map[string]string{
		"startup": "PASS", "pod-network": "PASS", "service-and-dns": "NOT-RUN",
		"probes": "SKIPPED-UNSUPPORTED", "exec": "SKIPPED-UNSUPPORTED", "logs": "SKIPPED-UNSUPPORTED",
		"volumes": "SKIPPED-UNSUPPORTED", "agent-restart": "PASS", "warm-pause-resume": "SKIPPED-UNSUPPORTED",
		"deletion-races": "NOT-RUN", "memory-footprint": "NOT-RUN", "cleanup": "PASS",
	}
	with := func(changes map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range changes {
			out[k] = v
		}
		return out
	}
	for _, tt := range []struct {
		name     string
		features string
		kubectl  []mockResponse
		ssh      []mockResponse
		code     int
		want     map[string]string
	}{
		{name: "unsupported features are skipped", code: 2, want: base},
		{
			name: "VM replaced across restart",
			ssh:  []mockResponse{{Match: "pgrep -x firecracker", Output: "4242 ", Times: 1}, {Match: "pgrep -x firecracker", Output: "5151 "}},
			code: 1, want: with(map[string]string{"agent-restart": "FAIL"}),
		},
		{
			name: "guest rebooted across restart",
			ssh:  []mockResponse{{Match: "curl -fsS", Output: "boot-1234", Times: 2}, {Match: "curl -fsS", Output: "boot-9999"}},
			code: 1, want: with(map[string]string{"agent-restart": "FAIL"}),
		},
		{
			name: "no VM observed",
			ssh:  []mockResponse{{Match: "pgrep -x firecracker", Output: ""}},
			code: 1, want: with(map[string]string{"agent-restart": "FAIL"}),
		},
		{
			name:    "Pod never Ready",
			kubectl: []mockResponse{{Match: "wait --for=condition=Ready pod/m1-web", Exit: 1}},
			code:    1,
			want:    with(map[string]string{"startup": "FAIL", "pod-network": "FAIL", "agent-restart": "FAIL", "cleanup": "FAIL"}),
		},
		{
			name: "sandbox left after deletion",
			ssh:  []mockResponse{{Match: "crictl pods -q", Output: "sandbox-1"}},
			code: 1, want: with(map[string]string{"cleanup": "FAIL"}),
		},
		{
			name:     "declared exec and logs are exercised",
			features: "exec logs",
			kubectl: []mockResponse{
				{Match: "exec m1-web -n brie-m1-r20261113 -- cat /proc/sys/kernel/random/boot_id", Output: "boot-1234"},
				{Match: "logs m1-web", Output: "brie-m1-started\n"},
			},
			code: 2, want: with(map[string]string{"exec": "PASS", "logs": "PASS"}),
		},
		{
			name:     "declared features that fail",
			features: "exec logs probes",
			kubectl: []mockResponse{
				{Match: "exec m1-web", Exit: 1},
				{Match: "wait --for=condition=Ready pod/m1-probe", Exit: 1},
			},
			code: 1, want: with(map[string]string{"exec": "FAIL", "logs": "FAIL", "probes": "FAIL"}),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := m1Lab(t, mockState{
				Kubectl: append(tt.kubectl, m1Kubectl()...),
				SSH:     append(tt.ssh, m1SSH()...),
			})
			if tt.features != "" {
				l.env["M1_FEATURES"] = tt.features
			}
			r := l.run("m1-harness.sh")
			if r.code != tt.code {
				t.Fatalf("exit=%d, want %d; stdout=%q stderr=%q", r.code, tt.code, r.stdout, r.stderr)
			}
			results, dir := m1Results(t, l)
			for step, want := range tt.want {
				if results[step] != want {
					t.Errorf("%s=%q, want %q", step, results[step], want)
				}
			}
			if len(results) != len(tt.want) {
				t.Errorf("results=%v; want exactly %v", results, tt.want)
			}

			cmd := exec.CommandContext(t.Context(), "sha256sum", "--check", "--strict", "--quiet", "SHA256SUMS")
			cmd.Dir = dir
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("SHA256SUMS does not verify: %v\n%s", err, out)
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			sums, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range files {
				data, err := os.ReadFile(filepath.Join(dir, f.Name()))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), "SECRET-TOKEN-VALUE") {
					t.Errorf("%s contains kubeconfig credentials", f.Name())
				}
				if f.Name() != "SHA256SUMS" && !strings.Contains(string(sums), "./"+f.Name()+"\n") {
					t.Errorf("%s is not covered by SHA256SUMS", f.Name())
				}
			}
			for _, call := range l.calls() {
				if call.Tool == "kubectl" && call.Args[0] != "--kubeconfig="+l.env["M1_KUBECONFIG"] {
					t.Errorf("kubectl call without the explicit kubeconfig: %q", call.Args)
				}
			}
			if deletes := namespaceDeletes(l); len(deletes) != 1 || deletes[0] != "delete --raw /api/v1/namespaces/brie-m1-r20261113 -f -" {
				t.Errorf("namespace deletes %q; want one UID-preconditioned raw delete", deletes)
			}
		})
	}
}

// namespaceDeletes returns the kubectl calls that could delete a namespace.
func namespaceDeletes(l *lab) []string {
	var out []string
	for _, call := range l.calls() {
		args := strings.Join(call.Args, " ")
		_, after, _ := strings.Cut(args, "--request-timeout=60s ")
		if call.Tool == "kubectl" && strings.HasPrefix(after, "delete ") &&
			(strings.Contains(after, "namespaces/") || strings.Contains(after, "namespace ") || strings.Contains(after, " ns ")) {
			out = append(out, after)
		}
	}
	return out
}

func TestM1HarnessNamespaceOwnership(t *testing.T) {
	for _, tt := range []struct {
		name    string
		kubectl []mockResponse
		stderr  string
		created bool
	}{
		{
			name:    "existing namespace is never reused or deleted",
			kubectl: []mockResponse{{Match: "create -f -", Exit: 1}},
			stderr:  "refusing to reuse an existing namespace", created: true,
		},
		{
			name:    "failed preflight deletes nothing",
			kubectl: []mockResponse{{Match: " version", Exit: 1}},
			stderr:  "cannot reach the API server",
		},
		{
			name:    "node not Ready deletes nothing",
			kubectl: []mockResponse{{Match: "get node brie-worker-1", Output: "False"}},
			stderr:  "is not Ready",
		},
		{
			name:    "unrecognizable UID is not used",
			kubectl: []mockResponse{{Match: "create -f -", Output: "not-a-uid"}},
			stderr:  "without a recognizable UID", created: true,
		},
		{
			name: "replaced namespace is refused",
			kubectl: []mockResponse{
				{Match: "get namespace brie-m1-r20261113", Output: "aaaaaaaa-1111-4222-8333-444455556666 " + testRunID},
			},
			stderr: "REFUSED to delete namespace brie-m1-r20261113", created: true,
		},
		{
			name:    "namespace relabelled by another run is refused",
			kubectl: []mockResponse{{Match: "get namespace brie-m1-r20261113", Output: testNamespaceUID + " r20261112"}},
			stderr:  "REFUSED to delete namespace", created: true,
		},
		{
			name:    "unreadable namespace is not deleted",
			kubectl: []mockResponse{{Match: "get namespace brie-m1-r20261113", Exit: 1}},
			stderr:  "could not read namespace", created: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := m1Lab(t, mockState{Kubectl: append(tt.kubectl, m1Kubectl()...), SSH: m1SSH()})
			r := l.run("m1-harness.sh")
			if !strings.Contains(r.stderr, tt.stderr) {
				t.Fatalf("exit=%d stderr=%q; want stderr containing %q", r.code, r.stderr, tt.stderr)
			}
			if r.code == 0 && !strings.Contains(tt.stderr, "REFUSED") && !strings.Contains(tt.stderr, "could not read") {
				t.Fatalf("exit=0; want failure")
			}
			if deletes := namespaceDeletes(l); len(deletes) != 0 {
				t.Errorf("namespace deleted: %q", deletes)
			}
			created := false
			for _, call := range l.calls() {
				args := strings.Join(call.Args, " ")
				if call.Tool == "kubectl" && strings.Contains(args, "create -f -") {
					created = true
				}
				if call.Tool == "kubectl" && strings.Contains(args, " apply ") && !strings.Contains(args, ".yaml") {
					t.Errorf("namespace applied instead of created exclusively: %q", call.Args)
				}
			}
			if created != tt.created {
				t.Errorf("namespace create attempted=%v, want %v", created, tt.created)
			}
		})
	}
}

func TestM1HarnessInputs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		env    map[string]string
		labels map[string]string
		stderr string
	}{
		{name: "missing kubeconfig", env: map[string]string{"M1_KUBECONFIG": ""}, stderr: "required input M1_KUBECONFIG"},
		{name: "relative kubeconfig", env: map[string]string{"M1_KUBECONFIG": "kubeconfig"}, stderr: "absolute path"},
		{name: "missing node", env: map[string]string{"NODE_NAME": ""}, stderr: "required input NODE_NAME"},
		{name: "tagged image", env: map[string]string{"M1_IMAGE": "docker.io/library/busybox:1.37"}, stderr: "pinned by digest"},
		{name: "unknown feature", env: map[string]string{"M1_FEATURES": "exec pause"}, stderr: "unknown M1_FEATURES entry pause"},
		{name: "unlabelled worker", labels: map[string]string{}, stderr: "does not carry label " + testMarker},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := m1Lab(t, mockState{Kubectl: m1Kubectl(), SSH: m1SSH()})
			if tt.labels != nil {
				state := l.state()
				state.Resources[0].Labels = tt.labels
				l.setState(state)
			}
			for k, v := range tt.env {
				l.env[k] = v
			}
			r := l.run("m1-harness.sh")
			if r.code == 0 || !strings.Contains(r.stderr, tt.stderr) {
				t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
			}
			for _, call := range l.calls() {
				if call.Tool == "kubectl" || indexOf([]string{strings.Join(call.Args, " ")}, `compute ssh`) >= 0 {
					t.Fatalf("refused harness ran %s %q", call.Tool, call.Args)
				}
			}
		})
	}
}

// TestM1HarnessManifestsFitProfile is a regression test: the probes step's
// Pod used a tcpSocket port the container did not declare, and Brie's
// validator refused it on hardware. Every manifest the harness applies must
// pass the profile it is meant to exercise.
func TestM1HarnessManifestsFitProfile(t *testing.T) {
	l := m1Lab(t, mockState{Kubectl: m1Kubectl(), SSH: m1SSH()})
	l.env["M1_FEATURES"] = "probes"
	if r := l.run("m1-harness.sh"); r.code != 2 {
		t.Fatalf("exit=%d, want 2; stdout=%q stderr=%q", r.code, r.stdout, r.stderr)
	}
	_, dir := m1Results(t, l)
	for _, tt := range []struct {
		manifest string
		features []nodeagent.ProfileFeature
	}{
		{manifest: "m1-web.yaml"},
		{manifest: "m1-probe.yaml", features: []nodeagent.ProfileFeature{nodeagent.ProfileProbes}},
	} {
		t.Run(tt.manifest, func(t *testing.T) {
			file, err := os.Open(filepath.Join(dir, tt.manifest))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			var pod corev1.Pod
			if err := yaml.NewYAMLOrJSONDecoder(file, 4096).Decode(&pod); err != nil {
				t.Fatal(err)
			}
			// The API server and the scheduler set these fields, including the
			// probe defaults the harness leaves unset.
			pod.UID, pod.Spec.NodeName = types.UID("uid-"+pod.Name), l.env["NODE_NAME"]
			if probe := pod.Spec.Containers[0].ReadinessProbe; probe != nil {
				probe.TimeoutSeconds, probe.SuccessThreshold, probe.FailureThreshold = 1, 1, 3
			}
			if err := nodeagent.ValidatePodProfile(&pod, l.env["NODE_NAME"], "kata-fc", tt.features...); err != nil {
				t.Fatalf("%s does not fit the profile: %v", tt.manifest, err)
			}
		})
	}
}
