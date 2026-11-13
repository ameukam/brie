// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// filledChecksums replaces every placeholder in checksums.template with a
// syntactically valid test value. The digests are not real.
func filledChecksums(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("checksums.template")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ReplaceAll(string(data), "<RUNC_VERSION>", "1.3.0")
	text = strings.ReplaceAll(text, "<K8S_PATCH>", "0")
	for i := 0; strings.Contains(text, "<SHA256>"); i++ {
		sum := sha256.Sum256([]byte(fmt.Sprint(i)))
		text = strings.Replace(text, "<SHA256>", hex.EncodeToString(sum[:]), 1)
	}
	return text
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestChecksumsFile(t *testing.T) {
	template, err := os.ReadFile("checksums.template")
	if err != nil {
		t.Fatal(err)
	}
	filled := filledChecksums(t)
	digest := strings.Repeat("a", 64)
	for _, tt := range []struct {
		name    string
		content string
		missing bool
		host    string // expected install-host --check-only stderr; empty means success
		control string // expected install-control-plane --check-only stderr
	}{
		{name: "filled", content: filled},
		{name: "template placeholders", content: string(template), host: "still contains a placeholder", control: "still contains a placeholder"},
		{name: "missing file", missing: true, host: "missing or unreadable", control: "missing or unreadable"},
		{
			name:    "one placeholder left",
			content: strings.Replace(filled, digest, "<SHA256>", 1) + "binary /opt/x <SHA256>\n",
			host:    "placeholder", control: "placeholder",
		},
		{
			name:    "unpinned containerd",
			content: strings.ReplaceAll(filled, "containerd 2.4.2", "containerd 2.4.1"),
			host:    "containerd must be the pinned version 2.4.2", control: "pinned version 2.4.2",
		},
		{
			name:    "unpinned kata",
			content: strings.Replace(filled, "kata-static 4.3.1", "kata-static 4.3.0", 1),
			host:    "kata-static must be the pinned version 4.3.1", control: "pinned version 4.3.1",
		},
		{
			name:    "plain HTTP",
			content: strings.Replace(filled, "https://github.com/containernetworking", "http://github.com/containernetworking", 1),
			host:    "cni-plugins URL must be an HTTPS release URL", control: "HTTPS release URL",
		},
		{
			name:    "untrusted host",
			content: strings.Replace(filled, "https://github.com/kubernetes-sigs/cri-tools", "https://example.com/cri-tools", 1),
			host:    "crictl URL must be an HTTPS release URL", control: "HTTPS release URL",
		},
		{
			name:    "URL for another version",
			content: strings.Replace(filled, "releases/download/v1.37.0/crictl-v1.37.0", "releases/download/v1.36.0/crictl-v1.36.0", 1),
			host:    "crictl URL does not name version 1.37.0", control: "does not name version",
		},
		{
			name:    "uppercase digest",
			content: replaceLastField(filled, "binary /opt/kata/bin/jailer ", strings.Repeat("A", 64)),
			host:    "/opt/kata/bin/jailer SHA-256 must be 64 lowercase hex digits", control: "must be 64 lowercase hex digits",
		},
		{
			name:    "short digest",
			content: replaceLastField(filled, "artifact containerd 2.4.2 ", "abc"),
			host:    "containerd SHA-256 must be 64 lowercase hex digits", control: "must be 64 lowercase hex digits",
		},
		{
			name:    "extra field",
			content: replaceLastField(filled, "artifact crictl ", digest+" extra"),
			host:    "want 'artifact NAME VERSION URL SHA256'", control: "want 'artifact NAME VERSION URL SHA256'",
		},
		{
			name:    "duplicate artifact",
			content: filled + "artifact crictl 1.37.0 https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.37.0/x " + digest + "\n",
			host:    "duplicate artifact crictl", control: "duplicate artifact crictl",
		},
		{
			name:    "missing firecracker digest",
			content: removeLine(filled, "binary /opt/kata/bin/firecracker"),
			host:    "no binary digest for /opt/kata/bin/firecracker",
		},
		{
			name:    "missing kata artifact",
			content: removeLine(filled, "artifact kata-static"),
			host:    "no artifact kata-static",
		},
		{
			name:    "missing runc",
			content: removeLine(filled, "artifact runc"),
			control: "no artifact runc",
		},
		{
			name:    "Kubernetes 1.36",
			content: strings.ReplaceAll(filled, "1.37.0 https://dl.k8s.io/v1.37.0", "1.36.4 https://dl.k8s.io/v1.36.4"),
			control: "kubeadm must be a Kubernetes 1.37 release, not 1.36.4",
		},
		{
			name:    "kubelet skew",
			content: strings.Replace(filled, "kubelet 1.37.0 https://dl.k8s.io/v1.37.0", "kubelet 1.37.1 https://dl.k8s.io/v1.37.1", 1),
			control: "kubelet 1.37.1 differs from kubeadm 1.37.0",
		},
		{
			name:    "unknown record",
			content: filled + "image busybox " + digest + "\n",
			host:    "unknown record type image", control: "unknown record type image",
		},
		{
			name:    "relative binary path",
			content: filled + "binary opt/kata/bin/x " + digest + "\n",
			host:    "binary path must be absolute", control: "binary path must be absolute",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{})
			path := filepath.Join(l.dir, "checksums.txt")
			if !tt.missing {
				writeFile(t, l.dir, "checksums.txt", tt.content)
			}
			for script, want := range map[string]string{"install-host.sh": tt.host, "install-control-plane.sh": tt.control} {
				r := l.run(script, "--check-only", path)
				if want == "" && r.code != 0 {
					t.Errorf("%s: exit=%d stderr=%q; want success", script, r.code, r.stderr)
				}
				if want != "" && (r.code == 0 || !strings.Contains(r.stderr, want)) {
					t.Errorf("%s: exit=%d stderr=%q; want failure containing %q", script, r.code, r.stderr, want)
				}
			}
			if calls := l.calls(); len(calls) != 0 {
				t.Errorf("host scripts called gcloud: %v", calls)
			}
		})
	}
}

// replaceLastField replaces the final field of the line starting with prefix.
func replaceLastField(text, prefix, value string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			lines[i] = line[:strings.LastIndex(line, " ")+1] + value
		}
	}
	return strings.Join(lines, "\n")
}

func removeLine(text, prefix string) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, prefix) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// TestControlPlaneTokenOutput guards the bootstrap credentials: the script's
// output is forwarded by run-on-host.sh, so no kubeadm call may print a token.
func TestControlPlaneTokenOutput(t *testing.T) {
	script := string(mustRead(t, "install-control-plane.sh"))
	var calls []string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "kubeadm ") || strings.Contains(line, "$(kubeadm ") {
			calls = append(calls, strings.TrimSpace(line))
		}
	}
	for _, tt := range []struct {
		name  string
		match func(call string) bool
		want  int
	}{
		{name: "init skips the token print", match: func(c string) bool {
			return strings.HasPrefix(c, "kubeadm init ") && strings.HasSuffix(c, " --skip-token-print")
		}, want: 1},
		{name: "init without the skip flag", match: func(c string) bool {
			return strings.HasPrefix(c, "kubeadm init ") && !strings.Contains(c, "--skip-token-print")
		}},
		{name: "token commands", match: func(c string) bool { return strings.Contains(c, "kubeadm token") }},
		{name: "join command print", match: func(c string) bool { return strings.Contains(c, "print-join-command") }},
		{name: "certificate key print", match: func(c string) bool { return strings.Contains(c, "upload-certs") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := 0
			for _, c := range calls {
				if tt.match(c) {
					got++
				}
			}
			if got != tt.want {
				t.Errorf("%d kubeadm calls match, want %d: %q", got, tt.want, calls)
			}
		})
	}
}

func TestInstallHostRefusals(t *testing.T) {
	l := newLab(t, mockState{})
	checksums := writeFile(t, l.dir, "checksums.txt", filledChecksums(t))
	for _, script := range []string{"install-host.sh", "install-control-plane.sh"} {
		for _, args := range [][]string{nil, {"--unknown"}, {checksums, "extra"}, {"--check-only"}} {
			if r := l.run(script, args...); r.code == 0 || !strings.Contains(r.stderr, "usage:") {
				t.Errorf("%s %q: exit=%d stderr=%q; want usage", script, args, r.code, r.stderr)
			}
		}
		if os.Geteuid() != 0 {
			if r := l.run(script, checksums); r.code == 0 || !strings.Contains(r.stderr, "must run as root") {
				t.Errorf("%s as non-root: exit=%d stderr=%q", script, r.code, r.stderr)
			}
		}
	}
}

func TestDigestManifest(t *testing.T) {
	for _, tt := range []struct {
		name   string
		modify func(dir string, manifest *string)
		stderr string
	}{
		{name: "match"},
		{
			name:   "tampered firecracker",
			modify: func(dir string, _ *string) { writeFile(t, dir, "firecracker", "patched") },
			stderr: "do not match digest manifest",
		},
		{
			name:   "missing jailer",
			modify: func(dir string, _ *string) { _ = os.Remove(filepath.Join(dir, "jailer")) },
			stderr: "do not match digest manifest",
		},
		{
			name:   "malformed line",
			modify: func(_ string, manifest *string) { *manifest += "not a digest line\n" },
			stderr: "malformed line",
		},
		{
			name:   "empty manifest",
			modify: func(_ string, manifest *string) { *manifest = "" },
			stderr: "missing or empty",
		},
		{
			name:   "truncated to one valid entry",
			modify: func(_ string, manifest *string) { *manifest = strings.SplitAfter(*manifest, "\n")[0] },
			stderr: "does not cover",
		},
		{
			name: "missing kata-runtime entry",
			modify: func(_ string, manifest *string) {
				lines := strings.SplitAfter(*manifest, "\n")
				*manifest = strings.Join(lines[:3], "")
			},
			stderr: "/kata-runtime",
		},
		{
			name: "duplicate entry replaces a required one",
			modify: func(_ string, manifest *string) {
				lines := strings.SplitAfter(*manifest, "\n")
				*manifest = strings.Join([]string{lines[0], lines[1], lines[2], lines[0]}, "")
			},
			stderr: "more than once",
		},
		{
			name: "unexpected extra binary",
			modify: func(dir string, manifest *string) {
				sum := sha256.Sum256([]byte("extra"))
				*manifest += hex.EncodeToString(sum[:]) + "  " + writeFile(t, dir, "extra", "extra") + "\n"
			},
			stderr: "unexpected binary",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{})
			bin := filepath.Join(l.dir, "kata")
			if err := os.MkdirAll(bin, 0o755); err != nil {
				t.Fatal(err)
			}
			var manifest string
			for _, name := range []string{"firecracker", "jailer", "containerd-shim-kata-v2", "kata-runtime"} {
				content := "binary " + name
				sum := sha256.Sum256([]byte(content))
				path := writeFile(t, bin, name, content)
				manifest += hex.EncodeToString(sum[:]) + "  " + path + "\n"
			}
			if tt.modify != nil {
				tt.modify(bin, &manifest)
			}
			path := writeFile(t, l.dir, "hw-digests.sha256", manifest)
			lib, err := filepath.Abs("checksums.sh")
			if err != nil {
				t.Fatal(err)
			}
			// The required list names /opt/kata/bin; point it at the fixture binaries.
			r := l.runCommand("bash", "-c", `source "$1"; HW_MANIFEST_BINARIES=("$2"/firecracker "$2"/jailer "$2"/containerd-shim-kata-v2 "$2"/kata-runtime); hw_verify_manifest "$3"`,
				"verify", lib, bin, path)
			if tt.stderr == "" && r.code != 0 {
				t.Fatalf("exit=%d stderr=%q; want success", r.code, r.stderr)
			}
			if tt.stderr != "" && (r.code == 0 || !strings.Contains(r.stderr, tt.stderr)) {
				t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
			}
		})
	}
}

func TestRunOnHost(t *testing.T) {
	host := testPrefix + "-metal-1"
	for _, tt := range []struct {
		name   string
		labels map[string]string
		env    map[string]string
		args   []string
		stderr string
	}{
		{name: "labelled host", labels: runLabels(), args: []string{"install-host.sh", "checksums.txt"}},
		{
			name: "unsafe forwarded value", labels: runLabels(), env: map[string]string{"NODE_NAME": "x;reboot"},
			args: []string{"node-conformance.sh", "checksums.txt"}, stderr: "cannot be forwarded",
		},
		{name: "unlabelled host", args: []string{"install-host.sh", "checksums.txt"}, stderr: "does not carry label " + testMarker},
		{name: "unknown script", labels: runLabels(), args: []string{"teardown.sh"}, stderr: "not a host-side lab script"},
		{name: "missing input", labels: runLabels(), args: []string{"install-host.sh", "absent.txt"}, stderr: "does not exist"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{Resources: []mockResource{
				{Kind: "instances", Name: host, Zone: testZone, Labels: tt.labels, Status: "RUNNING"},
			}})
			writeFile(t, l.dir, "checksums.txt", filledChecksums(t))
			l.env["NODE_NAME"] = "brie-worker-1"
			for k, v := range tt.env {
				l.env[k] = v
			}
			r := l.run("run-on-host.sh", append([]string{host, testZone}, tt.args...)...)
			commands := l.commands()
			if tt.stderr != "" {
				if r.code == 0 || !strings.Contains(r.stderr, tt.stderr) {
					t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
				}
				if indexOf(commands, `^compute (ssh|scp) `) >= 0 {
					t.Fatalf("refused run reached the host: %q", commands)
				}
				return
			}
			if r.code != 0 {
				t.Fatalf("exit=%d stderr=%q", r.code, r.stderr)
			}
			scp := indexOf(commands, `^compute scp .*install-host\.sh .*checksums\.sh .*checksums\.txt `+host+`:/tmp/brie-hw-`+testRunID+`/`)
			run := indexOf(commands, `^compute ssh `+host+` .*--command=sudo env NODE_NAME=brie-worker-1 bash /tmp/brie-hw-`+testRunID+`/install-host\.sh /tmp/brie-hw-`+testRunID+`/checksums\.txt`)
			if scp < 0 || run < scp || indexOf(commands, `^compute instances describe `+host) > scp {
				t.Fatalf("unexpected command sequence: %q", commands)
			}
		})
	}
}
