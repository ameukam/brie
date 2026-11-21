// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var (
	fuzzSHA256 = regexp.MustCompile(`^[0-9a-f]{64}$`)
	fuzzURL    = regexp.MustCompile(`^https://(github\.com|raw\.githubusercontent\.com|dl\.k8s\.io)/[A-Za-z0-9._/-]+$`)
	fuzzPrice  = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)
	fuzzPath   = regexp.MustCompile(`^/[A-Za-z0-9._/-]+$`)
)

// runParser runs a bash snippet that sources one of the lab libraries with a
// fuzzed file as $1.
func runParser(t *testing.T, script, content string) (string, string, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "input")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	hwDir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-c", script, "fuzz", path)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HW_DIR=" + hwDir}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("parser did not finish: %v", ctx.Err())
	}
	return stdout.String(), stderr.String(), err
}

func FuzzChecksumsFile(f *testing.F) {
	template, err := os.ReadFile("checksums.template")
	if err != nil {
		f.Fatal(err)
	}
	digest := strings.Repeat("0", 64)
	for _, seed := range []string{
		string(template),
		"",
		"# comment only\n",
		"artifact crictl 1.37.0 https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.37.0/crictl-v1.37.0-linux-arm64.tar.gz " + digest + "\n",
		"binary /opt/kata/bin/firecracker " + digest + "\n",
		"artifact containerd 2.4.2 http://github.com/containerd/2.4.2 " + digest + "\n",
		"binary ../x " + digest + "\r\n",
	} {
		f.Add(seed)
	}
	const script = `set -euo pipefail
source "$HW_DIR/checksums.sh"
hw_checksums_load "$1"
hw_artifact_summary
for path in "${!HW_BINARY_SHA[@]}"; do
	printf 'binary %s %s\n' "$path" "${HW_BINARY_SHA[$path]}"
done`
	f.Fuzz(func(t *testing.T, content string) {
		stdout, stderr, err := runParser(t, script, content)
		if err != nil {
			if strings.TrimSpace(stderr) == "" {
				t.Fatalf("rejected without an error message: %v", err)
			}
			return
		}
		for _, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			switch {
			case fields[0] == "artifact" && len(fields) == 5:
				if !fuzzURL.MatchString(fields[3]) || !strings.Contains(fields[3], fields[2]) || !fuzzSHA256.MatchString(fields[4]) {
					t.Fatalf("accepted invalid artifact %q", line)
				}
			case fields[0] == "binary" && len(fields) == 3:
				if !fuzzPath.MatchString(fields[1]) || strings.Contains(fields[1], "..") || !fuzzSHA256.MatchString(fields[2]) {
					t.Fatalf("accepted invalid binary %q", line)
				}
			default:
				t.Fatalf("unexpected parsed record %q", line)
			}
			if strings.ContainsAny(line, "<>") {
				t.Fatalf("accepted a placeholder: %q", line)
			}
		}
	})
}

func FuzzPriceTable(f *testing.F) {
	f.Add(testPrices)
	f.Add("")
	f.Add(strings.Replace(testPrices, "0.5", "<USD>", 1))
	f.Add(testPrices + "c4a-standard-8 1\n")
	f.Add(strings.Replace(testPrices, "0.5", "1e3", 1))
	const script = `set -euo pipefail
source "$HW_DIR/lib.sh"
HW_SCRIPT=fuzz PRICE_TABLE=$1 PLANNED_HOURS=1
hw_load_prices
for key in "${HW_PRICE_KEYS[@]}"; do
	printf '%s %s\n' "$key" "$(hw_price "$key")"
done`
	f.Fuzz(func(t *testing.T, content string) {
		stdout, stderr, err := runParser(t, script, content)
		if err != nil {
			if strings.TrimSpace(stderr) == "" {
				t.Fatalf("rejected without an error message: %v", err)
			}
			return
		}
		lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
		if len(lines) != 5 {
			t.Fatalf("accepted table yields %d prices: %q", len(lines), stdout)
		}
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) != 2 || !fuzzPrice.MatchString(fields[1]) {
				t.Fatalf("accepted invalid price %q", line)
			}
		}
	})
}
