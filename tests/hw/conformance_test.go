// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func junitCase(name, body string) string {
	return `<testcase name="` + name + `" classname="Kubernetes e2e suite">` + body + `</testcase>`
}

func TestNodeConformanceSummary(t *testing.T) {
	pass := junitCase("[sig-node] Pods should get a host IP [NodeConformance] [Conformance]", "")
	fail := func(n string) string {
		return junitCase("[sig-node] case "+n+" [NodeConformance]", `<failure message="timed out waiting for pod `+n+`">stack</failure>`)
	}
	errored := junitCase("[sig-node] errored [NodeConformance]", `<error message="setup error">x</error>`)
	skipped := junitCase("[sig-node] skipped [NodeConformance]", `<skipped message="skip"/>`)
	suite := func(cases ...string) string {
		return `<?xml version="1.0"?><testsuites><testsuite name="Kubernetes e2e suite">` + strings.Join(cases, "") + `</testsuite></testsuites>`
	}
	for _, tt := range []struct {
		name     string
		files    map[string]string
		code     int
		summary  string
		failures []string
		stderr   string
	}{
		{name: "all pass", files: map[string]string{"junit_01.xml": suite(pass, skipped)}, summary: "total 2\npassed 1\nfailed 0\nskipped 1\n"},
		{
			name:     "every failure is published",
			files:    map[string]string{"junit_01.xml": suite(pass, fail("a"), fail("b")), "junit_02.xml": suite(fail("c"), errored)},
			code:     1,
			summary:  "total 5\npassed 1\nfailed 4\nskipped 0\n",
			failures: []string{"case a [NodeConformance]\ttimed out waiting for pod a", "case b", "case c", "errored [NodeConformance]\tsetup error"},
		},
		{name: "no tests", files: map[string]string{"junit_01.xml": suite()}, code: 1, stderr: "contains no tests"},
		{name: "no report", files: map[string]string{"e2e.log": "x"}, code: 1, stderr: "no JUnit report"},
		{name: "malformed report", files: map[string]string{"junit_01.xml": "<testsuites>"}, code: 1, stderr: "cannot parse"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{})
			dir := filepath.Join(l.dir, "report")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, content := range tt.files {
				writeFile(t, dir, name, content)
			}
			r := l.run("node-conformance.sh", "--summarize", dir)
			if r.code != tt.code || !strings.Contains(r.stderr, tt.stderr) {
				t.Fatalf("exit=%d stderr=%q; want exit=%d containing %q", r.code, r.stderr, tt.code, tt.stderr)
			}
			if tt.summary == "" {
				return
			}
			summary, err := os.ReadFile(filepath.Join(dir, "summary.txt"))
			if err != nil || string(summary) != tt.summary {
				t.Errorf("summary=%q err=%v; want %q", summary, err, tt.summary)
			}
			failures, err := os.ReadFile(filepath.Join(dir, "failures.txt"))
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Count(string(failures), "FAIL\t"); got != len(tt.failures) {
				t.Errorf("failures.txt lists %d failures, want %d:\n%s", got, len(tt.failures), failures)
			}
			for _, want := range tt.failures {
				if !strings.Contains(string(failures), want) || !strings.Contains(r.stdout, want) {
					t.Errorf("failure %q not published: file=%q stdout=%q", want, failures, r.stdout)
				}
			}
		})
	}
}

func TestNodeConformanceInputs(t *testing.T) {
	filled := filledChecksums(t)
	for _, tt := range []struct {
		name    string
		content string
		args    []string
		noFile  bool
		stderr  string
	}{
		{name: "pinned", content: filled, args: []string{"--check-only"}},
		{name: "missing kubernetes-test", content: removeLine(filled, "artifact kubernetes-test"), args: []string{"--check-only"}, stderr: "no artifact kubernetes-test"},
		{
			name:    "kubernetes-test 1.36",
			content: strings.Replace(filled, "kubernetes-test 1.37.0 https://dl.k8s.io/v1.37.0", "kubernetes-test 1.36.2 https://dl.k8s.io/v1.36.2", 1),
			args:    []string{"--check-only"}, stderr: "must be a 1.37 release, not 1.36.2",
		},
		{name: "placeholder", content: string(mustRead(t, "checksums.template")), args: []string{"--check-only"}, stderr: "placeholder"},
		{name: "no node name", content: filled, stderr: "required input NODE_NAME"},
		{name: "usage", content: filled, args: []string{"--summarize"}, noFile: true, stderr: "usage:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{})
			path := writeFile(t, l.dir, "checksums.txt", tt.content)
			args := tt.args
			if !tt.noFile {
				args = append(args, path)
			}
			r := l.run("node-conformance.sh", args...)
			if tt.stderr == "" && r.code != 0 {
				t.Fatalf("exit=%d stderr=%q; want success", r.code, r.stderr)
			}
			if tt.stderr != "" && (r.code == 0 || !strings.Contains(r.stderr, tt.stderr)) {
				t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
			}
		})
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
